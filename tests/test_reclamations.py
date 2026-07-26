import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.partner import (
    EMAIL_OUTBOX_STATUS,
    RECLAMATION_ATTACHMENT_KIND,
    RECLAMATION_ITEM_SOURCE,
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    EmailOutbox,
    Provider,
    Reclamation,
    ReclamationAttachment,
    ReclamationItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.email_outbox import (
    _flag_source_email_answered_sync,
    mark_outbox_error,
    mark_outbox_sent,
)
from dz_fastapi.services.reclamation_armtek import (
    ARMTEK_APPROVED_STATUS,
    ArmtekPortalClient,
    ArmtekPortalConfig,
    ArmtekPortalError,
    ArmtekReturnRef,
    build_armtek_snapshot,
    is_armtek_portal_notice,
    parse_armtek_return_url,
    send_armtek_decision,
    sync_armtek_open_returns,
)
from dz_fastapi.services.reclamation_attachment_parser import parse_torg2_sheet
from dz_fastapi.services.reclamation_check import (
    _elapsed_return_days,
    _order_return_start_date,
    run_reclamation_check,
)
from dz_fastapi.services.reclamation_froza import (
    FrozaPortalClient,
    FrozaPortalError,
    build_froza_snapshot,
    parse_froza_question_url,
    send_froza_decision,
)
from dz_fastapi.services.reclamation_replies import (
    apply_and_enqueue_customer_reply,
    build_customer_reply_template,
    enqueue_customer_reply,
    enqueue_supplier_request,
)
from dz_fastapi.services.reclamations import (
    ReclamationInboundAttachment,
    ReclamationInboundEmail,
    _match_oems_in_text,
    assign_shortage_reviewer,
    classify_attachment_kind,
    classify_reclamation_type,
    confirm_shortage,
    extract_fields,
    extract_froza_email_item,
    extract_greenlight_return_items,
    extract_inline_return_items,
    extract_links,
    extract_sender_email,
    extract_shortage_items,
    ingest_reclamation_email,
    list_reclamations,
    postpone_shortage_review,
    recognize_reclamation_items,
)


def test_extract_sender_email():
    assert (
        extract_sender_email("Иван Петров <Ivan@Client.RU>")
        == "ivan@client.ru"
    )
    assert extract_sender_email("plain@mail.ru") == "plain@mail.ru"
    assert extract_sender_email("нет адреса") == ""


def test_extract_links():
    body = (
        "Смотрите рекламацию тут: https://portal.client.ru/rekl/123 "
        "и дубль http://portal.client.ru/rekl/123, спасибо."
    )
    links = extract_links(body)
    assert links == [
        "https://portal.client.ru/rekl/123",
        "http://portal.client.ru/rekl/123",
    ]


def test_extract_links_decodes_html_entities():
    links = extract_links(
        "https://froza.ru/supplier/one-question/"
        "?token=0123456789abcdef0123456789abcdef&amp;id=824111"
    )

    assert links == [
        "https://froza.ru/supplier/one-question/"
        "?token=0123456789abcdef0123456789abcdef&id=824111"
    ]


def test_extract_froza_email_item_reads_quantity_and_position():
    item = extract_froza_email_item(
        """
        Товар: РЕЗИНКА ГЛУШИТЕЛЯ
        Артикул: DZ1200015K00
        Производитель: DragonZap
        Количество: 4

        Причина возврата: Отказ клиента
        Комментарий: Деталь не устанавливалась.
        """
    )

    assert item == {
        "oem_number": "DZ1200015K00",
        "brand_name": "DragonZap",
        "autopart_name": "РЕЗИНКА ГЛУШИТЕЛЯ",
        "quantity": 4,
        "reason": "Отказ клиента",
        "comment": "Деталь не устанавливалась.",
    }


def _greenlight_email_body() -> str:
    return """
    Добрый день! Заявляем о возврате товарных позиций.
    Номер документа поступления
    Дата поступления
    Артикул
    Номенклатура (к возврату)
    Производитель
    Количество
    Сумма
    Причина возврата
    3378 23.07.2026 19501-R6F-G00 Шланг вода верхн. Honda 1 8 064
    Отказ от товара по инициативе клиента
    Основные причины формирования возвратов Поставщику:
    НЕКОМПЛЕКТ (Отмена при приемке)
    НЕКОНДИЦИЯ (Отмена при приемке) - механические повреждения.
    """


def test_extract_greenlight_return_ignores_boilerplate_reasons():
    items = extract_greenlight_return_items(_greenlight_email_body())
    fields = extract_fields("", _greenlight_email_body())

    assert items == [
        {
            "document_number": "3378",
            "document_date": "2026-07-23",
            "oem_number": "19501R6FG00",
            "autopart_name": "Шланг вода верхн.",
            "brand_name": "Honda",
            "quantity": 1,
            "total_amount": 8064.0,
            "reason": "Отказ от товара по инициативе клиента",
        }
    ]
    assert fields["document_number"] == "3378"
    assert fields["reclamation_type"] == "customer_refusal"


def _shortage_email_body() -> str:
    return """
    Приходная накладная №3391 от 24.07.2026 - ООО "АВТОПАРТС"
    Недовоз:
    Втулка уплотнительная клапанной крышки 11139-86500 1113986500
    SUZUKI — 1шт

    ООО «Космопарт»
    """


def test_extract_shortage_item_and_document():
    items = extract_shortage_items(_shortage_email_body())
    fields = extract_fields("", _shortage_email_body())

    assert items == [
        {
            "oem_number": "1113986500",
            "brand_name": "SUZUKI",
            "autopart_name": "Втулка уплотнительная клапанной крышки",
            "quantity": 1,
            "reason": "Недовоз",
        }
    ]
    assert fields["document_number"] == "3391"
    assert fields["document_date"] == "2026-07-24"
    assert fields["reclamation_type"] == "shortage"


def _avtoformula_return_body() -> str:
    return """
    Добрый день!
    Согласно действующим договоренностям между нашими компаниями
    мы хотим вернуть товар надлежащего качества
    по причине отказа конечного покупателя

    19115RGA000 HONDA КРЫШКА БАЧКА ОМЫВАТЕЛЯ ПЛАСТИКОВАЯ
    в количестве 1
    по документу №3091 от 10.07.26
    """


def _quoted_return_body() -> str:
    return """
    <div>Добрый день</div>
    <div>Клиент хочет вернуть. Просьба согласовать возврат</div>
    <div>Тема: 1575A082;</div>
    <blockquote>
      <div>1575A082 - 3</div>
      <div>Накладная от 16.07</div>
      <p style="margin:0cm 0cm 0.0001pt 0cm">
        Причина – конструктивные отличия
      </p>
      <div>С уважением ООО "АвтоПартс" DragonZap</div>
    </blockquote>
    """


def test_extract_inline_return_items_uses_explicit_quantity():
    avtoformula = extract_inline_return_items(_avtoformula_return_body())
    quoted = extract_inline_return_items(_quoted_return_body())

    assert avtoformula == [
        {
            "oem_number": "19115RGA000",
            "brand_name": "HONDA",
            "autopart_name": "КРЫШКА БАЧКА ОМЫВАТЕЛЯ ПЛАСТИКОВАЯ",
            "quantity": 1,
            "reason": "отказа конечного покупателя",
        }
    ]
    assert quoted == [
        {
            "oem_number": "1575A082",
            "quantity": 3,
            "reason": "конструктивные отличия",
        }
    ]


@pytest.mark.asyncio
async def test_ingest_avtoformula_ignores_document_date_as_position(
    test_session: AsyncSession,
):
    brand = Brand(name="AVTOFORMULA RETURN TEST")
    test_session.add(brand)
    await test_session.flush()
    test_session.add_all(
        [
            AutoPart(
                brand_id=brand.id,
                oem_number="19115RGA000",
                name="Крышка бачка омывателя",
            ),
            AutoPart(
                brand_id=brand.id,
                oem_number="100726",
                name="Ложное совпадение с датой",
            ),
        ]
    )
    await test_session.commit()

    reclamation = await ingest_reclamation_email(
        test_session,
        ReclamationInboundEmail(
            from_="sulimenko_i@avtoformula.ru",
            subject="Возврат товара",
            body_text=_avtoformula_return_body(),
            message_id="<avtoformula-return@example.test>",
        ),
    )

    assert reclamation is not None
    assert reclamation.stated_document_number == "3091"
    assert reclamation.stated_document_date == date(2026, 7, 10)
    assert reclamation.reclamation_type == "customer_refusal"
    assert len(reclamation.items) == 1
    assert reclamation.items[0].oem_number == "19115RGA000"
    assert reclamation.items[0].quantity == 1
    assert reclamation.items[0].brand_name == "HONDA"


@pytest.mark.asyncio
async def test_recheck_quoted_return_removes_html_and_signature_tokens(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        email_body=_quoted_return_body(),
        items=[
            ReclamationItem(oem_number="1575A082", quantity=1),
            ReclamationItem(oem_number="1607", quantity=1),
            ReclamationItem(oem_number="DRAGONZAP", quantity=1),
            ReclamationItem(oem_number="00001PT", quantity=1),
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()

    await recognize_reclamation_items(test_session, reclamation)
    await test_session.commit()

    assert [item.oem_number for item in reclamation.items] == ["1575A082"]
    assert reclamation.items[0].quantity == 3
    assert reclamation.items[0].reason == "конструктивные отличия"


@pytest.mark.asyncio
async def test_ingest_shortage_excludes_document_number_from_items(
    test_session: AsyncSession,
):
    brand = Brand(name="SHORTAGE TEST")
    test_session.add(brand)
    await test_session.flush()
    test_session.add_all(
        [
            AutoPart(
                brand_id=brand.id,
                oem_number="1113986500",
                name="Втулка",
            ),
            AutoPart(
                brand_id=brand.id,
                oem_number="3391",
                name="Случайно совпавший артикул",
            ),
        ]
    )
    await test_session.commit()

    reclamation = await ingest_reclamation_email(
        test_session,
        ReclamationInboundEmail(
            from_="returns@example.com",
            subject="Недовоз",
            body_text=_shortage_email_body(),
            message_id="<shortage@example.test>",
            uid="7788",
            email_account_id=6,
            folder="INBOX",
        ),
    )

    assert reclamation is not None
    assert reclamation.reclamation_type == "shortage"
    assert reclamation.stated_document_number == "3391"
    assert reclamation.stated_document_date == date(2026, 7, 24)
    assert reclamation.stated_reason == "Недовоз"
    assert reclamation.extracted_data["mailbox"] == {
        "email_account_id": 6,
        "folder": "INBOX",
        "uid": "7788",
    }
    assert [item.oem_number for item in reclamation.items] == [
        "1113986500"
    ]
    assert reclamation.items[0].brand_name == "SUZUKI"
    assert reclamation.items[0].quantity == 1


@pytest.mark.asyncio
async def test_recheck_shortage_removes_spurious_document_item(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        stated_document_number="3391",
        email_body=_shortage_email_body(),
        items=[
            ReclamationItem(oem_number="1113986500", quantity=1),
            ReclamationItem(oem_number="3391", quantity=1),
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()

    await recognize_reclamation_items(test_session, reclamation)
    await test_session.commit()

    assert [item.oem_number for item in reclamation.items] == [
        "1113986500"
    ]
    assert reclamation.reclamation_type == "shortage"


@pytest.mark.asyncio
async def test_ingest_greenlight_email_uses_only_return_table_position(
    test_session: AsyncSession,
):
    brand = Brand(name="GREENLIGHT TEST")
    test_session.add(brand)
    await test_session.flush()
    test_session.add_all(
        [
            AutoPart(
                brand_id=brand.id,
                oem_number="19501R6FG00",
                name="Патрубок",
            ),
            AutoPart(
                brand_id=brand.id,
                oem_number="3378",
                name="Кран для канистры",
            ),
        ]
    )
    await test_session.commit()

    reclamation = await ingest_reclamation_email(
        test_session,
        ReclamationInboundEmail(
            from_="returns@greenlight.example",
            subject="Возврат поставщику",
            body_text=_greenlight_email_body(),
            message_id="<greenlight-return@example.test>",
        ),
    )

    assert reclamation is not None
    assert reclamation.stated_document_number == "3378"
    assert reclamation.stated_document_date == date(2026, 7, 23)
    assert reclamation.stated_reason == (
        "Отказ от товара по инициативе клиента"
    )
    assert reclamation.reclamation_type == "customer_refusal"
    assert len(reclamation.items) == 1
    assert reclamation.items[0].oem_number == "19501R6FG00"
    assert reclamation.items[0].quantity == 1


@pytest.mark.asyncio
async def test_recognize_greenlight_email_removes_spurious_document_item(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        email_body=_greenlight_email_body(),
        items=[
            ReclamationItem(
                oem_number="19501R6FG00",
                autopart_name="Патрубок",
                quantity=1,
            ),
            ReclamationItem(
                oem_number="3378",
                autopart_name="Кран для канистры",
                quantity=1,
            ),
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()

    await recognize_reclamation_items(test_session, reclamation)
    await test_session.commit()

    assert [item.oem_number for item in reclamation.items] == [
        "19501R6FG00"
    ]
    assert reclamation.reclamation_type == "customer_refusal"
    assert reclamation.stated_document_number == "3378"
    assert reclamation.stated_reason == (
        "Отказ от товара по инициативе клиента"
    )


@pytest.mark.asyncio
async def test_ingest_froza_email_stores_stated_quantity(
    test_session: AsyncSession,
):
    brand = Brand(name="FROZA DOCUMENT NUMBER TEST")
    test_session.add(brand)
    await test_session.flush()
    test_session.add_all(
        [
            AutoPart(
                brand_id=brand.id,
                oem_number="9030107024",
                name="Кольцо уплотнительное",
            ),
            AutoPart(
                brand_id=brand.id,
                oem_number="3320",
                name="Патрубок",
            ),
        ]
    )
    await test_session.commit()

    reclamation = await ingest_reclamation_email(
        test_session,
        ReclamationInboundEmail(
            from_="postvozvrat@froza.ru",
            subject="Просьба согласовать возврат",
            body_text=(
                "Номер входящего документа: 3320\n"
                "Дата входящего документа: 23.07.2026\n"
                "Товар: КОЛЬЦО\n"
                "Артикул: 9030107024\n"
                "Производитель: TOYOTA\n"
                "Количество: 6\n"
                "Причина возврата: Отказ клиента\n"
                "Комментарий: Отказ клиента\n"
            ),
            body_html=(
                '<a href="https://froza.ru/supplier/one-question/'
                '?token=0123456789abcdef0123456789abcdef&amp;id=827199">'
                "Согласовать</a>"
            ),
            message_id="<froza-quantity@example.test>",
        ),
    )

    assert reclamation is not None
    assert reclamation.stated_document_number == "3320"
    assert reclamation.stated_document_date == date(2026, 7, 23)
    assert reclamation.stated_reason == "Отказ клиента"
    assert reclamation.reclamation_type == "customer_refusal"
    assert len(reclamation.items) == 1
    assert reclamation.items[0].oem_number == "9030107024"
    assert reclamation.items[0].brand_name == "TOYOTA"
    assert reclamation.items[0].autopart_name == "Кольцо уплотнительное"
    assert reclamation.items[0].quantity == 6
    assert reclamation.extracted_data["froza_email_item"]["quantity"] == 6


@pytest.mark.asyncio
async def test_recheck_froza_removes_spurious_document_item(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        stated_document_number="3320",
        email_body=(
            "Номер входящего документа: 3320\n"
            "Дата входящего документа: 23.07.2026\n"
            "Товар: КОЛЬЦО\n"
            "Артикул: 9030107024\n"
            "Производитель: TOYOTA\n"
            "Количество: 6\n"
            "Причина возврата: Отказ клиента\n"
            "Комментарий: Отказ клиента\n"
        ),
        items=[
            ReclamationItem(
                oem_number="3320",
                autopart_name="Патрубок",
                quantity=1,
                item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
            ),
            ReclamationItem(
                oem_number="9030107024",
                autopart_name="Кольцо уплотнительное",
                quantity=6,
                item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
            ),
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()

    await recognize_reclamation_items(test_session, reclamation)
    await test_session.commit()

    assert [item.oem_number for item in reclamation.items] == [
        "9030107024"
    ]
    assert reclamation.items[0].quantity == 6
    assert reclamation.items[0].reason == "Отказ клиента"
    assert reclamation.extracted_data["froza_email_item"]["quantity"] == 6


@pytest.mark.asyncio
async def test_existing_reclamation_recovers_froza_link_from_html(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.APPROVED,
        email_message_id="<froza-173@example.test>",
        sender_email="postvozvrat@froza.ru",
        extracted_data={},
    )
    test_session.add(reclamation)
    await test_session.commit()

    result = await ingest_reclamation_email(
        test_session,
        ReclamationInboundEmail(
            from_="postvozvrat@froza.ru",
            subject="Просьба согласовать возврат",
            body_text="Для подтверждения перейдите по ссылке.",
            body_html=(
                '<a href="https://froza.ru/suppliers">Личный кабинет</a>'
                '<a href="https://froza.ru/supplier/one-question/'
                '?token=secret&amp;id=823408">Подтвердить возврат</a>'
            ),
            message_id="<froza-173@example.test>",
        ),
    )

    assert result is None
    await test_session.refresh(reclamation)
    assert reclamation.source_link == (
        "https://froza.ru/supplier/one-question/"
        "?token=secret&id=823408"
    )
    assert len(reclamation.extracted_data["links"]) == 2


@pytest.mark.asyncio
async def test_list_reclamations_orders_newest_by_effective_date(
    test_session: AsyncSession,
):
    older = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.NEW,
        email_subject="Старая",
        email_received_at=datetime(2026, 7, 20, 10, 0, tzinfo=UTC),
        created_at=datetime(2026, 7, 20, 10, 1, tzinfo=UTC),
    )
    manual = Reclamation(
        source=RECLAMATION_SOURCE.MANUAL,
        status=RECLAMATION_STATUS.NEW,
        email_subject="Ручная",
        email_received_at=None,
        created_at=datetime(2026, 7, 21, 10, 0, tzinfo=UTC),
    )
    newest = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.NEW,
        email_subject="Свежая",
        email_received_at=datetime(2026, 7, 22, 10, 0, tzinfo=UTC),
        created_at=datetime(2026, 7, 22, 10, 1, tzinfo=UTC),
    )
    test_session.add_all([older, manual, newest])
    await test_session.commit()

    rows = await list_reclamations(test_session, order="newest")

    assert [row["email_subject"] for row in rows] == [
        "Свежая",
        "Ручная",
        "Старая",
    ]


def _armtek_payload(*, state="pending", quantity=1, oem="14775PCX000"):
    status = {
        "pending": "Ожидается решение поставщика",
        "approved": "Подтверждено поставщиком",
        "rejected": "Отказ поставщика",
    }[state]
    return {
        "RequestNumber": "823408",
        "RequestPosition": "10",
        "StatusName": status,
        "SupplierMaterial": oem,
        "Brand": "HONDA",
        "MaterialName": "Трубка вентиляции",
        "Quantity": quantity,
        "Price": "479,00",
        "ExternalInvoiceNumber": "3016",
        "InvoiceDate": "2026-07-20T00:00:00+03:00",
        "ReasonReturn": "Отказ клиента",
        "WarehouseName": "Москва",
    }


def test_armtek_notice_and_return_url_are_strict():
    assert is_armtek_portal_notice(
        sender="CROSS@ARMTEK.RU",
        body=(
            "Открытые возвраты: "
            "https://srm.armtek.ru/returns-management/opened"
        ),
    )
    assert not is_armtek_portal_notice(
        sender="attacker@example.com",
        body="https://srm.armtek.ru/returns-management/opened",
    )

    ref = parse_armtek_return_url(
        "https://srm.armtek.ru/returns-management/opened/823408"
        "?RequestPosition=10"
    )
    assert ref == ArmtekReturnRef("823408", "10")
    with pytest.raises(ArmtekPortalError):
        parse_armtek_return_url(
            "https://example.com/returns-management/opened/823408"
            "?RequestPosition=10"
        )


def test_build_armtek_snapshot_normalizes_fields():
    snapshot = build_armtek_snapshot(
        _armtek_payload(),
        supplier_id="SUPPLIER-1",
    )

    assert snapshot["external_id"] == "823408:10"
    assert snapshot["state"] == "pending"
    assert snapshot["oem_number"] == "14775PCX000"
    assert snapshot["quantity"] == 1
    assert snapshot["price"] == 479.0
    assert snapshot["supplier_id"] == "SUPPLIER-1"
    assert "accessToken" not in snapshot


@pytest.mark.asyncio
async def test_armtek_client_uses_login_and_returns_contract():
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith("/auth/login"):
            return httpx.Response(
                200,
                json={"data": {"accessToken": "token", "refreshToken": "r"}},
            )
        if request.url.path.endswith("/auth/profile"):
            return httpx.Response(
                200,
                json={"data": {"SupplierData": [{"Supplier": "SUP-1"}]}},
            )
        if request.url.path.endswith("/returns/list"):
            return httpx.Response(
                200,
                json={"data": {"items": [_armtek_payload()]}},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = ArmtekPortalClient(
            config=ArmtekPortalConfig(
                login="user@example.com",
                password="secret",
            ),
            client=http_client,
        )
        supplier_id = await client.resolve_supplier_id()
        rows = await client.list_open_returns(supplier_id)

    assert supplier_id == "SUP-1"
    assert len(rows) == 1
    login_request = requests[0]
    assert login_request.headers["x-auth-system"]
    list_request = requests[-1]
    assert list_request.headers["authorization"] == "Bearer token"
    assert json.loads(list_request.read()) == {
        "Supplier": "SUP-1",
        "Opened": True,
        "Status": "awaiting_supplier_decision",
    }


class _FakeArmtekClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.submissions = []

    async def resolve_supplier_id(self):
        return "SUP-1"

    async def list_open_returns(self, _supplier_id):
        return [_armtek_payload()]

    async def get_return(self, _ref):
        return self.payloads.pop(0)

    async def submit_decision(self, ref, *, decision, comment=None):
        self.submissions.append((ref, decision, comment))
        return {"data": True}


@pytest.mark.asyncio
async def test_sync_armtek_returns_is_idempotent(
    test_session: AsyncSession,
):
    first_client = _FakeArmtekClient([_armtek_payload()])
    first = await sync_armtek_open_returns(
        test_session,
        customer_id=None,
        client=first_client,
    )
    second_client = _FakeArmtekClient([_armtek_payload()])
    second = await sync_armtek_open_returns(
        test_session,
        customer_id=None,
        client=second_client,
    )

    assert first["created"] == 1
    assert second["created"] == 0
    assert second["updated"] == 1
    rows = (
        await test_session.execute(
            select(Reclamation).where(
                Reclamation.source_link.like(
                    "%/returns-management/opened/823408%"
                )
            )
        )
    ).scalars().all()
    assert len(rows) == 1
    assert rows[0].items[0].oem_number == "14775PCX000"
    assert rows[0].extracted_data["armtek"]["external_id"] == "823408:10"


@pytest.mark.asyncio
async def test_send_armtek_decision_verifies_result(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.LINK,
        status=RECLAMATION_STATUS.APPROVED,
        resolution="approved",
        source_link=(
            "https://srm.armtek.ru/returns-management/opened/823408"
            "?RequestPosition=10"
        ),
        items=[
            ReclamationItem(
                oem_number="14775-PCX-000",
                brand_name="HONDA",
                quantity=1,
            )
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()
    client = _FakeArmtekClient(
        [_armtek_payload(), _armtek_payload(state="approved")]
    )

    _, snapshot, already_sent = await send_armtek_decision(
        test_session,
        reclamation_id=reclamation.id,
        user_id=7,
        client=client,
    )

    assert already_sent is False
    assert snapshot["state"] == "approved"
    assert client.submissions[0][1] == "approved"
    assert client.submissions[0][2] is None
    await test_session.refresh(reclamation)
    assert reclamation.extracted_data["armtek"]["sent_by_user_id"] == 7


@pytest.mark.asyncio
async def test_armtek_client_reject_payload_requires_comment():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"data": True})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = ArmtekPortalClient(
            config=ArmtekPortalConfig(
                login="user@example.com",
                password="secret",
                supplier_id="SUP-1",
            ),
            client=http_client,
        )
        client._access_token = "token"
        with pytest.raises(ArmtekPortalError, match="комментарий"):
            await client.submit_decision(
                ArmtekReturnRef("823408", "10"),
                decision="rejected",
            )
        await client.submit_decision(
            ArmtekReturnRef("823408", "10"),
            decision="approved",
        )

    body = json.loads(requests[0].read())
    assert body["Data"][0]["StatusName"] == ARMTEK_APPROVED_STATUS


def _froza_payload(*, state="pending", quantity=1, oem="14775PCX000"):
    return {
        "id": 824111,
        "order": {
            "quantity": quantity,
            "price": 479,
            "detail": {
                "num": oem,
                "makeName": "HONDA",
                "description": "Трубка вентиляции",
            },
            "invoice": {
                "number": "3230",
                "date": "2026-07-20T00:00:00+03:00",
            },
        },
        "quantity": quantity,
        "waitingResponse": state == "pending",
        "archived": state == "archived",
        "isSupplierAgreedReturn": state == "approved",
        "isSupplierRejectedReturn": state == "rejected",
        "isFullReturnAgree": False,
        "messages": [{"text": "Отказ клиента"}],
    }


def test_parse_froza_question_url_accepts_only_expected_form():
    ref = parse_froza_question_url(
        "https://froza.ru/supplier/one-question/"
        "?token=0123456789abcdef0123456789abcdef&id=824111"
    )

    assert ref.question_id == 824111
    assert ref.token == "0123456789abcdef0123456789abcdef"

    encoded_ref = parse_froza_question_url(
        "https://froza.ru/supplier/one-question/"
        "?token=0123456789abcdef0123456789abcdef&amp;id=824111"
    )
    assert encoded_ref == ref

    with pytest.raises(FrozaPortalError):
        parse_froza_question_url(
            "https://example.com/supplier/one-question/"
            "?token=0123456789abcdef0123456789abcdef&id=824111"
        )


def test_build_froza_snapshot_does_not_include_token():
    snapshot = build_froza_snapshot(_froza_payload())

    assert snapshot["question_id"] == 824111
    assert snapshot["state"] == "pending"
    assert snapshot["oem_number"] == "14775PCX000"
    assert "token" not in snapshot


@pytest.mark.asyncio
async def test_froza_client_uses_expected_accept_contract():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"success": True})

    async with httpx.AsyncClient(
        base_url="https://froza.ru",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = FrozaPortalClient(client=http_client)
        ref = parse_froza_question_url(
            "https://froza.ru/supplier/one-question/"
            "?token=0123456789abcdef0123456789abcdef&id=824111"
        )
        await client.submit_decision(ref, decision="approved")

    assert requests[0].method == "POST"
    assert requests[0].url.path.endswith("/824111")
    assert json.loads(requests[0].read()) == {"maxRedirects": 0}


class _FakeFrozaClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.submissions = []

    async def get_question(self, _ref):
        return self.payloads.pop(0)

    async def submit_decision(self, _ref, *, decision, comment=None):
        self.submissions.append((decision, comment))
        return {"success": True}


@pytest.mark.asyncio
async def test_send_froza_decision_verifies_and_records_result(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.LINK,
        status=RECLAMATION_STATUS.APPROVED,
        resolution="approved",
        source_link=(
            "https://froza.ru/supplier/one-question/"
            "?token=0123456789abcdef0123456789abcdef&id=824111"
        ),
        items=[
            ReclamationItem(
                oem_number="14775-PCX-000",
                brand_name="HONDA",
                quantity=1,
            )
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()
    fake_client = _FakeFrozaClient(
        [_froza_payload(), _froza_payload(state="approved")]
    )

    _, snapshot, already_sent = await send_froza_decision(
        test_session,
        reclamation_id=reclamation.id,
        user_id=7,
        client=fake_client,
    )

    assert already_sent is False
    assert snapshot["state"] == "approved"
    assert fake_client.submissions == [("approved", None)]
    await test_session.refresh(reclamation)
    assert reclamation.extracted_data["froza"]["state"] == "approved"
    assert reclamation.extracted_data["froza"]["sent_by_user_id"] == 7
    assert "token" not in reclamation.extracted_data["froza"]


@pytest.mark.asyncio
async def test_send_froza_decision_blocks_quantity_mismatch(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.LINK,
        status=RECLAMATION_STATUS.APPROVED,
        resolution="approved",
        source_link=(
            "https://froza.ru/supplier/one-question/"
            "?token=0123456789abcdef0123456789abcdef&id=824111"
        ),
        items=[
            ReclamationItem(
                oem_number="14775PCX000",
                brand_name="HONDA",
                quantity=1,
            )
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()
    fake_client = _FakeFrozaClient([_froza_payload(quantity=12)])

    with pytest.raises(FrozaPortalError, match="Количество"):
        await send_froza_decision(
            test_session,
            reclamation_id=reclamation.id,
            user_id=7,
            client=fake_client,
        )
    assert fake_client.submissions == []


@pytest.mark.asyncio
async def test_send_froza_decision_repairs_default_quantity_from_email(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.APPROVED,
        resolution="approved",
        email_body=(
            "Товар: РЕЗИНКА ГЛУШИТЕЛЯ\n"
            "Артикул: DZ1200015K00\n"
            "Производитель: DragonZap\n"
            "Количество: 4\n"
        ),
        source_link=(
            "https://froza.ru/supplier/one-question/"
            "?token=0123456789abcdef0123456789abcdef&id=827199"
        ),
        items=[
            ReclamationItem(
                oem_number="DZ1200015K00",
                brand_name="DragonZap",
                quantity=1,
            )
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()
    fake_client = _FakeFrozaClient(
        [
            _froza_payload(quantity=4, oem="DZ1200015K00"),
            _froza_payload(
                state="approved",
                quantity=4,
                oem="DZ1200015K00",
            ),
        ]
    )

    _, snapshot, already_sent = await send_froza_decision(
        test_session,
        reclamation_id=reclamation.id,
        user_id=7,
        client=fake_client,
    )

    assert already_sent is False
    assert snapshot["state"] == "approved"
    assert fake_client.submissions == [("approved", None)]
    await test_session.refresh(reclamation, attribute_names=["items"])
    assert reclamation.items[0].quantity == 4


@pytest.mark.asyncio
async def test_customer_email_reply_is_blocked_for_froza(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.LINK,
        status=RECLAMATION_STATUS.CHECKED,
        sender_email="postvozvrat@froza.ru",
        source_link=(
            "https://froza.ru/supplier/one-question/"
            "?token=c88da2a1924bd3661ba4fda977ca0e52&id=823408"
        ),
    )
    test_session.add(reclamation)
    await test_session.commit()

    with pytest.raises(ValueError, match="через портал"):
        await enqueue_customer_reply(
            test_session,
            reclamation_id=reclamation.id,
            body_text="Согласовано",
        )


def test_classify_reclamation_type():
    assert classify_reclamation_type("товар с браком, течёт") == "defect"
    assert (
        classify_reclamation_type("клиент отказался, не подошла деталь")
        == "customer_refusal"
    )
    assert classify_reclamation_type("просто вопрос") is None


def test_extract_fields_doc_number_and_date():
    subject = "Возврат по документу УТ-1042"
    body = "Отгрузка № УТ-1042 от 15.06.2026, деталь не подошла клиенту."
    fields = extract_fields(subject, body)
    assert fields["document_number"] == "УТ-1042"
    assert fields["document_date"] == date(2026, 6, 15).isoformat()
    assert fields["reclamation_type"] == "customer_refusal"


def test_extract_fields_defect_type():
    fields = extract_fields("Рекламация", "Насос гудит, явный брак")
    assert fields["reclamation_type"] == "defect"


def test_classify_attachment_kind():
    assert classify_attachment_kind("заказ-наряд снятие.pdf") == (
        "removal_order"
    )
    assert classify_attachment_kind("установка_н123.pdf") == (
        "installation_order"
    )
    assert classify_attachment_kind("Дефектовка.docx") == "defect_report"
    assert classify_attachment_kind("photo_1.jpg") == "photo"
    assert classify_attachment_kind("письмо.txt") == "other"


class _FakeSheet:
    def __init__(self, values):
        self._values = values
        self.nrows = max(row for row, _ in values) + 1
        self.ncols = max(col for _, col in values) + 1

    def cell_value(self, row, col):
        return self._values.get((row, col), "")


def test_parse_torg2_sheet_extracts_document_item_and_reason():
    sheet = _FakeSheet(
        {
            (1, 80): "Унифицированная форма № ТОРГ-2",
            (25, 37): "УПД №2801 от 25.06.26",
            (106, 4): "Товар (наименование)",
            (110, 4): (
                "14775PCX000 HONDA 14775PCX000 HONDA "
                "Седло пружины клапана HONDA"
            ),
            (112, 4): "Фактически оказалось",
            (116, 4): "14775PCX000",
            (116, 17): "8",
            (116, 24): "479.00",
            (116, 32): "3832.00",
            (136, 0): "Подробное описание дефектов",
            (138, 0): "14775PCX000 - Отказ клиента.",
            (140, 0): "Заключение комиссии",
        }
    )

    parsed = parse_torg2_sheet(sheet)

    assert parsed == {
        "parser": "torg2_xls",
        "document_number": "2801",
        "document_date": "2026-06-25",
        "reason": "Отказ клиента",
        "items": [
            {
                "oem_number": "14775PCX000",
                "reason": "Отказ клиента",
                "quantity": 8,
                "unit_price": 479.0,
                "line_sum": 3832.0,
                "brand_name": "HONDA",
                "autopart_name": "Седло пружины клапана HONDA",
            }
        ],
    }


def test_approved_reply_uses_customer_facing_return_instructions():
    reclamation = SimpleNamespace(
        id=15,
        email_subject="Возврат детали",
        stated_document_number="123984",
        stated_document_date=date(2026, 7, 7),
        resolution_comment="Внутренняя причина не должна попасть клиенту",
        items=[
            SimpleNamespace(
                brand_name="HONDA",
                oem_number="14775PCX000",
                autopart_name="Трубка вентиляции",
                quantity=1,
            )
        ],
    )

    subject, body = build_customer_reply_template(reclamation, "approved")

    assert subject == "Re: Возврат детали"
    assert "Возврат по вашей рекламации по документу 123984" in body
    assert "от 07.07.2026 согласован" in body
    assert "Позиции к возврату:" in body
    assert "HONDA 14775PCX000 — 1 шт." in body
    assert "передать товар нашему водителю" in body
    assert "Комментарий:" not in body
    assert reclamation.resolution_comment not in body


def test_rejected_reply_includes_customer_facing_reason():
    reclamation = SimpleNamespace(
        id=16,
        email_subject="Возврат детали",
        stated_document_number=None,
        stated_document_date=None,
        resolution_comment="Истёк установленный срок возврата",
        items=[
            SimpleNamespace(
                brand_name="TOYOTA",
                oem_number="9031362001",
                autopart_name="Сальник",
                quantity=1,
            )
        ],
    )

    subject, body = build_customer_reply_template(reclamation, "rejected")

    assert subject == "Re: Возврат детали"
    assert "согласовать возврат" in body
    assert "Причина отказа: Истёк установленный срок возврата." in body
    assert "TOYOTA 9031362001 — 1 шт." in body


def test_request_documents_reply_includes_required_service_documents():
    reclamation = SimpleNamespace(
        id=17,
        email_subject="Рекламация по браку",
        stated_document_number="551",
        stated_document_date=date(2026, 7, 20),
        resolution_comment=None,
        check_result={
            "documents": {
                "missing_labels": [
                    "Заказ-наряд на установку",
                    "Дефектовка",
                ]
            }
        },
        items=[
            SimpleNamespace(
                brand_name="TOYOTA",
                oem_number="9031362001",
                autopart_name="Сальник",
                quantity=1,
            )
        ],
    )

    _, body = build_customer_reply_template(
        reclamation,
        "request_documents",
    )

    assert "Заказ-наряд на установку" in body
    assert "Заказ-наряд на снятие" in body
    assert "Акт дефектовки" in body
    assert "После получения документов" in body


def test_shortage_reply_contains_confirmation_and_apology():
    reclamation = SimpleNamespace(
        id=18,
        email_subject="Недовоз",
        stated_document_number="3391",
        stated_document_date=date(2026, 7, 24),
        resolution_comment=None,
        items=[
            SimpleNamespace(
                brand_name="SUZUKI",
                oem_number="1113986500",
                autopart_name="Втулка",
                quantity=1,
            )
        ],
    )

    _, body = build_customer_reply_template(
        reclamation,
        "shortage_confirmed",
    )

    assert "Недовоз по документу 3391 от 24.07.2026 подтверждаем" in body
    assert "SUZUKI 1113986500 — 1 шт." in body
    assert "Приносим извинения" in body


@pytest.mark.asyncio
async def test_shortage_assignment_notifies_and_confirmation_is_audited(
    test_session: AsyncSession,
):
    customer = Customer(name="Клиент недовоза")
    reviewer = User(
        name="Сотрудник склада",
        email="warehouse@example.com",
        password_hash="not-used",
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.CHECKED,
        reclamation_type="shortage",
        customer=customer,
        sender_email="customer@example.com",
        stated_document_number="3391",
        items=[
            ReclamationItem(
                oem_number="1113986500",
                brand_name="SUZUKI",
                quantity=1,
            )
        ],
    )
    test_session.add_all([customer, reviewer, reclamation])
    await test_session.commit()

    await assign_shortage_reviewer(
        test_session,
        reclamation_id=reclamation.id,
        user_id=reviewer.id,
    )
    notification = (
        await test_session.execute(
            select(AppNotification).where(
                AppNotification.user_id == reviewer.id
            )
        )
    ).scalar_one()
    assert reclamation.shortage_status == "pending_confirmation"
    assert notification.link == f"/reclamations?openId={reclamation.id}"
    assert notification.payload["notification_type"] == (
        "reclamation_shortage"
    )
    assert notification.payload["customer_name"] == "Клиент недовоза"
    assert notification.payload["items"][0]["oem_number"] == "1113986500"

    await postpone_shortage_review(
        test_session,
        reclamation_id=reclamation.id,
        minutes=15,
        user_id=reviewer.id,
    )
    reminder = (
        await test_session.execute(
            select(AppNotification)
            .where(AppNotification.user_id == reviewer.id)
            .order_by(AppNotification.id.desc())
        )
    ).scalars().first()
    assert reminder.available_at is not None
    assert reminder.payload["reclamation_id"] == reclamation.id
    assert reclamation.shortage_snoozed_until == reminder.available_at

    await confirm_shortage(
        test_session,
        reclamation_id=reclamation.id,
        confirmed=True,
        comment="Позиции не было в коробке",
        user_id=reviewer.id,
    )

    assert reclamation.shortage_status == "confirmed"
    assert reclamation.shortage_confirmed_by_user_id == reviewer.id
    assert reclamation.shortage_comment == "Позиции не было в коробке"
    assert reclamation.resolution == "approved"
    assert reclamation.status == RECLAMATION_STATUS.APPROVED


@pytest.mark.asyncio
async def test_reclamation_check_resolves_supplier_from_customer_order(
    test_session: AsyncSession,
):
    customer = Customer(name="Клиент с транзитным заказом")
    provider = Provider(
        name="Поставщик заказа",
        return_allowed=True,
        return_window_days=30,
    )
    order = CustomerOrder(
        customer=customer,
        order_number="ORD-77",
        order_date=date(2026, 7, 20),
        received_at=datetime(2026, 7, 20, 9, 0, tzinfo=UTC),
        items=[
            CustomerOrderItem(
                oem="1113986500",
                brand="SUZUKI",
                requested_qty=2,
                ship_qty=1,
                supplier=provider,
            )
        ],
    )
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        reclamation_type="customer_refusal",
        customer=customer,
        items=[
            ReclamationItem(
                oem_number="1113986500",
                brand_name="SUZUKI",
                autopart_name="Втулка",
                quantity=1,
            )
        ],
    )
    test_session.add_all([customer, provider, order, reclamation])
    await test_session.commit()

    await run_reclamation_check(
        test_session,
        reclamation_id=reclamation.id,
    )

    item = reclamation.items[0]
    checked_item = reclamation.check_result["items"][0]
    assert checked_item["supplier_id"] == provider.id
    assert checked_item["supplier_name"] == "Поставщик заказа"
    assert checked_item["supplier_source"] == "customer_order"
    assert checked_item["customer_order_date"] == "2026-07-20"
    assert checked_item["verdict"] == "approve"
    assert checked_item["supplier_action"] == "request_supplier"
    assert reclamation.check_result["recommendation_code"] == "approve"
    assert (
        reclamation.check_result["supplier_action_code"]
        == "request_supplier"
    )
    assert item.source_provider_id == provider.id
    assert str(getattr(item.item_source, "value", item.item_source)) == (
        "supplier_transit"
    )

    provider.return_allowed = False
    reclamation.status = RECLAMATION_STATUS.WAITING_SUPPLIER
    await test_session.commit()
    await run_reclamation_check(
        test_session,
        reclamation_id=reclamation.id,
    )

    assert reclamation.check_result["recommendation_code"] == "approve"
    assert reclamation.check_result["supplier_action_code"] == "unavailable"
    assert reclamation.status == RECLAMATION_STATUS.CHECKED
    assert reclamation.check_result["items"][0]["verdict"] == "approve"
    assert (
        reclamation.check_result["items"][0]["supplier_action"]
        == "unavailable"
    )


@pytest.mark.asyncio
async def test_supplier_return_request_contains_order_context_and_defect_files(
    test_session: AsyncSession,
    tmp_path,
):
    provider = Provider(
        name="Поставщик возврата",
        return_request_email="returns@supplier.example",
    )
    customer = Customer(name="Клиент возврата")
    evidence_path = tmp_path / "defect.pdf"
    evidence_path.write_bytes(b"defect")
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.CHECKED,
        reclamation_type="defect",
        customer=customer,
        stated_document_number="DOC-15",
        stated_document_date=date(2026, 7, 25),
        stated_reason="Брак: течь детали",
        items=[
            ReclamationItem(
                oem_number="19501R6FG00",
                brand_name="HONDA",
                autopart_name="Патрубок",
                quantity=2,
                reason="Течь детали",
                item_source=RECLAMATION_ITEM_SOURCE.SUPPLIER_TRANSIT,
                source_provider_id=provider.id,
            )
        ],
        attachments=[
            ReclamationAttachment(
                kind=RECLAMATION_ATTACHMENT_KIND.DEFECT_REPORT,
                file_name="defect.pdf",
                content_type="application/pdf",
                local_file_path=str(evidence_path),
                size_bytes=6,
            )
        ],
    )
    test_session.add_all([provider, customer])
    await test_session.flush()
    reclamation.items[0].source_provider_id = provider.id
    test_session.add(reclamation)
    await test_session.commit()
    reclamation.check_result = {
        "items": [
            {
                "item_id": reclamation.items[0].id,
                "supplier_id": provider.id,
                "supplier_action": "request_supplier",
                "customer_order_number": "ORD-15",
                "customer_order_date": "2026-07-20",
            }
        ]
    }
    await test_session.commit()

    rows = await enqueue_supplier_request(
        test_session,
        reclamation_id=reclamation.id,
    )

    assert len(rows) == 1
    await test_session.refresh(reclamation)
    assert reclamation.status == RECLAMATION_STATUS.CHECKED
    assert rows[0].to_email == "returns@supplier.example"
    assert "HONDA 19501R6FG00 — Патрубок; 2 шт." in rows[0].body_text
    assert "заказ ORD-15 от 2026-07-20" in rows[0].body_text
    assert "Причина возврата: Течь детали." in rows[0].body_text
    assert rows[0].attachments[0]["file_name"] == "defect.pdf"


@pytest.mark.asyncio
async def test_mark_sent_marks_source_email_answered_without_duplicate_retry(
    test_session: AsyncSession,
    monkeypatch,
):
    account = EmailAccount(
        name="Reclamations",
        email="reclamations@example.com",
        password="secret",
        imap_host="imap.example.com",
        imap_port=993,
        imap_folder="INBOX",
        purposes=["reclamation"],
        is_active=True,
    )
    test_session.add(account)
    await test_session.flush()
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.APPROVED,
        extracted_data={
            "mailbox": {
                "email_account_id": account.id,
                "folder": "INBOX",
                "uid": "12345",
            }
        },
    )
    test_session.add(reclamation)
    await test_session.flush()
    outbox = EmailOutbox(
        status=EMAIL_OUTBOX_STATUS.PENDING,
        from_email=account.email,
        to_email="customer@example.com",
        source_type="reclamation",
        source_id=reclamation.id,
    )
    test_session.add(outbox)
    await test_session.commit()
    flagged = {}

    def fake_flag(**kwargs):
        flagged.update(kwargs)

    monkeypatch.setattr(
        "dz_fastapi.services.email_outbox."
        "_flag_source_email_answered_sync",
        fake_flag,
    )

    await mark_outbox_sent(test_session, outbox_id=outbox.id)
    await test_session.refresh(reclamation)

    assert flagged["uid"] == "12345"
    assert flagged["folder"] == "INBOX"
    mailbox = reclamation.extracted_data["mailbox"]
    assert mailbox["answered_flag_status"] == "marked"
    assert mailbox["reply_outbox_id"] == outbox.id

    await mark_outbox_error(
        test_session,
        outbox_id=outbox.id,
        error="HTTP response lost",
        retry=True,
    )
    await test_session.refresh(outbox)
    assert outbox.status == EMAIL_OUTBOX_STATUS.SENT


def test_source_email_is_marked_read_answered_and_flagged(monkeypatch):
    captured = {}

    class FakeFolder:
        def set(self, folder):
            captured["folder"] = folder

    class FakeMailbox:
        folder = FakeFolder()

        def login(self, email, password):
            captured["email"] = email
            captured["password"] = password
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def flag(self, uid, flags, value):
            captured["uid"] = uid
            captured["flags"] = flags
            captured["value"] = value

    monkeypatch.setattr(
        "dz_fastapi.services.email._create_mailbox",
        lambda host, port, ssl: FakeMailbox(),
    )

    _flag_source_email_answered_sync(
        host="imap.example.com",
        port=993,
        email="reclamations@example.com",
        password="secret",
        folder="INBOX",
        uid="12345",
    )

    assert captured["folder"] == "INBOX"
    assert captured["uid"] == "12345"
    assert captured["flags"] == [r"\Seen", r"\Answered", r"\Flagged"]
    assert captured["value"] is True


@pytest.mark.asyncio
async def test_resolve_and_reply_saves_decision_and_email_atomically(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.CHECKED,
        sender_email="returns@example.com",
        email_subject="Возврат детали",
        items=[
            ReclamationItem(
                oem_number="9031362001",
                brand_name="TOYOTA",
                quantity=1,
            )
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()

    outbox = await apply_and_enqueue_customer_reply(
        test_session,
        reclamation_id=reclamation.id,
        action="rejected",
        resolution_comment="Товар утратил товарный вид",
        resolved_by_user_id=None,
    )

    await test_session.refresh(reclamation)
    assert reclamation.status == RECLAMATION_STATUS.REJECTED
    assert reclamation.resolution == "rejected"
    assert reclamation.resolution_comment == "Товар утратил товарный вид"
    assert outbox.to_email == "returns@example.com"
    assert "Причина отказа: Товар утратил товарный вид." in outbox.body_text


@pytest.mark.asyncio
async def test_resolve_and_reply_requires_rejection_reason(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.CHECKED,
        sender_email="returns@example.com",
    )
    test_session.add(reclamation)
    await test_session.commit()

    with pytest.raises(ValueError, match="обязательно укажите причину"):
        await apply_and_enqueue_customer_reply(
            test_session,
            reclamation_id=reclamation.id,
            action="rejected",
            resolution_comment="",
            resolved_by_user_id=None,
        )

    await test_session.refresh(reclamation)
    assert reclamation.resolution is None
    assert reclamation.status == RECLAMATION_STATUS.CHECKED


@pytest.mark.asyncio
async def test_apply_and_reply_requests_documents_atomically(
    test_session: AsyncSession,
):
    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.CHECKED,
        sender_email="returns@example.com",
        email_subject="Рекламация по браку",
        check_result={
            "documents": {
                "missing_labels": [
                    "Заказ-наряд на снятие",
                    "Акт дефектовки",
                ]
            }
        },
        items=[
            ReclamationItem(
                oem_number="9031362001",
                brand_name="TOYOTA",
                quantity=1,
            )
        ],
    )
    test_session.add(reclamation)
    await test_session.commit()

    outbox = await apply_and_enqueue_customer_reply(
        test_session,
        reclamation_id=reclamation.id,
        action="request_documents",
        resolution_comment=None,
        resolved_by_user_id=None,
    )

    await test_session.refresh(reclamation)
    assert reclamation.status == RECLAMATION_STATUS.WAITING_DOCS
    assert reclamation.resolution is None
    assert "Заказ-наряд на снятие" in outbox.body_text
    assert "Акт дефектовки" in outbox.body_text


@pytest.mark.asyncio
async def test_ingest_reclamation_uses_structured_attachment_data(
    test_session: AsyncSession,
    monkeypatch,
):
    customer = Customer(
        name="Attachment reclamation customer",
        email_contact="returns@example.com",
        type_prices="Wholesale",
    )
    test_session.add(customer)
    await test_session.commit()

    monkeypatch.setattr(
        "dz_fastapi.services.reclamations.parse_reclamation_attachment",
        lambda _filename, _payload: {
            "parser": "torg2_xls",
            "document_number": "2801",
            "document_date": "2026-06-25",
            "reason": "Отказ клиента",
            "items": [
                {
                    "oem_number": "14775PCX000",
                    "brand_name": "HONDA",
                    "autopart_name": "Седло пружины клапана HONDA",
                    "quantity": 8,
                    "reason": "Отказ клиента",
                    "unit_price": 479.0,
                    "line_sum": 3832.0,
                }
            ],
        },
    )

    reclamation = await ingest_reclamation_email(
        test_session,
        ReclamationInboundEmail(
            from_="returns@example.com",
            subject="Акт расхождений",
            body_text="Просим рассмотреть возврат во вложении.",
            message_id="<torg2-test@example.com>",
            attachments=[
                ReclamationInboundAttachment(
                    filename="akt00136.xls",
                    payload=b"test-xls",
                    content_type="application/vnd.ms-excel",
                )
            ],
        ),
    )

    assert reclamation is not None
    assert reclamation.customer_id == customer.id
    assert reclamation.stated_document_number == "2801"
    assert reclamation.stated_document_date == date(2026, 6, 25)
    assert reclamation.stated_reason == "Отказ клиента"
    assert reclamation.reclamation_type == "customer_refusal"
    assert len(reclamation.items) == 1
    assert reclamation.items[0].oem_number == "14775PCX000"
    assert reclamation.items[0].brand_name == "HONDA"
    assert reclamation.items[0].quantity == 8
    assert reclamation.items[0].reason == "Отказ клиента"


def test_order_return_start_date_uses_moscow_cutoff_and_workday():
    before_cutoff = datetime(2026, 7, 17, 11, 59, tzinfo=UTC)
    after_cutoff = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)

    holiday_set = {date(2026, 7, 20)}

    assert _order_return_start_date(
        {"received_at": before_cutoff}, holiday_set
    ) == date(2026, 7, 17)
    assert _order_return_start_date(
        {"received_at": after_cutoff}, holiday_set
    ) == date(2026, 7, 21)


def test_elapsed_return_days_excludes_holidays():
    elapsed, excluded = _elapsed_return_days(
        date(2026, 1, 1),
        date(2026, 1, 6),
        {date(2026, 1, 2), date(2026, 1, 5)},
    )

    assert elapsed == 3
    assert excluded == 2


@pytest.mark.asyncio
async def test_match_oem_uses_customer_order_history(
    test_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(
        "dz_fastapi.services.reclamation_check.now_moscow",
        lambda: datetime(2026, 7, 21, 9, 0, tzinfo=UTC),
    )
    customer = Customer(
        name="Reclamation customer",
        email_contact="reclamation-customer@example.com",
        type_prices="Wholesale",
    )
    test_session.add(customer)
    await test_session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        order_number="ORDER-948",
        order_date=date(2026, 7, 17),
        received_at=datetime(2026, 7, 17, 12, 0, tzinfo=UTC),
    )
    test_session.add(order)
    await test_session.flush()
    test_session.add(
        CustomerOrderItem(
            order_id=order.id,
            oem="14775-PCX-000",
            brand="HONDA",
            name="Трубка вентиляции",
            requested_qty=1,
        )
    )
    await test_session.commit()

    matches = await _match_oems_in_text(
        test_session,
        "Возврат позиции 14775PCX000",
        customer_id=customer.id,
    )

    assert matches == [
        {
            "autopart_id": None,
            "oem_number": "14775PCX000",
            "autopart_name": "Трубка вентиляции",
            "brand_name": "HONDA",
        }
    ]

    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.EMAIL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        customer_id=customer.id,
        email_subject="Рекламация клиента",
        email_body="Просим вернуть позицию 14775PCX000",
    )
    test_session.add(reclamation)
    await test_session.commit()

    checked = await run_reclamation_check(
        test_session,
        reclamation_id=reclamation.id,
    )
    item_check = checked.check_result["items"][0]
    assert len(checked.items) == 1
    assert checked.items[0].oem_number == "14775PCX000"
    assert checked.items[0].brand_name == "HONDA"
    assert item_check["customer_order_found"] is True
    assert item_check["customer_order_number"] == "ORDER-948"
    assert item_check["customer_order_requested_qty"] == 1
    assert item_check["shipment_found"] is False
    assert item_check["return_reference_source"] == "customer_order"
    assert item_check["return_start_date"] == "2026-07-20"
    assert item_check["verdict"] == "approve"
