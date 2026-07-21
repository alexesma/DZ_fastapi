import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.partner import (
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    Reclamation,
    ReclamationItem,
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
from dz_fastapi.services.reclamation_replies import build_customer_reply_template
from dz_fastapi.services.reclamations import (
    ReclamationInboundAttachment,
    ReclamationInboundEmail,
    _match_oems_in_text,
    classify_attachment_kind,
    classify_reclamation_type,
    extract_fields,
    extract_links,
    extract_sender_email,
    ingest_reclamation_email,
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
