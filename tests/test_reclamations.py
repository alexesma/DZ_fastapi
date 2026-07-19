from datetime import date

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.partner import (
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    Customer,
    CustomerOrder,
    CustomerOrderItem,
    Reclamation,
)
from dz_fastapi.services.reclamation_check import run_reclamation_check
from dz_fastapi.services.reclamations import (
    _match_oems_in_text,
    classify_attachment_kind,
    classify_reclamation_type,
    extract_fields,
    extract_links,
    extract_sender_email,
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


@pytest.mark.asyncio
async def test_match_oem_uses_customer_order_history(
    test_session: AsyncSession,
):
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
    assert item_check["verdict"] == "manual"
