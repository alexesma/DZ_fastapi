from datetime import timedelta
from decimal import Decimal
from io import BytesIO

import pytest
from openpyxl import load_workbook

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
)
from dz_fastapi.services import shipment_upd_email
from dz_fastapi.services.diadoc_outgoing_pipeline import signing_queue
from dz_fastapi.services.shipment_upd_email import (
    auto_email_posted_shipments,
    send_shipment_upd_email,
)


async def _shipment(session, customer, autopart, *, status=ShipmentDocumentStatus.POSTED):
    shipment = ShipmentDocument(
        status=status,
        customer_id=customer.id,
        doc_number="ДЗ-000007",
        posted_at=now_moscow(),
        edo_auto_error="У клиента нет ящика Диадока — документ по ЭДО не отправить",
    )
    session.add(shipment)
    await session.flush()
    session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=autopart.id,
            quantity=2,
            price=Decimal("150.00"),
        )
    )
    await session.commit()
    return shipment.id


@pytest.fixture
def outbox(monkeypatch):
    sent = []

    def fake_send(to_email, subject, body, attachment, file_name, is_html, **kwargs):
        sent.append(
            {
                "to": to_email,
                "subject": subject,
                "file_name": file_name,
                "attachment": attachment,
                "from": kwargs.get("from_email"),
            }
        )
        return True

    monkeypatch.setattr(shipment_upd_email, "send_email_with_attachment", fake_send)
    return sent


async def _mail_account(session):
    session.add(
        EmailAccount(
            name="Исходящие",
            email="docs@example.com",
            password="unused",
            purposes=["orders_out"],
            is_active=True,
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_upd_is_emailed_and_marked_as_sent(
    test_session, created_customers, created_autopart, outbox
):
    customer = created_customers[0]
    customer.email_contact = "buyer@example.com"
    await _mail_account(test_session)
    shipment_id = await _shipment(test_session, customer, created_autopart)
    assert [row["shipment_id"] for row in (await signing_queue(test_session))["blocked"]] == [
        shipment_id
    ]

    result = await send_shipment_upd_email(test_session, shipment_id, user_id=None)
    await test_session.commit()

    assert result["sent_to"] == "buyer@example.com"
    [letter] = outbox
    assert letter["to"] == "buyer@example.com"
    assert letter["from"] == "docs@example.com"
    assert "ДЗ-000007" in letter["subject"]
    assert letter["file_name"] == "UPD_ДЗ-000007.xlsx"
    sheet = load_workbook(BytesIO(letter["attachment"])).active
    cells = [str(cell.value) for row in sheet.iter_rows() for cell in row if cell.value]
    assert any("Счёт-фактура № ДЗ-000007" in value for value in cells)
    assert any(created_autopart.oem_number in value for value in cells)
    # 2 шт. по 150 ₽ с НДС — итог числом, чтобы его можно было складывать.
    numbers = [
        cell.value
        for row in sheet.iter_rows()
        for cell in row
        if isinstance(cell.value, (int, float))
    ]
    assert 300 in numbers and 54.1 in numbers
    shipment = await test_session.get(ShipmentDocument, shipment_id)
    assert shipment.upd_emailed_to == "buyer@example.com"
    assert shipment.upd_emailed_at is not None
    # Отправленный документ из очереди «нужно исправить» уходит.
    assert (await signing_queue(test_session))["blocked"] == []


@pytest.mark.asyncio
async def test_emailing_needs_an_address_and_a_posted_document(
    test_session, created_customers, created_autopart, outbox
):
    customer = created_customers[0]
    customer.email_contact = None
    await _mail_account(test_session)
    shipment_id = await _shipment(test_session, customer, created_autopart)
    with pytest.raises(ValueError):
        await send_shipment_upd_email(test_session, shipment_id)
    # Адрес можно указать вручную.
    result = await send_shipment_upd_email(
        test_session, shipment_id, to_email="other@example.com"
    )
    assert result["sent_to"] == "other@example.com"

    draft_id = await _shipment(
        test_session, customer, created_autopart, status=ShipmentDocumentStatus.DRAFT
    )
    with pytest.raises(ValueError):
        await send_shipment_upd_email(test_session, draft_id, to_email="x@example.com")
    assert len(outbox) == 1


@pytest.mark.asyncio
async def test_every_customer_gets_the_letter_once_including_edo_customers(
    test_session, created_customers, created_autopart, outbox
):
    with_mail, without_mail = created_customers[0], created_customers[1]
    with_mail.email_contact = "buyer@example.com"
    without_mail.email_contact = None
    await _mail_account(test_session)
    since = now_moscow() - timedelta(hours=1)
    first = await _shipment(test_session, with_mail, created_autopart)
    second = await _shipment(test_session, without_mail, created_autopart)
    # Клиент с ЭДО: причины «нет ящика» у реализации нет, письмо всё равно уходит.
    edo = await test_session.get(ShipmentDocument, first)
    edo.edo_auto_error = None
    await test_session.commit()

    result = await auto_email_posted_shipments(test_session, enabled=True, since=since)

    assert result == {"sent": 1, "failed": 1}
    assert [letter["to"] for letter in outbox] == ["buyer@example.com"]
    failed = await test_session.get(ShipmentDocument, second)
    assert "почты" in failed.upd_email_error

    # Повторный проход не шлёт письмо второй раз.
    again = await auto_email_posted_shipments(test_session, enabled=True, since=since)
    assert again["sent"] == 0 and len(outbox) == 1

    off = await auto_email_posted_shipments(test_session, enabled=False, since=since)
    assert off["skipped"] == "disabled"
