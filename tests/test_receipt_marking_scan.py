from datetime import date

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dz_fastapi.models.autopart import ScannedCodeAlias
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.receipt_marking_scan import (
    NotMarkingCode,
    marking_scan_state,
    record_marking_scan,
)

GTIN = "04600000000017"
CODE_A = f"01{GTIN}21AAAAAAAAAAAAA"
CODE_B = f"01{GTIN}21BBBBBBBBBBBBB"
CODE_X = f"01{GTIN}21XXXXXXXXXXXXX"


async def _receipt(session, provider, autopart, codes):
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number="R-MARK",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=autopart.id,
            received_quantity=len(codes) or 1,
            marking_codes=codes or None,
        )
    ]
    session.add(receipt)
    await session.commit()
    return (
        await session.execute(
            select(SupplierReceipt)
            .where(SupplierReceipt.id == receipt.id)
            .options(selectinload(SupplierReceipt.items))
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_scan_confirms_document_code_and_reports_the_rest(
    test_session, created_autopart, created_providers
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A, CODE_B]
    )
    # Сканер отдаёт полный код с ключом и криптохвостом — сверяем по КИ.
    result = await record_marking_scan(
        test_session, receipt, f"{CODE_A}\x1d91EE11\x1d92abcdef"
    )
    assert result["status"] == "confirmed"
    assert result["duplicate"] is False
    assert result["autopart_id"] == created_autopart.id

    state = await marking_scan_state(test_session, receipt)
    assert (state["expected"], state["confirmed"], state["missing"]) == (2, 1, 1)
    item_state = state["by_item"][receipt.items[0].id]
    assert item_state["missing_codes"] == [CODE_B]
    assert state["extra"] == []


@pytest.mark.asyncio
async def test_repeated_scan_is_a_duplicate_and_is_not_counted_twice(
    test_session, created_autopart, created_providers
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A]
    )
    await record_marking_scan(test_session, receipt, CODE_A)
    again = await record_marking_scan(test_session, receipt, CODE_A)
    assert again["duplicate"] is True
    state = await marking_scan_state(test_session, receipt)
    assert state["confirmed"] == 1


@pytest.mark.asyncio
async def test_code_missing_from_document_is_extra(
    test_session, created_autopart, created_providers
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A]
    )
    test_session.add(
        ScannedCodeAlias(code=f"GTIN:{GTIN}", autopart_id=created_autopart.id)
    )
    await test_session.flush()
    result = await record_marking_scan(test_session, receipt, CODE_X)
    assert result["status"] == "extra"
    # Товар узнаём по привязке GTIN, чтобы показать, к какой строке код.
    assert result["autopart_id"] == created_autopart.id
    state = await marking_scan_state(test_session, receipt)
    assert [row["code"] for row in state["extra"]] == [CODE_X]
    assert state["missing"] == 1


@pytest.mark.asyncio
async def test_document_without_codes_still_keeps_scans(
    test_session, created_autopart, created_providers
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, []
    )
    result = await record_marking_scan(test_session, receipt, CODE_A)
    assert result["status"] == "extra"
    assert result["document_has_codes"] is False
    state = await marking_scan_state(test_session, receipt)
    assert state["document_has_codes"] is False
    assert len(state["extra"]) == 1


@pytest.mark.asyncio
async def test_plain_barcode_is_rejected(
    test_session, created_autopart, created_providers
):
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A]
    )
    with pytest.raises(NotMarkingCode):
        await record_marking_scan(test_session, receipt, "4600000000017")


@pytest.mark.asyncio
async def test_plan_and_scan_endpoints(
    async_client, test_session, created_autopart, created_providers
):
    # Текущий пользователь в тестах подменён без записи в БД, а скан
    # ссылается на него внешним ключом.
    test_session.add(
        User(
            id=1,
            name="Test Admin",
            email="test-admin@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    receipt = await _receipt(
        test_session, created_providers[0], created_autopart, [CODE_A, CODE_B]
    )
    response = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/marking-scan",
        json={"code": CODE_A},
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "confirmed"

    response = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/marking-scan",
        json={"code": CODE_X},
    )
    extra_id = response.json()["id"]

    plan = (
        await async_client.get(f"/receiving-scan/receipts/{receipt.id}")
    ).json()
    line = plan["lines"][0]
    assert (line["marking_expected"], line["marking_confirmed"]) == (2, 1)
    assert line["marking_missing"] == [CODE_B]
    assert plan["marking"]["missing"] == 1
    assert [row["id"] for row in plan["marking"]["extra"]] == [extra_id]

    response = await async_client.delete(
        f"/receiving-scan/receipts/{receipt.id}/marking-scan/{extra_id}"
    )
    assert response.status_code == 204
    plan = (
        await async_client.get(f"/receiving-scan/receipts/{receipt.id}")
    ).json()
    assert plan["marking"]["extra"] == []

    response = await async_client.post(
        f"/receiving-scan/receipts/{receipt.id}/marking-scan",
        json={"code": "not-a-marking-code"},
    )
    assert response.status_code == 400
