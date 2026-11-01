import pytest

from dz_fastapi.models.autopart import AutoPart, ScannedCodeAlias
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import MarkingCodeStatus, ProductMarkingCode
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services import gis_mt
from dz_fastapi.services import marking_receipt_check as mrc
from dz_fastapi.services.supplier_workflow import serialize_supplier_receipt

GTIN = "04601234567890"


def kiz(serial: str, gtin: str = GTIN) -> str:
    return f"01{gtin}21{serial}"


async def make_receipt(session, provider, items):
    provider.inn = "7701234567"
    receipt = SupplierReceipt(provider_id=provider.id, document_number="УПД-1")
    session.add(receipt)
    await session.flush()
    for codes, qty, autopart_id in items:
        session.add(
            SupplierReceiptItem(
                receipt_id=receipt.id,
                received_quantity=qty,
                marking_codes=codes,
                autopart_id=autopart_id,
            )
        )
    await session.flush()
    await session.refresh(receipt, attribute_names=["items"])
    receipt.provider = provider
    return receipt


def reasons(item):
    return sorted(i["reason"] for i in item.marking_check["issues"])


@pytest.mark.asyncio
async def test_local_checks_find_quantity_duplicates_and_registry(
    test_session, created_providers
):
    busy = kiz("BUSY000000001")
    test_session.add(
        ProductMarkingCode(code=busy, status=MarkingCodeStatus.IN_STOCK)
    )
    receipt = await make_receipt(
        test_session,
        created_providers[0],
        [
            ([kiz("AAA0000000001"), kiz("AAA0000000002"), busy], 2, None),
            ([kiz("AAA0000000001"), "garbage"], 2, None),
        ],
    )
    assert await mrc.check_receipt_marking(test_session, receipt, use_gis=False)
    first, second = receipt.items
    assert reasons(first) == [
        "already_in_registry",
        "duplicate_in_document",
        "quantity_mismatch",
    ]
    assert reasons(second) == ["duplicate_in_document", "invalid_format"]
    assert first.marking_check["status"] == "error"
    assert first.marking_check["gis_checked"] is False
    summary = serialize_supplier_receipt(receipt)
    assert summary["marking_status"] == "error"
    assert summary["marking_error_count"] == 5
    # без изменений данных повторно не считаем
    assert not await mrc.check_receipt_marking(
        test_session, receipt, use_gis=False
    )


@pytest.mark.asyncio
async def test_clean_receipt_is_ok_and_item_without_codes_is_ignored(
    test_session, created_providers
):
    receipt = await make_receipt(
        test_session,
        created_providers[0],
        [([kiz("OK00000000001")], 1, None), (None, 5, None)],
    )
    await mrc.check_receipt_marking(test_session, receipt, use_gis=False)
    assert receipt.items[0].marking_check["status"] == "ok"
    assert receipt.items[1].marking_check is None
    assert serialize_supplier_receipt(receipt)["marking_status"] == "ok"


@pytest.mark.asyncio
async def test_receipt_without_codes_has_no_marking_status(
    test_session, created_providers
):
    receipt = await make_receipt(
        test_session, created_providers[0], [(None, 3, None)]
    )
    assert not await mrc.check_receipt_marking(test_session, receipt)
    assert serialize_supplier_receipt(receipt)["marking_status"] == "none"


@pytest.mark.asyncio
async def test_gtin_bound_to_other_autopart(test_session, created_providers):
    brand = Brand(name="TOYOTA")
    test_session.add(brand)
    await test_session.flush()
    bound = AutoPart(brand_id=brand.id, oem_number="1111111111", name="A")
    other = AutoPart(brand_id=brand.id, oem_number="2222222222", name="B")
    test_session.add_all([bound, other])
    await test_session.flush()
    test_session.add(
        ScannedCodeAlias(code=f"GTIN:{GTIN}", autopart_id=bound.id)
    )
    receipt = await make_receipt(
        test_session,
        created_providers[0],
        [([kiz("G000000000001")], 1, other.id)],
    )
    await mrc.check_receipt_marking(test_session, receipt, use_gis=False)
    assert reasons(receipt.items[0]) == ["gtin_mismatch"]


@pytest.mark.asyncio
async def test_gis_check_status_owner_gtin_and_missing(
    test_session, created_providers, monkeypatch
):
    good, retired, foreign, wrong_gtin, missing = (
        kiz("G1"), kiz("G2"), kiz("G3"), kiz("G4"), kiz("G5")
    )
    receipt = await make_receipt(
        test_session,
        created_providers[0],
        [([good, retired, foreign, wrong_gtin, missing], 5, None)],
    )

    async def fake_settings(session):
        return object()

    async def fake_fetch(session, codes):
        def info(code, status="INTRODUCED", owner="7701234567", gtin=GTIN):
            return {"cis": code, "status": status, "owner_inn": owner,
                    "gtin": gtin}
        return {
            good: info(good),
            retired: info(retired, status="RETIRED"),
            foreign: info(foreign, owner="5000000000"),
            wrong_gtin: info(wrong_gtin, gtin="04600000000000"),
        }, None

    monkeypatch.setattr(gis_mt, "get_or_create_gis_mt_settings", fake_settings)
    monkeypatch.setattr(gis_mt, "gis_mt_token_is_active", lambda row: True)
    monkeypatch.setattr(mrc, "_fetch_gis_info", fake_fetch)

    await mrc.check_receipt_marking(test_session, receipt, use_gis=True)
    check = receipt.items[0].marking_check
    assert check["gis_checked"] is True
    by_code = {i["code"]: i["reason"] for i in check["issues"]}
    assert by_code == {
        retired: "gis_status",
        foreign: "gis_owner",
        wrong_gtin: "gis_gtin",
        missing: "gis_not_found",
    }
    assert serialize_supplier_receipt(receipt)["marking_gis_checked"] is True


@pytest.mark.asyncio
async def test_local_check_is_upgraded_when_gis_becomes_available(
    test_session, created_providers, monkeypatch
):
    code = kiz("UP00000000001")
    receipt = await make_receipt(
        test_session, created_providers[0], [([code], 1, None)]
    )
    await mrc.check_receipt_marking(test_session, receipt, use_gis=False)
    assert receipt.items[0].marking_check["gis_checked"] is False

    async def fake_settings(session):
        return object()

    async def fake_fetch(session, codes):
        return {}, "timeout"

    monkeypatch.setattr(gis_mt, "get_or_create_gis_mt_settings", fake_settings)
    monkeypatch.setattr(gis_mt, "gis_mt_token_is_active", lambda row: True)
    monkeypatch.setattr(mrc, "_fetch_gis_info", fake_fetch)
    assert await mrc.check_receipt_marking(test_session, receipt, use_gis=True)
    check = receipt.items[0].marking_check
    assert check["gis_checked"] is False
    assert reasons(receipt.items[0]) == ["gis_unavailable"]
    assert check["status"] == "warning"
