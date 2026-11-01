from types import SimpleNamespace

import pytest

from dz_fastapi.models.inventory import MarkingCodeStatus, ProductMarkingCode
from dz_fastapi.services import gis_mt
from dz_fastapi.services.diadoc_outgoing import _collect_shipment_item_marking_codes
from dz_fastapi.services.marking_shipment_check import check_shipment_marking

GTIN = "04601234567890"


def kiz(serial):
    return f"01{GTIN}21{serial}"


def make_doc(items):
    return SimpleNamespace(id=10, items=items)


def make_item(codes, quantity, lot_codes=None, item_id=1):
    return SimpleNamespace(
        id=item_id,
        autopart_id=None,
        quantity=quantity,
        allocations=[SimpleNamespace(marking_codes=codes)] if codes else [],
        lot=SimpleNamespace(marking_codes=lot_codes) if lot_codes else None,
    )


def test_lot_codes_not_used_when_they_belong_to_other_pieces():
    item = make_item(None, 1, lot_codes=[kiz(f"S{i}") for i in range(5)])
    assert _collect_shipment_item_marking_codes(item) == []
    exact = make_item(None, 2, lot_codes=[kiz("A"), kiz("B")])
    assert _collect_shipment_item_marking_codes(exact) == [kiz("A"), kiz("B")]


@pytest.mark.asyncio
async def test_quantity_and_registry_state(test_session):
    other = ProductMarkingCode(code=kiz("A"), status=MarkingCodeStatus.IN_STOCK)
    test_session.add(other)
    await test_session.flush()
    doc = make_doc([make_item([kiz("A")], 2)])
    result = await check_shipment_marking(test_session, doc)
    reasons = sorted(i["reason"] for i in result["issues"])
    assert reasons == ["quantity_mismatch", "registry_state"]
    assert result["status"] == "error"
    assert result["gis_checked"] is False


@pytest.mark.asyncio
async def test_unmarked_shipment_has_no_issues(test_session):
    result = await check_shipment_marking(
        test_session, make_doc([make_item(None, 3)])
    )
    assert result == {
        "shipment_id": 10, "codes_total": 0, "gis_checked": False,
        "status": "none", "issues": [],
    }


@pytest.mark.asyncio
async def test_gis_status_and_owner(test_session, monkeypatch):
    good, retired, foreign = kiz("G"), kiz("R"), kiz("F")

    async def fake_settings(session):
        return object()

    async def fake_fetch(session, codes):
        def info(code, status="INTRODUCED", owner="7701234567"):
            return {"cis": code, "status": status, "owner_inn": owner,
                    "gtin": GTIN}
        return {
            good: info(good),
            retired: info(retired, status="RETIRED"),
            foreign: info(foreign, owner="5000000000"),
        }, None

    async def fake_inn(session):
        return "7701234567"

    import dz_fastapi.services.marking_shipment_check as msc

    monkeypatch.setattr(gis_mt, "get_or_create_gis_mt_settings", fake_settings)
    monkeypatch.setattr(gis_mt, "gis_mt_token_is_active", lambda row: True)
    monkeypatch.setattr(msc, "_fetch_gis_info", fake_fetch)
    monkeypatch.setattr(msc, "_own_inn", fake_inn)

    for code in (good, retired, foreign):
        test_session.add(
            ProductMarkingCode(code=code, status=MarkingCodeStatus.IN_STOCK)
        )
    await test_session.flush()
    doc = make_doc([make_item([good, retired, foreign], 3)])
    result = await check_shipment_marking(test_session, doc, use_gis=True)
    assert result["gis_checked"] is True
    by_reason = {}
    for issue in result["issues"]:
        by_reason.setdefault(issue["reason"], []).append(issue["code"])
    assert by_reason["gis_status"] == [retired]
    assert by_reason["gis_owner"] == [foreign]
