from types import SimpleNamespace

import pytest

from dz_fastapi.models.inventory import MarkingCodeStatus, ProductMarkingCode
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
    assert _collect_shipment_item_marking_codes(exact) == []


@pytest.mark.asyncio
async def test_quantity_and_registry_state(test_session):
    other = ProductMarkingCode(code=kiz("A"), status=MarkingCodeStatus.IN_STOCK)
    test_session.add(other)
    await test_session.flush()
    doc = make_doc([make_item([kiz("A")], 2)])
    result = await check_shipment_marking(test_session, doc)
    reasons = sorted(i["reason"] for i in result["issues"])
    assert reasons == ["quantity_mismatch", "registry_state", "scan_mismatch"]
    assert result["status"] == "error"
    assert result["gis_checked"] is False


@pytest.mark.asyncio
async def test_unmarked_shipment_has_no_issues(test_session):
    result = await check_shipment_marking(
        test_session, make_doc([make_item(None, 3)]), use_gis=True
    )
    assert result == {
        "shipment_id": 10, "codes_total": 0, "gis_checked": False,
        "gis_state": "no_codes", "status": "none", "issues": [],
    }


@pytest.mark.asyncio
async def test_gis_status_and_owner(test_session, monkeypatch, active_shipment_gis):
    good, retired, foreign = kiz("G"), kiz("R"), kiz("F")

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


@pytest.mark.asyncio
async def test_legacy_lot_and_unknown_allocated_code_block_upd(test_session):
    item = make_item(None, 1, lot_codes=[kiz("OLD")])
    result = await check_shipment_marking(test_session, make_doc([item]))
    assert [issue["reason"] for issue in result["issues"]] == ["quantity_mismatch"]
    item = make_item([kiz("UNKNOWN")], 1)
    result = await check_shipment_marking(test_session, make_doc([item]))
    assert any(issue["reason"] == "registry_state" and issue["code"] == kiz("UNKNOWN")
               for issue in result["issues"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["expired", "unauthorized", "timeout", "empty", "owner_missing", "seller_missing"]
)
async def test_gis_failure_blocks_and_retry_verifies(test_session, active_shipment_gis, failure):
    from datetime import timedelta

    from sqlalchemy import select

    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.http.gis_mt_client import GisMtApiError
    from dz_fastapi.models.settings import DiadocIntegrationSettings, GisMtSettings
    from dz_fastapi.services.marking_shipment_check import split_marking_issues

    code = kiz("A")
    doc = make_doc([make_item([code], 1)])
    settings = await test_session.scalar(select(GisMtSettings))
    seller = await test_session.scalar(select(DiadocIntegrationSettings))
    success = active_shipment_gis.side_effect
    if failure == "expired":
        settings.token_expires_at = now_moscow() - timedelta(minutes=1)
    elif failure == "seller_missing":
        seller.organization_inn = None
    elif failure == "unauthorized":
        active_shipment_gis.side_effect = GisMtApiError(401, "invalid token")
    elif failure == "timeout":
        active_shipment_gis.side_effect = TimeoutError()
    else:
        active_shipment_gis.side_effect = None
        active_shipment_gis.return_value = (
            [] if failure == "empty" else [{"cis": code, "status": "INTRODUCED"}]
        )

    result = await check_shipment_marking(test_session, doc, use_gis=True)
    gis_issues = [issue for issue in result["issues"] if issue["reason"].startswith("gis_")]
    assert split_marking_issues(gis_issues)[0]
    assert result["gis_state"] in {"rejected", "deferred"}
    if failure in {"expired", "seller_missing"}:
        active_shipment_gis.assert_not_awaited()
    settings.token_expires_at = now_moscow() + timedelta(hours=1)
    seller.organization_inn = "7700000000"
    active_shipment_gis.side_effect = success
    retry = await check_shipment_marking(test_session, doc, use_gis=True)
    assert retry["gis_state"] == "verified"
