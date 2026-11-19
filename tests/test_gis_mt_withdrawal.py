import asyncio
import base64
import json
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.gis_mt_client import GisMtApiError, GisMtClient
from dz_fastapi.models.inventory import MarkingCodeStatus as Status
from dz_fastapi.models.inventory import ProductMarkingCode, ProductMarkingCodeMovement
from dz_fastapi.models.settings import GisMtSettings
from dz_fastapi.services import gis_mt
from dz_fastapi.services.diadoc_cloud_sign import confirm_cloud_sign_task

pytestmark = pytest.mark.asyncio


async def prepare(session, monkeypatch):
    settings = GisMtSettings(
        product_group="tires", token="test", token_expires_at=now_moscow() + timedelta(hours=1)
    )
    rows = [
        ProductMarkingCode(code="CODE-A", status=Status.RECEIVED),
        ProductMarkingCode(code="CODE-B", status=Status.IN_STOCK),
    ]
    session.add_all([settings, *rows])
    await session.commit()
    signer = AsyncMock()
    signer.cloud_sign.return_value = "sms-token"
    signer.cloud_sign_confirm.return_value = ["signature"]
    client = AsyncMock(spec=GisMtClient)
    client.create_document.return_value = "document-id"
    monkeypatch.setattr(gis_mt, "_require_active_client", lambda _: client)
    task = await gis_mt.start_gis_mt_withdraw_task(
        session,
        diadoc_client=signer,
        environment="test",
        organization_inn="123",
        marking_code_ids=[row.id for row in rows],
        action="OWN_USE",
    )
    return task, rows, signer, client


async def confirm(session, task, signer):
    return await confirm_cloud_sign_task(
        session, task=task, client=signer, confirmation_code="1234"
    )


async def movement_count(session):
    return await session.scalar(select(func.count(ProductMarkingCodeMovement.id)))


async def test_polls_until_confirmed_and_records_each_code_once(test_session, monkeypatch):
    task, rows, signer, client = await prepare(test_session, monkeypatch)
    settings = await gis_mt.get_or_create_gis_mt_settings(test_session)
    settings.product_group = "shoes"
    await test_session.commit()
    await confirm(test_session, task, signer)
    assert task.state == "gis_pending"
    assert all(row.status == Status.WITHDRAWAL_PENDING and row.withdrawn_at is None for row in rows)
    assert client.create_document.await_args.kwargs["product_group"] == "tires"
    assert (await gis_mt.recent_withdrawals(test_session))[0]["document_id"] == "document-id"
    with pytest.raises(ValueError):
        await confirm(test_session, task, signer)
    client.create_document.assert_awaited_once()
    client.withdrawal_info.side_effect = [
        GisMtApiError(503, "unavailable"),
        {"status": "PROCESSING_ERROR"},
        {"status": "IN_PROGRESS"},
        {"status": "CHECKED_OK"},
    ]
    task_id = task.id
    for _ in range(3):
        await gis_mt.poll_withdrawals(test_session)
        assert await movement_count(test_session) == 0
    await gis_mt.poll_withdrawals(test_session)
    await gis_mt.refresh_withdrawal(test_session, task_id)
    assert task.state == "completed"
    for row in rows:
        await test_session.refresh(row)
        assert row.status == Status.WITHDRAWN and row.withdrawn_at
    assert await movement_count(test_session) == 2
    assert client.withdrawal_info.await_count == 4
    client.withdrawal_info.assert_awaited_with("document-id", "tires", content=False)


@pytest.mark.parametrize("reason", ["CHECKED_NOT_OK", "PARSE_ERROR", "http400"])
async def test_rejection_restores_each_previous_status(test_session, monkeypatch, reason):
    task, rows, signer, client = await prepare(test_session, monkeypatch)
    if reason == "http400":
        client.create_document.side_effect = GisMtApiError(400, "rejected")
    await confirm(test_session, task, signer)
    if reason != "http400":
        client.withdrawal_info.return_value = {"status": reason, "errors": ["rejected"]}
        await gis_mt.refresh_withdrawal(test_session, task.id)
    assert task.state == "gis_rejected" and "rejected" in task.error_details
    assert [row.status for row in rows] == [Status.RECEIVED, Status.IN_STOCK]
    assert await movement_count(test_session) == 0


async def test_lost_response_requires_matching_signed_content(test_session, monkeypatch):
    task, rows, signer, client = await prepare(test_session, monkeypatch)
    payload = json.loads(base64.b64decode(task.files[0]["content_b64"]))
    client.create_document.side_effect = httpx.ReadTimeout("timeout")
    await confirm(test_session, task, signer)
    assert task.state == "gis_unknown"
    await gis_mt.poll_withdrawals(test_session)
    client.withdrawal_info.assert_not_called()
    with pytest.raises(ValueError):
        await confirm(test_session, task, signer)
    client.withdrawal_info.return_value = {
        "status": "CHECKED_OK",
        "content": {**payload, "inn": "999"},
    }
    with pytest.raises(ValueError, match="отличается"):
        await gis_mt.refresh_withdrawal(test_session, task.id, "wrong")
    assert task.state == "gis_unknown" and all(
        row.status == Status.WITHDRAWAL_PENDING for row in rows
    )
    client.withdrawal_info.return_value = {"status": "CHECKED_OK", "content": json.dumps(payload)}
    await gis_mt.refresh_withdrawal(test_session, task.id, "right")
    assert task.params["gis_document_id"] == "right" and task.state == "completed"
    client.create_document.assert_awaited_once()
    assert await movement_count(test_session) == 2


async def test_codes_revalidated_before_confirm(test_session, monkeypatch):
    task, rows, signer, client = await prepare(test_session, monkeypatch)
    rows[1].status = Status.SHIPPED
    await test_session.commit()
    with pytest.raises(ValueError, match="отгружен"):
        await confirm(test_session, task, signer)
    assert rows[0].status == Status.RECEIVED
    client.create_document.assert_not_called()
    signer.cloud_sign_confirm.assert_not_called()


async def test_v4_info_selects_only_matching_id_and_type(monkeypatch):
    monkeypatch.delenv("GIS_MT_V4_BASE_URL", raising=False)
    client = GisMtClient(base_url="https://example.test/api/v3/true-api")
    row = {"number": "x/y", "type": "LK_RECEIPT", "status": "CHECKED_OK"}
    mock = AsyncMock(return_value=httpx.Response(200, json=[{**row, "type": "OTHER"}, row]))
    monkeypatch.setattr(client, "_request", mock)
    assert await client.withdrawal_info("x/y", "tires", content=True) == row
    mock.assert_awaited_once_with(
        "GET",
        "/doc/x%2Fy/info",
        base_url="https://example.test/api/v4/true-api",
        params={"pg": "tires", "content": "true"},
    )
    with pytest.raises(GisMtApiError):
        await client.withdrawal_info("other", "tires")


async def test_reservation_visible_during_network_request(test_session, test_engine, monkeypatch):
    task, rows, signer, client = await prepare(test_session, monkeypatch)
    post_started = asyncio.Event()
    finish_post = asyncio.Event()

    async def delayed_create(**kwargs):
        post_started.set()
        await finish_post.wait()
        return "confirmed-id"

    client.create_document.side_effect = delayed_create
    sending = asyncio.create_task(confirm(test_session, task, signer))
    try:
        await asyncio.wait_for(post_started.wait(), timeout=10)
        async with AsyncSession(test_engine, expire_on_commit=False) as other:
            concurrent_task = await other.get(type(task), task.id)
            assert concurrent_task.state == "gis_unknown"
            selected = await other.get(ProductMarkingCode, rows[0].id)
            assert selected.status == Status.WITHDRAWAL_PENDING
            with pytest.raises(ValueError):
                await confirm(other, concurrent_task, signer)
    finally:
        finish_post.set()
        await sending
    assert task.state == "gis_pending"
    signer.cloud_sign_confirm.assert_awaited_once()
    client.create_document.assert_awaited_once()
