from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart, StorageLocation
from dz_fastapi.models.inventory import StockByLocation, StockLot


async def _sbl_qty(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> int:
    stmt = select(StockByLocation).where(
        StockByLocation.autopart_id == autopart_id,
        StockByLocation.storage_location_id == storage_location_id,
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    return int(row.quantity) if row else 0


async def _lot_sum(
    session: AsyncSession,
    *,
    autopart_id: int,
    storage_location_id: int,
) -> int:
    stmt = select(
        func.coalesce(func.sum(StockLot.remaining_quantity), 0)
    ).where(
        StockLot.autopart_id == autopart_id,
        StockLot.storage_location_id == storage_location_id,
    )
    return int((await session.execute(stmt)).scalar_one())


@pytest.mark.asyncio
async def test_api_manual_movement_returns_lot_trace_and_updates_stock(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    print("created_autopart =", created_autopart)
    print("created_storage =", created_storage)
    payload = {
        'autopart_id': created_autopart.id,
        'storage_location_id': created_storage.id,
        'movement_type': 'manual',
        'quantity': 5,
        'notes': 'api manual in',
        'operation_uid': 'op-manual-in-1',
    }
    response = await async_client.post('/inventory/movements/', json=payload)
    assert response.status_code == 201, response.text
    movement = response.json()
    assert movement['stock_lot_id'] is not None
    assert movement['lot_source_type'] == 'manual'
    assert movement['operation_uid'] == 'op-manual-in-1'

    list_response = await async_client.get(
        '/inventory/movements/',
        params={'autopart_id': created_autopart.id, 'movement_type': 'manual'},
    )
    assert list_response.status_code == 200, list_response.text
    movements = list_response.json()
    assert any(m['id'] == movement['id'] for m in movements)
    assert any(m['stock_lot_id'] is not None for m in movements)

    assert await _sbl_qty(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
    ) == 5
    assert await _lot_sum(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
    ) == 5


@pytest.mark.asyncio
async def test_api_inventory_complete_shortage_creates_fifo_inventory_movement(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    # seed stock with two lots using API movements
    for qty in (3, 2):
        seed_resp = await async_client.post(
            '/inventory/movements/',
            json={
                'autopart_id': created_autopart.id,
                'storage_location_id': created_storage.id,
                'movement_type': 'manual',
                'quantity': qty,
            },
        )
        assert seed_resp.status_code == 201, seed_resp.text

    start_resp = await async_client.post(
        '/inventory/sessions/',
        json={'name': 'INV shortage API', 'scope_type': 'full'},
    )
    assert start_resp.status_code == 201, start_resp.text
    inv_data = start_resp.json()
    session_id = inv_data['id']
    assert len(inv_data['items']) >= 1
    item = next(
        it for it in inv_data['items']
        if it['autopart_id'] == created_autopart.id
        and it['storage_location_id'] == created_storage.id
    )

    count_resp = await async_client.patch(
        f"/inventory/sessions/{session_id}/items/{item['id']}/",
        json={'actual_qty': 1},
    )
    assert count_resp.status_code == 200, count_resp.text

    complete_resp = await async_client.post(
        f'/inventory/sessions/{session_id}/complete/',
        json={'apply_adjustments': True},
    )
    assert complete_resp.status_code == 200, complete_resp.text
    assert complete_resp.json()['status'] == 'completed'

    mv_resp = await async_client.get(
        '/inventory/movements/',
        params={
            'autopart_id': created_autopart.id,
            'storage_location_id': created_storage.id,
            'movement_type': 'inventory',
        },
    )
    assert mv_resp.status_code == 200, mv_resp.text
    inv_movements = mv_resp.json()
    assert inv_movements, 'Expected inventory movements after shortage'
    assert any(m['quantity'] < 0 for m in inv_movements)
    assert any(m['stock_lot_id'] is not None for m in inv_movements)

    assert await _sbl_qty(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
    ) == 1
    assert await _lot_sum(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
    ) == 1


@pytest.mark.asyncio
async def test_api_inventory_complete_surplus_creates_correction_lot_trace(
    async_client: AsyncClient,
    test_session: AsyncSession,
    created_autopart: AutoPart,
    created_storage: StorageLocation,
):
    # Session includes only non-zero rows, so seed 1 item first.
    seed_resp = await async_client.post(
        '/inventory/movements/',
        json={
            'autopart_id': created_autopart.id,
            'storage_location_id': created_storage.id,
            'movement_type': 'manual',
            'quantity': 1,
        },
    )
    assert seed_resp.status_code == 201, seed_resp.text

    start_resp = await async_client.post(
        '/inventory/sessions/',
        json={'name': 'INV surplus API', 'scope_type': 'full'}
    )
    assert start_resp.status_code == 201, start_resp.text
    inv_data = start_resp.json()
    session_id = inv_data['id']
    item = next(
        it for it in inv_data['items']
        if it['autopart_id'] == created_autopart.id
        and it['storage_location_id'] == created_storage.id
    )

    count_resp = await async_client.patch(
        f"/inventory/sessions/{session_id}/items/{item['id']}/",
        json={'actual_qty': 4},
    )
    assert count_resp.status_code == 200, count_resp.text

    complete_resp = await async_client.post(
        f'/inventory/sessions/{session_id}/complete/',
        json={'apply_adjustments': True},
    )
    assert complete_resp.status_code == 200, complete_resp.text

    mv_resp = await async_client.get(
        '/inventory/movements/',
        params={
            'autopart_id': created_autopart.id,
            'storage_location_id': created_storage.id,
            'movement_type': 'inventory',
        },
    )
    assert mv_resp.status_code == 200, mv_resp.text
    inv_movements = mv_resp.json()
    assert inv_movements, 'Expected inventory movement after surplus'
    assert any(m['quantity'] > 0 for m in inv_movements)
    assert any(
        m['lot_source_type'] == 'inventory_correction' for m in inv_movements
    )

    assert await _sbl_qty(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
    ) == 4
    assert await _lot_sum(
        test_session,
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
    ) == 4
