import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import StockLot
from dz_fastapi.models.partner import SupplierReceipt
from dz_fastapi.models.user import UserRole, UserStatus
from dz_fastapi.services.gtd_report import build_gtd_report
from tests.test_diadoc_api import _create_user


async def _lot(session, autopart, storage, receipt, gtd, initial, remaining):
    session.add(
        StockLot(
            autopart_id=autopart.id,
            storage_location_id=storage.id,
            source_receipt_id=receipt.id,
            gtd_number=gtd,
            country_name='Япония' if gtd else None,
            initial_quantity=initial,
            remaining_quantity=remaining,
            received_at=now_moscow(),
        )
    )


@pytest.mark.asyncio
async def test_gtd_report_totals_and_missing(
    async_client,
    test_session,
    created_providers,
    created_autopart,
    created_storage,
):
    first, second = created_providers[0], created_providers[1]
    r1 = SupplierReceipt(provider_id=first.id, posted_at=now_moscow())
    r2 = SupplierReceipt(provider_id=second.id, posted_at=now_moscow())
    test_session.add_all([r1, r2])
    await test_session.flush()
    gtd = '10702070/120626/0011223'
    await _lot(test_session, created_autopart, created_storage, r1, gtd, 10, 4)
    await _lot(test_session, created_autopart, created_storage, r1, gtd, 5, 5)
    await _lot(test_session, created_autopart, created_storage, r2, None, 7, 7)
    await test_session.commit()

    report = await build_gtd_report(test_session)
    assert len(report['items']) == 1
    row = report['items'][0]
    assert row['gtd_number'] == gtd
    assert (row['received'], row['shipped'], row['remaining']) == (15, 6, 9)
    assert row['provider_id'] == first.id and row['lots'] == 2
    assert report['without_gtd'][0]['provider_id'] == second.id
    assert report['without_gtd'][0]['remaining'] == 7

    only = await build_gtd_report(test_session, gtd_number='0011223')
    assert len(only['items']) == 1
    none = await build_gtd_report(test_session, gtd_number='nope')
    assert none['items'] == []

    await _create_user(
        test_session,
        email='gtd@example.com',
        password='secret123',
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    login = await async_client.post(
        '/auth/login',
        json={'email': 'gtd@example.com', 'password': 'secret123'},
    )
    assert login.status_code == 200
    response = await async_client.get(
        '/inventory/gtd-report', params={'only_remaining': 'true'}
    )
    assert response.status_code == 200, response.text
    assert response.json()['items'][0]['remaining'] == 9
