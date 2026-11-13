from datetime import date, timedelta

import pytest
from sqlalchemy import func, select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.models.settings import GisMtSettings
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.gis_mt import get_gis_mt_status, remind_gis_mt_login

CODE = "010460000000001721AAAAAAAAAAAAA"


async def _count(session):
    return await session.scalar(select(func.count(AppNotification.id)))


@pytest.mark.asyncio
async def test_reminder_is_sent_once_a_day_when_codes_wait(
    test_session, created_autopart, created_providers
):
    test_session.add(
        User(
            name="Admin",
            email="gis-admin@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    settings_row = GisMtSettings(
        token="expired", token_expires_at=now_moscow() - timedelta(hours=1)
    )
    test_session.add(settings_row)
    await test_session.commit()

    # Токен истёк, но сверять нечего — не беспокоим.
    result = await remind_gis_mt_login(test_session)
    assert result == {
        "reminded": False,
        "pending_receipts": 0,
        "pending_shipments": 0,
        "reason": "nothing_pending",
    }

    receipt = SupplierReceipt(
        provider_id=created_providers[0].id,
        document_number="R-GIS",
        document_date=date.today(),
    )
    receipt.items = [
        SupplierReceiptItem(
            autopart_id=created_autopart.id,
            received_quantity=1,
            marking_codes=[CODE],
        )
    ]
    test_session.add(receipt)
    await test_session.commit()

    result = await remind_gis_mt_login(test_session)
    assert result["reminded"] is True
    assert result["pending_receipts"] == 1
    assert await _count(test_session) == 1

    again = await remind_gis_mt_login(test_session)
    assert again["reason"] == "already_reminded_today"
    assert await _count(test_session) == 1

    status = await get_gis_mt_status(test_session)
    assert status["token_active"] is False
    assert status["pending_receipts"] == 1

    settings_row.token_expires_at = now_moscow() + timedelta(hours=5)
    await test_session.commit()
    assert (await remind_gis_mt_login(test_session))["reason"] == "token_active"


@pytest.mark.asyncio
async def test_no_reminder_before_the_first_login(test_session):
    assert (await remind_gis_mt_login(test_session))["reason"] == "never_logged_in"
