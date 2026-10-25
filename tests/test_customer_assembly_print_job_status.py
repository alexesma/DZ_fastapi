"""Employees can inspect only their own print jobs."""

import pytest

from dz_fastapi.models.printing import PrintDevice, PrintJob
from dz_fastapi.models.user import User, UserRole, UserStatus


@pytest.mark.asyncio
async def test_print_job_status_is_limited_to_its_owner(async_client, test_session):
    users = [
        User(id=number, email=f"printer-{number}@example.com",
             password_hash="unused", role=UserRole.MANAGER,
             status=UserStatus.ACTIVE)
        for number in (1, 2)
    ]
    device = PrintDevice(machine_id="test", system_name="Test printer",
                         display_name="Test printer")
    test_session.add_all([*users, device])
    await test_session.flush()
    own = PrintJob(request_key="own-print-status", user_id=1,
                   device_id=device.id, kind="label", title="Own", html="<p>Own</p>")
    other = PrintJob(request_key="other-print-status", user_id=2,
                     device_id=device.id, kind="label", title="Other", html="<p>Other</p>")
    test_session.add_all([own, other])
    await test_session.commit()

    response = await async_client.get(f"/print-jobs/{own.id}")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    forbidden = await async_client.get(f"/print-jobs/{other.id}")
    assert forbidden.status_code == 404
