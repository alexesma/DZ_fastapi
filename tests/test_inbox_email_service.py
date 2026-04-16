import os
import time
from types import SimpleNamespace

import pytest

from dz_fastapi.services import inbox_email as inbox_email_service


def test_cleanup_orphan_inbox_attachment_files_sync(tmp_path):
    base = tmp_path / "uploads" / "inbox_attachments" / "3" / "20260416"
    base.mkdir(parents=True, exist_ok=True)

    referenced_file = base / "referenced.xlsx"
    orphan_old_file = base / "orphan_old.xlsx"
    orphan_new_file = base / "orphan_new.xlsx"
    for file_path in (referenced_file, orphan_old_file, orphan_new_file):
        file_path.write_bytes(b"test")

    now_ts = time.time()
    cutoff_ts = now_ts - 7 * 24 * 60 * 60
    old_ts = cutoff_ts - 60
    new_ts = cutoff_ts + 60
    os.utime(orphan_old_file, (old_ts, old_ts))
    os.utime(orphan_new_file, (new_ts, new_ts))
    os.utime(referenced_file, (old_ts, old_ts))

    removed_files, removed_dirs = (
        inbox_email_service._cleanup_orphan_inbox_attachment_files_sync(
            root_dir=str(tmp_path / "uploads" / "inbox_attachments"),
            referenced_paths={os.path.realpath(referenced_file)},
            cutoff_ts=cutoff_ts,
        )
    )

    assert removed_files == 1
    assert removed_dirs == 0
    assert referenced_file.exists()
    assert not orphan_old_file.exists()
    assert orphan_new_file.exists()


@pytest.mark.asyncio
async def test_process_customer_order_returns_missing_config_status(
    monkeypatch,
):
    notifications = []

    async def fake_find_matching_configs(*_args, **_kwargs):
        return []

    async def fake_create_admin_notifications(**kwargs):
        notifications.append(kwargs)

    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_customer_order_configs',
        fake_find_matching_configs,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.notifications.create_admin_notifications',
        fake_create_admin_notifications,
    )

    inbox_email = SimpleNamespace(
        from_email='orders@example.com',
        subject='Заказ #123',
        email_account_id=1,
        attachment_info=[],
    )

    result, error = await inbox_email_service._process_customer_order(
        session=None,
        inbox_email=inbox_email,
    )

    assert error is None
    assert result is not None
    assert result['status'] == 'missing_config'
    assert 'Не найден активный конфиг заказа клиента' in result['reason']
    assert notifications
    assert notifications[0]['level'] == 'warning'


@pytest.mark.asyncio
async def test_process_customer_order_returns_queued_with_matched_config_ids(
    monkeypatch,
):
    notifications = []

    async def fake_find_matching_configs(*_args, **_kwargs):
        return [7, 9]

    async def fake_create_admin_notifications(**kwargs):
        notifications.append(kwargs)

    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_customer_order_configs',
        fake_find_matching_configs,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.notifications.create_admin_notifications',
        fake_create_admin_notifications,
    )

    inbox_email = SimpleNamespace(
        from_email='orders@example.com',
        subject='Заказ #124',
        email_account_id=1,
        attachment_info=[{'name': 'order.xlsx'}],
    )

    result, error = await inbox_email_service._process_customer_order(
        session=None,
        inbox_email=inbox_email,
    )

    assert error is None
    assert result is not None
    assert result['status'] == 'queued'
    assert result['matched_config_ids'] == [7, 9]
    assert notifications
    assert notifications[0]['level'] == 'info'
