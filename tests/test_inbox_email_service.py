import os
import time
from datetime import datetime
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
async def test_restore_inbox_email_attachments_from_source(monkeypatch):
    class DummySession:
        def __init__(self):
            self.added = []
            self.commits = 0

        def add(self, obj):
            self.added.append(obj)

        async def commit(self):
            self.commits += 1

    session = DummySession()
    inbox_email = SimpleNamespace(
        id=15,
        uid='196146',
        email_account_id=3,
        folder='INBOX',
        has_attachments=False,
        attachment_info=[],
        fetched_at=None,
    )
    account = SimpleNamespace(
        id=3,
        transport='smtp',
        imap_host='imap.gmail.com',
        imap_port=993,
        imap_folder='INBOX',
        email='orders@example.com',
        password='secret',
    )

    async def fake_get_account(_session, _account_id):
        return account

    def fake_fetch_by_uid(**_kwargs):
        return SimpleNamespace(
            attachments=[SimpleNamespace(filename='order.xlsx', payload=b'1')]
        )

    async def fake_build_attachment_info(_msg, *, account_id):
        assert account_id == 3
        return [
            {
                'name': 'order.xlsx',
                'size': 1,
                'path': 'uploads/inbox_attachments/3/20260416/order.xlsx',
            }
        ]

    monkeypatch.setattr(
        inbox_email_service.crud_email_account,
        'get',
        fake_get_account,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_fetch_inbox_message_by_uid_imap_sync',
        fake_fetch_by_uid,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_build_attachment_info_for_message',
        fake_build_attachment_info,
    )

    restored = await (
        inbox_email_service.restore_inbox_email_attachments_from_source(
            session,
            inbox_email=inbox_email,
        )
    )

    assert restored is True
    assert inbox_email.has_attachments is True
    assert inbox_email.attachment_info
    assert session.commits == 1


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


@pytest.mark.asyncio
async def test_force_process_email_triggers_customer_order_configs(
    monkeypatch,
):
    class DummySession:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0
            self.refreshed = []

        async def commit(self):
            self.commits += 1

        async def rollback(self):
            self.rollbacks += 1

        async def refresh(self, obj):
            self.refreshed.append(obj)

    session = DummySession()
    inbox_email = SimpleNamespace(
        id=101,
        rule_type='customer_order',
        from_email='orders@example.com',
        subject='Заказ #501',
        email_account_id=3,
        received_at=datetime(2026, 4, 18, 10, 30, 0),
        processed=False,
        processing_result=None,
        processing_error=None,
    )
    called_configs = []
    audit_calls = []

    async def fake_get_inbox_email(_session, _email_id):
        return inbox_email

    async def fake_find_configs(*_args, **_kwargs):
        return [11, 12]

    async def fake_process_customer_orders(_session, **kwargs):
        called_configs.append(kwargs.get('config_id'))

    async def fake_mark_processed(_session, *, email, result=None, error=None):
        email.processed = True
        email.processing_result = result
        email.processing_error = error
        return email

    async def fake_create_audit(*_args, **kwargs):
        audit_calls.append(kwargs)
        return SimpleNamespace(id=501, details=kwargs.get('details'))

    monkeypatch.setattr(
        inbox_email_service,
        'get_inbox_email',
        fake_get_inbox_email,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_customer_order_configs',
        fake_find_configs,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.customer_orders.process_customer_orders',
        fake_process_customer_orders,
    )
    monkeypatch.setattr(
        inbox_email_service,
        'mark_processed',
        fake_mark_processed,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_create_force_process_audit_record',
        fake_create_audit,
    )

    result = await inbox_email_service.force_process_email(
        session,
        email_id=101,
        user_id=77,
        allow_reprocess=True,
    )

    assert called_configs == [11, 12]
    assert result['processed'] is True
    assert result['processing_error'] is None
    assert result['processing_result']['status'] == 'triggered'
    assert result['processing_result']['reason_code'] == 'triggered'
    assert result['processing_result']['triggered_config_ids'] == [11, 12]
    assert result['processing_result']['summary']['matched_configs_count'] == 2
    assert (
        result['processing_result']['summary']['triggered_configs_count'] == 2
    )
    assert result['processing_result']['audit_id'] == 501
    assert audit_calls and audit_calls[0]['status'] == 'triggered'
    assert session.commits == 1
    assert session.rollbacks == 0
    assert session.refreshed == [inbox_email]


@pytest.mark.asyncio
async def test_force_process_email_triggers_supplier_response_retry(
    monkeypatch,
):
    class DummySession:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0
            self.refreshed = []

        async def commit(self):
            self.commits += 1

        async def rollback(self):
            self.rollbacks += 1

        async def refresh(self, obj):
            self.refreshed.append(obj)

    session = DummySession()
    inbox_email = SimpleNamespace(
        id=201,
        rule_type='order_reply',
        from_email='supplier@example.com',
        subject='Ответ на заказ',
        email_account_id=3,
        folder='INBOX',
        uid='999',
        received_at=datetime(2026, 4, 18, 12, 5, 0),
        processed=False,
        processing_result=None,
        processing_error=None,
    )
    called = []
    audit_calls = []

    async def fake_get_inbox_email(_session, _email_id):
        return inbox_email

    async def fake_find_matching_configs(*_args, **_kwargs):
        return [SimpleNamespace(id=31, provider_id=919)]

    async def fake_reset_markers(*_args, **_kwargs):
        return 1

    async def fake_process_supplier_response_messages(
        session=None,
        provider_id=None,
        supplier_response_config_id=None,
        date_from=None,
        date_to=None,
    ):
        called.append(
            {
                'provider_id': provider_id,
                'config_id': supplier_response_config_id,
                'date_from': date_from,
                'date_to': date_to,
            }
        )
        return {
            'processed_messages': 1,
            'matched_orders': 1,
            'skipped_messages': 0,
        }

    async def fake_mark_processed(_session, *, email, result=None, error=None):
        email.processed = True
        email.processing_result = result
        email.processing_error = error
        return email

    async def fake_create_audit(*_args, **kwargs):
        audit_calls.append(kwargs)
        return SimpleNamespace(id=777, details=kwargs.get('details'))

    monkeypatch.setattr(
        inbox_email_service,
        'get_inbox_email',
        fake_get_inbox_email,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_supplier_response_configs',
        fake_find_matching_configs,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_reset_supplier_message_source_markers',
        fake_reset_markers,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.supplier_order_responses.'
        'process_supplier_response_messages',
        fake_process_supplier_response_messages,
    )
    monkeypatch.setattr(
        inbox_email_service,
        'mark_processed',
        fake_mark_processed,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_create_force_process_audit_record',
        fake_create_audit,
    )

    result = await inbox_email_service.force_process_email(
        session,
        email_id=201,
        user_id=77,
        allow_reprocess=True,
    )

    assert result['processed'] is True
    assert result['processing_error'] is None
    assert result['processing_result']['status'] == 'triggered'
    assert result['processing_result']['reason_code'] == 'triggered'
    assert result['processing_result']['triggered_config_ids'] == [31]
    assert result['processing_result']['reprocess_reset_messages'] == 1
    assert (
        result['processing_result']['summary']['reprocess_reset_messages']
        == 1
    )
    assert result['processing_result']['audit_id'] == 777
    assert audit_calls and audit_calls[0]['status'] == 'triggered'
    assert called and called[0]['provider_id'] == 919
    assert called[0]['config_id'] == 31
    assert str(called[0]['date_from']) == '2026-04-18'
    assert session.commits == 2
    assert session.rollbacks == 0
    assert session.refreshed == [inbox_email]


@pytest.mark.asyncio
async def test_force_process_email_rejects_unsupported_rule(monkeypatch):
    async def fake_get_inbox_email(_session, _email_id):
        return SimpleNamespace(id=5, rule_type='price_list')

    monkeypatch.setattr(
        inbox_email_service,
        'get_inbox_email',
        fake_get_inbox_email,
    )

    with pytest.raises(ValueError):
        await inbox_email_service.force_process_email(
            session=SimpleNamespace(),
            email_id=5,
            user_id=1,
            allow_reprocess=True,
        )


@pytest.mark.asyncio
async def test_force_process_email_returns_missing_config_reason(monkeypatch):
    class DummySession:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0

        async def commit(self):
            self.commits += 1

        async def rollback(self):
            self.rollbacks += 1

        async def refresh(self, _obj):
            return None

    session = DummySession()
    inbox_email = SimpleNamespace(
        id=301,
        rule_type='customer_order',
        from_email='orders@example.com',
        subject='Заказ #999',
        email_account_id=3,
        processed=False,
        processing_result=None,
        processing_error=None,
    )

    async def fake_get_inbox_email(_session, _email_id):
        return inbox_email

    async def fake_find_configs(*_args, **_kwargs):
        return []

    async def fake_mark_processed(_session, *, email, result=None, error=None):
        email.processed = True
        email.processing_result = result
        email.processing_error = error
        return email

    async def fake_create_audit(*_args, **_kwargs):
        return SimpleNamespace(id=998, details={})

    monkeypatch.setattr(
        inbox_email_service,
        'get_inbox_email',
        fake_get_inbox_email,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_find_matching_customer_order_configs',
        fake_find_configs,
    )
    monkeypatch.setattr(
        inbox_email_service,
        'mark_processed',
        fake_mark_processed,
    )
    monkeypatch.setattr(
        inbox_email_service,
        '_create_force_process_audit_record',
        fake_create_audit,
    )

    result = await inbox_email_service.force_process_email(
        session=session,
        email_id=301,
        user_id=55,
        allow_reprocess=False,
    )

    assert result['processing_result']['status'] == 'missing_config'
    assert result['processing_result']['reason_code'] == 'missing_config'
    assert 'Не найден активный CustomerOrderConfig' in (
        result['processing_result']['reason']
    )
    assert result['processing_result']['mode'] == 'check'
    assert result['processing_result']['summary']['matched_configs_count'] == 0
    assert session.commits == 1
