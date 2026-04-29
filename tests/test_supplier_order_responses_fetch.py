import asyncio
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from dz_fastapi.services import supplier_order_responses as response_service


def test_build_sender_filters_preserves_config_order():
    configs = [
        SimpleNamespace(sender_emails=["otvet@aruda.ru"]),
        SimpleNamespace(sender_emails=["honda315@rambler.ru"]),
        SimpleNamespace(sender_emails=["zakaz@cosmopart.ru"]),
        SimpleNamespace(sender_emails=["avtek3915@yandex.ru"]),
        SimpleNamespace(sender_emails=["otvet@aruda.ru"]),
    ]

    assert response_service._build_sender_filters_from_configs(configs) == [
        "otvet@aruda.ru",
        "honda315@rambler.ru",
        "zakaz@cosmopart.ru",
        "avtek3915@yandex.ru",
    ]


@pytest.mark.asyncio
async def test_fetch_supplier_responses_skips_missing_imap_folder(
    monkeypatch,
):
    class MissingFolderError(Exception):
        pass

    account = SimpleNamespace(
        id=101,
        email="info@dragonzap.ru",
        imap_host="imap.yandex.ru",
        transport="smtp",
        imap_folder="INBOX",
        imap_additional_folders=["MISSING_FOLDER"],
        oauth_provider=None,
        password="secret",
        imap_port=993,
    )
    message = SimpleNamespace(
        uid="5001",
        external_id="ok-message",
        received_at=datetime(2026, 4, 21, 14, 30, 0),
        date=None,
        from_="otvet@aruda.ru",
        subject="Ответ",
    )

    async def fake_get_active_by_purpose(_session, _purpose):
        return [account]

    async def fake_fetch_order_messages(
        host,
        email,
        password,
        folder,
        date_from,
        mark_seen,
        *,
        port,
        ssl,
        from_email=None,
    ):
        del (
            host,
            email,
            password,
            date_from,
            mark_seen,
            port,
            ssl,
            from_email,
        )
        if folder == "MISSING_FOLDER":
            raise MissingFolderError("SELECT No such folder")
        return [message]

    monkeypatch.setattr(
        response_service,
        "MailboxFolderSelectError",
        MissingFolderError,
    )
    monkeypatch.setattr(
        response_service.crud_email_account,
        "get_active_by_purpose",
        fake_get_active_by_purpose,
    )
    monkeypatch.setattr(
        response_service,
        "_fetch_order_messages",
        fake_fetch_order_messages,
    )

    result = await response_service._fetch_supplier_response_messages(
        None,
        date_from=date(2026, 4, 20),
        include_default_orders_out=True,
    )

    assert len(result) == 1
    fetched_message, fetched_account = result[0]
    assert fetched_message.external_id == "ok-message"
    assert fetched_account.id == account.id


@pytest.mark.asyncio
async def test_fetch_supplier_responses_ignores_internal_senders(
    monkeypatch,
):
    account = SimpleNamespace(
        id=102,
        email="masterzapzakaz@gmail.com",
        imap_host="imap.gmail.com",
        transport="smtp",
        imap_folder="INBOX",
        imap_additional_folders=[],
        oauth_provider=None,
        password="secret",
        imap_port=993,
    )
    internal_message = SimpleNamespace(
        uid="6001",
        external_id="internal-message",
        received_at=datetime(2026, 4, 22, 10, 0, 0),
        date=None,
        from_="masterzapzakaz@gmail.com",
        subject="Заказ",
    )
    supplier_message = SimpleNamespace(
        uid="6002",
        external_id="supplier-message",
        received_at=datetime(2026, 4, 22, 10, 5, 0),
        date=None,
        from_="zakaz@cosmopart.ru",
        subject="Re: Заказ",
    )

    async def fake_get_active_by_purpose(_session, _purpose):
        return [account]

    async def fake_fetch_order_messages(
        host,
        email,
        password,
        folder,
        date_from,
        mark_seen,
        *,
        port,
        ssl,
        from_email=None,
    ):
        del (
            host,
            email,
            password,
            folder,
            date_from,
            mark_seen,
            port,
            ssl,
            from_email,
        )
        return [internal_message, supplier_message]

    class _Scalars:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return _Scalars(self._rows)

    class _Session:
        async def execute(self, _stmt):
            return _Result(
                [SimpleNamespace(email="masterzapzakaz@gmail.com", id=999)]
            )

    monkeypatch.setattr(
        response_service.crud_email_account,
        "get_active_by_purpose",
        fake_get_active_by_purpose,
    )
    monkeypatch.setattr(
        response_service,
        "_fetch_order_messages",
        fake_fetch_order_messages,
    )
    monkeypatch.setattr(
        response_service,
        "_SUPPLIER_RESPONSE_IGNORE_INTERNAL_SENDERS",
        True,
    )

    result = await response_service._fetch_supplier_response_messages(
        _Session(),
        date_from=date(2026, 4, 20),
        include_default_orders_out=True,
    )

    assert len(result) == 1
    fetched_message, fetched_account = result[0]
    assert fetched_message.external_id == "supplier-message"
    assert fetched_account.id == account.id


@pytest.mark.asyncio
async def test_fetch_supplier_responses_continues_after_sender_timeout(
    monkeypatch,
):
    account = SimpleNamespace(
        id=103,
        email="masterzapzakaz@gmail.com",
        imap_host="imap.gmail.com",
        transport="smtp",
        imap_folder="INBOX",
        imap_additional_folders=[],
        oauth_provider=None,
        password="secret",
        imap_port=993,
    )
    supplier_message = SimpleNamespace(
        uid="7002",
        external_id="fast-message",
        received_at=datetime(2026, 4, 27, 10, 5, 0),
        date=None,
        from_="fast@example.com",
        subject="Re: Заказ",
    )
    called_filters = []

    async def fake_get_active_by_purpose(_session, _purpose):
        return [account]

    async def fake_fetch_order_messages(
        host,
        email,
        password,
        folder,
        date_from,
        mark_seen,
        *,
        port,
        ssl,
        from_email=None,
    ):
        del (
            host,
            email,
            password,
            folder,
            date_from,
            mark_seen,
            port,
            ssl,
        )
        called_filters.append(from_email)
        if from_email == "slow@example.com":
            raise asyncio.TimeoutError()
        return [supplier_message]

    monkeypatch.setattr(
        response_service.crud_email_account,
        "get_active_by_purpose",
        fake_get_active_by_purpose,
    )
    monkeypatch.setattr(
        response_service,
        "_fetch_order_messages",
        fake_fetch_order_messages,
    )

    result = await response_service._fetch_supplier_response_messages(
        None,
        date_from=date(2026, 4, 27),
        include_default_orders_out=True,
        from_email_filters=["slow@example.com", "fast@example.com"],
    )

    assert called_filters == ["slow@example.com", "fast@example.com"]
    assert len(result) == 1
    fetched_message, fetched_account = result[0]
    assert fetched_message.external_id == "fast-message"
    assert fetched_account.id == account.id


@pytest.mark.asyncio
async def test_fetch_supplier_responses_can_filter_sender_client_side(
    monkeypatch,
):
    account = SimpleNamespace(
        id=104,
        email="info@dragonzap.ru",
        imap_host="imap.yandex.ru",
        transport="smtp",
        imap_folder="INBOX",
        imap_additional_folders=[],
        oauth_provider=None,
        password="secret",
        imap_port=993,
    )
    wanted_message = SimpleNamespace(
        uid="8001",
        external_id="wanted-message",
        received_at=datetime(2026, 4, 29, 5, 9, 19),
        date=None,
        from_="m.syrov@avtoformula.ru",
        subject='ТехноАвто -> ООО "АВТОПАРТС" УПД №т-60429-000005',
    )
    other_message = SimpleNamespace(
        uid="8002",
        external_id="other-message",
        received_at=datetime(2026, 4, 29, 5, 9, 20),
        date=None,
        from_="docs@example.com",
        subject="Другой документ",
    )
    called_filters = []

    async def fake_get_active_by_purpose(_session, _purpose):
        return [account]

    async def fake_fetch_order_messages(
        host,
        email,
        password,
        folder,
        date_from,
        mark_seen,
        *,
        port,
        ssl,
        from_email=None,
    ):
        del host, email, password, folder, date_from, mark_seen, port, ssl
        called_filters.append(from_email)
        return [wanted_message, other_message]

    monkeypatch.setattr(
        response_service.crud_email_account,
        "get_active_by_purpose",
        fake_get_active_by_purpose,
    )
    monkeypatch.setattr(
        response_service,
        "_fetch_order_messages",
        fake_fetch_order_messages,
    )
    monkeypatch.setattr(
        response_service,
        "_SUPPLIER_RESPONSE_IGNORE_INTERNAL_SENDERS",
        False,
    )

    result = await response_service._fetch_supplier_response_messages(
        None,
        date_from=date(2026, 4, 29),
        include_default_orders_out=True,
        from_email_filters=["m.syrov@avtoformula.ru"],
        use_server_side_from_filters=False,
    )

    assert called_filters == [None]
    assert len(result) == 1
    fetched_message, fetched_account = result[0]
    assert fetched_message.external_id == "wanted-message"
    assert fetched_account.id == account.id
