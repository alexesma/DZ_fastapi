from datetime import date, datetime
from types import SimpleNamespace

import pytest

from dz_fastapi.services import supplier_order_responses as response_service


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
    ):
        del host, email, password, date_from, mark_seen, port, ssl
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
