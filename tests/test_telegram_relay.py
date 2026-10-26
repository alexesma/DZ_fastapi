import base64

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.partner import TELEGRAM_OUTBOX_STATUS
from dz_fastapi.services.telegram_outbox import (
    claim_pending_telegram_outbox,
    enqueue_telegram_document,
    enqueue_telegram_message,
    mark_telegram_outbox_error,
    mark_telegram_outbox_sent,
    serialize_telegram_outbox_for_relay,
)
from scripts.email_relay.relay import RelayConfig, process_telegram_once


@pytest.mark.asyncio
async def test_telegram_outbox_claim_retry_and_sent(
    test_session: AsyncSession,
):
    row = await enqueue_telegram_message(
        test_session,
        chat_id="-100123",
        text="Проверка очереди",
        parse_mode="HTML",
    )

    claimed = await claim_pending_telegram_outbox(
        test_session,
        worker="test-worker",
        limit=10,
    )
    assert [item.id for item in claimed] == [row.id]
    assert claimed[0].claimed_by == "test-worker"

    failed = await mark_telegram_outbox_error(
        test_session,
        outbox_id=row.id,
        error="temporary timeout",
        retry=True,
    )
    assert failed.status == TELEGRAM_OUTBOX_STATUS.PENDING
    assert failed.attempts == 1
    assert failed.claimed_by is None

    sent = await mark_telegram_outbox_sent(
        test_session,
        outbox_id=row.id,
    )
    assert sent.status == TELEGRAM_OUTBOX_STATUS.SENT
    assert sent.attempts == 2
    assert sent.sent_at is not None


@pytest.mark.asyncio
async def test_telegram_document_is_serialized_and_removed_after_send(
    test_session: AsyncSession,
    monkeypatch,
    tmp_path,
):
    storage = tmp_path / "telegram_outbox"
    monkeypatch.setattr(
        "dz_fastapi.services.telegram_outbox.TELEGRAM_OUTBOX_DIR",
        storage,
    )
    row = await enqueue_telegram_document(
        test_session,
        chat_id="-100123",
        file_bytes=b"xlsx-data",
        file_name="report.xlsx",
        caption="Отчёт",
    )
    file_path = row.document_path

    payload = serialize_telegram_outbox_for_relay(row)
    assert payload["document_error"] is None
    assert base64.b64decode(payload["document_base64"]) == b"xlsx-data"

    await mark_telegram_outbox_sent(test_session, outbox_id=row.id)
    assert not storage.joinpath(file_path.split("/")[-1]).exists()


@pytest.mark.asyncio
async def test_permanent_telegram_error_removes_queued_document(
    test_session: AsyncSession,
    monkeypatch,
    tmp_path,
):
    storage = tmp_path / "telegram_outbox"
    monkeypatch.setattr(
        "dz_fastapi.services.telegram_outbox.TELEGRAM_OUTBOX_DIR",
        storage,
    )
    row = await enqueue_telegram_document(
        test_session,
        chat_id="invalid-chat",
        file_bytes=b"document",
        file_name="report.xlsx",
    )
    file_path = storage / row.document_path.split("/")[-1]

    failed = await mark_telegram_outbox_error(
        test_session,
        outbox_id=row.id,
        error="Bad Request: chat not found",
        retry=False,
    )

    assert failed.status == TELEGRAM_OUTBOX_STATUS.ERROR
    assert failed.document_path is None
    assert not file_path.exists()


def test_local_relay_processes_telegram_queue(monkeypatch):
    config = RelayConfig(
        {
            "api_base_url": "https://example.test/api",
            "relay_api_token": "relay-token",
            "telegram": {
                "enabled": True,
                "bot_token": "bot-token",
            },
        }
    )

    class FakeClient:
        sent_ids = []
        errors = []

        def telegram_claim(self):
            return [
                {
                    "id": 7,
                    "chat_id": "-100123",
                    "text": "Сообщение",
                }
            ]

        def telegram_pending(self):
            return []

        def telegram_mark_sent(self, outbox_id):
            self.sent_ids.append(outbox_id)

        def telegram_mark_error(self, outbox_id, error, retry=True):
            self.errors.append((outbox_id, error, retry))

    delivered = []
    monkeypatch.setattr(
        "scripts.email_relay.relay.send_to_telegram",
        lambda _config, item: delivered.append(item["id"]),
    )
    client = FakeClient()

    assert process_telegram_once(client, config) == 1
    assert delivered == [7]
    assert client.sent_ids == [7]
    assert client.errors == []


@pytest.mark.asyncio
async def test_telegram_network_failure_does_not_burn_attempts(
    test_session: AsyncSession,
):
    row = await enqueue_telegram_message(
        test_session, chat_id="-100123", text="Сеть", parse_mode=None,
    )
    for _ in range(8):
        failed = await mark_telegram_outbox_error(
            test_session,
            outbox_id=row.id,
            error="Нет связи с api.telegram.org (ConnectTimeout): "
                  "Connection timed out",
            retry=True,
        )
    assert failed.status == TELEGRAM_OUTBOX_STATUS.PENDING
    assert failed.attempts == 0


def test_telegram_outage_hides_token_and_pauses(monkeypatch):
    import requests

    from scripts.email_relay import relay

    token = "123456:SECRET-TOKEN"
    config = RelayConfig({
        "api_base_url": "https://example.test/api",
        "relay_api_token": "relay-token",
        "telegram": {"enabled": True, "bot_token": token},
    })

    def boom(*args, **kwargs):
        raise requests.ConnectTimeout(f"HTTPSConnectionPool: /bot{token}/sendMessage")

    monkeypatch.setattr(relay.requests, "post", boom)
    monkeypatch.setattr(relay, "_telegram_down_until", 0.0)

    class FakeClient:
        errors = []
        claims = 0

        def telegram_claim(self):
            self.claims += 1
            return [{"id": 1, "chat_id": "1", "text": "a"},
                    {"id": 2, "chat_id": "1", "text": "b"}]

        def telegram_mark_error(self, outbox_id, error, retry=True):
            self.errors.append((outbox_id, error, retry))

    client = FakeClient()
    assert process_telegram_once(client, config) == 0
    assert [row[0] for row in client.errors] == [1]  # второе не трогаем
    assert token not in client.errors[0][1]
    assert client.errors[0][2] is True
    # пауза: следующий проход даже не забирает сообщения
    assert process_telegram_once(client, config) == 0
    assert client.claims == 1
