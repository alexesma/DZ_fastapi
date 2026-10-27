import threading

from scripts.email_relay import relay


def test_recipient_addresses_supports_multiple_recipients():
    assert relay.recipient_addresses(
        "first@example.com, second@example.com; FIRST@example.com"
    ) == ["first@example.com", "second@example.com"]


def test_send_via_smtp_uses_individual_envelope_recipients(monkeypatch):
    captured = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            captured["connection"] = (host, port, timeout)

        def login(self, username, password):
            captured["login"] = (username, password)

        def sendmail(self, from_email, recipients, message):
            captured["sendmail"] = (from_email, recipients, message)

        def quit(self):
            pass

    monkeypatch.setattr(relay.smtplib, "SMTP_SSL", FakeSMTP)
    message = relay.build_message(
        {
            "to_email": "one@example.com,two@example.com",
            "subject": "Price",
            "body_text": "Attached",
        },
        "price@dragonzap.ru",
    )

    relay.send_via_smtp(
        {
            "host": "smtp.yandex.ru",
            "port": 465,
            "username": "price@dragonzap.ru",
            "password": "secret",
            "use_ssl": True,
        },
        "price@dragonzap.ru",
        "one@example.com,two@example.com",
        message,
    )

    assert captured["connection"] == ("smtp.yandex.ru", 465, 60)
    assert captured["sendmail"][0] == "price@dragonzap.ru"
    assert captured["sendmail"][1] == ["one@example.com", "two@example.com"]


def _fake_smtp(monkeypatch, refused):
    class FakeSMTP:
        def __init__(self, host, port, timeout):
            pass

        def login(self, username, password):
            pass

        def sendmail(self, from_email, recipients, message):
            return refused

        def quit(self):
            pass

    monkeypatch.setattr(relay.smtplib, "SMTP_SSL", FakeSMTP)


def test_partial_refusal_is_returned_not_raised(monkeypatch):
    _fake_smtp(monkeypatch, {"two@example.com": (550, b"rejected")})
    message = relay.build_message(
        {"to_email": "one@example.com,two@example.com", "subject": "P"},
        "price@dragonzap.ru",
    )
    refused = relay.send_via_smtp(
        {"password": "secret"},
        "price@dragonzap.ru",
        "one@example.com,two@example.com",
        message,
    )
    assert "two@example.com" in refused


def test_message_id_uses_sender_domain():
    message = relay.build_message(
        {"to_email": "a@example.com", "subject": "P"}, "price@dragonzap.ru"
    )
    assert message["Message-ID"].endswith("@dragonzap.ru>")


class _Client:
    def __init__(self, fail_mark=False):
        self.fail_mark = fail_mark
        self.marked, self.errors = [], []

    def claim(self):
        return [{"id": 7, "to_email": "a@example.com", "subject": "S",
                 "from_email": "price@dragonzap.ru"}]

    def renew(self, outbox_id):
        pass

    def mark_sent(self, outbox_id):
        if self.fail_mark:
            raise ConnectionError("server down")
        self.marked.append(outbox_id)

    def mark_error(self, outbox_id, error, retry=True):
        self.errors.append(outbox_id)


def _config():
    return relay.RelayConfig({
        "api_base_url": "https://x", "relay_api_token": "t",
        "default_smtp": {"password": "p", "save_to_sent": False},
    })


def test_mark_sent_failure_never_requeues_or_resends(monkeypatch):
    sends = []
    monkeypatch.setattr(
        relay, "send_via_smtp", lambda *a, **k: sends.append(a) or {}
    )
    monkeypatch.setattr(relay, "UNCONFIRMED", relay.UnconfirmedSends())
    client = _Client(fail_mark=True)

    assert relay.process_once(client, _config()) == 0
    assert client.errors == [] and len(sends) == 1
    assert "mail:7" in relay.UNCONFIRMED

    # следующий проход только подтверждает, второе письмо не уходит
    client.fail_mark = False
    relay.process_once(client, _config())
    assert len(sends) == 1 and client.marked == [7]
    assert "mail:7" not in relay.UNCONFIRMED


def test_unconfirmed_survives_restart(tmp_path):
    path = tmp_path / "u.json"
    relay.UnconfirmedSends(path).add("mail:1")
    assert "mail:1" in relay.UnconfirmedSends(path)


def test_down_smtp_host_is_tried_once_per_pass(monkeypatch):
    calls = []

    def boom(*a, **k):
        calls.append(1)
        raise TimeoutError("timed out")

    monkeypatch.setattr(relay, "send_via_smtp", boom)
    monkeypatch.setattr(relay, "UNCONFIRMED", relay.UnconfirmedSends())
    client = _Client()
    client.claim = lambda: [
        {"id": i, "to_email": "a@example.com", "subject": "S",
         "from_email": "price@dragonzap.ru"} for i in (1, 2, 3)
    ]
    relay.process_once(client, _config())
    assert len(calls) == 1 and client.errors == [1, 2, 3]


def test_worker_id_is_stable():
    assert ":" not in _config().worker_id


def test_placeholder_tokens_from_example_config_are_rejected():
    import pytest

    with pytest.raises(ValueError, match="relay_api_token"):
        relay.RelayConfig({
            "api_base_url": "https://x",
            "relay_api_token": "СОВПАДАЕТ_С_EMAIL_RELAY_API_TOKEN",
        })
    with pytest.raises(ValueError, match="bot_token"):
        relay.RelayConfig({
            "api_base_url": "https://x",
            "relay_api_token": "abc123",
            "telegram": {"enabled": True, "bot_token": "ТОКЕН_БОТА"},
        })


def test_printing_continues_while_telegram_is_blocked(monkeypatch):
    config = relay.RelayConfig({
        "api_base_url": "https://x",
        "relay_api_token": "t",
        "printing": {"enabled": True},
        "telegram": {"enabled": True, "bot_token": "test-token"},
    })
    stop = threading.Event()
    telegram_entered = threading.Event()
    release_telegram = threading.Event()
    print_polled = threading.Event()
    mail_polled = threading.Event()

    def blocked_telegram(*_args, **_kwargs):
        telegram_entered.set()
        release_telegram.wait(2)

    def poll_print(*_args, **_kwargs):
        if telegram_entered.wait(1):
            print_polled.set()

    def poll_mail(*_args, **_kwargs):
        if telegram_entered.wait(1):
            mail_polled.set()

    monkeypatch.setattr(relay, "ApiClient", lambda *_args: object())
    monkeypatch.setattr(relay, "retry_unconfirmed", lambda *_args, **_kw: None)
    monkeypatch.setattr(relay, "process_telegram_once", blocked_telegram)
    monkeypatch.setattr(relay, "process_print_once", poll_print)
    monkeypatch.setattr(relay, "process_once", poll_mail)

    runner = threading.Thread(
        target=relay.run_forever, args=(config, object(), stop), daemon=True,
    )
    runner.start()
    try:
        assert telegram_entered.wait(2)
        assert print_polled.wait(2)
        assert mail_polled.wait(2)
    finally:
        stop.set()
        release_telegram.set()
        runner.join(3)
    assert not runner.is_alive()
