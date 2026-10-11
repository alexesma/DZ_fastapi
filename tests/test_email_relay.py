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


def test_send_via_smtp_raises_when_server_refuses_a_recipient(monkeypatch):
    class FakeSMTP:
        def __init__(self, host, port, timeout):
            pass

        def login(self, username, password):
            pass

        def sendmail(self, from_email, recipients, message):
            return {"two@example.com": (550, b"recipient rejected")}

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

    try:
        relay.send_via_smtp(
            {"password": "secret"},
            "price@dragonzap.ru",
            "one@example.com,two@example.com",
            message,
        )
    except relay.smtplib.SMTPRecipientsRefused as exc:
        assert "two@example.com" in exc.recipients
    else:
        raise AssertionError("refused recipient must fail relay delivery")


class _FakeClient:
    def __init__(self, items):
        self.items = items
        self.calls = []

    def claim(self):
        return self.items

    def renew(self, outbox_id):
        self.calls.append(("renew", outbox_id))

    def mark_error(self, outbox_id, error, retry=True):
        self.calls.append(("mark_error", outbox_id))

    def mark_sent(self, outbox_id):
        self.calls.append(("mark_sent", outbox_id))


class _FakeConfig:
    def smtp_for(self, from_email):
        return {"host": "smtp.example.test", "port": 465, "password": "x"}


def test_unreachable_smtp_leaves_letters_in_queue_without_burning_attempts(monkeypatch):
    items = [
        {"id": 1, "to_email": "a@example.com", "from_email": "p@example.com"},
        {"id": 2, "to_email": "b@example.com", "from_email": "p@example.com"},
    ]
    client = _FakeClient(items)
    probes = []

    def unreachable(host, port, timeout=10.0):
        probes.append(host)
        return False

    def must_not_send(*args, **kwargs):
        raise AssertionError("при недоступном SMTP отправка не должна начинаться")

    monkeypatch.setattr(relay, "smtp_reachable", unreachable)
    monkeypatch.setattr(relay, "send_via_smtp", must_not_send)

    sent = relay.process_once(client, _FakeConfig())

    assert sent == 0
    assert probes == ["smtp.example.test"]  # одна проверка на всю пачку
    assert client.calls == []  # ни renew, ни mark_error


def test_lost_claim_is_skipped_quietly(monkeypatch):
    import requests

    class LostClaimClient(_FakeClient):
        def renew(self, outbox_id):
            response = requests.Response()
            response.status_code = 409
            raise requests.HTTPError("409", response=response)

    item = {"id": 7, "to_email": "a@example.com", "from_email": "p@example.com"}
    client = LostClaimClient([item])
    monkeypatch.setattr(relay, "smtp_reachable", lambda *a, **k: True)

    assert relay.process_once(client, _FakeConfig()) == 0
    assert ("mark_error", 7) not in client.calls
