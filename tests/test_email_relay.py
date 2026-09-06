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
