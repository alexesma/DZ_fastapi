import base64
from types import SimpleNamespace

from dz_fastapi.services.email import (GMAIL_API_SEND_URL,
                                       build_email_delivery_kwargs,
                                       send_email_message,
                                       send_email_with_attachment)


def test_build_email_delivery_kwargs_gmail_api():
    account = SimpleNamespace(
        email='info@gmail.com',
        transport='gmail_api',
        oauth_refresh_token='refresh-token',
    )

    kwargs = build_email_delivery_kwargs(account)

    assert kwargs == {
        'transport': 'gmail_api',
        'from_email': 'info@gmail.com',
        'oauth_refresh_token': 'refresh-token',
    }


def test_build_email_delivery_kwargs_resend_api():
    account = SimpleNamespace(
        email='orders@dragonzap.online',
        transport='resend_api',
        resend_api_key='re_test',
        resend_timeout=30,
    )

    kwargs = build_email_delivery_kwargs(account)

    assert kwargs == {
        'transport': 'resend_api',
        'from_email': 'orders@dragonzap.online',
        'resend_api_key': 're_test',
        'resend_timeout': 30,
    }


def test_send_email_with_attachment_gmail_api(monkeypatch):
    captured = {}

    class _Response:
        status_code = 200

        def raise_for_status(self):
            return None

    def fake_refresh_token(refresh_token):
        captured['refresh_token'] = refresh_token
        return {'access_token': 'access-token'}

    def fake_post(url, json, headers, timeout):
        captured['url'] = url
        captured['json'] = json
        captured['headers'] = headers
        captured['timeout'] = timeout
        return _Response()

    monkeypatch.setattr(
        'dz_fastapi.services.email.refresh_google_access_token_sync',
        fake_refresh_token,
    )
    monkeypatch.setattr('dz_fastapi.services.email.httpx.post', fake_post)

    result = send_email_with_attachment(
        to_email='one@gmail.com; two@gmail.com',
        subject='Test',
        body='Plain body',
        attachment_bytes=b'test-bytes',
        attachment_filename='report.txt',
        transport='gmail_api',
        oauth_refresh_token='refresh-token',
        from_email='info@gmail.com',
    )

    raw_bytes = base64.urlsafe_b64decode(
        captured['json']['raw'] + '=='
    )
    decoded = raw_bytes.decode('utf-8', errors='ignore')

    assert result is True
    assert captured['refresh_token'] == 'refresh-token'
    assert captured['url'] == GMAIL_API_SEND_URL
    assert captured['headers']['Authorization'] == 'Bearer access-token'
    assert captured['timeout'] == 20
    assert 'Subject: Test' in decoded
    assert 'Plain body' in decoded
    assert 'report.txt' in decoded


def test_send_email_with_attachment_gmail_api_fallbacks_to_smtp(monkeypatch):
    calls = []

    def fake_gmail_api(**kwargs):
        calls.append(('gmail_api', kwargs))
        return False

    def fake_smtp(**kwargs):
        calls.append(('smtp', kwargs))
        return True

    monkeypatch.setattr(
        'dz_fastapi.services.email._send_email_via_gmail_api',
        fake_gmail_api,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.email._send_email_via_smtp',
        fake_smtp,
    )

    result = send_email_with_attachment(
        to_email='client@example.com',
        subject='Fallback',
        body='Fallback body',
        attachment_bytes=b'fallback',
        attachment_filename='fallback.txt',
        transport='gmail_api',
        oauth_refresh_token='refresh-token',
        smtp_host='smtp.example.com',
        smtp_user='info@example.com',
        smtp_password='smtp-pass',
        from_email='info@example.com',
    )

    assert result is True
    assert [name for name, _ in calls] == ['gmail_api', 'smtp']


def test_send_email_message_gmail_api_without_attachment(monkeypatch):
    captured = {}

    class _Response:
        status_code = 200

        def raise_for_status(self):
            return None

    def fake_refresh_token(refresh_token):
        captured['refresh_token'] = refresh_token
        return {'access_token': 'access-token'}

    def fake_post(url, json, headers, timeout):
        captured['url'] = url
        captured['json'] = json
        captured['headers'] = headers
        captured['timeout'] = timeout
        return _Response()

    monkeypatch.setattr(
        'dz_fastapi.services.email.refresh_google_access_token_sync',
        fake_refresh_token,
    )
    monkeypatch.setattr('dz_fastapi.services.email.httpx.post', fake_post)

    result = send_email_message(
        to_email='one@gmail.com',
        subject='No attachment',
        body='<b>Hello</b>',
        is_html=True,
        transport='gmail_api',
        oauth_refresh_token='refresh-token',
        from_email='info@gmail.com',
    )

    raw_bytes = base64.urlsafe_b64decode(
        captured['json']['raw'] + '=='
    )
    decoded = raw_bytes.decode('utf-8', errors='ignore')

    assert result is True
    assert captured['url'] == GMAIL_API_SEND_URL
    assert 'No attachment' in decoded
    assert '<b>Hello</b>' in decoded


def test_send_email_with_attachment_resend_api(monkeypatch):
    captured = {}

    def fake_send_resend(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(
        'dz_fastapi.services.email.send_email_via_resend',
        fake_send_resend,
    )

    result = send_email_with_attachment(
        to_email='one@example.com;two@example.com',
        subject='Resend test',
        body='Hello from Resend',
        attachment_bytes=b'binary',
        attachment_filename='report.txt',
        transport='resend_api',
        resend_api_key='re_test',
        resend_timeout=15,
        from_email='orders@dragonzap.online',
    )

    assert result is True
    assert captured['api_key'] == 're_test'
    assert captured['from_email'] == 'orders@dragonzap.online'
    assert captured['to_email'] == ['one@example.com', 'two@example.com']
    assert captured['subject'] == 'Resend test'
    assert captured['attachment_filename'] == 'report.txt'
    assert captured['timeout'] == 15
