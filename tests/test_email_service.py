import base64
from types import SimpleNamespace

from dz_fastapi.services.email import (HTTP_API_DEFAULT_URLS,
                                       build_email_delivery_kwargs,
                                       send_email_with_attachment)


def test_build_email_delivery_kwargs_http_api():
    account = SimpleNamespace(
        email='info@example.com',
        transport='http_api',
        http_api_provider='resend',
        http_api_url=None,
        http_api_key='api-key',
        http_api_timeout=30,
    )

    kwargs = build_email_delivery_kwargs(account)

    assert kwargs == {
        'transport': 'http_api',
        'from_email': 'info@example.com',
        'http_api_provider': 'resend',
        'http_api_url': None,
        'http_api_key': 'api-key',
        'http_api_timeout': 30,
    }


def test_send_email_with_attachment_resend_http_api(monkeypatch):
    captured = {}

    class _Response:
        status_code = 202

        def raise_for_status(self):
            return None

    def fake_post(url, json, headers, timeout):
        captured['url'] = url
        captured['json'] = json
        captured['headers'] = headers
        captured['timeout'] = timeout
        return _Response()

    monkeypatch.setattr('dz_fastapi.services.email.httpx.post', fake_post)

    send_email_with_attachment(
        to_email='one@example.com; two@example.com',
        subject='Test',
        body='Plain body',
        attachment_bytes=b'test-bytes',
        attachment_filename='report.xlsx',
        transport='http_api',
        http_api_provider='resend',
        http_api_key='resend-key',
        from_email='info@example.com',
    )

    assert captured['url'] == HTTP_API_DEFAULT_URLS['resend']
    assert captured['headers']['Authorization'] == 'Bearer resend-key'
    assert captured['timeout'] == 20
    assert captured['json']['from'] == 'info@example.com'
    assert captured['json']['to'] == ['one@example.com', 'two@example.com']
    assert captured['json']['text'] == 'Plain body'
    assert (
        captured['json']['attachments'][0]['content']
        == base64.b64encode(b'test-bytes').decode('ascii')
    )


def test_send_email_with_attachment_brevo_http_api(monkeypatch):
    captured = {}

    class _Response:
        status_code = 201

        def raise_for_status(self):
            return None

    def fake_post(url, json, headers, timeout):
        captured['url'] = url
        captured['json'] = json
        captured['headers'] = headers
        captured['timeout'] = timeout
        return _Response()

    monkeypatch.setattr('dz_fastapi.services.email.httpx.post', fake_post)

    send_email_with_attachment(
        to_email='client@example.com',
        subject='HTML Test',
        body='<b>Hello</b>',
        attachment_bytes=b'xlsx-content',
        attachment_filename='prices.xlsx',
        is_html=True,
        transport='http_api',
        http_api_provider='brevo',
        http_api_key='brevo-key',
        http_api_timeout=45,
        from_email='sales@example.com',
    )

    assert captured['url'] == HTTP_API_DEFAULT_URLS['brevo']
    assert captured['headers']['api-key'] == 'brevo-key'
    assert captured['timeout'] == 45
    assert captured['json']['sender'] == {'email': 'sales@example.com'}
    assert captured['json']['to'] == [{'email': 'client@example.com'}]
    assert captured['json']['htmlContent'] == '<b>Hello</b>'
    assert captured['json']['attachment'][0]['name'] == 'prices.xlsx'


def test_send_email_with_attachment_fallbacks_to_smtp(monkeypatch):
    calls = []

    def fake_http_api(**kwargs):
        calls.append(('http_api', kwargs))
        return False

    def fake_smtp(**kwargs):
        calls.append(('smtp', kwargs))
        return True

    monkeypatch.setattr(
        'dz_fastapi.services.email._send_email_via_http_api',
        fake_http_api,
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
        attachment_filename='fallback.xlsx',
        transport='http_api',
        http_api_provider='resend',
        http_api_key='resend-key',
        smtp_host='smtp.example.com',
        smtp_user='info@example.com',
        smtp_password='smtp-pass',
        from_email='info@example.com',
    )

    assert result is True
    assert [name for name, _ in calls] == ['http_api', 'smtp']
