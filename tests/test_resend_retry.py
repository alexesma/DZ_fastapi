"""Повторы при временных отказах Resend.

На рассылке прайсов повторялось «HTTP 408 Operation timed out. Please try
again later»: одинаковые по объёму прайсы то уходили, то нет. Повтор в
коде был, но срабатывал только на клиентском таймауте, а ответ 408 от
самого Resend приходил как HTTPStatusError и уходил в ветку без повтора —
хотя сервер прямым текстом просил повторить.
"""
import httpx
import pytest

from dz_fastapi.services import resend_api


def _response(status: int) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        text='{"message": "Operation timed out"}',
        request=httpx.Request('POST', 'https://api.resend.com/emails'),
    )


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Паузы между попытками в тестах не выдерживаем."""
    monkeypatch.setattr(resend_api.time, 'sleep', lambda _: None)


def _send(monkeypatch, ответы, *, idempotency_key='pricelist/1'):
    """Прогоняет отправку, подсовывая заданную череду ответов."""
    попытки = {'n': 0}

    def fake_post(*args, **kwargs):
        попытки['n'] += 1
        следующий = ответы[min(попытки['n'] - 1, len(ответы) - 1)]
        if isinstance(следующий, Exception):
            raise следующий
        return следующий

    monkeypatch.setattr(resend_api.httpx, 'post', fake_post)
    результат = resend_api.send_email_via_resend(
        api_key='key',
        from_email='price@dragonzap.online',
        to_email=['client@example.com'],
        subject='Прайс',
        body='текст',
        is_html=False,
        idempotency_key=idempotency_key,
    )
    return результат, попытки['n']


def test_retries_on_408_and_succeeds(monkeypatch):
    """Тот самый случай: сервер просит повторить — повторяем."""
    ok, попыток = _send(monkeypatch, [_response(408), _response(200)])
    assert ok is True
    assert попыток == 2


@pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
def test_retries_on_other_transient_statuses(monkeypatch, status):
    ok, попыток = _send(monkeypatch, [_response(status), _response(200)])
    assert ok is True
    assert попыток == 2


def test_gives_up_after_three_attempts(monkeypatch):
    ok, попыток = _send(monkeypatch, [_response(408)])
    assert ok is False
    assert попыток == resend_api.RESEND_SEND_ATTEMPTS


def test_no_retry_without_idempotency_key(monkeypatch):
    """Без ключа повтор запрещён: первый запрос мог дойти, и клиент
    получил бы прайс дважды."""
    ok, попыток = _send(
        monkeypatch, [_response(408), _response(200)], idempotency_key=None
    )
    assert ok is False
    assert попыток == 1


def test_permanent_error_is_not_retried(monkeypatch):
    """422 — неверный адрес или домен: повторять бессмысленно."""
    ok, попыток = _send(monkeypatch, [_response(422), _response(200)])
    assert ok is False
    assert попыток == 1


def test_retries_on_client_timeout(monkeypatch):
    """Прежнее поведение сохраняем: клиентский таймаут тоже повторяем."""
    таймаут = httpx.ReadTimeout('slow')
    ok, попыток = _send(monkeypatch, [таймаут, _response(200)])
    assert ok is True
    assert попыток == 2


def test_success_on_first_try_sends_once(monkeypatch):
    ok, попыток = _send(monkeypatch, [_response(200)])
    assert ok is True
    assert попыток == 1
