import pytest

from dz_fastapi import scheduler_runner


class _SessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _SessionFactory:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return _SessionContext(self.session)


@pytest.mark.asyncio
async def test_wait_for_database_retries_timeout_and_recreates_pool(monkeypatch):
    attempts = 0
    disposed = 0
    delays = []

    async def execute_probe(_statement):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TimeoutError("postgres is temporarily busy")

    async def dispose():
        nonlocal disposed
        disposed += 1

    async def sleep(delay):
        delays.append(delay)

    class _ProbeSession:
        execute = staticmethod(execute_probe)

    factory = _SessionFactory(_ProbeSession())
    monkeypatch.setattr(scheduler_runner, "get_async_session", lambda: factory)
    monkeypatch.setattr(scheduler_runner, "dispose_engines", dispose)
    monkeypatch.setattr(scheduler_runner.asyncio, "sleep", sleep)
    monkeypatch.setattr(
        scheduler_runner, "SCHEDULER_DB_STARTUP_ATTEMPTS", 4
    )
    monkeypatch.setattr(
        scheduler_runner, "SCHEDULER_DB_RETRY_DELAY_SECONDS", 2
    )
    monkeypatch.setattr(
        scheduler_runner, "SCHEDULER_DB_RETRY_MAX_DELAY_SECONDS", 3
    )

    async def ensure_admin(_session):
        return None

    monkeypatch.setattr(scheduler_runner, "ensure_admin_user", ensure_admin)

    result = await scheduler_runner.wait_for_database()

    assert result is factory
    assert attempts == 3
    assert disposed == 2
    assert delays == [2, 3]


@pytest.mark.asyncio
async def test_wait_for_database_raises_after_attempt_limit(monkeypatch):
    class _FailingSession:
        async def execute(self, _statement):
            raise TimeoutError("postgres is unavailable")

    factory = _SessionFactory(_FailingSession())
    monkeypatch.setattr(scheduler_runner, "get_async_session", lambda: factory)
    monkeypatch.setattr(
        scheduler_runner, "SCHEDULER_DB_STARTUP_ATTEMPTS", 2
    )
    monkeypatch.setattr(
        scheduler_runner, "SCHEDULER_DB_RETRY_DELAY_SECONDS", 1
    )
    monkeypatch.setattr(
        scheduler_runner, "SCHEDULER_DB_RETRY_MAX_DELAY_SECONDS", 1
    )

    disposed = 0

    async def dispose():
        nonlocal disposed
        disposed += 1

    async def sleep(_delay):
        return None

    monkeypatch.setattr(scheduler_runner, "dispose_engines", dispose)
    monkeypatch.setattr(scheduler_runner.asyncio, "sleep", sleep)

    with pytest.raises(TimeoutError, match="postgres is unavailable"):
        await scheduler_runner.wait_for_database()

    assert disposed == 1
