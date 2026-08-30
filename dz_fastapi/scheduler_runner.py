import asyncio
import logging
import os
import signal
import time

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from dz_fastapi.core.db import dispose_engines, get_async_session
from dz_fastapi.services.auth import ensure_admin_user
from dz_fastapi.services.scheduler import start_scheduler

logger = logging.getLogger("dz_fastapi.scheduler_runner")
SCHEDULER_STARTUP_DELAY_SECONDS = max(
    0,
    int(os.getenv("SCHEDULER_STARTUP_DELAY_SECONDS", "20")),
)
SCHEDULER_DB_STARTUP_ATTEMPTS = max(
    1,
    int(os.getenv("SCHEDULER_DB_STARTUP_ATTEMPTS", "8")),
)
SCHEDULER_DB_RETRY_DELAY_SECONDS = max(
    1,
    int(os.getenv("SCHEDULER_DB_RETRY_DELAY_SECONDS", "5")),
)
SCHEDULER_DB_RETRY_MAX_DELAY_SECONDS = max(
    SCHEDULER_DB_RETRY_DELAY_SECONDS,
    int(os.getenv("SCHEDULER_DB_RETRY_MAX_DELAY_SECONDS", "30")),
)


async def wait_for_database():
    """Дожидается PostgreSQL и возвращает рабочую фабрику сессий.

    ``depends_on: service_healthy`` проверяется только при создании
    контейнера. PostgreSQL может быть временно недоступен позже, например
    после перезапуска или тяжёлой транзакции, поэтому scheduler не должен
    завершаться после единственного тайм-аута подключения.
    """
    for attempt in range(1, SCHEDULER_DB_STARTUP_ATTEMPTS + 1):
        try:
            session_factory = get_async_session()
            async with session_factory() as session:
                await session.execute(text("SELECT 1"))
                await ensure_admin_user(session)
            if attempt > 1:
                logger.info(
                    "Database connection restored on scheduler startup "
                    "attempt %s/%s",
                    attempt,
                    SCHEDULER_DB_STARTUP_ATTEMPTS,
                )
            return session_factory
        except (TimeoutError, OSError, SQLAlchemyError) as error:
            if attempt >= SCHEDULER_DB_STARTUP_ATTEMPTS:
                logger.exception(
                    "Database is unavailable after %s scheduler startup "
                    "attempts",
                    SCHEDULER_DB_STARTUP_ATTEMPTS,
                )
                raise
            delay = min(
                SCHEDULER_DB_RETRY_DELAY_SECONDS * (2 ** (attempt - 1)),
                SCHEDULER_DB_RETRY_MAX_DELAY_SECONDS,
            )
            logger.warning(
                "Database connection failed on scheduler startup "
                "attempt %s/%s (%s: %s). Retrying in %s seconds",
                attempt,
                SCHEDULER_DB_STARTUP_ATTEMPTS,
                type(error).__name__,
                error,
                delay,
            )
            await dispose_engines()
            await asyncio.sleep(delay)

    raise RuntimeError("Database startup retry loop finished unexpectedly")


async def main() -> None:
    if SCHEDULER_STARTUP_DELAY_SECONDS > 0:
        logger.info(
            "Delaying standalone scheduler startup for %s seconds",
            SCHEDULER_STARTUP_DELAY_SECONDS,
        )
        await asyncio.sleep(SCHEDULER_STARTUP_DELAY_SECONDS)

    session_factory = await wait_for_database()

    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.is_shutting_down = False
    app.state.started_at = time.time()

    scheduler = start_scheduler(app)
    app.state.scheduler = scheduler
    logger.info("Scheduler started in standalone mode (without HTTP server).")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        if not stop_event.is_set():
            stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _request_stop)
        loop.add_signal_handler(signal.SIGINT, _request_stop)
    except NotImplementedError:
        logger.warning("Signal handlers are not supported in this environment.")

    await stop_event.wait()

    logger.info("Shutting down standalone scheduler...")
    app.state.is_shutting_down = True
    scheduler.shutdown(wait=True)
    await dispose_engines()


if __name__ == "__main__":
    asyncio.run(main())
