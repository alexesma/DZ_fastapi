import asyncio
import logging
import signal
import time

from fastapi import FastAPI

from dz_fastapi.core.db import dispose_engines, get_async_session
from dz_fastapi.services.auth import ensure_admin_user
from dz_fastapi.services.scheduler import start_scheduler

logger = logging.getLogger("dz_fastapi.scheduler_runner")


async def main() -> None:
    session_factory = get_async_session()

    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.is_shutting_down = False
    app.state.started_at = time.time()

    async with session_factory() as session:
        await ensure_admin_user(session)

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
