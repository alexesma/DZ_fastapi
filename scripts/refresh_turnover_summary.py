"""Rebuild the turnover and purchasing snapshot from the command line."""

import asyncio
import logging

from dz_fastapi.analytics.turnover import refresh_turnover_summary
from dz_fastapi.core.base import Base  # noqa: F401
from dz_fastapi.core.db import get_async_session

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


async def main() -> None:
    session_factory = get_async_session()
    async with session_factory() as session:
        updated_rows = await refresh_turnover_summary(session)
    print(f"Turnover summary updated: {updated_rows} rows")


if __name__ == "__main__":
    asyncio.run(main())
