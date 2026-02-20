import asyncio
import logging

from sqlalchemy import text

from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.utils import normalize_mixed_cyrillic

logger = logging.getLogger('dz_fastapi')
logging.basicConfig(level=logging.INFO)


async def normalize_batch(limit: int, offset: int) -> tuple[int, int]:
    session_factory = get_async_session()
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT id, name FROM autopart "
                "ORDER BY id ASC "
                "LIMIT :limit OFFSET :offset"
            ),
            {"limit": limit, "offset": offset},
        )
        rows = result.fetchall()
        if not rows:
            return 0, 0

        updated = 0
        total = 0
        for row in rows:
            total += 1
            name = row.name
            if not name:
                continue
            normalized = normalize_mixed_cyrillic(name)
            if normalized != name:
                await session.execute(
                    text("UPDATE autopart SET name = :name WHERE id = :id"),
                    {"name": normalized, "id": row.id},
                )
                updated += 1
        await session.commit()
        return total, updated


async def main(batch_size: int = 500):
    offset = 0
    total_rows = 0
    updated_rows = 0

    while True:
        total, updated = await normalize_batch(batch_size, offset)
        if total == 0:
            break
        total_rows += total
        updated_rows += updated
        offset += batch_size
        logger.info('Processed %s rows (updated %s)', total_rows, updated_rows)

    logger.info('Done. Total rows=%s updated=%s', total_rows, updated_rows)


if __name__ == '__main__':
    asyncio.run(main())
