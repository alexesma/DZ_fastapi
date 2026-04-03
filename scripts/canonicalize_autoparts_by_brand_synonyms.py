import argparse
import asyncio
import logging

from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.autopart_dedup import \
    canonicalize_autoparts_by_brand_synonyms

logger = logging.getLogger('dz_fastapi')
logging.basicConfig(level=logging.INFO)


async def main(apply_changes: bool) -> None:
    session_factory = get_async_session()
    async with session_factory() as session:
        summary = await canonicalize_autoparts_by_brand_synonyms(
            session=session,
            dry_run=not apply_changes,
        )
        if apply_changes:
            await session.commit()
            logger.info('Canonicalization applied: %s', summary)
        else:
            await session.rollback()
            logger.info('Dry run summary: %s', summary)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Canonicalize autoparts by brand synonym groups '
            'and merge duplicates'
        )
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help=(
            'Persist changes. '
            'Without this flag the script runs in dry-run mode.'
        ),
    )
    args = parser.parse_args()
    asyncio.run(main(apply_changes=bool(args.apply)))
