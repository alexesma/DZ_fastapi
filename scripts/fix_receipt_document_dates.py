"""Исправление даты поступлений из XML-УПД, у которых стоит дата обработки.

Запуск внутри контейнера backend. По умолчанию только отчёт::

    poetry run python scripts/fix_receipt_document_dates.py --ids 1910 1912 1913

Применить (меняется только document_date, в комментарий поступления
дописывается запись, остатки и проведение не затрагиваются)::

    poetry run python scripts/fix_receipt_document_dates.py --ids 1910 1912 1913 --apply
"""

from __future__ import annotations

import argparse
import asyncio

from dotenv import load_dotenv

from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.supplier_receipt_fixes import fix_receipt_document_date


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ids", nargs="+", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_dotenv(".env")
    async with get_async_session()() as session:
        for receipt_id in args.ids:
            result = await fix_receipt_document_date(
                session, receipt_id=receipt_id, apply=args.apply
            )
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
