"""Разбор отсканированных кодов запчастей и поиск товара по ним.

Оригинальные коды приходят в разном виде: ``52245-SBA-A21`` (Honda, с
дефисами), артикул, за которым через Tab/пробелы идёт ещё символ (Toyota),
или с разделителем GS из GS1, либо этикетка с полями через ``;``
(``00HB;;75722-TL0-G01;1;HB5M19;CHINA;CN;156``). Ключ поиска — артикул без всего, кроме
латиницы и цифр, в верхнем регистре, как хранится ``AutoPart.oem_number``.
"""

import re

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.autopart import AutoPart, ScannedCodeAlias, preprocess_oem_number
from dz_fastapi.services.marking_code_parse import parse_marking_code

_SEPARATORS = re.compile(r"[\s\x1d;]+")
MIN_OEM_LENGTH = 5


def alias_code(raw: str) -> str:
    """Ключ привязки: у кода маркировки это GTIN (серийный номер у каждой штуки свой)."""
    raw = (raw or "").strip()
    parsed = parse_marking_code(raw)
    return f"GTIN:{parsed.gtin}" if parsed else raw


def code_keys(raw: str) -> list[str]:
    """Кандидаты артикула из сырого скана: целиком, затем по частям."""
    raw = (raw or "").strip()
    keys: list[str] = []

    def add(value: str) -> None:
        key = preprocess_oem_number(value)
        if len(key) >= MIN_OEM_LENGTH and key not in keys:
            keys.append(key)

    add(raw)
    for token in _SEPARATORS.split(raw):
        add(token)
    return keys


async def find_autoparts(session: AsyncSession, raw: str) -> list[AutoPart]:
    """Товары по скану: свой штрихкод, привязка, затем артикул."""
    raw = (raw or "").strip()
    if not raw:
        return []
    options = (selectinload(AutoPart.brand),)
    alias = alias_code(raw)
    for query in (
        select(AutoPart).where(AutoPart.barcode == raw),
        select(AutoPart).join(
            ScannedCodeAlias, ScannedCodeAlias.autopart_id == AutoPart.id
        ).where(ScannedCodeAlias.code.in_({raw, alias})),
    ):
        rows = (await session.execute(query.options(*options))).scalars().all()
        if rows:
            return list(rows)
    if alias != raw:  # код маркировки без привязки: артикула в нём нет
        return []
    for key in code_keys(raw):
        rows = (await session.execute(
            select(AutoPart).where(AutoPart.oem_number == key)
            .options(*options).order_by(AutoPart.id)
        )).scalars().all()
        if rows:
            return list(rows)
    return []
