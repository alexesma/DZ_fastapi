"""Агрегаты «Честного знака»: транспортные и групповые упаковки.

На коробке или палете стоит один код — транспортный (SSCC, ``00`` и 18
цифр) или групповой (выглядит как обычный код товара). В УПД и в учёте
нужны коды штук, поэтому упаковку раскрываем через ГИС МТ: спрашиваем
вложенные коды, пока не дойдём до отдельных единиц товара.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.settings import GisMtSettings
from dz_fastapi.services.marking_code_parse import fix_russian_layout, marking_code_ki

_SSCC_RE = re.compile(r"^(?:\](?:C1|d2|Q3))?\(?00\)?(\d{18})$")
MAX_DEPTH = 4
MAX_UNITS = 5000
_CHUNK = 1000


class AggregateError(ValueError):
    """Упаковку раскрыть не удалось; текст — для кладовщика."""


def parse_transport_code(raw: object) -> Optional[str]:
    """Код транспортной упаковки (SSCC) в виде ``00`` + 18 цифр или ``None``."""
    text, _fixed = fix_russian_layout(str(raw or "").strip())
    match = _SSCC_RE.match(text.replace("\x1d", ""))
    return f"00{match.group(1)}" if match else None


def _info(raw: Any) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    info = payload.get("cisInfo")
    return info if isinstance(info, dict) else payload


async def gis_mt_client_or_none(session: AsyncSession):
    """Клиент ГИС МТ, если вход выполнен."""
    from dz_fastapi.http.gis_mt_client import GisMtClient
    from dz_fastapi.services.gis_mt import gis_mt_token_is_active

    settings_row = (
        await session.execute(select(GisMtSettings).limit(1))
    ).scalars().first()
    if settings_row is None or not gis_mt_token_is_active(settings_row):
        return None
    return GisMtClient(token=str(settings_row.token))


async def expand_aggregate(session: AsyncSession, code: str) -> list[str]:
    """Коды штук внутри упаковки; пустой список — это не упаковка.

    ``AggregateError`` — вход в ГИС МТ не выполнен или ГИС МТ не ответил.
    """
    client = await gis_mt_client_or_none(session)
    if client is None:
        raise AggregateError(
            "Чтобы раскрыть упаковку, выполните вход в ГИС МТ"
        )
    units: list[str] = []
    queue = [code]
    seen: set[str] = set()
    try:
        for depth in range(MAX_DEPTH):
            next_level: list[str] = []
            for offset in range(0, len(queue), _CHUNK):
                for raw in await client.get_cises_info(queue[offset:offset + _CHUNK]):
                    info = _info(raw)
                    children = [
                        str(child).strip()
                        for child in (info.get("child") or [])
                        if str(child or "").strip()
                    ]
                    if children:
                        next_level.extend(c for c in children if c not in seen)
                        seen.update(children)
                    elif depth > 0:
                        cis = str(
                            info.get("requestedCis") or info.get("cis") or ""
                        ).strip()
                        if cis:
                            units.append(marking_code_ki(cis) or cis)
            if not next_level:
                break
            queue = next_level
            if len(units) + len(queue) > MAX_UNITS:
                raise AggregateError(
                    f"В упаковке больше {MAX_UNITS} кодов — раскройте её по частям"
                )
    except AggregateError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise AggregateError(f"ГИС МТ не ответил: {str(exc)[:200]}") from exc
    return list(dict.fromkeys(units))
