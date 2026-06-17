"""Импорт кроссов из файла-выгрузки (1С): группировка по идентификатору.

Формат файла — 2 значимые колонки:
- идентификатор группы аналогов (любой текст/GUID);
- № по каталогу (= наш OEM).

Все номера с одинаковым идентификатором — взаимные кроссы. Бренд не важен:
номер матчится к нашим автозапчастям по OEM (любой бренд), и найденные
позиции связываются кроссами друг с другом. Номера, которых нет в нашей
базе, в кроссы не превращаются (попадают в «не найдено» — повторный импорт
после появления позиций их подхватит).
"""
from __future__ import annotations

import asyncio
import io
import logging
import zipfile
from typing import Any

import pandas as pd
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.services.crosses import _load_invalid_pair_keys

logger = logging.getLogger("dz_fastapi")

IDENTIFIER_HEADER_HINTS = ("идентификатор", "identifier", "группа", "group")
OEM_HEADER_HINTS = ("каталог", "oem", "номер", "артикул", "number")
CROSS_IMPORT_COMMENT = "1c_import"
_INSERT_CHUNK = 5000
_LOOKUP_CHUNK = 1000


def _normalize_oem(value: Any) -> str:
    if value is None:
        return ""
    return preprocess_oem_number(str(value).strip())


def _is_junk_oem(normalized_oem: str) -> bool:
    """Отсев явного мусора: пусто, слишком коротко или одни нули."""
    if len(normalized_oem) < 3:
        return True
    if set(normalized_oem) <= {"0"}:
        return True
    return False


def _read_table(content: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        for sep in (",", ";", "\t"):
            try:
                df = pd.read_csv(io.BytesIO(content), dtype=str, sep=sep)
                if df.shape[1] >= 2:
                    return df
            except Exception:
                continue
        return pd.read_csv(io.BytesIO(content), dtype=str)
    try:
        return pd.read_excel(io.BytesIO(content), dtype=str)
    except KeyError as exc:
        # Выгрузка 1С с битым регистром имени xl/SharedStrings.xml —
        # перепаковываем в память с корректным именем записи.
        if "sharedstrings" not in str(exc).lower():
            raise
        fixed = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(content)) as zin, zipfile.ZipFile(
            fixed, "w", zipfile.ZIP_DEFLATED
        ) as zout:
            for item in zin.infolist():
                target = item.filename
                if target == "xl/SharedStrings.xml":
                    target = "xl/sharedStrings.xml"
                zout.writestr(target, zin.read(item.filename))
        fixed.seek(0)
        return pd.read_excel(fixed, dtype=str)


def _resolve_columns(df: pd.DataFrame) -> tuple[str, str]:
    """Находит колонки идентификатора и OEM по заголовкам (или по позиции)."""
    id_col = None
    oem_col = None
    for col in df.columns:
        low = str(col).strip().lower()
        if id_col is None and any(h in low for h in IDENTIFIER_HEADER_HINTS):
            id_col = col
        elif oem_col is None and any(h in low for h in OEM_HEADER_HINTS):
            oem_col = col
    if id_col is not None and oem_col is not None:
        return id_col, oem_col
    # Fallback: первые две непустые колонки.
    usable = [c for c in df.columns if not str(c).startswith("Unnamed")]
    if len(usable) < 2:
        usable = list(df.columns)
    if len(usable) < 2:
        raise ValueError(
            "В файле должно быть минимум 2 колонки: идентификатор и № по каталогу"
        )
    return usable[0], usable[1]


def group_rows_by_identifier(
    rows: list[tuple[str, str]],
) -> dict[str, set[str]]:
    """Группирует нормализованные OEM по идентификатору (чистая функция)."""
    groups: dict[str, set[str]] = {}
    for identifier, raw_oem in rows:
        ident = str(identifier or "").strip()
        oem = _normalize_oem(raw_oem)
        if not ident or _is_junk_oem(oem):
            continue
        groups.setdefault(ident, set()).add(oem)
    return groups


def parse_cross_file(content: bytes, filename: str) -> dict[str, set[str]]:
    df = _read_table(content, filename)
    id_col, oem_col = _resolve_columns(df)
    rows = list(
        zip(
            df[id_col].astype(str).tolist(),
            df[oem_col].astype(str).tolist(),
        )
    )
    return group_rows_by_identifier(rows)


async def _load_autoparts_by_oem(
    session: AsyncSession,
    normalized_oems: list[str],
) -> dict[str, list[tuple[int, int, str]]]:
    """OEM → список (autopart_id, brand_id, normalized_oem) по всем брендам."""
    result: dict[str, list[tuple[int, int, str]]] = {}
    unique = sorted(set(normalized_oems))
    for start in range(0, len(unique), _LOOKUP_CHUNK):
        chunk = unique[start:start + _LOOKUP_CHUNK]
        rows = (
            await session.execute(
                select(
                    AutoPart.id,
                    AutoPart.brand_id,
                    AutoPart.oem_number,
                ).where(AutoPart.oem_number.in_(chunk))
            )
        ).all()
        for ap_id, brand_id, oem in rows:
            normalized = _normalize_oem(oem)
            result.setdefault(normalized, []).append(
                (int(ap_id), int(brand_id), normalized)
            )
    return result


async def _load_existing_cross_keys(
    session: AsyncSession,
    source_ids: list[int],
) -> set[tuple[int, int, str]]:
    existing: set[tuple[int, int, str]] = set()
    unique = sorted(set(source_ids))
    for start in range(0, len(unique), _LOOKUP_CHUNK):
        chunk = unique[start:start + _LOOKUP_CHUNK]
        rows = (
            await session.execute(
                select(
                    AutoPartCross.source_autopart_id,
                    AutoPartCross.cross_brand_id,
                    AutoPartCross.cross_oem_number,
                ).where(AutoPartCross.source_autopart_id.in_(chunk))
            )
        ).all()
        for src, brand_id, oem in rows:
            existing.add((int(src), int(brand_id), _normalize_oem(oem)))
    return existing


async def import_crosses_from_file(
    session: AsyncSession,
    *,
    content: bytes,
    filename: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    # Парсинг xlsx (pandas) — CPU-тяжёлый, выносим из event loop.
    groups = await asyncio.to_thread(parse_cross_file, content, filename)

    total_groups = len(groups)
    groups_with_pair = sum(1 for oems in groups.values() if len(oems) >= 2)
    all_oems = {oem for oems in groups.values() for oem in oems}

    autoparts_by_oem = await _load_autoparts_by_oem(session, list(all_oems))
    matched_oems = {oem for oem in all_oems if oem in autoparts_by_oem}
    unmatched_oems = all_oems - matched_oems

    # Все автозапчасти, которые будут источниками кроссов.
    involved_source_ids = {
        ap_id
        for oem in matched_oems
        for ap_id, _brand_id, _oem in autoparts_by_oem.get(oem, [])
    }
    existing_keys = await _load_existing_cross_keys(
        session, list(involved_source_ids)
    )
    invalid_keys = await _load_invalid_pair_keys(session)

    insert_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[int, int, str]] = set()
    crosses_created = 0
    crosses_existing = 0
    crosses_skipped_invalid = 0
    groups_linked = 0

    def _add_directed(src: tuple[int, int, str], dst: tuple[int, int, str]) -> None:
        nonlocal crosses_created, crosses_existing, crosses_skipped_invalid
        src_id, _src_brand, _src_oem = src
        dst_id, dst_brand, dst_oem = dst
        if src_id == dst_id:
            return
        key = (src_id, dst_brand, dst_oem)
        if key in invalid_keys:
            crosses_skipped_invalid += 1
            return
        if key in existing_keys:
            crosses_existing += 1
            return
        if key in seen_keys:
            return
        seen_keys.add(key)
        crosses_created += 1
        insert_rows.append(
            {
                "source_autopart_id": src_id,
                "cross_brand_id": dst_brand,
                "cross_oem_number": dst_oem,
                "cross_autopart_id": dst_id,
                "is_bidirectional": True,
                "priority": 100,
                "comment": CROSS_IMPORT_COMMENT,
            }
        )

    for oems in groups.values():
        # Уникальные автозапчасти группы (один OEM может дать несколько
        # запчастей разных брендов).
        members: dict[int, tuple[int, int, str]] = {}
        for oem in oems:
            for ap_id, brand_id, normalized in autoparts_by_oem.get(oem, []):
                members[ap_id] = (ap_id, brand_id, normalized)
        member_list = list(members.values())
        if len(member_list) < 2:
            continue
        groups_linked += 1
        for i in range(len(member_list)):
            for j in range(i + 1, len(member_list)):
                _add_directed(member_list[i], member_list[j])
                _add_directed(member_list[j], member_list[i])

    if not dry_run and insert_rows:
        for start in range(0, len(insert_rows), _INSERT_CHUNK):
            await session.execute(
                insert(AutoPartCross),
                insert_rows[start:start + _INSERT_CHUNK],
            )
        await session.commit()

    return {
        "dry_run": dry_run,
        "total_groups": total_groups,
        "groups_with_pair": groups_with_pair,
        "groups_linked": groups_linked,
        "total_numbers": len(all_oems),
        "matched_numbers": len(matched_oems),
        "unmatched_numbers": len(unmatched_oems),
        "crosses_created": crosses_created,
        "crosses_already_existed": crosses_existing,
        "crosses_skipped_invalid": crosses_skipped_invalid,
        "unmatched_sample": sorted(list(unmatched_oems))[:50],
    }
