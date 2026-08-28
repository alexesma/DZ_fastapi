"""Правила «не требует сертификации» по наименованию позиции.

Правило срабатывает по вхождению шаблона в наименование. Побеждает
самый длинный совпавший шаблон: так уточнение перебивает общее правило.
Это нужно из-за случаев вроде натяжного ролика — для ГРМ он требует
оценки соответствия, для приводного ремня нет, и различает их только
уточнение в наименовании.
"""
from __future__ import annotations

import re
from typing import Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.certificate import CertificationExemptionRule

# Латинские буквы, визуально неотличимые от кириллических. В присланных
# списках встречаются «Cальник» с латинской C и «Щyп ypoвня мacлa» —
# без приведения такие шаблоны не совпадут ни с одним наименованием.
_HOMOGLYPHS = str.maketrans(
    {
        "a": "а", "c": "с", "e": "е", "o": "о", "p": "р", "x": "х",
        "y": "у", "b": "ь", "h": "н", "k": "к", "m": "м", "t": "т",
        "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К",
        "M": "М", "O": "О", "P": "Р", "T": "Т", "X": "Х", "Y": "У",
    }
)


def normalize_name(value: Optional[str]) -> str:
    """Наименование → форма для сравнения.

    Приводим к нижнему регистру, чиним латинские двойники, схлопываем
    пробелы и убираем всё, кроме букв, цифр и пробелов: разделители у
    поставщиков разные («Сальник | КПП», «Сальник, КПП»).
    """
    text = str(value or "").strip().lower().translate(_HOMOGLYPHS)
    text = re.sub(r"[^0-9a-zа-яё]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# Наименования, по которым сертификация не требуется. Список задан
# заказчиком; правовое содержание — его зона ответственности, здесь
# только механика применения.
EXEMPT_PATTERNS: tuple[str, ...] = (
    "сальник",
    "сальник двигателя",
    "сальник кпп",
    "бачок омывателя",
    "бачок расширительный",
    "болт",
    "винт",
    "втулка",
    "гайка",
    "держатель датчика",
    "заглушка",
    "зажим",
    "защита двигателя",
    "защита моторного отсека",
    "клепка",
    "клипса",
    "колодец свечной",
    "кольцо стопорное",
    "кольцо уплотнительное",
    "крепеж",
    "крепление",
    "кронштейн",
    "крыльчатка вентилятора",
    "крышка",
    "крышка грм",
    "крышка маслозаливной горловины",
    "крышка форсунки",
    "крышки двигателя",
    "крючок боковин грузового отсека",
    "локер",
    "масляный поддон",
    "направляющая клапана",
    "натяжитель ремня",
    "окантовка датчика парковки",
    "опора двигателя",
    "опора двигателя задняя",
    "ось вилки кпп",
    "патрубки системы охлаждения",
    "патрубок",
    "петля буксировочная",
    "пистон",
    "планка",
    "пластина закладная",
    "подвес глушителя",
    "поддон картера",
    "поддон масляный",
    "подкрылок",
    "подушка акпп",
    "подушка двс",
    "подушка коробки скоростей",
    "подушки двигателя",
    "саморез",
    "сапун",
    "сливная пробка",
    "солдатик",
    "стакан регулировочный",
    "стопорное кольцо",
    "сухарь",
    "тарелка клапана",
    "толкатель клапана",
    "трос замка двери",
    "трос капота",
    "трубка",
    "трубка омывателя",
    "форсунка омывателя",
    "хомут",
    "шайба",
    "шайба регулировачная",
    "шайба стопорная",
    "шестерня",
    "шкив",
    "шланг",
    "шланг вентиляции картера",
    "шланг отопителя",
    "штуцер",
    "щуп уровня масла",
    "эксцентрик",
    "эмблема",
)

# Уточнения, которые перебивают общее правило: ГРМ-элементы относятся к
# безопасности и оценку соответствия проходят, в отличие от однотипных
# деталей привода навесного оборудования.
REQUIRED_PATTERNS: tuple[str, ...] = (
    "натяжитель грм",
    "натяжитель ремня грм",
    "натяжитель цепи грм",
    "шкив грм",
    "шестерня грм",
)


async def sync_exemption_rules(
    session: AsyncSession,
    *,
    exempt: Iterable[str] = EXEMPT_PATTERNS,
    required: Iterable[str] = REQUIRED_PATTERNS,
) -> dict[str, int]:
    """Заливает справочник правил, не трогая уже заведённые вручную."""
    existing = {
        (row.normalized_pattern, row.certification_required)
        for row in (
            await session.execute(select(CertificationExemptionRule))
        ).scalars()
    }
    created = 0
    for patterns, required_flag in ((exempt, False), (required, True)):
        for pattern in patterns:
            normalized = normalize_name(pattern)
            if not normalized or (normalized, required_flag) in existing:
                continue
            session.add(
                CertificationExemptionRule(
                    pattern=pattern,
                    normalized_pattern=normalized,
                    certification_required=required_flag,
                )
            )
            existing.add((normalized, required_flag))
            created += 1
    if created:
        await session.commit()
    return {"created": created, "total": len(existing)}


def match_rule(
    name: str,
    rules: list[tuple[str, bool]],
) -> Optional[tuple[str, bool]]:
    """Находит правило для наименования.

    Выигрывает самый длинный совпавший шаблон — так «натяжитель ремня
    грм» перебивает «натяжитель ремня». При равной длине приоритет у
    правила «требует», чтобы неоднозначность решалась в безопасную
    сторону.
    """
    normalized = normalize_name(name)
    if not normalized:
        return None
    best: Optional[tuple[str, bool]] = None
    for pattern, required_flag in rules:
        if pattern not in normalized:
            continue
        if best is None:
            best = (pattern, required_flag)
            continue
        if len(pattern) > len(best[0]) or (
            len(pattern) == len(best[0]) and required_flag
        ):
            best = (pattern, required_flag)
    return best


async def apply_exemption_rules(
    session: AsyncSession,
    *,
    dry_run: bool = True,
    only_unset: bool = True,
    limit: Optional[int] = None,
) -> dict[str, object]:
    """Проставляет признак сертификации по наименованию.

    По умолчанию трогает только позиции, где признак ещё не определён:
    данные из документов поставщика и ручной ввод правилом не
    перебиваются.
    """
    rules = [
        (row.normalized_pattern, row.certification_required)
        for row in (
            await session.execute(
                select(CertificationExemptionRule).where(
                    CertificationExemptionRule.is_active.is_(True)
                )
            )
        ).scalars()
    ]
    if not rules:
        return {"rules": 0, "checked": 0, "matched": 0, "updated": 0}

    stmt = select(AutoPart)
    if only_unset:
        stmt = stmt.where(AutoPart.certification_required.is_(None))
    if limit:
        stmt = stmt.limit(limit)

    stats: dict[str, object] = {
        "rules": len(rules),
        "checked": 0,
        "matched": 0,
        "updated": 0,
        "exempted": 0,
        "required": 0,
    }
    by_pattern: dict[str, int] = {}

    for part in (await session.execute(stmt)).scalars():
        stats["checked"] += 1
        hit = match_rule(part.name, rules)
        if hit is None:
            continue
        pattern, required_flag = hit
        stats["matched"] += 1
        by_pattern[pattern] = by_pattern.get(pattern, 0) + 1
        if required_flag:
            stats["required"] += 1
        else:
            stats["exempted"] += 1
        if part.certification_required == required_flag:
            continue
        stats["updated"] += 1
        if not dry_run:
            part.certification_required = required_flag
            part.regulatory_source = "rule"
            session.add(part)

    if not dry_run:
        await session.commit()
    stats["top_patterns"] = sorted(
        by_pattern.items(), key=lambda item: -item[1]
    )[:20]
    return stats
