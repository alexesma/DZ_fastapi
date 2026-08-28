"""Обязательные реквизиты прайса: импорт, распространение, покрытие.

Поставщики отдают прайс с колонками ТН ВЭД, ОКПД 2, признаком «Честный
знак», номером сертификата ЕАС и ссылкой ФГИС. Здесь мы их принимаем,
раскладываем по карточкам и считаем, чего не хватает.

Про распространение сертификата. На разборе реального прайса поставщика
(25 447 строк, 252 сертификата) видно, что 233 сертификата из 252
покрывают ровно один бренд, но внутри бренда — сотни типов деталей.
То есть сертификат выдан на бренд, а не на артикул, и распространение
внутри бренда воспроизводит то, что уже утверждает сам поставщик.

Чего здесь сознательно нет — переноса сертификата между поставщиками и
на собственный бренд. Сертификат привязан к заявителю: два поставщика
одного бренда могут ввозить товар по разным документам, а для продукции
под своим брендом нужна собственная декларация.
"""
from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from typing import Any, Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.certificate import Certificate
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation, Provider
from dz_fastapi.services.utils import CERTIFICATION_NOT_REQUIRED_TEXT

logger = logging.getLogger("dz_fastapi")

# Значение, которым нельзя затирать ручной ввод.
MANUAL_SOURCE = "manual"

# Заголовки колонок поставщиков. Список открытый: у разных поставщиков
# написание отличается («ТНВЭД» / «ТН ВЭД»), поэтому сверяем по
# нормализованному ключу, а не по точному совпадению.
COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "brand": ("бренд", "производитель", "марка"),
    "article": ("артикул", "каталожныйномер", "код"),
    "name": ("описание", "наименование"),
    "tnved_code": ("тнвэд", "тнвэдкод", "кодтнвэд"),
    "okpd2_code": ("окпд2", "окпд"),
    "honest_sign": ("подключенкчз", "честныйзнак", "чз"),
    "eac_cert_number": ("номерсертификатаеас", "сертификатеас", "сертификат"),
    "eac_cert_url": ("ссылкафгис", "фгис", "ссылканасертификат"),
}


def _normalize_header(value: str) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _map_columns(header: list[str]) -> dict[str, int]:
    """Заголовок файла → индексы нужных колонок."""
    normalized = [_normalize_header(item) for item in header]
    mapping: dict[str, int] = {}
    for field, aliases in COLUMN_ALIASES.items():
        for index, cell in enumerate(normalized):
            if cell in aliases:
                mapping[field] = index
                break
    return mapping


def parse_supplier_regulatory_file(
    content: bytes,
    *,
    encoding: Optional[str] = None,
    delimiter: str = ";",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Разбирает CSV поставщика с обязательными реквизитами.

    Кодировка у таких выгрузок обычно cp1251, поэтому пробуем её первой,
    а не полагаемся на utf-8 по умолчанию.
    """
    text: Optional[str] = None
    for candidate in ([encoding] if encoding else []) + ["cp1251", "utf-8-sig", "utf-8"]:
        if not candidate:
            continue
        try:
            text = content.decode(candidate)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    if text is None:
        raise ValueError("Не удалось определить кодировку файла")

    reader = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    if not reader:
        return [], {}
    columns = _map_columns(reader[0])
    missing = {"brand", "article"} - set(columns)
    if missing:
        raise ValueError(
            "В файле не найдены обязательные колонки: "
            + ", ".join(sorted(missing))
        )

    rows: list[dict[str, Any]] = []
    for raw in reader[1:]:
        if not raw:
            continue

        def cell(field: str) -> str:
            index = columns.get(field)
            if index is None or index >= len(raw):
                return ""
            return str(raw[index] or "").strip()

        brand = cell("brand")
        article = cell("article")
        if not brand or not article:
            continue
        rows.append(
            {
                "brand": brand,
                "article": article,
                "name": cell("name"),
                "tnved_code": cell("tnved_code") or None,
                "okpd2_code": cell("okpd2_code") or None,
                "honest_sign": cell("honest_sign") or None,
                "eac_cert_number": cell("eac_cert_number") or None,
                "eac_cert_url": cell("eac_cert_url") or None,
            }
        )
    return rows, columns


def _split_certificate(value: Optional[str]) -> tuple[Optional[bool], Optional[str]]:
    """Текст в колонке сертификата → (требуется ли, номер).

    Поставщики пишут «Не требует сертификации» прямо в поле номера;
    у нас это отдельный флаг, чтобы такие позиции можно было считать.
    """
    text = (value or "").strip()
    if not text:
        return None, None
    if text.casefold() == CERTIFICATION_NOT_REQUIRED_TEXT.casefold():
        return False, None
    return True, text


async def _load_autopart_index(
    session: AsyncSession,
    keys: Iterable[tuple[str, str]],
) -> dict[tuple[str, str], int]:
    """(нормализованный бренд, нормализованный артикул) → id карточки."""
    articles = {article for _, article in keys}
    if not articles:
        return {}
    index: dict[tuple[str, str], int] = {}
    stmt = (
        select(AutoPart.id, AutoPart.oem_number, func.lower(Brand.name))
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(AutoPart.oem_number.in_(sorted(articles)))
    )
    for autopart_id, oem, brand_lower in (await session.execute(stmt)).all():
        index[(str(brand_lower).strip(), str(oem))] = int(autopart_id)
    return index


async def _upsert_certificates(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    """Заводит сертификаты из файла, номер — естественный ключ."""
    wanted: dict[str, dict[str, Any]] = {}
    for row in rows:
        required, number = _split_certificate(row.get("eac_cert_number"))
        if not required or not number:
            continue
        entry = wanted.setdefault(
            number, {"url": None, "brands": set()}
        )
        if row.get("eac_cert_url") and not entry["url"]:
            entry["url"] = row["eac_cert_url"]
        entry["brands"].add(str(row.get("brand") or "").strip().lower())
    if not wanted:
        return {}

    existing = {
        row.number: row
        for row in (
            await session.execute(
                select(Certificate).where(
                    Certificate.number.in_(sorted(wanted))
                )
            )
        ).scalars()
    }
    brand_index = {
        str(name).strip().lower(): int(brand_id)
        for brand_id, name in (
            await session.execute(select(Brand.id, Brand.name))
        ).all()
    }

    result: dict[str, int] = {}
    for number, entry in wanted.items():
        certificate = existing.get(number)
        # Бренд проставляем, только когда он в файле однозначен: клиенты
        # сверяют формальное соответствие бренда и сертификата.
        brand_id = None
        if len(entry["brands"]) == 1:
            brand_id = brand_index.get(next(iter(entry["brands"])))
        if certificate is None:
            certificate = Certificate(
                number=number,
                url=entry["url"],
                brand_id=brand_id,
                source="supplier_file",
            )
            session.add(certificate)
        else:
            if entry["url"] and not certificate.url:
                certificate.url = entry["url"]
            if brand_id and not certificate.brand_id:
                certificate.brand_id = brand_id
            session.add(certificate)
        await session.flush()
        result[number] = int(certificate.id)
    return result


async def import_supplier_regulatory(
    session: AsyncSession,
    rows: list[dict[str, Any]],
    *,
    dry_run: bool = True,
    overwrite_manual: bool = False,
) -> dict[str, Any]:
    """Переносит реквизиты из прайса поставщика в карточки.

    Ручной ввод по умолчанию не затирается: человек, который проверил
    сертификат в реестре, знает больше, чем строка в чужом файле.
    """
    prepared: list[tuple[tuple[str, str], dict[str, Any]]] = []
    for row in rows:
        key = (
            str(row["brand"]).strip().lower(),
            preprocess_oem_number(str(row["article"])),
        )
        prepared.append((key, row))

    index = await _load_autopart_index(session, [key for key, _ in prepared])

    stats = {
        "rows": len(prepared),
        "matched": 0,
        "unmatched": 0,
        "updated": 0,
        "skipped_manual": 0,
        "unchanged": 0,
        "fields_filled": defaultdict(int),
        "unmatched_brands": defaultdict(int),
    }

    targets: dict[int, dict[str, Any]] = {}
    for key, row in prepared:
        autopart_id = index.get(key)
        if autopart_id is None:
            stats["unmatched"] += 1
            stats["unmatched_brands"][row["brand"]] += 1
            continue
        stats["matched"] += 1
        # Последняя строка по позиции выигрывает — файл может содержать
        # дубли артикула с разной полнотой заполнения.
        targets[autopart_id] = row

    if not targets:
        stats["fields_filled"] = dict(stats["fields_filled"])
        stats["unmatched_brands"] = dict(stats["unmatched_brands"])
        return stats

    # Сертификаты заводим один раз на номер и связываем с позициями:
    # один документ покрывает сотни артикулов, дублировать его в каждой
    # карточке бессмысленно.
    certificate_ids = (
        {} if dry_run else await _upsert_certificates(session, list(targets.values()))
    )
    stats["certificates"] = len(certificate_ids)
    stats["links_created"] = 0

    parts = (
        (
            await session.execute(
                select(AutoPart)
                .where(AutoPart.id.in_(sorted(targets)))
                .options(selectinload(AutoPart.certificates))
            )
        )
        .scalars()
        .all()
    )
    for part in parts:
        row = targets[part.id]
        if (
            part.regulatory_source == MANUAL_SOURCE
            and not overwrite_manual
        ):
            stats["skipped_manual"] += 1
            continue

        required, cert_number = _split_certificate(row.get("eac_cert_number"))
        changes: dict[str, Any] = {}
        if row.get("tnved_code") and not part.tnved_code:
            changes["tnved_code"] = row["tnved_code"]
        if row.get("okpd2_code") and not part.okpd2_code:
            changes["okpd2_code"] = row["okpd2_code"]
        if required is not None and part.certification_required is None:
            changes["certification_required"] = required
        if cert_number and not part.eac_cert_number:
            changes["eac_cert_number"] = cert_number
        if row.get("eac_cert_url") and not part.eac_cert_url:
            changes["eac_cert_url"] = row["eac_cert_url"]

        # Связь ставим независимо от того, менялись ли поля карточки:
        # позиция могла уже иметь номер, но не иметь связи с документом.
        certificate_id = certificate_ids.get(cert_number) if cert_number else None
        if certificate_id and not dry_run:
            linked = {item.id for item in part.certificates}
            if certificate_id not in linked:
                part.certificates.append(
                    await session.get(Certificate, certificate_id)
                )
                stats["links_created"] += 1
                session.add(part)

        if not changes:
            stats["unchanged"] += 1
            continue
        stats["updated"] += 1
        for field in changes:
            stats["fields_filled"][field] += 1
        if not dry_run:
            for field, value in changes.items():
                setattr(part, field, value)
            part.regulatory_source = "supplier_doc"
            session.add(part)

    if not dry_run:
        await session.commit()

    stats["fields_filled"] = dict(stats["fields_filled"])
    stats["unmatched_brands"] = dict(
        sorted(
            stats["unmatched_brands"].items(),
            key=lambda item: -item[1],
        )[:20]
    )
    return stats


async def propagate_certificates_by_brand(
    session: AsyncSession,
    *,
    provider_id: int,
    brand_ids: Optional[list[int]] = None,
    dry_run: bool = True,
    min_confidence: float = 0.9,
    min_evidence: int = 20,
    max_expansion_ratio: float = 10.0,
) -> dict[str, Any]:
    """Распространяет сертификат бренда на позиции того же поставщика.

    Три ограничения, без которых распространение превращается в переброс
    сертификата на чужой товар:

    * ``provider_id`` — цели берутся только из прайса того поставщика,
      который сам этот сертификат и заявил. Каталог одного бренда мы
      получаем от нескольких поставщиков, а сертификат привязан к
      заявителю: у другого поставщика тот же бренд ввезён по своим
      документам.
    * ``min_evidence`` — сколько позиций с сертификатом должно быть в
      основании. Три строки ничего не говорят о тысячах.
    * ``max_expansion_ratio`` — во сколько раз позволено расширить.
      Отношение целей к основанию выше порога означает, что мы угадываем.

    ``min_confidence`` остаётся, но сам по себе он слаб: при одном
    уникальном сертификате доля всегда равна 1.0 независимо от объёма.
    """
    provider_parts = (
        select(PriceListAutoPartAssociation.autopart_id)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .where(PriceList.provider_id == provider_id)
        .distinct()
        .subquery()
    )

    stmt = (
        select(
            AutoPart.brand_id,
            AutoPart.eac_cert_number,
            AutoPart.eac_cert_url,
            func.count().label("cnt"),
        )
        .where(
            AutoPart.eac_cert_number.is_not(None),
            AutoPart.id.in_(select(provider_parts.c.autopart_id)),
        )
        .group_by(
            AutoPart.brand_id, AutoPart.eac_cert_number, AutoPart.eac_cert_url
        )
    )
    if brand_ids:
        stmt = stmt.where(AutoPart.brand_id.in_(brand_ids))

    by_brand: dict[int, list[tuple[str, Optional[str], int]]] = defaultdict(list)
    for brand_id, cert, url, count in (await session.execute(stmt)).all():
        by_brand[int(brand_id)].append((cert, url, int(count)))

    result = {
        "brands_considered": len(by_brand),
        "brands_applied": 0,
        "brands_ambiguous": 0,
        "brands_thin_evidence": 0,
        "brands_over_expansion": 0,
        "positions_updated": 0,
        "details": [],
        "rejected": [],
    }

    for brand_id, certs in by_brand.items():
        total = sum(item[2] for item in certs)
        certs.sort(key=lambda item: -item[2])
        dominant_cert, dominant_url, dominant_count = certs[0]
        confidence = dominant_count / total if total else 0.0
        if confidence < min_confidence:
            result["brands_ambiguous"] += 1
            continue
        if dominant_count < min_evidence:
            result["brands_thin_evidence"] += 1
            result["rejected"].append(
                {
                    "brand_id": brand_id,
                    "reason": "мало оснований",
                    "evidence": dominant_count,
                }
            )
            continue

        blanks_stmt = select(AutoPart).where(
            AutoPart.brand_id == brand_id,
            AutoPart.eac_cert_number.is_(None),
            AutoPart.certification_required.is_(None),
            AutoPart.id.in_(select(provider_parts.c.autopart_id)),
        )
        blanks = (await session.execute(blanks_stmt)).scalars().all()
        if not blanks:
            continue
        if len(blanks) > dominant_count * max_expansion_ratio:
            result["brands_over_expansion"] += 1
            result["rejected"].append(
                {
                    "brand_id": brand_id,
                    "reason": "слишком широкое расширение",
                    "evidence": dominant_count,
                    "targets": len(blanks),
                }
            )
            continue

        result["brands_applied"] += 1
        result["positions_updated"] += len(blanks)
        result["details"].append(
            {
                "brand_id": brand_id,
                "certificate": dominant_cert,
                "confidence": round(confidence, 3),
                "evidence": dominant_count,
                "positions": len(blanks),
            }
        )
        if not dry_run:
            for part in blanks:
                part.eac_cert_number = dominant_cert
                part.eac_cert_url = dominant_url
                part.certification_required = True
                part.regulatory_source = "brand_rule"
                session.add(part)

    if not dry_run:
        await session.commit()
    result["details"] = sorted(
        result["details"], key=lambda item: -item["positions"]
    )[:50]
    result["rejected"] = sorted(
        result["rejected"], key=lambda item: -item.get("targets", 0)
    )[:50]
    return result


# В номерах сертификатов орган и литеры пишутся кириллицей, а код страны
# латиницей: «ЕАЭС RU С-BE.НВ07.В.00826/23». В присланных вручную номерах
# часто стоят латинские двойники (C, H, B, P, A, E, K, M, O, T, X, У),
# и такой номер не найдётся в реестре при проверке клиентом.
_CERT_HOMOGLYPHS = str.maketrans(
    {
        "C": "С", "H": "Н", "B": "В", "P": "Р", "A": "А", "E": "Е",
        "K": "К", "M": "М", "O": "О", "T": "Т", "X": "Х", "Y": "У",
    }
)


def normalize_certificate_number(value: str) -> str:
    """Приводит номер сертификата к кириллическому написанию.

    Код страны после дефиса (RU, BE, JP, CN) остаётся латиницей — это
    ISO-код, он так и печатается в бланке.
    """
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    parts = text.split("-", 1)
    head = parts[0].translate(_CERT_HOMOGLYPHS)
    if len(parts) == 1:
        return head
    tail = parts[1]
    # Первый сегмент хвоста — код страны, его не трогаем.
    country, dot, rest = tail.partition(".")
    return f"{head}-{country}{dot}{rest.translate(_CERT_HOMOGLYPHS)}"


async def apply_brand_certificate(
    session: AsyncSession,
    *,
    brand_id: int,
    number: str,
    url: Optional[str] = None,
    scope: Optional[str] = None,
    valid_until: Optional[Any] = None,
    dry_run: bool = True,
    only_undetermined: bool = True,
) -> dict[str, Any]:
    """Привязывает один сертификат ко всему бренду.

    Это явное решение пользователя, а не вывод по данным: для брендов,
    где поставщик оформил один документ на весь ассортимент, перечислять
    артикулы бессмысленно. Помечается ``covers_whole_brand``, источник
    ``brand_certificate`` — так операция отличима и обратима.

    По умолчанию трогаются только позиции без определённого признака:
    данные из документов поставщика и ручной ввод не перетираются.
    """
    canonical = normalize_certificate_number(number)
    if not canonical:
        raise ValueError("Пустой номер сертификата")

    certificate = (
        await session.execute(
            select(Certificate).where(Certificate.number == canonical)
        )
    ).scalar_one_or_none()
    created = certificate is None
    if certificate is None:
        certificate = Certificate(number=canonical, source="manual")
    certificate.url = url or certificate.url
    certificate.brand_id = brand_id
    certificate.covers_whole_brand = True
    if scope:
        certificate.scope = scope
    if valid_until:
        certificate.valid_until = valid_until
    if not dry_run:
        session.add(certificate)
        await session.flush()

    targets_stmt = select(AutoPart).where(AutoPart.brand_id == brand_id)
    if only_undetermined:
        targets_stmt = targets_stmt.where(
            AutoPart.certification_required.is_(None)
        )
    targets_stmt = targets_stmt.options(selectinload(AutoPart.certificates))
    targets = (await session.execute(targets_stmt)).scalars().all()

    result = {
        "certificate": canonical,
        "certificate_created": created,
        "input_number": number,
        "normalized": canonical != " ".join(str(number or "").split()),
        "positions": len(targets),
        "linked": 0,
    }
    if dry_run:
        return result

    for part in targets:
        if certificate.id not in {item.id for item in part.certificates}:
            part.certificates.append(certificate)
            result["linked"] += 1
        part.eac_cert_number = canonical
        part.eac_cert_url = url or certificate.url
        part.certification_required = True
        part.regulatory_source = "brand_certificate"
        session.add(part)
    await session.commit()
    return result


async def regulatory_coverage(
    session: AsyncSession,
    *,
    only_in_stock: bool = True,
) -> dict[str, Any]:
    """Покрытие реквизитами по позициям, которые уходят в прайсы.

    Считаем по актуальным прайсам поставщиков: именно эти позиции
    транслируются клиентам, и именно по ним требование обязательно.
    """
    latest = (
        select(
            PriceList.provider_config_id,
            func.max(PriceList.id).label("pricelist_id"),
        )
        .where(PriceList.provider_config_id.is_not(None))
        .group_by(PriceList.provider_config_id)
        .subquery()
    )
    stmt = (
        select(
            Brand.name.label("brand"),
            Provider.name.label("provider"),
            AutoPart.tnved_code,
            AutoPart.okpd2_code,
            AutoPart.eac_cert_number,
            AutoPart.certification_required,
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            latest,
            latest.c.pricelist_id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(Provider, Provider.id == PriceList.provider_id)
        .join(AutoPart, AutoPart.id == PriceListAutoPartAssociation.autopart_id)
        .join(Brand, Brand.id == AutoPart.brand_id)
    )
    if only_in_stock:
        stmt = stmt.where(PriceListAutoPartAssociation.quantity > 0)

    totals = {
        "positions": 0,
        "tnved": 0,
        "okpd2": 0,
        "certificate": 0,
        "complete": 0,
    }
    by_brand: dict[str, dict[str, int]] = defaultdict(
        lambda: {"positions": 0, "missing": 0}
    )
    by_provider: dict[str, dict[str, int]] = defaultdict(
        lambda: {"positions": 0, "missing": 0}
    )

    for row in (await session.execute(stmt)).all():
        totals["positions"] += 1
        has_tnved = bool(row.tnved_code)
        has_okpd2 = bool(row.okpd2_code)
        # Сертификат считается закрытым и когда он не требуется.
        has_cert = (
            bool(row.eac_cert_number) or row.certification_required is False
        )
        totals["tnved"] += int(has_tnved)
        totals["okpd2"] += int(has_okpd2)
        totals["certificate"] += int(has_cert)
        complete = has_tnved and has_okpd2 and has_cert
        totals["complete"] += int(complete)
        for bucket, key in (
            (by_brand, row.brand or "—"),
            (by_provider, row.provider or "—"),
        ):
            cell = bucket[key]
            cell["positions"] += 1
            if not complete:
                cell["missing"] += 1

    def pct(part: int) -> Optional[float]:
        if not totals["positions"]:
            return None
        return round(part / totals["positions"] * 100, 1)

    return {
        "positions": totals["positions"],
        "tnved_pct": pct(totals["tnved"]),
        "okpd2_pct": pct(totals["okpd2"]),
        "certificate_pct": pct(totals["certificate"]),
        "complete_pct": pct(totals["complete"]),
        "brands": sorted(
            (
                {
                    "brand": brand,
                    "positions": cell["positions"],
                    "missing": cell["missing"],
                }
                for brand, cell in by_brand.items()
                if cell["missing"]
            ),
            key=lambda item: -item["missing"],
        )[:50],
        "providers": sorted(
            (
                {
                    "provider": provider,
                    "positions": cell["positions"],
                    "missing": cell["missing"],
                }
                for provider, cell in by_provider.items()
            ),
            key=lambda item: -item["missing"],
        )[:50],
    }
