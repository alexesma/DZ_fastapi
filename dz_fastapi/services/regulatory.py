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
import re
from collections import defaultdict
from datetime import date
from typing import Any, Iterable, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.models.certificate import (
    Certificate,
    TnvedOkpd2Match,
    autopart_certificate_association,
)
from dz_fastapi.models.nomenclature import HonestSignCategory
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation, Provider
from dz_fastapi.services.utils import (
    CERTIFICATION_NOT_REQUIRED_TEXT,
    is_certificate_expired,
    is_certificate_usable,
)

logger = logging.getLogger("dz_fastapi")

# Значение, которым нельзя затирать ручной ввод.
MANUAL_SOURCE = "manual"
# Документ пришёл из прайса поставщика: за ввезённый товар отвечает
# он, поэтому его документ важнее нашего собственного.
SUPPLIER_SOURCE = "supplier_file"

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


# asyncpg не принимает больше 32767 параметров на запрос, а IN-список
# строится из артикулов файла или id позиций прайса — там бывают десятки
# тысяч значений. Держим запас: часть параметров уходит на остальные
# условия запроса.
IN_CHUNK = 10000


def chunked(values: list, size: int = IN_CHUNK):
    """Режет список на порции для безопасного IN."""
    for start in range(0, len(values), size):
        yield values[start:start + size]


def normalize_brand_key(value: Optional[str]) -> str:
    """Ключ для сопоставления брендов между файлом и каталогом.

    Поставщики пишут бренд по-своему: «Hyundai/Kia» против нашего
    «HYUNDAI-KIA», «Master KiT» против «MASTERKIT». Сравнение по точному
    имени теряло такие позиции целиком, поэтому убираем регистр и все
    разделители.
    """
    return re.sub(r"[^0-9a-zа-яё]", "", str(value or "").lower())


# Значения, которыми поставщики отвечают «да» в колонке «Подключен к ЧЗ».
# Сам по себе флаг категорию не называет, поэтому по нему карточку не
# заполняем — только считаем, чтобы значение не пропадало молча.
_HONEST_SIGN_FLAGS = frozenset(
    {"да", "yes", "true", "1", "+", "нет", "no", "false", "0", "-"}
)


def _normalize_category_key(value: Optional[str]) -> str:
    return re.sub(r"[^0-9a-zа-яё]", "", str(value or "").lower())


async def load_honest_sign_index(
    session: AsyncSession,
) -> dict[str, int]:
    """Название категории Честного знака → id.

    Поставщики пишут в эту колонку либо флаг «да/нет», либо название
    категории. Название сопоставляем со справочником, флаг — нет: он не
    говорит, какой именно категории подлежит товар, а угадать её нельзя.
    """
    return {
        _normalize_category_key(name): int(category_id)
        for category_id, name in (
            await session.execute(
                select(HonestSignCategory.id, HonestSignCategory.name)
            )
        ).all()
        if _normalize_category_key(name)
    }


async def load_brand_groups(
    session: AsyncSession,
) -> tuple[dict[str, int], dict[int, int]]:
    """Группы синонимов бренда в двух видах.

    Первый — написание бренда → id основного бренда группы; по нему
    сопоставляются файлы поставщиков. Второй — id любого бренда → id
    основного; по нему сверяется, что бренд сертификата и бренд позиции
    это один и тот же бренд, а не два разных написания.

    «ЛУКОЙЛ» и «LUKOIL» — один бренд, но транслитерацией их не связать:
    «й» превращается и в i, и в y, и в j, а «точка опоры» в каталоге
    может оказаться переводом. Поэтому связь берём из brand_synonyms,
    где её завёл человек.
    """
    brands = [
        (int(brand_id), name, bool(main))
        for brand_id, name, main in (
            await session.execute(
                select(Brand.id, Brand.name, Brand.main_brand)
            )
        ).all()
    ]
    parent = {brand_id: brand_id for brand_id, _, _ in brands}

    def find(node: int) -> int:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    edges = (
        await session.execute(
            select(brand_synonyms.c.brand_id, brand_synonyms.c.synonym_id)
        )
    ).all()
    for left, right in edges:
        if int(left) in parent and int(right) in parent:
            parent[find(int(left))] = find(int(right))

    groups: dict[int, list[tuple[int, bool]]] = defaultdict(list)
    for brand_id, _, main in brands:
        groups[find(brand_id)].append((brand_id, main))

    # Каноничный бренд группы — помеченный основным; если основных нет
    # или их несколько, берём наименьший id, как и подбор кроссов.
    canonical: dict[int, int] = {}
    for root, members in groups.items():
        mains = sorted(item for item, main in members if main)
        canonical[root] = (
            mains[0] if mains else min(item for item, _ in members)
        )

    # Разные бренды могут дать один ключ («Master KiT» и «MASTERKIT»).
    # Если они при этом в разных группах, написание неоднозначно —
    # такой ключ выкидываем: молча выбрать один из брендов значит
    # приписать позициям чужой сертификат.
    by_key: dict[str, set[int]] = defaultdict(set)
    by_id: dict[int, int] = {}
    for brand_id, name, _ in brands:
        group = canonical[find(brand_id)]
        by_key[normalize_brand_key(name)].add(group)
        by_id[brand_id] = group
    unique = {
        key: next(iter(ids)) for key, ids in by_key.items() if len(ids) == 1
    }
    return unique, by_id


async def load_brand_key_index(session: AsyncSession) -> dict[str, int]:
    """Написание бренда → id основного бренда его группы синонимов."""
    by_key, _ = await load_brand_groups(session)
    return by_key


# Почему связь считается спорной. Первые две — блокирующие: такую связь
# не создаём и в прайс не выпускаем. Остальные показываем в отчёте:
# запретить их нельзя, иначе разом обнулится всё покрытие.
LINK_BRAND_MISMATCH = "brand_mismatch"
LINK_NOT_ACTIVE = "not_active"
LINK_BRAND_UNKNOWN = "brand_unknown"

BLOCKING_LINK_PROBLEMS = frozenset({LINK_BRAND_MISMATCH, LINK_NOT_ACTIVE})


def certificate_link_problems(
    part: AutoPart,
    certificate: Certificate,
    brand_groups: dict[int, int],
    today: Optional[date] = None,
) -> list[str]:
    """Что не так со связью позиции и документа.

    Главная проверка — бренд: клиент сверяет формальное соответствие
    бренда и сертификата первым делом, и документ на чужой бренд хуже
    пустой ячейки. Разные написания одного бренда считаем совпадением,
    для этого и нужны группы синонимов.
    """
    problems: list[str] = []
    if certificate.brand_id is None:
        problems.append(LINK_BRAND_UNKNOWN)
    elif brand_groups.get(certificate.brand_id, certificate.brand_id) != (
        brand_groups.get(part.brand_id, part.brand_id)
    ):
        problems.append(LINK_BRAND_MISMATCH)

    if not is_certificate_usable(
        certificate.valid_from,
        certificate.valid_until,
        certificate.status,
        today,
    ):
        problems.append(LINK_NOT_ACTIVE)
    # «В реестре не сверялся» сюда не входит: это свойство документа, а
    # не связи, и сейчас оно верно для всех до единой. В отчёте такой
    # признак превратил бы список в шум, поэтому он идёт отдельным
    # счётчиком.
    return problems


async def _load_autopart_index(
    session: AsyncSession,
    keys: Iterable[tuple[Optional[int], str]],
    brand_keys: dict[str, int],
) -> dict[tuple[int, str], int]:
    """(id основного бренда, нормализованный артикул) → id карточки."""
    articles = sorted({article for _, article in keys})
    if not articles:
        return {}
    index: dict[tuple[int, str], int] = {}
    for chunk in chunked(articles):
        stmt = (
            select(AutoPart.id, AutoPart.oem_number, Brand.name)
            .join(Brand, Brand.id == AutoPart.brand_id)
            .where(AutoPart.oem_number.in_(chunk))
        )
        for autopart_id, oem, brand_name in (
            await session.execute(stmt)
        ).all():
            brand_id = brand_keys.get(normalize_brand_key(brand_name))
            if brand_id is None:
                continue
            index[(brand_id, str(oem))] = int(autopart_id)
    return index


async def _upsert_certificates(
    session: AsyncSession,
    rows: list[dict[str, Any]],
    brand_keys: dict[str, int],
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
        entry["brands"].add(str(row.get("brand") or "").strip())
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
    result: dict[str, int] = {}
    for number, entry in wanted.items():
        certificate = existing.get(number)
        # Бренд проставляем, только когда он в файле однозначен: клиенты
        # сверяют формальное соответствие бренда и сертификата.
        brand_id = None
        source_brand = None
        if len(entry["brands"]) == 1:
            source_brand = next(iter(entry["brands"]))
            brand_id = brand_keys.get(normalize_brand_key(source_brand))
        if certificate is None:
            certificate = Certificate(
                number=number,
                url=entry["url"],
                brand_id=brand_id,
                source_brand=source_brand,
                source=SUPPLIER_SOURCE,
            )
            session.add(certificate)
        else:
            if entry["url"] and not certificate.url:
                certificate.url = entry["url"]
            if brand_id and not certificate.brand_id:
                certificate.brand_id = brand_id
            if source_brand and not certificate.source_brand:
                certificate.source_brand = source_brand
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
    brand_keys = await load_brand_key_index(session)
    prepared: list[tuple[tuple[Optional[int], str], dict[str, Any]]] = []
    for row in rows:
        key = (
            brand_keys.get(normalize_brand_key(row["brand"])),
            preprocess_oem_number(str(row["article"])),
        )
        prepared.append((key, row))

    index = await _load_autopart_index(
        session, [key for key, _ in prepared], brand_keys
    )

    honest_sign_index = await load_honest_sign_index(session)

    stats = {
        "rows": len(prepared),
        "matched": 0,
        "unmatched": 0,
        "updated": 0,
        "skipped_manual": 0,
        "unchanged": 0,
        "fields_filled": defaultdict(int),
        "unmatched_brands": defaultdict(int),
        # Значения колонки «Подключен к ЧЗ», которые не удалось разложить
        # по нашим категориям. Раньше они молча терялись.
        "honest_sign_linked": 0,
        "honest_sign_flag_only": 0,
        "honest_sign_unknown": defaultdict(int),
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
        stats["honest_sign_unknown"] = dict(stats["honest_sign_unknown"])
        return stats

    # Сертификаты заводим один раз на номер и связываем с позициями:
    # один документ покрывает сотни артикулов, дублировать его в каждой
    # карточке бессмысленно.
    certificate_ids = (
        {}
        if dry_run
        else await _upsert_certificates(
            session, list(targets.values()), brand_keys
        )
    )
    stats["certificates"] = len(certificate_ids)
    stats["links_created"] = 0

    parts = []
    for chunk in chunked(sorted(targets)):
        parts.extend(
            (
                await session.execute(
                    select(AutoPart)
                    .where(AutoPart.id.in_(chunk))
                    .options(
                        selectinload(AutoPart.certificates),
                        selectinload(AutoPart.honest_sign_categories),
                    )
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

        # Честный знак. У нас это категории маркировки, у поставщика в
        # той же колонке может стоять и название категории, и просто
        # «да». Название сопоставляем со справочником, флаг — считаем.
        honest_sign = (row.get("honest_sign") or "").strip()
        if honest_sign and not dry_run:
            if honest_sign.casefold() in _HONEST_SIGN_FLAGS:
                stats["honest_sign_flag_only"] += 1
            else:
                category_id = honest_sign_index.get(
                    _normalize_category_key(honest_sign)
                )
                if category_id is None:
                    stats["honest_sign_unknown"][honest_sign] += 1
                elif category_id not in {
                    item.id for item in part.honest_sign_categories
                }:
                    part.honest_sign_categories.append(
                        await session.get(HonestSignCategory, category_id)
                    )
                    part.honest_sign_category = ", ".join(
                        sorted(
                            item.name
                            for item in part.honest_sign_categories
                            if item.name
                        )
                    )[:100] or None
                    stats["honest_sign_linked"] += 1
                    session.add(part)
        elif honest_sign and dry_run:
            if honest_sign.casefold() in _HONEST_SIGN_FLAGS:
                stats["honest_sign_flag_only"] += 1
            elif _normalize_category_key(honest_sign) in honest_sign_index:
                stats["honest_sign_linked"] += 1
            else:
                stats["honest_sign_unknown"][honest_sign] += 1

        # Связь ставим независимо от того, менялись ли поля карточки:
        # позиция могла уже иметь номер, но не иметь связи с документом.
        if dry_run:
            # В предпросмотре сертификаты ещё не заведены, поэтому считаем
            # связи по номеру: иначе отчёт всегда показывал бы ноль.
            if cert_number and cert_number not in {
                item.number for item in part.certificates
            }:
                stats["links_created"] += 1
        elif cert_number:
            certificate_id = certificate_ids.get(cert_number)
            if certificate_id and certificate_id not in {
                item.id for item in part.certificates
            }:
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
    stats["honest_sign_unknown"] = dict(
        sorted(
            stats["honest_sign_unknown"].items(),
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


# Стандартная форма номера: «… С-BE.НВ07.В.00826/23» — до дефиса тип
# документа, сразу после дефиса двухбуквенный код страны, дальше орган и
# литеры. Только такой номер и нормализуем.
_CERT_STANDARD_RE = re.compile(r"^(.*?)-([A-Za-z]{2})\.(.+)$", re.DOTALL)


def normalize_certificate_number(value: str) -> str:
    """Приводит номер сертификата к кириллическому написанию.

    Код страны после дефиса (RU, BE, JP, CN) остаётся латиницей — это
    ISO-код, он так и печатается в бланке.

    Номера нестандартной формы возвращаются как есть. Раньше они
    переводились целиком, и белорусский «ЕАЭС BY/112 02.01. ТР018 …»
    превращался в «ЕАЭС ВУ/112 …»: латинский код страны BY уходил в
    кириллицу, и документ переставал находиться в реестре. Оставить
    номер нетронутым безопаснее, чем испортить.
    """
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    match = _CERT_STANDARD_RE.match(text)
    if not match:
        return text
    head, country, rest = match.groups()
    return (
        f"{head.translate(_CERT_HOMOGLYPHS)}-{country}."
        f"{rest.translate(_CERT_HOMOGLYPHS)}"
    )


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
    # Сменить бренд у документа, который уже покрывает позиции другого
    # бренда, нельзя: brand_id перепишется, а старые связи останутся, и
    # сертификат станет показывать один бренд, покрывая другой.
    if certificate is not None:
        foreign = (
            await session.execute(
                select(func.count())
                .select_from(autopart_certificate_association)
                .join(
                    AutoPart,
                    AutoPart.id
                    == autopart_certificate_association.c.autopart_id,
                )
                .where(
                    autopart_certificate_association.c.certificate_id
                    == certificate.id,
                    AutoPart.brand_id != brand_id,
                )
            )
        ).scalar_one()
        if foreign:
            raise ValueError(
                f"Сертификат уже привязан к {foreign} позициям другого "
                f"бренда. Сначала отвяжите их или заведите отдельный "
                f"документ."
            )

    if not dry_run:
        if certificate is None:
            certificate = Certificate(number=canonical, source="manual")
        certificate.url = url or certificate.url
        certificate.brand_id = brand_id
        certificate.covers_whole_brand = True
        if scope:
            certificate.scope = scope
        if valid_until:
            certificate.valid_until = valid_until
        session.add(certificate)
        await session.flush()
    # При dry_run объект не трогаем вовсе: загруженный из сессии
    # сертификат стал бы «грязным», и любой последующий commit в том же
    # запросе записал бы предпросмотр в базу.

    targets_stmt = select(AutoPart).where(
        AutoPart.brand_id == brand_id,
        # Ручной ввод не перетираем ни в каком режиме: интерфейс это
        # обещает, и обещание должно держаться и при снятом ограничении
        # «только позиции без признака».
        func.coalesce(AutoPart.regulatory_source, "") != MANUAL_SOURCE,
    )
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


async def refresh_autopart_certificate_cache(
    session: AsyncSession,
    autopart_ids: Iterable[int],
    *,
    commit: bool = True,
) -> int:
    """Пересобирает кэш сертификата на карточке из связей M2M.

    Поля eac_cert_* на карточке — денормализация ради быстрой выгрузки
    прайса, источник истины — связь. Любая правка связей или самого
    документа должна пройти через эту функцию, иначе в прайс уедет
    отвязанный или переименованный сертификат.

    Из нескольких привязанных документов выбираем действующий с самым
    поздним сроком; документ без срока считается действующим, но
    проигрывает документу с явной датой. Карточки с ручным вводом не
    трогаем — человек знает больше.
    """
    ids = sorted({int(item) for item in autopart_ids if item is not None})
    if not ids:
        return 0

    parts = []
    for chunk in chunked(ids):
        parts.extend(
            (
                await session.execute(
                    select(AutoPart)
                    .where(AutoPart.id.in_(chunk))
                    .options(selectinload(AutoPart.certificates))
                )
            )
            .scalars()
            .all()
        )
    today = date.today()
    _, brand_groups = await load_brand_groups(session)
    changed = 0
    for part in parts:
        if part.regulatory_source == MANUAL_SOURCE:
            continue
        # Последняя защита перед прайсом: связь могли создать в обход
        # проверок — миграцией, старым импортом, руками в базе.
        active = [
            item
            for item in part.certificates
            if not (
                set(
                    certificate_link_problems(
                        part, item, brand_groups, today
                    )
                )
                & BLOCKING_LINK_PROBLEMS
            )
        ]
        # Документ поставщика важнее нашего: поставщик отвечает за товар,
        # который сам и ввёз. Дальше — явный срок предпочтительнее
        # пустого, а при равенстве берём самый последний документ.
        best = max(
            active,
            key=lambda item: (
                item.source == SUPPLIER_SOURCE,
                item.valid_until is not None,
                item.valid_until or date.min,
                item.id,
            ),
            default=None,
        )
        number = best.number if best else None
        url = best.url if best else None
        valid_until = best.valid_until if best else None
        # Привязанный документ противоречит признаку «не требует»: в
        # выгрузке этот признак сильнее номера, и позиция уехала бы к
        # клиенту с текстом «Не требует сертификации», имея сертификат.
        # При отвязке признак не сбрасываем: то, что документа больше нет,
        # не означает, что товар сертификации не подлежит.
        required = True if best else part.certification_required
        if (
            part.eac_cert_number == number
            and part.eac_cert_url == url
            and part.eac_cert_valid_until == valid_until
            and part.certification_required == required
        ):
            continue
        part.eac_cert_number = number
        part.eac_cert_url = url
        part.eac_cert_valid_until = valid_until
        part.certification_required = required
        session.add(part)
        changed += 1
    if commit and changed:
        await session.commit()
    return changed


async def backfill_certificate_brands(
    session: AsyncSession,
    *,
    dry_run: bool = True,
) -> dict[str, int]:
    """Проставляет бренд сертификату по связанным позициям.

    При импорте бренд ставился только если в файле поставщика документ
    покрывал ровно один бренд. Но связи в каталоге дают ту же информацию
    точнее: если все привязанные позиции одного бренда, документ ему и
    принадлежит. Клиенты сверяют формальное соответствие бренда и
    сертификата, поэтому пустое поле здесь мешает.

    Неоднозначные (позиции нескольких брендов) не трогаем: угадывать
    нельзя, и такой документ действительно межбрендовый.
    """
    rows = (
        await session.execute(
            select(
                Certificate.id,
                func.min(AutoPart.brand_id),
                func.count(func.distinct(AutoPart.brand_id)),
            )
            .select_from(Certificate)
            .join(
                autopart_certificate_association,
                autopart_certificate_association.c.certificate_id
                == Certificate.id,
            )
            .join(
                AutoPart,
                AutoPart.id == autopart_certificate_association.c.autopart_id,
            )
            .where(Certificate.brand_id.is_(None))
            .group_by(Certificate.id)
        )
    ).all()

    stats = {"considered": len(rows), "updated": 0, "ambiguous": 0}
    for certificate_id, brand_id, brand_count in rows:
        if int(brand_count) != 1 or brand_id is None:
            stats["ambiguous"] += 1
            continue
        stats["updated"] += 1
        if not dry_run:
            certificate = await session.get(Certificate, int(certificate_id))
            certificate.brand_id = int(brand_id)
            session.add(certificate)
    if not dry_run and stats["updated"]:
        await session.commit()
    return stats


async def autopart_ids_for_certificate(
    session: AsyncSession,
    certificate_id: int,
) -> list[int]:
    return [
        int(row[0])
        for row in (
            await session.execute(
                select(autopart_certificate_association.c.autopart_id).where(
                    autopart_certificate_association.c.certificate_id
                    == certificate_id
                )
            )
        ).all()
    ]


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
            AutoPart.eac_cert_valid_until,
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
        "expired": 0,
        "undated": 0,
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
        # Истёкший документ не закрывает позицию: в прайс он не уедет.
        expired = is_certificate_expired(row.eac_cert_valid_until)
        has_cert = (
            (bool(row.eac_cert_number) and not expired)
            or row.certification_required is False
        )
        if expired:
            totals["expired"] += 1
        elif row.eac_cert_number and row.eac_cert_valid_until is None:
            # Срок не заполнен — документ выгружается, но не проверен.
            totals["undated"] += 1
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
        "expired_certificates": totals["expired"],
        "undated_certificates": totals["undated"],
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


async def suspicious_certificate_links(
    session: AsyncSession,
    *,
    limit: int = 500,
) -> dict[str, Any]:
    """Связи позиция-документ, не прошедшие проверку.

    Блокирующие в прайс не попадают, но и молча висеть в базе не должны:
    их нужно разобрать руками. Предупреждения — повод сверить документ с
    реестром, а не признак ошибки.
    """
    _, brand_groups = await load_brand_groups(session)
    today = date.today()
    rows = (
        await session.execute(
            select(AutoPart, Certificate, Brand.name)
            .select_from(autopart_certificate_association)
            .join(
                AutoPart,
                AutoPart.id
                == autopart_certificate_association.c.autopart_id,
            )
            .join(
                Certificate,
                Certificate.id
                == autopart_certificate_association.c.certificate_id,
            )
            .outerjoin(Brand, Brand.id == AutoPart.brand_id)
        )
    ).all()

    counts: dict[str, int] = defaultdict(int)
    items: list[dict[str, Any]] = []
    with_problems = 0
    unverified = {
        certificate.id
        for _, certificate, _ in rows
        if certificate.registry_checked_at is None
    }
    for part, certificate, brand_name in rows:
        problems = certificate_link_problems(
            part, certificate, brand_groups, today
        )
        if not problems:
            continue
        with_problems += 1
        for problem in problems:
            counts[problem] += 1
        if len(items) < limit:
            items.append(
                {
                    "autopart_id": part.id,
                    "oem_number": part.oem_number,
                    "name": part.name,
                    "brand_name": brand_name,
                    "certificate_id": certificate.id,
                    "number": certificate.number,
                    "problems": problems,
                    "blocking": bool(
                        set(problems) & BLOCKING_LINK_PROBLEMS
                    ),
                }
            )
    return {
        "links": len(rows),
        "with_problems": with_problems,
        "unverified_certificates": len(unverified),
        "blocking": sum(
            count
            for problem, count in counts.items()
            if problem in BLOCKING_LINK_PROBLEMS
        ),
        "by_problem": dict(counts),
        "items": items,
    }


# ── ОКПД 2 по ТН ВЭД ────────────────────────────────────────────────────

TNVED_COLUMN_ALIASES = ("тнвэд", "кодтнвэд", "тнвэдеаэс")
OKPD2_COLUMN_ALIASES = ("окпд2", "окпд", "кодокпд2")


def normalize_code(value: Optional[str]) -> str:
    """Код без пробелов, точек и прочих разделителей.

    В таблицах соответствия один и тот же код пишут и «8708 80 100 0»,
    и «8708801000», а ОКПД 2 — с точками. Сравниваем по цифрам.
    """
    return re.sub(r"[^0-9]", "", str(value or ""))


def parse_tnved_okpd2_file(
    content: bytes,
    *,
    delimiter: str = ";",
) -> list[dict[str, str]]:
    """Разбирает таблицу соответствия ТН ВЭД — ОКПД 2."""
    text: Optional[str] = None
    for candidate in ("utf-8-sig", "cp1251", "utf-8"):
        try:
            text = content.decode(candidate)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    if text is None:
        raise ValueError("Не удалось определить кодировку файла")

    reader = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    if not reader:
        return []
    header = [_normalize_header(cell) for cell in reader[0]]
    tnved_index = next(
        (i for i, cell in enumerate(header) if cell in TNVED_COLUMN_ALIASES),
        None,
    )
    okpd2_index = next(
        (i for i, cell in enumerate(header) if cell in OKPD2_COLUMN_ALIASES),
        None,
    )
    if tnved_index is None or okpd2_index is None:
        raise ValueError(
            "В файле не найдены колонки ТН ВЭД и ОКПД 2"
        )

    rows: list[dict[str, str]] = []
    for raw in reader[1:]:
        if len(raw) <= max(tnved_index, okpd2_index):
            continue
        tnved = normalize_code(raw[tnved_index])
        okpd2 = str(raw[okpd2_index] or "").strip()
        if not tnved or not okpd2:
            continue
        rows.append({"tnved_prefix": tnved, "okpd2_code": okpd2})
    return rows


async def import_tnved_okpd2_table(
    session: AsyncSession,
    rows: list[dict[str, str]],
    *,
    source: Optional[str] = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Загружает таблицу соответствия, не трогая уже загруженное."""
    wanted = {
        (row["tnved_prefix"], row["okpd2_code"]) for row in rows
    }
    if not wanted:
        return {"rows": 0, "created": 0, "existing": 0}

    existing = {
        (prefix, code)
        for prefix, code in (
            await session.execute(
                select(
                    TnvedOkpd2Match.tnved_prefix, TnvedOkpd2Match.okpd2_code
                )
            )
        ).all()
    }
    missing = sorted(wanted - existing)
    if not dry_run and missing:
        session.add_all(
            [
                TnvedOkpd2Match(
                    tnved_prefix=prefix, okpd2_code=code, source=source
                )
                for prefix, code in missing
            ]
        )
        await session.commit()
    return {
        "rows": len(rows),
        "created": len(missing),
        "existing": len(wanted) - len(missing),
    }


async def apply_okpd2_from_tnved(
    session: AsyncSession,
    *,
    dry_run: bool = True,
    only_empty: bool = True,
) -> dict[str, Any]:
    """Проставляет ОКПД 2 позициям, у которых заполнен ТН ВЭД.

    Соответствие один ко многим, поэтому заполняем только там, где по
    самому длинному подошедшему префиксу код ровно один. Неоднозначное
    считаем и оставляем человеку: выбрать за него значит поставить в
    прайс код, которого товар не касается.
    """
    by_prefix: dict[str, set[str]] = defaultdict(set)
    for prefix, code in (
        await session.execute(
            select(TnvedOkpd2Match.tnved_prefix, TnvedOkpd2Match.okpd2_code)
        )
    ).all():
        by_prefix[prefix].add(code)
    if not by_prefix:
        return {
            "table_rows": 0,
            "positions": 0,
            "updated": 0,
            "ambiguous": 0,
            "no_match": 0,
        }

    stmt = select(AutoPart).where(
        AutoPart.tnved_code.is_not(None),
        AutoPart.tnved_code != "",
        func.coalesce(AutoPart.regulatory_source, "") != MANUAL_SOURCE,
    )
    if only_empty:
        stmt = stmt.where(
            or_(AutoPart.okpd2_code.is_(None), AutoPart.okpd2_code == "")
        )
    parts = (await session.execute(stmt)).scalars().all()

    stats = {
        "table_rows": sum(len(codes) for codes in by_prefix.values()),
        "positions": len(parts),
        "updated": 0,
        "ambiguous": 0,
        "no_match": 0,
    }
    for part in parts:
        digits = normalize_code(part.tnved_code)
        codes: set[str] = set()
        # Самый длинный подошедший префикс точнее укрупнённого.
        for length in range(len(digits), 1, -1):
            candidate = by_prefix.get(digits[:length])
            if candidate:
                codes = candidate
                break
        if not codes:
            stats["no_match"] += 1
        elif len(codes) > 1:
            stats["ambiguous"] += 1
        else:
            stats["updated"] += 1
            if not dry_run:
                part.okpd2_code = next(iter(codes))
                session.add(part)
    if not dry_run and stats["updated"]:
        await session.commit()
    return stats
