"""Сверка сертификатов с реестрами ФГИС и SWIS.

Из прайсов поставщиков приходят номер и ссылка, а иногда только ссылка.
Для SWIS восстанавливаем номер из публичной карточки документа. Срок и
статус также берём из первоисточника, на который ведёт ссылка в прайсе.

Разбор ответа вынесен в отдельную функцию и намеренно не привязан к
конкретной вложенности: реестр меняет структуру ответа, а нам нужны
всего три значения. Незнакомый ответ не должен ронять сверку — документ
просто останется непроверенным.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.certificate import Certificate
from dz_fastapi.services.regulatory import (
    autopart_ids_for_certificate,
    refresh_autopart_certificate_cache,
)

logger = logging.getLogger("dz_fastapi")

FSA_HOST = "pub.fsa.gov.ru"
FSA_TIMEOUT = 20.0
# Реестр не любит частых обращений, а сверять нужно сотни документов.
FSA_PAUSE_SECONDS = 0.3
# Реестр либо доступен, либо нет: после нескольких подряд неудач
# продолжать бессмысленно, а таймауты складываются в минуты.
MAX_CONSECUTIVE_FAILURES = 5

SWIS_HOST = "swis.trade.kg"
SWIS_TIMEOUT = 15.0
SWIS_CONCURRENCY = 5

# https://pub.fsa.gov.ru/rss/certificate/view/3246778/baseInfo
# https://pub.fsa.gov.ru/rds/declaration/view/21352747/common
_FSA_REFERENCE = re.compile(r"pub\.fsa\.gov\.ru/(rss|rds)/(certificate|declaration)/view/(\d+)")

_SWIS_DOC_REFERENCE = re.compile(
    r"^/Doc/[0-9a-f-]+/?$",
    re.IGNORECASE,
)

_API_PATH = {
    "rss": "https://pub.fsa.gov.ru/api/v1/rss/common/certificates/{id}",
    "rds": "https://pub.fsa.gov.ru/api/v1/rds/common/declarations/{id}",
}

# Как реестр называет состояние документа.
_STATUS_MAP = {
    "действует": "active",
    "действителен": "active",
    "приостановлен": "suspended",
    "приостановлено": "suspended",
    "прекращен": "terminated",
    "прекращён": "terminated",
    "прекращено": "terminated",
    "аннулирован": "terminated",
    "архивный": "archived",
    "архивная": "archived",
}

# Названия полей меняются между разделами реестра, поэтому ищем по набору.
_FROM_KEYS = ("certregdate", "declregdate", "regdate", "datebegin", "startdate")
_UNTIL_KEYS = ("certenddate", "declenddate", "enddate", "dateend", "validuntil")
_STATUS_KEYS = ("status", "statusname", "certstatus", "declstatus")


def parse_registry_reference(url: Optional[str]) -> Optional[tuple[str, str]]:
    """(раздел реестра, идентификатор) из ссылки на карточку."""
    match = _FSA_REFERENCE.search(str(url or ""))
    if not match:
        return None
    return match.group(1), match.group(3)


def parse_swis_document_reference(url: Optional[str]) -> Optional[str]:
    """Публичная карточка документа SWIS, из которой можно взять номер."""
    try:
        parsed = urlparse(str(url or "").strip())
    except ValueError:
        return None
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or (parsed.hostname or "").casefold() != SWIS_HOST
        or not _SWIS_DOC_REFERENCE.fullmatch(parsed.path)
    ):
        return None
    return parsed.geturl()


class _SwisTableParser(HTMLParser):
    """Минимальный HTML-разборщик: SWIS отдаёт нужные поля парами td."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._depth = 0
        self._chunks: list[str] = []
        self.cells: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.casefold() == "td":
            if self._depth == 0:
                self._chunks = []
            self._depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() != "td" or self._depth == 0:
            return
        self._depth -= 1
        if self._depth == 0:
            self.cells.append(" ".join("".join(self._chunks).split()))

    def handle_data(self, data: str) -> None:
        if self._depth:
            self._chunks.append(data)


def extract_swis_document_fields(html: str) -> dict[str, Any]:
    """Номер, срок и состояние из публичной HTML-карточки SWIS."""
    parser = _SwisTableParser()
    try:
        parser.feed(str(html or ""))
    except Exception:  # pragma: no cover - HTMLParser почти не падает
        return {}

    values: dict[str, str] = {}
    for index in range(0, len(parser.cells) - 1):
        key = parser.cells[index].strip().casefold()
        if key in {
            "регистрационный номер документа",
            "дата начала действия",
            "дата окончания действия",
            "признак действия",
        }:
            values[key] = parser.cells[index + 1].strip()

    number = " ".join(values.get("регистрационный номер документа", "").split())
    if not number or "еаэс" not in number.casefold():
        return {}

    result: dict[str, Any] = {"number": number}
    valid_from = _as_date(values.get("дата начала действия"))
    valid_until = _as_date(values.get("дата окончания действия"))
    if valid_from:
        result["valid_from"] = valid_from
    if valid_until:
        result["valid_until"] = valid_until
    status_text = values.get("признак действия", "").casefold()
    for label, status in _STATUS_MAP.items():
        if label in status_text:
            result["status"] = status
            break
    return result


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z]", "", str(value).lower())


def _as_date(value: Any) -> Optional[date]:
    if value in (None, "", 0):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    # Реестр отдаёт и ISO, и «дд.мм.гггг», и метку времени в миллисекундах.
    if text.isdigit() and len(text) >= 10:
        try:
            seconds = int(text)
            if seconds > 10_000_000_000:
                seconds //= 1000
            return datetime.utcfromtimestamp(seconds).date()
        except (ValueError, OSError, OverflowError):
            return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[: len(fmt) + 2], fmt).date()
        except ValueError:
            continue
    return None


def _walk(payload: Any):
    """Все пары ключ-значение вложенного ответа."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _walk(item)


def extract_registry_fields(payload: Any) -> dict[str, Any]:
    """Срок и состояние документа из ответа реестра.

    Ищем по имени поля, а не по пути: разделы реестра называют одно и то
    же по-разному, а вложенность меняется от версии к версии.
    """
    found: dict[str, Any] = {}
    for key, value in _walk(payload):
        if not isinstance(key, str):
            continue
        name = _normalize_key(key)
        nested = isinstance(value, (dict, list))
        if name in _STATUS_KEYS and "status" not in found:
            # Состояние приходит и строкой, и объектом вида
            # {"status": {"name": "Действует"}} — собираем весь текст
            # ветки, иначе документ останется без статуса.
            text = (
                (
                    " ".join(str(item) for _, item in _walk(value) if isinstance(item, str))
                    if nested
                    else str(value or "")
                )
                .strip()
                .lower()
            )
            for label, code in _STATUS_MAP.items():
                if label in text:
                    found["status"] = code
                    break
            continue
        if nested:
            continue
        if name in _FROM_KEYS and "valid_from" not in found:
            parsed = _as_date(value)
            if parsed:
                found["valid_from"] = parsed
        elif name in _UNTIL_KEYS and "valid_until" not in found:
            parsed = _as_date(value)
            if parsed:
                found["valid_until"] = parsed
    return found


async def fetch_registry_document(
    client: httpx.AsyncClient,
    section: str,
    external_id: str,
) -> Optional[dict]:
    """Карточка документа из открытого API реестра."""
    template = _API_PATH.get(section)
    if not template:
        return None
    try:
        response = await client.get(template.format(id=external_id))
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError) as error:
        logger.warning(
            "Реестр не ответил по документу %s/%s: %s",
            section,
            external_id,
            error,
        )
        return None


async def fetch_swis_document(
    client: httpx.AsyncClient,
    url: str,
) -> Optional[dict[str, Any]]:
    """Публичная карточка SWIS; неизвестный HTML считается пустым ответом."""
    if parse_swis_document_reference(url) is None:
        return None
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.HTTPError as error:
        logger.warning("SWIS не ответил по документу %s: %s", url, error)
        return None
    fields = extract_swis_document_fields(response.text)
    if not fields:
        logger.warning("SWIS ответил, но номер документа не распознан: %s", url)
        return None
    return fields


async def resolve_swis_certificate_numbers(
    session: AsyncSession,
    *,
    limit: Optional[int] = 50,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Заполняет номера у карточек, где поставщик дал только SWIS URL.

    Один URL загружается один раз, даже если документ указан у сотен
    товаров. Ошибка внешнего сайта оставляет ссылку без номера и не
    мешает загрузке прайса или обработке остальных документов.
    """
    stmt = (
        select(AutoPart.eac_cert_url)
        .where(
            or_(
                AutoPart.eac_cert_number.is_(None),
                AutoPart.eac_cert_number == "",
            ),
            AutoPart.eac_cert_url.is_not(None),
            func.lower(AutoPart.eac_cert_url).contains("swis.trade.kg/doc/"),
        )
        .distinct()
        .order_by(AutoPart.eac_cert_url)
    )
    if limit:
        stmt = stmt.limit(limit)
    urls = [
        url for url in (await session.scalars(stmt)).all() if parse_swis_document_reference(url)
    ]
    stats = {
        "swis_candidates": len(urls),
        "swis_answered": 0,
        "swis_numbers_found": 0,
        "swis_cards_filled": 0,
    }
    if not urls:
        return stats

    headers = {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": (
            "Mozilla/5.0 (compatible; DZ-regulatory/1.0; " "+certificate-number-resolution)"
        ),
    }
    semaphore = asyncio.Semaphore(SWIS_CONCURRENCY)
    async with httpx.AsyncClient(
        timeout=SWIS_TIMEOUT, headers=headers, follow_redirects=True
    ) as client:

        async def fetch(url: str):
            async with semaphore:
                return url, await fetch_swis_document(client, url)

        fetched = await asyncio.gather(*(fetch(url) for url in urls))

    resolved = {url: fields for url, fields in fetched if fields}
    stats["swis_answered"] = len(resolved)
    stats["swis_numbers_found"] = len({str(fields["number"]) for fields in resolved.values()})
    if dry_run or not resolved:
        return stats

    numbers = {str(fields["number"]) for fields in resolved.values()}
    certificates = {
        item.number: item
        for item in (
            await session.scalars(select(Certificate).where(Certificate.number.in_(numbers)))
        ).all()
    }
    checked_at = now_moscow()
    for url, fields in resolved.items():
        number = str(fields["number"])
        certificate = certificates.get(number)
        if certificate is None:
            certificate = Certificate(
                number=number,
                url=url,
                source="registry",
            )
            session.add(certificate)
            certificates[number] = certificate
        elif not certificate.url:
            certificate.url = url
        for field in ("valid_from", "valid_until", "status"):
            value = fields.get(field)
            if value is not None:
                setattr(certificate, field, value)
        certificate.registry_checked_at = checked_at
        session.add(certificate)
    await session.flush()

    parts = (
        (
            await session.scalars(
                select(AutoPart)
                .where(
                    or_(
                        AutoPart.eac_cert_number.is_(None),
                        AutoPart.eac_cert_number == "",
                    ),
                    AutoPart.eac_cert_url.in_(resolved),
                )
                .options(selectinload(AutoPart.certificates))
            )
        )
        .unique()
        .all()
    )
    touched: set[int] = set()
    for part in parts:
        fields = resolved.get(str(part.eac_cert_url))
        if not fields:
            continue
        certificate = certificates[str(fields["number"])]
        if certificate.id not in {item.id for item in part.certificates}:
            part.certificates.append(certificate)
        part.certification_required = True
        part.regulatory_source = "supplier_doc"
        session.add(part)
        touched.add(part.id)
    await session.commit()

    if touched:
        await refresh_autopart_certificate_cache(session, touched)
    stats["swis_cards_filled"] = len(touched)
    return stats


async def refresh_certificates_from_registry(
    session: AsyncSession,
    *,
    only_linked: bool = True,
    only_unchecked: bool = True,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Проставляет срок и состояние документов из реестра.

    По умолчанию сверяем только те, что реально покрывают позиции и ещё
    не проверялись: реестр отвечает медленно, а документов в справочнике
    сотни.
    """
    swis_stats = await resolve_swis_certificate_numbers(
        session,
        limit=limit,
        dry_run=dry_run,
    )
    stmt = select(Certificate).where(Certificate.url.is_not(None))
    if only_unchecked:
        stmt = stmt.where(Certificate.registry_checked_at.is_(None))
    if only_linked:
        stmt = stmt.where(
            or_(
                Certificate.autoparts.any(),
                Certificate.covers_whole_brand.is_(True),
            )
        )
    certificates = (await session.execute(stmt)).unique().scalars().all()

    targets = []
    for certificate in certificates:
        reference = parse_registry_reference(certificate.url)
        if reference:
            targets.append((certificate, *reference))
    if limit:
        targets = targets[:limit]

    stats = {
        **swis_stats,
        "candidates": len(certificates),
        "supported": len(targets),
        "answered": 0,
        "updated": 0,
        "dated": 0,
        "not_active": 0,
        "dry_run": dry_run,
        "cards_refreshed": 0,
        "aborted": False,
    }
    touched: list[int] = []
    failures = 0
    if not targets:
        return stats

    headers = {
        "Accept": "application/json",
        # Реестр отдаёт JSON только браузероподобным клиентам.
        "User-Agent": ("Mozilla/5.0 (compatible; DZ-regulatory/1.0; " "+registry-verification)"),
    }
    async with httpx.AsyncClient(
        timeout=FSA_TIMEOUT, headers=headers, follow_redirects=True
    ) as client:
        for index, (certificate, section, external_id) in enumerate(targets):
            if index:
                await asyncio.sleep(FSA_PAUSE_SECONDS)
            payload = await fetch_registry_document(client, section, external_id)
            if payload is None:
                failures += 1
                # Реестр может быть недоступен целиком (на сервере вне
                # РФ он не открывается вовсе). Ждать таймаут по каждому
                # из сотен документов бессмысленно — выходим.
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    stats["aborted"] = True
                    logger.warning(
                        "Реестр не отвечает подряд %s раз — сверка " "остановлена",
                        failures,
                    )
                    break
                continue
            failures = 0
            stats["answered"] += 1
            fields = extract_registry_fields(payload)
            if not fields:
                logger.info(
                    "Реестр ответил, но срок и статус не распознаны: %s",
                    certificate.number,
                )
                continue
            if fields.get("valid_until"):
                stats["dated"] += 1
            if fields.get("status") and fields["status"] != "active":
                stats["not_active"] += 1
            if dry_run:
                continue
            changed = False
            for field, value in fields.items():
                if getattr(certificate, field) != value:
                    setattr(certificate, field, value)
                    changed = True
            certificate.registry_checked_at = now_moscow()
            session.add(certificate)
            if changed:
                stats["updated"] += 1
                touched.append(certificate.id)
    if dry_run:
        return stats

    await session.commit()
    # Кэш на карточках собран из сроков: документ, который перестал
    # действовать, обязан из него уйти, иначе номер останется в прайсе.
    affected: set[int] = set()
    for certificate_id in touched:
        affected.update(await autopart_ids_for_certificate(session, certificate_id))
    if affected:
        stats["cards_refreshed"] = await refresh_autopart_certificate_cache(session, affected)
    return stats
