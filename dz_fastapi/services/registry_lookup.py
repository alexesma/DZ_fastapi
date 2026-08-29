"""Сверка сертификатов с реестром ФГИС Росаккредитации.

Из прайсов поставщиков приходят только номер и ссылка: сроков действия
и состояния документа там нет вовсе (545 документов — 0 дат). Поэтому
срок и статус берём из первоисточника — карточки реестра, на которую и
так ведёт ссылка в прайсе.

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
from typing import Any, Optional

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
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

# https://pub.fsa.gov.ru/rss/certificate/view/3246778/baseInfo
# https://pub.fsa.gov.ru/rds/declaration/view/21352747/common
_FSA_REFERENCE = re.compile(
    r"pub\.fsa\.gov\.ru/(rss|rds)/(certificate|declaration)/view/(\d+)"
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
                " ".join(
                    str(item)
                    for _, item in _walk(value)
                    if isinstance(item, str)
                )
                if nested
                else str(value or "")
            ).strip().lower()
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
        "User-Agent": (
            "Mozilla/5.0 (compatible; DZ-regulatory/1.0; "
            "+registry-verification)"
        ),
    }
    async with httpx.AsyncClient(
        timeout=FSA_TIMEOUT, headers=headers, follow_redirects=True
    ) as client:
        for index, (certificate, section, external_id) in enumerate(targets):
            if index:
                await asyncio.sleep(FSA_PAUSE_SECONDS)
            payload = await fetch_registry_document(
                client, section, external_id
            )
            if payload is None:
                failures += 1
                # Реестр может быть недоступен целиком (на сервере вне
                # РФ он не открывается вовсе). Ждать таймаут по каждому
                # из сотен документов бессмысленно — выходим.
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    stats["aborted"] = True
                    logger.warning(
                        "Реестр не отвечает подряд %s раз — сверка "
                        "остановлена",
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
        affected.update(
            await autopart_ids_for_certificate(session, certificate_id)
        )
    if affected:
        stats["cards_refreshed"] = await refresh_autopart_certificate_cache(
            session, affected
        )
    return stats
