"""Направление входящего документа Диадока: поставщик или клиент.

Один контрагент (например, ФРОЗА МСК) может быть и нашим поставщиком, и
нашим клиентом, тогда ящик привязан к обеим записям. Привязка не говорит,
кто в конкретном документе продавец, поэтому смотрим реквизиты самого
документа (УПД/УКД/КСЧФ XML) и сравниваем с ИНН нашей организации из
настроек интеграции.

* покупатель = мы, продавец — контрагент → "supplier" (поступление);
* продавец = мы, покупатель — контрагент → "customer" (документ по нашей
  продаже; поступление не создаётся);
* иначе → "unknown": автоматически не обрабатываем.
Без разбора XML направление берём из привязок: только поставщик (или
привязки нет) — "supplier", только клиент — "customer", обе — "unknown".
"""

from __future__ import annotations

import os
import re
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.diadoc_outgoing import resolve_customer_by_diadoc_counteragent_box
from dz_fastapi.services.upd_xml import extract_document_parties

_PAYMENT_INVOICE_NAME_RE = re.compile(r"^\s*сч[её]т", re.IGNORECASE)

DIRECTION_SUPPLIER = "supplier"
DIRECTION_CUSTOMER = "customer"
DIRECTION_UNKNOWN = "unknown"


async def get_our_inn(session: AsyncSession, environment: str) -> Optional[str]:
    row = (
        (
            await session.execute(
                select(DiadocIntegrationSettings)
                .where(DiadocIntegrationSettings.environment == environment)
                .order_by(DiadocIntegrationSettings.id.desc())
            )
        )
        .scalars()
        .first()
    )
    inn = str(getattr(row, "organization_inn", "") or "").strip()
    return inn or None


def direction_from_parties(
    parties, our_inn: Optional[str]
) -> Optional[str]:
    """None — по реквизитам решить нельзя (нет XML или нашего ИНН)."""
    if parties is None or not our_inn:
        return None
    seller_is_us = parties.seller_inn == our_inn
    buyer_is_us = parties.buyer_inn == our_inn
    if buyer_is_us and not seller_is_us:
        return DIRECTION_SUPPLIER
    if seller_is_us and not buyer_is_us:
        return DIRECTION_CUSTOMER
    return DIRECTION_UNKNOWN


async def detect_document_direction(
    session: AsyncSession, document: DiadocIncomingDocument
) -> str:
    """Определяет и сохраняет direction/customer_id документа."""
    parties = None
    path = document.local_file_path
    if path and os.path.exists(os.path.abspath(path)):
        with open(os.path.abspath(path), "rb") as fh:
            parties = extract_document_parties(fh.read())
    our_inn = await get_our_inn(session, document.environment)
    customer = await resolve_customer_by_diadoc_counteragent_box(
        session, document.counteragent_box_id
    )
    direction = direction_from_parties(parties, our_inn)
    if direction is None:
        has_provider = bool(document.provider_id)
        if (
            customer is not None
            and has_provider
            and _PAYMENT_INVOICE_NAME_RE.match(str(document.file_name or ""))
        ):
            # Входящий «Счёт на оплату» выставляют нам, а не мы клиенту.
            direction = DIRECTION_SUPPLIER
        elif customer is not None and has_provider:
            direction = DIRECTION_UNKNOWN
        elif customer is not None:
            direction = DIRECTION_CUSTOMER
        else:
            direction = DIRECTION_SUPPLIER
    document.direction = direction
    document.customer_id = (
        int(customer.id)
        if customer is not None and direction == DIRECTION_CUSTOMER
        else None
    )
    session.add(document)
    return direction


async def has_customer_binding(
    session: AsyncSession, document: DiadocIncomingDocument
) -> bool:
    customer = await resolve_customer_by_diadoc_counteragent_box(
        session, document.counteragent_box_id
    )
    return customer is not None
