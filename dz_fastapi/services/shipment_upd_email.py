"""Отправка УПД клиенту на почту в формате Excel.

Письмо получают все клиенты — и те, кому документ уходит по ЭДО, и те, у
кого ЭДО нет. В реализации остаётся отметка: когда и на какой адрес.
Юридически значимый документ при этом — подписанный в ЭДО или бумажный.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.models.inventory import ShipmentDocument, ShipmentDocumentStatus
from dz_fastapi.services.email import build_email_delivery_kwargs, send_email_with_attachment

logger = logging.getLogger("dz_fastapi")

_UNSAFE = re.compile(r"[^0-9A-Za-zА-Яа-яЁё._-]+")


def _file_stem(number: object, shipment_id: int) -> str:
    text = _UNSAFE.sub("_", str(number or "").strip()).strip("_")
    return f"UPD_{text or shipment_id}"


async def send_shipment_upd_email(
    session: AsyncSession,
    shipment_id: int,
    *,
    to_email: Optional[str] = None,
    user_id: Optional[int] = None,
) -> dict[str, Any]:
    """Отправляет УПД в Excel на почту клиента. Коммит — на вызывающем."""
    from dz_fastapi.services.diadoc_outgoing import _load_shipment_document
    from dz_fastapi.services.shipment_upd_xlsx import render_upd_xlsx

    document = await _load_shipment_document(session, shipment_id=shipment_id)
    if document is None:
        raise LookupError("Реализация не найдена")
    if document.status != ShipmentDocumentStatus.POSTED:
        raise ValueError("Отправить можно только проведённую реализацию")
    customer = getattr(document, "customer", None)
    if customer is None:
        raise ValueError("В реализации не указан клиент")
    recipient = str(to_email or getattr(customer, "email_contact", None) or "").strip()
    if not recipient or "@" not in recipient:
        raise ValueError("У клиента не указан адрес электронной почты")

    accounts = await crud_email_account.get_active_by_purpose(session, "orders_out")
    account = accounts[0] if accounts else None
    if account is None:
        raise ValueError("Не настроен почтовый ящик для исходящих писем")

    number = document.doc_number or f"#{document.id}"
    date_text = document.doc_date.strftime("%d.%m.%Y") if document.doc_date else ""
    attachment = await render_upd_xlsx(session, document)
    file_name = f"{_file_stem(document.doc_number, document.id)}.xlsx"
    body = (
        f"<p>Здравствуйте!</p><p>Во вложении универсальный передаточный документ "
        f"№ {number} от {date_text} в формате Excel.</p>"
    )
    sent = await asyncio.to_thread(
        send_email_with_attachment,
        recipient,
        f"УПД № {number} от {date_text}",
        body,
        attachment,
        file_name,
        True,
        **build_email_delivery_kwargs(account),
    )
    if not sent:
        raise ValueError("Почтовый сервис не принял письмо — попробуйте ещё раз")

    shipment = await session.get(ShipmentDocument, shipment_id)
    shipment.upd_emailed_at = now_moscow()
    shipment.upd_emailed_to = recipient[:255]
    shipment.upd_emailed_by_user_id = user_id
    shipment.upd_email_error = None
    await session.flush()
    return {
        "shipment_id": shipment_id,
        "sent_to": recipient,
        "sent_at": shipment.upd_emailed_at,
        "file_name": file_name,
    }


async def auto_email_posted_shipments(
    session: AsyncSession,
    *,
    enabled: bool,
    since: Any,
    limit: int = 20,
) -> dict[str, Any]:
    """Рассылает УПД в Excel по реализациям, проведённым после включения.

    Письмо уходит каждому клиенту один раз. Причина неудачи (нет адреса,
    почта не приняла письмо) записывается в реализацию и видна в очереди;
    попытка повторится на следующем проходе.
    """
    from sqlalchemy import select

    result: dict[str, Any] = {"sent": 0, "failed": 0}
    if not enabled or since is None:
        result["skipped"] = "disabled"
        return result
    shipment_ids = list(
        (
            await session.execute(
                select(ShipmentDocument.id)
                .where(
                    ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
                    ShipmentDocument.posted_at >= since,
                    ShipmentDocument.customer_id.is_not(None),
                    ShipmentDocument.upd_emailed_at.is_(None),
                )
                .order_by(ShipmentDocument.id)
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    for shipment_id in shipment_ids:
        try:
            await send_shipment_upd_email(session, shipment_id)
            await session.commit()
            result["sent"] += 1
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            shipment = await session.get(ShipmentDocument, shipment_id)
            if shipment is not None:
                shipment.upd_email_error = str(exc)[:500]
            await session.commit()
            result["failed"] += 1
            if not isinstance(exc, (ValueError, LookupError)):
                logger.warning("Auto UPD email for shipment %s failed: %s", shipment_id, exc)
    return result
