"""Очередь исходящих писем (EmailOutbox).

На проде исходящие SMTP-порты закрыты хостером, а ответы клиентам должны
уходить С адреса Яндекс-ящика (требование клиента). Поэтому письма кладутся
в очередь, а внешний скрипт-релей (на машине с открытыми портами) забирает
их по HTTPS и отправляет через smtp.yandex.ru:465.

Здесь — постановка письма в очередь и контракт для релея:
list_pending / mark_sent / mark_error.
"""
import asyncio
import base64
import logging
import os
from datetime import timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import raiseload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import (
    EMAIL_OUTBOX_STATUS,
    CustomerPriceList,
    CustomerPriceListConfig,
    EmailOutbox,
)
from dz_fastapi.services.reclamation_audit import record_reclamation_event

logger = logging.getLogger("dz_fastapi")

MAX_SEND_ATTEMPTS = 5
# Признаки того, что до почтового сервера не достучаться (сеть, VPN,
# блокировка провайдера). Это не вина письма: такие неудачи не расходуют
# попытки отправки, иначе за время недоступности сети письма выгорают в
# статус «ошибка» и после починки их приходится возвращать вручную.
NETWORK_UNREACHABLE_MARKERS = (
    "timed out",
    "timeouterror",
    "network is unreachable",
    "no route to host",
    "connection refused",
    "connection reset",
    "name or service not known",
    "temporary failure in name resolution",
    "nodename nor servname",
)


def is_network_unreachable_error(error: str | None) -> bool:
    text = str(error or "").casefold()
    return any(marker in text for marker in NETWORK_UNREACHABLE_MARKERS)


# Аренда захвата: если воркер «умер», не отметив письмо, через это время
# письмо снова доступно для захвата другим воркером.
DEFAULT_CLAIM_LEASE_SECONDS = 300
MAX_RELAY_ATTACHMENT_BYTES = max(
    1,
    int(os.getenv("EMAIL_RELAY_MAX_ATTACHMENT_MB", "25")),
) * 1024 * 1024
MAX_RELAY_TOTAL_ATTACHMENT_BYTES = max(
    1,
    int(os.getenv("EMAIL_RELAY_MAX_TOTAL_ATTACHMENT_MB", "50")),
) * 1024 * 1024


def _allowed_attachment_roots() -> list[str]:
    configured = [
        value.strip()
        for value in str(
            os.getenv("EMAIL_OUTBOX_ATTACHMENT_ROOTS") or ""
        ).split(os.pathsep)
        if value.strip()
    ]
    configured.append("uploads")
    return [os.path.realpath(value) for value in configured]


def _safe_attachment_path(value: object) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    resolved = os.path.realpath(raw)
    for root in _allowed_attachment_roots():
        try:
            if os.path.commonpath((resolved, root)) == root:
                return resolved
        except ValueError:
            continue
    return None


def serialize_outbox_for_relay(row: EmailOutbox) -> dict[str, Any]:
    """Builds a relay payload without exposing local filesystem paths."""
    serialized_attachments: list[dict[str, Any]] = []
    attachment_errors: list[str] = []
    total_size = 0
    for raw_attachment in list(row.attachments or []):
        attachment = dict(raw_attachment or {})
        filename = str(
            attachment.get("filename")
            or attachment.get("file_name")
            or "attachment"
        )
        content_base64 = attachment.get("content_base64")
        if content_base64:
            try:
                payload = base64.b64decode(content_base64, validate=True)
            except (ValueError, TypeError):
                attachment_errors.append(
                    f"{filename}: повреждено содержимое base64"
                )
                continue
        else:
            local_path = _safe_attachment_path(
                attachment.get("local_file_path")
            )
            if local_path is None:
                attachment_errors.append(
                    f"{filename}: недопустимый путь к файлу"
                )
                continue
            try:
                size = os.path.getsize(local_path)
                if size > MAX_RELAY_ATTACHMENT_BYTES:
                    attachment_errors.append(
                        f"{filename}: файл больше допустимого размера"
                    )
                    continue
                with open(local_path, "rb") as file_handle:
                    payload = file_handle.read(
                        MAX_RELAY_ATTACHMENT_BYTES + 1
                    )
            except OSError as exc:
                attachment_errors.append(f"{filename}: {exc}")
                continue
            if len(payload) > MAX_RELAY_ATTACHMENT_BYTES:
                attachment_errors.append(
                    f"{filename}: файл больше допустимого размера"
                )
                continue
            content_base64 = base64.b64encode(payload).decode("ascii")

        total_size += len(payload)
        if total_size > MAX_RELAY_TOTAL_ATTACHMENT_BYTES:
            attachment_errors.append(
                "Общий размер вложений превышает допустимый лимит"
            )
            break
        serialized_attachments.append(
            {
                "filename": filename,
                "content_type": attachment.get("content_type"),
                "content_base64": content_base64,
            }
        )

    return {
        "id": row.id,
        "status": str(getattr(row.status, "value", row.status)),
        "from_email": row.from_email,
        "to_email": row.to_email,
        "subject": row.subject,
        "body_text": row.body_text,
        "body_html": row.body_html,
        "in_reply_to": row.in_reply_to,
        "references": row.references,
        "reply_to": row.reply_to,
        "source_type": row.source_type,
        "source_id": row.source_id,
        "attempts": int(row.attempts or 0),
        "last_error": row.last_error,
        "sent_at": row.sent_at,
        "created_at": row.created_at,
        "attachments": serialized_attachments,
        "attachment_errors": attachment_errors,
    }


async def enqueue_email(
    session: AsyncSession,
    *,
    to_email: str,
    from_email: Optional[str] = None,
    subject: Optional[str] = None,
    body_text: Optional[str] = None,
    body_html: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    reply_to: Optional[str] = None,
    attachments: Optional[list[dict[str, Any]]] = None,
    source_type: Optional[str] = None,
    source_id: Optional[int] = None,
    commit: bool = True,
) -> EmailOutbox:
    if not to_email:
        raise ValueError("Не задан адрес получателя")
    row = EmailOutbox(
        status=EMAIL_OUTBOX_STATUS.PENDING,
        to_email=to_email,
        from_email=from_email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        in_reply_to=in_reply_to,
        references=references,
        reply_to=reply_to,
        attachments=attachments or [],
        source_type=source_type,
        source_id=source_id,
        attempts=0,
    )
    session.add(row)
    if commit:
        await session.commit()
        await session.refresh(row)
    else:
        await session.flush()
    return row


async def cancel_superseded_customer_pricelist_outbox(
    session: AsyncSession,
    *,
    customer_config_id: int,
    current_pricelist_id: int,
) -> int:
    """Cancel unclaimed letters for older files of the same configuration."""
    old_pricelist_ids = list(
        (
            await session.scalars(
                select(CustomerPriceList.id).where(
                    CustomerPriceList.customer_config_id == customer_config_id,
                    CustomerPriceList.id != current_pricelist_id,
                )
            )
        ).all()
    )
    if not old_pricelist_ids:
        return 0

    rows = list(
        (
            await session.scalars(
                select(EmailOutbox)
                .where(
                    EmailOutbox.source_type == "customer_pricelist",
                    EmailOutbox.source_id.in_(old_pricelist_ids),
                    EmailOutbox.status == EMAIL_OUTBOX_STATUS.PENDING,
                    EmailOutbox.claimed_at.is_(None),
                )
                .with_for_update(skip_locked=True)
            )
        ).all()
    )
    if not rows:
        return 0

    affected_pricelist_ids: set[int] = set()
    for row in rows:
        row.status = EMAIL_OUTBOX_STATUS.CANCELLED
        row.last_error = "Отменено: сформирован более новый прайс-лист"
        affected_pricelist_ids.add(int(row.source_id))
        session.add(row)

    superseded_at = now_moscow()
    affected_pricelists = list(
        (
            await session.scalars(
                select(CustomerPriceList)
                .options(raiseload("*"))
                .where(CustomerPriceList.id.in_(affected_pricelist_ids))
            )
        ).all()
    )
    for pricelist in affected_pricelists:
        summary = dict(pricelist.generation_summary or {})
        delivery = dict(summary.get("delivery") or {})
        delivery.update(
            {
                "status": "superseded",
                "superseded_at": superseded_at.isoformat(),
                "superseded_by_pricelist_id": int(current_pricelist_id),
            }
        )
        summary["delivery"] = delivery
        pricelist.generation_summary = summary
        pricelist.generation_status = "superseded"
        pricelist.decision_reason = "Автоматически заменён более новым прайс-листом"
        pricelist.send_error = None
        session.add(pricelist)

    await session.flush()
    return len(rows)


async def record_customer_pricelist_delivery_channel(
    session: AsyncSession,
    *,
    customer_pricelist_id: int,
    kind: str,
    status: str,
    from_email: str | None,
    transport: str | None = None,
    account_id: int | None = None,
    recipients: list[str] | None = None,
    error: str | None = None,
) -> None:
    """Persist one delivery channel without adding another database table.

    Direct API sends do not pass through EmailOutbox, so previously the UI
    could only see the pending SMTP relay half of a multi-sender delivery.
    The compact audit below lives with the exact generated file and is later
    combined with recipient-level EmailOutbox state by ``_sync...``.
    """
    pricelist = await session.get(CustomerPriceList, int(customer_pricelist_id))
    if pricelist is None:
        return
    summary = dict(pricelist.generation_summary or {})
    delivery = dict(summary.get("delivery") or {})
    channels = [dict(item) for item in delivery.get("channels") or []]
    channel_key = (
        str(kind or ""),
        int(account_id) if account_id is not None else None,
        str(from_email or "").casefold(),
    )
    channels = [
        item
        for item in channels
        if (
            str(item.get("kind") or ""),
            int(item["account_id"]) if item.get("account_id") is not None else None,
            str(item.get("from_email") or "").casefold(),
        )
        != channel_key
    ]
    now = now_moscow()
    channel = {
        "kind": str(kind),
        "status": str(status),
        "from_email": from_email,
        "transport": transport,
        "account_id": account_id,
        "recipients": list(recipients or []),
        "updated_at": now.isoformat(),
    }
    if status == "sent":
        channel["sent_at"] = now.isoformat()
        pricelist.published_at = pricelist.published_at or now
    if error:
        channel["error"] = str(error)[:2000]
    channels.append(channel)
    delivery["channels"] = channels
    delivery["status"] = str(status)
    delivery["updated_at"] = now.isoformat()
    summary["delivery"] = delivery
    pricelist.generation_summary = summary
    session.add(pricelist)
    await session.flush()


def customer_pricelist_has_successful_delivery(
    pricelist: CustomerPriceList,
) -> bool:
    """Return whether at least one recorded channel accepted the file."""
    summary = pricelist.generation_summary or {}
    delivery = summary.get("delivery") or {}
    return any(
        item.get("status") == "sent"
        for item in delivery.get("channels") or []
        if isinstance(item, dict)
    )


async def list_pending_outbox(
    session: AsyncSession, *, limit: int = 50
) -> list[EmailOutbox]:
    rows = (
        await session.execute(
            select(EmailOutbox)
            .where(EmailOutbox.status == EMAIL_OUTBOX_STATUS.PENDING)
            .order_by(EmailOutbox.id.asc())
            .limit(max(1, min(int(limit or 50), 200)))
        )
    ).scalars().all()
    return list(rows)


async def list_outbox_for_source(
    session: AsyncSession, *, source_type: str, source_id: int
) -> list[EmailOutbox]:
    rows = (
        await session.execute(
            select(EmailOutbox)
            .where(
                EmailOutbox.source_type == source_type,
                EmailOutbox.source_id == int(source_id),
            )
            .order_by(EmailOutbox.id.desc())
        )
    ).scalars().all()
    return list(rows)


async def claim_pending_outbox(
    session: AsyncSession,
    *,
    worker: str,
    limit: int = 25,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
) -> list[EmailOutbox]:
    """Атомарно «захватывает» pending-письма за воркером, чтобы несколько
    релеев не отправили одно письмо дважды. Берёт письма, у которых нет
    активного захвата (claimed_at пуст или аренда истекла), помечает их
    claimed_by/claimed_at и возвращает. Использует SKIP LOCKED."""
    now = now_moscow()
    cutoff = now - timedelta(seconds=max(30, int(lease_seconds)))
    limit = max(1, min(int(limit or 25), 200))
    locked = (
        await session.execute(
            select(EmailOutbox.id)
            .where(
                EmailOutbox.status == EMAIL_OUTBOX_STATUS.PENDING,
                or_(
                    EmailOutbox.claimed_at.is_(None),
                    EmailOutbox.claimed_at < cutoff,
                ),
            )
            .order_by(EmailOutbox.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()
    if not locked:
        return []
    rows = (
        await session.execute(
            select(EmailOutbox).where(EmailOutbox.id.in_(locked))
        )
    ).scalars().all()
    for row in rows:
        row.claimed_by = (worker or "relay")[:128]
        row.claimed_at = now
        session.add(row)
    await session.commit()
    for row in rows:
        await session.refresh(row)
    return list(rows)


async def mark_outbox_sent(
    session: AsyncSession,
    *,
    outbox_id: int,
    worker: str | None = None,
) -> EmailOutbox:
    row = await session.get(EmailOutbox, outbox_id)
    if row is None:
        raise ValueError("Письмо не найдено")
    if row.status == EMAIL_OUTBOX_STATUS.SENT:
        return row
    if worker and row.claimed_by != worker:
        raise ValueError("Захват письма принадлежит другому relay")
    row.status = EMAIL_OUTBOX_STATUS.SENT
    row.sent_at = now_moscow()
    row.attempts = int(row.attempts or 0) + 1
    row.last_error = None
    row.claimed_by = None
    row.claimed_at = None
    session.add(row)
    if row.source_type == "customer_pricelist" and row.source_id:
        await _sync_customer_pricelist_delivery_status(
            session, customer_pricelist_id=int(row.source_id)
        )
    if row.source_type in {"reclamation", "reclamation_supplier"} and row.source_id:
        await record_reclamation_event(
            session,
            reclamation_id=int(row.source_id),
            event_type="email_sent",
            details={
                "outbox_id": int(row.id),
                "recipient": row.to_email,
                "source_type": row.source_type,
            },
        )
    await session.commit()
    await session.refresh(row)
    if row.source_type == "reclamation" and row.source_id:
        await mark_reclamation_source_answered(
            session,
            reclamation_id=int(row.source_id),
            outbox_id=int(row.id),
            from_email=row.from_email,
            sent_at=row.sent_at,
        )
    return row


async def mark_outbox_error(
    session: AsyncSession,
    *,
    outbox_id: int,
    error: str,
    retry: bool = True,
    worker: str | None = None,
) -> EmailOutbox:
    row = await session.get(EmailOutbox, outbox_id)
    if row is None:
        raise ValueError("Письмо не найдено")
    if row.status == EMAIL_OUTBOX_STATUS.SENT:
        # SMTP мог пройти, а HTTP-ответ mark-sent потеряться. Никогда не
        # возвращаем уже отправленное письмо в очередь: иначе уйдёт дубль.
        return row
    if worker and row.claimed_by != worker:
        raise ValueError("Захват письма принадлежит другому relay")
    network_failure = retry and is_network_unreachable_error(error)
    if not network_failure:
        row.attempts = int(row.attempts or 0) + 1
    row.last_error = (error or "")[:2000]
    # Снимаем захват: письмо снова свободно для повторного взятия релеем
    row.claimed_by = None
    row.claimed_at = None
    # Пока не исчерпаны попытки и разрешён ретрай — оставляем pending
    if retry and (network_failure or row.attempts < MAX_SEND_ATTEMPTS):
        row.status = EMAIL_OUTBOX_STATUS.PENDING
    else:
        row.status = EMAIL_OUTBOX_STATUS.ERROR
    session.add(row)
    if row.source_type == "customer_pricelist" and row.source_id:
        await _sync_customer_pricelist_delivery_status(
            session, customer_pricelist_id=int(row.source_id)
        )
    if (
        row.status == EMAIL_OUTBOX_STATUS.ERROR
        and row.source_type in {"reclamation", "reclamation_supplier"}
        and row.source_id
    ):
        await record_reclamation_event(
            session,
            reclamation_id=int(row.source_id),
            event_type="email_send_failed",
            details={
                "outbox_id": int(row.id),
                "recipient": row.to_email,
                "source_type": row.source_type,
                "error": row.last_error,
            },
        )
    await session.commit()
    await session.refresh(row)
    return row


async def renew_outbox_claim(
    session: AsyncSession,
    *,
    outbox_id: int,
    worker: str,
) -> EmailOutbox:
    row = (
        await session.scalars(
            select(EmailOutbox)
            .where(EmailOutbox.id == outbox_id)
            .with_for_update()
        )
    ).first()
    if row is None:
        raise ValueError("Письмо не найдено")
    if row.status != EMAIL_OUTBOX_STATUS.PENDING:
        raise ValueError("Письмо уже не ожидает отправки")
    if row.claimed_by != worker:
        raise ValueError("Захват письма принадлежит другому relay")
    row.claimed_at = now_moscow()
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def _sync_customer_pricelist_delivery_status(
    session: AsyncSession, *, customer_pricelist_id: int
) -> None:
    """Сводит статус прайса по всем отдельным письмам его получателям."""
    await session.flush()
    customer_pricelist = (
        await session.scalars(
            select(CustomerPriceList)
            .where(CustomerPriceList.id == customer_pricelist_id)
            .with_for_update(of=CustomerPriceList)
        )
    ).first()
    if customer_pricelist is None:
        return
    rows = list(
        (
            await session.scalars(
                select(EmailOutbox).where(
                    EmailOutbox.source_type == "customer_pricelist",
                    EmailOutbox.source_id == customer_pricelist_id,
                )
            )
        ).all()
    )
    if not rows:
        return

    summary = dict(customer_pricelist.generation_summary or {})
    delivery = dict(summary.get("delivery") or {})
    direct_channels = [
        dict(item)
        for item in delivery.get("channels") or []
        if item.get("kind") != "relay"
    ]
    relay_channels: list[dict[str, Any]] = []
    rows_by_sender: dict[str, list[EmailOutbox]] = {}
    for item in rows:
        rows_by_sender.setdefault(str(item.from_email or "relay"), []).append(item)
    for sender, sender_rows in rows_by_sender.items():
        statuses = {
            str(getattr(item.status, "value", item.status)) for item in sender_rows
        }
        if EMAIL_OUTBOX_STATUS.PENDING.value in statuses:
            channel_status = "queued"
        elif EMAIL_OUTBOX_STATUS.ERROR.value in statuses:
            channel_status = "failed"
        elif statuses == {EMAIL_OUTBOX_STATUS.CANCELLED.value}:
            channel_status = "cancelled"
        else:
            channel_status = "sent"
        relay_channels.append(
            {
                "kind": "relay",
                "status": channel_status,
                "from_email": sender,
                "transport": "smtp_relay",
                "recipients": [
                    {
                        "email": item.to_email,
                        "status": str(getattr(item.status, "value", item.status)),
                        "attempts": int(item.attempts or 0),
                        "error": item.last_error,
                        "sent_at": item.sent_at.isoformat() if item.sent_at else None,
                    }
                    for item in sender_rows
                ],
                "updated_at": now_moscow().isoformat(),
            }
        )
    delivery["channels"] = direct_channels + relay_channels
    summary["delivery"] = delivery
    customer_pricelist.generation_summary = summary

    pending = [
        item for item in rows if item.status == EMAIL_OUTBOX_STATUS.PENDING
    ]
    failed = [
        item for item in rows if item.status == EMAIL_OUTBOX_STATUS.ERROR
    ]
    direct_sent = any(
        item.get("status") == "sent" for item in direct_channels
    )
    relay_sent = any(
        item.status == EMAIL_OUTBOX_STATUS.SENT for item in rows
    )
    any_sent = direct_sent or relay_sent
    if pending:
        customer_pricelist.generation_status = (
            "partially_sent" if any_sent else "queued"
        )
        customer_pricelist.sent_at = None
        customer_pricelist.send_error = None
        delivery["status"] = customer_pricelist.generation_status
    elif failed:
        customer_pricelist.generation_status = (
            "partially_sent" if any_sent else "send_failed"
        )
        customer_pricelist.sent_at = None
        customer_pricelist.send_error = "; ".join(
            f"{item.to_email}: {item.last_error or 'ошибка отправки'}"
            for item in failed
        )[:2000]
        delivery["status"] = customer_pricelist.generation_status
    elif any_sent:
        customer_pricelist.generation_status = "sent"
        sent_dates = [item.sent_at for item in rows if item.sent_at is not None]
        customer_pricelist.sent_at = max(sent_dates) if sent_dates else now_moscow()
        customer_pricelist.published_at = (
            customer_pricelist.published_at or customer_pricelist.sent_at
        )
        customer_pricelist.send_error = None
        delivery["status"] = "sent"
        if customer_pricelist.customer_config_id:
            config = await session.get(
                CustomerPriceListConfig,
                int(customer_pricelist.customer_config_id),
            )
            if config is not None:
                config.last_sent_at = customer_pricelist.sent_at
                session.add(config)
    else:
        # All recipient rows can be cancelled when a newer file supersedes
        # this one. Cancellation is not a successful delivery.
        customer_pricelist.generation_status = "superseded"
        customer_pricelist.sent_at = None
        customer_pricelist.send_error = None
        delivery["status"] = "superseded"
    delivery["updated_at"] = now_moscow().isoformat()
    summary["delivery"] = delivery
    customer_pricelist.generation_summary = summary
    session.add(customer_pricelist)


def _flag_source_email_answered_sync(
    *,
    host: str,
    port: int,
    email: str,
    password: str,
    folder: str,
    uid: str,
) -> None:
    from dz_fastapi.services.email import _create_mailbox

    mailbox_client = _create_mailbox(host, port, True).login(email, password)
    with mailbox_client as mailbox:
        mailbox.folder.set(folder)
        mailbox.flag(
            uid,
            [r"\Seen", r"\Answered", r"\Flagged"],
            True,
        )


async def mark_reclamation_source_answered(
    session: AsyncSession,
    *,
    reclamation_id: int,
    outbox_id: int,
    from_email: Optional[str],
    sent_at,
) -> None:
    from dz_fastapi.models.email_account import EmailAccount
    from dz_fastapi.models.partner import Reclamation

    reclamation = await session.get(Reclamation, reclamation_id)
    if reclamation is None:
        return
    extracted_data = dict(reclamation.extracted_data or {})
    mailbox_data = dict(extracted_data.get("mailbox") or {})
    mailbox_data.update(
        {
            "reply_outbox_id": outbox_id,
            "reply_sent_at": sent_at.isoformat() if sent_at else None,
        }
    )
    uid = str(mailbox_data.get("uid") or "").strip()
    account_id = mailbox_data.get("email_account_id")
    account = (
        await session.get(EmailAccount, int(account_id))
        if account_id
        else None
    )
    if account is None and from_email:
        account = (
            await session.execute(
                select(EmailAccount).where(
                    EmailAccount.email == from_email,
                    EmailAccount.is_active.is_(True),
                )
            )
        ).scalar_one_or_none()
    if account is None or not uid:
        mailbox_data["answered_flag_status"] = "unavailable"
        mailbox_data["answered_flag_error"] = (
            "Не сохранён UID исходного письма или почтовый аккаунт"
        )
    else:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    _flag_source_email_answered_sync,
                    host=str(account.imap_host or ""),
                    port=int(account.imap_port or 993),
                    email=str(account.email),
                    password=str(account.password),
                    folder=str(mailbox_data.get("folder") or "INBOX"),
                    uid=uid,
                ),
                timeout=20,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Ответ отправлен, но исходное письмо рекламации #%s "
                "не помечено Answered: %s",
                reclamation_id,
                exc,
            )
            mailbox_data["answered_flag_status"] = "error"
            mailbox_data["answered_flag_error"] = str(exc)[:1000]
        else:
            mailbox_data["answered_flag_status"] = "marked"
            mailbox_data["answered_flagged_at"] = now_moscow().isoformat()
            mailbox_data.pop("answered_flag_error", None)
    extracted_data["mailbox"] = mailbox_data
    reclamation.extracted_data = extracted_data
    session.add(reclamation)
    await session.commit()


async def cancel_outbox(
    session: AsyncSession, *, outbox_id: int
) -> EmailOutbox:
    row = await session.get(EmailOutbox, outbox_id)
    if row is None:
        raise ValueError("Письмо не найдено")
    if row.status == EMAIL_OUTBOX_STATUS.SENT:
        raise ValueError("Письмо уже отправлено")
    row.status = EMAIL_OUTBOX_STATUS.CANCELLED
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row
