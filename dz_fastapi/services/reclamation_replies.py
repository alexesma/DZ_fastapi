"""Этап 5: ответы по рекламациям через очередь EmailOutbox.

- Ответ клиенту уходит С адреса ящика рекламаций (Яндекс) — через очередь и
  внешний релей, потому что SMTP-порты на проде закрыты.
- Запрос поставщику уходит на provider.return_request_email для позиций,
  которые пришли транзитом.
"""
import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import RECLAMATION_STATUS, EmailOutbox, Provider, Reclamation
from dz_fastapi.services.email_outbox import enqueue_email
from dz_fastapi.services.reclamation_armtek import ArmtekPortalError, parse_armtek_return_url
from dz_fastapi.services.reclamation_froza import is_froza_question_url
from dz_fastapi.services.reclamations import get_reclamation_account

logger = logging.getLogger("dz_fastapi")

OUTBOX_SOURCE_CUSTOMER = "reclamation"
OUTBOX_SOURCE_SUPPLIER = "reclamation_supplier"

# Шаблоны ответа клиенту по виду решения
REPLY_KINDS = ("ack", "approved", "rejected", "request_documents")


def _items_block(rec: Reclamation) -> str:
    lines = []
    for it in rec.items or []:
        parts = [p for p in (it.brand_name, it.oem_number) if p]
        title = " ".join(parts) if parts else (it.autopart_name or "позиция")
        lines.append(f"  • {title} — {int(it.quantity or 1)} шт.")
    return "\n".join(lines)


def _doc_phrase(rec: Reclamation) -> str:
    if rec.stated_document_number:
        phrase = f"по документу {rec.stated_document_number}"
        if rec.stated_document_date:
            phrase += f" от {rec.stated_document_date.strftime('%d.%m.%Y')}"
        return phrase
    return "по вашему обращению"


def _missing_document_labels(rec: Reclamation) -> list[str]:
    documents = (rec.check_result or {}).get("documents") or {}
    labels = documents.get("missing_labels") or []
    cleaned = [str(label).strip() for label in labels if str(label).strip()]
    required = [
        "Заказ-наряд на установку",
        "Заказ-наряд на снятие",
        "Акт дефектовки",
    ]
    if not cleaned:
        return [*required, "Фотографии запчасти"]

    result = list(required)
    seen = {label.lower() for label in result}
    for label in cleaned:
        normalized = label.lower()
        if any(
            marker in normalized
            for marker in ("установ", "снят", "дефект")
        ):
            continue
        if normalized not in seen:
            result.append(label)
            seen.add(normalized)
    return result


def build_customer_reply_template(
    rec: Reclamation,
    kind: str,
    *,
    resolution_comment: Optional[str] = None,
) -> tuple[str, str]:
    """Возвращает (subject, body_text) шаблона ответа клиенту."""
    base_subject = rec.email_subject or "рекламация"
    subject = (
        base_subject
        if base_subject.lower().startswith("re:")
        else f"Re: {base_subject}"
    )
    items = _items_block(rec)
    doc = _doc_phrase(rec)
    reason = (
        resolution_comment
        if resolution_comment is not None
        else rec.resolution_comment
    )
    reason = str(reason or "").strip()

    if kind == "approved":
        body = (
            f"Здравствуйте!\n\n"
            f"Возврат по вашей рекламации {doc} согласован.\n\n"
            f"Позиции к возврату:\n{items}\n\n"
            f"Просим оформить возвратные документы и при ближайшей "
            f"возможности передать товар нашему водителю.\n\n"
            f"С уважением,\nотдел рекламаций"
        )
    elif kind == "rejected":
        body = (
            f"Здравствуйте!\n\n"
            f"К сожалению, согласовать возврат по вашей рекламации {doc} "
            f"не представляется возможным.\n\n"
            f"Позиции:\n{items}\n\n"
            f"{('Причина отказа: ' + reason + '.' + chr(10) + chr(10)) if reason else ''}"
            f"Если у вас есть дополнительные документы или уточнения — "
            f"пришлите их в ответ на это письмо.\n\n"
            f"С уважением,\nотдел рекламаций"
        )
    elif kind == "request_documents":
        requested_documents = "\n".join(
            f"  • {label};" for label in _missing_document_labels(rec)
        )
        body = (
            f"Здравствуйте!\n\n"
            f"Для рассмотрения рекламации по браку {doc} просим предоставить"
            f" комплект документов:\n"
            f"{requested_documents}\n\n"
            f"Позиции:\n{items}\n\n"
            f"После получения документов мы продолжим рассмотрение.\n\n"
            f"С уважением,\nотдел рекламаций"
        )
    else:  # ack — подтверждение получения
        body = (
            f"Здравствуйте!\n\n"
            f"Ваша рекламация {doc} получена и зарегистрирована"
            f" (№ {rec.id}). Мы рассмотрим её и вернёмся с решением.\n\n"
            f"Позиции:\n{items}\n\n"
            f"С уважением,\nотдел рекламаций"
        )
    return subject, body


async def _load_reclamation(session: AsyncSession, reclamation_id: int):
    rec = (
        await session.execute(
            select(Reclamation)
            .where(Reclamation.id == reclamation_id)
            .options(selectinload(Reclamation.items))
        )
    ).scalar_one_or_none()
    if rec is None:
        raise ValueError("Рекламация не найдена")
    return rec


def _is_armtek_return_url(source_link: Optional[str]) -> bool:
    try:
        parse_armtek_return_url(source_link)
    except ArmtekPortalError:
        return False
    return True


def _ensure_email_reply_allowed(rec: Reclamation) -> None:
    if is_froza_question_url(rec.source_link):
        raise ValueError(
            "Для рекламации Froza ответ передаётся через портал, а не "
            "электронной почтой. Используйте действие «Ответить клиенту "
            "во Froza»."
        )
    if _is_armtek_return_url(rec.source_link):
        raise ValueError(
            "Для рекламации Armtek ответ передаётся через портал, а не "
            "электронной почтой."
        )
    if not rec.sender_email:
        raise ValueError("У рекламации нет адреса отправителя для ответа")


async def _enqueue_loaded_customer_reply(
    session: AsyncSession,
    *,
    rec: Reclamation,
    subject: Optional[str],
    body_text: Optional[str],
    kind: Optional[str],
    commit: bool,
) -> EmailOutbox:
    _ensure_email_reply_allowed(rec)
    account = await get_reclamation_account(session)
    from_email = getattr(account, "email", None)

    if (not subject or not body_text) and kind:
        tpl_subject, tpl_body = build_customer_reply_template(rec, kind)
        subject = subject or tpl_subject
        body_text = body_text or tpl_body
    if not body_text:
        raise ValueError("Пустой текст письма")

    references = rec.email_message_id or None
    return await enqueue_email(
        session,
        to_email=rec.sender_email,
        from_email=from_email,
        subject=subject or f"Re: рекламация № {rec.id}",
        body_text=body_text,
        in_reply_to=rec.email_message_id,
        references=references,
        reply_to=from_email,
        source_type=OUTBOX_SOURCE_CUSTOMER,
        source_id=int(rec.id),
        commit=commit,
    )


async def enqueue_customer_reply(
    session: AsyncSession,
    *,
    reclamation_id: int,
    subject: Optional[str] = None,
    body_text: Optional[str] = None,
    kind: Optional[str] = None,
) -> EmailOutbox:
    rec = await _load_reclamation(session, reclamation_id)
    return await _enqueue_loaded_customer_reply(
        session,
        rec=rec,
        subject=subject,
        body_text=body_text,
        kind=kind,
        commit=True,
    )


async def apply_and_enqueue_customer_reply(
    session: AsyncSession,
    *,
    reclamation_id: int,
    action: str,
    resolution_comment: Optional[str],
    resolved_by_user_id: Optional[int],
    subject: Optional[str] = None,
    body_text: Optional[str] = None,
) -> EmailOutbox:
    """Atomically apply a manager action and queue the customer response."""
    rec = await _load_reclamation(session, reclamation_id)
    if action not in {"approved", "rejected", "request_documents"}:
        raise ValueError("Недопустимое действие по рекламации")

    comment = str(resolution_comment or "").strip()
    if action == "rejected" and not comment:
        raise ValueError("Для отказа обязательно укажите причину")
    if action == "request_documents" and rec.resolution:
        raise ValueError(
            "По рекламации уже сохранено итоговое решение; "
            "запрос документов невозможен"
        )

    if action == "request_documents":
        rec.status = RECLAMATION_STATUS.WAITING_DOCS
    else:
        rec.resolution = action
        rec.resolution_comment = comment or None
        rec.resolved_at = now_moscow()
        rec.resolved_by_user_id = resolved_by_user_id
        rec.status = (
            RECLAMATION_STATUS.APPROVED
            if action == "approved"
            else RECLAMATION_STATUS.REJECTED
        )
    session.add(rec)

    row = await _enqueue_loaded_customer_reply(
        session,
        rec=rec,
        subject=subject,
        body_text=body_text,
        kind=action,
        commit=False,
    )
    await session.commit()
    await session.refresh(row)
    return row


async def enqueue_supplier_request(
    session: AsyncSession,
    *,
    reclamation_id: int,
) -> list[EmailOutbox]:
    rec = await _load_reclamation(session, reclamation_id)

    # Группируем транзитные позиции по поставщику
    by_provider: dict[int, list] = {}
    for it in rec.items or []:
        source = str(getattr(it.item_source, "value", it.item_source))
        if source != "supplier_transit" or not it.source_provider_id:
            continue
        by_provider.setdefault(int(it.source_provider_id), []).append(it)

    if not by_provider:
        raise ValueError(
            "Нет позиций-транзита с указанным поставщиком. Отметьте источник"
            " «Транзит поставщика» и поставщика в позициях."
        )

    account = await get_reclamation_account(session)
    from_email = getattr(account, "email", None)
    created: list[EmailOutbox] = []
    for provider_id, items in by_provider.items():
        provider = await session.get(Provider, provider_id)
        if provider is None or not provider.return_request_email:
            logger.warning(
                "Поставщик %s без return_request_email — пропуск", provider_id
            )
            continue
        lines = []
        for it in items:
            parts = [p for p in (it.brand_name, it.oem_number) if p]
            title = " ".join(parts) if parts else (it.autopart_name or "позиция")
            lines.append(f"  • {title} — {int(it.quantity or 1)} шт.")
        doc = _doc_phrase(rec)
        body = (
            f"Здравствуйте!\n\n"
            f"Клиент вернул товар {doc}, поставленный транзитом от вас. "
            f"Просим согласовать возврат следующих позиций:\n"
            f"{chr(10).join(lines)}\n\n"
            f"Просьба подтвердить возможность и условия возврата.\n\n"
            f"С уважением,\nотдел рекламаций"
        )
        row = await enqueue_email(
            session,
            to_email=provider.return_request_email,
            from_email=from_email,
            subject=f"Запрос на возврат по рекламации № {rec.id}",
            body_text=body,
            reply_to=from_email,
            source_type=OUTBOX_SOURCE_SUPPLIER,
            source_id=int(rec.id),
        )
        created.append(row)

    if not created:
        raise ValueError(
            "У поставщиков транзитных позиций не заполнен email для запросов"
            " возврата (return_request_email)."
        )
    return created
