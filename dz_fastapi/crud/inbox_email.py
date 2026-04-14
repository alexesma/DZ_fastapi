import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inbox_email import EmailRulePattern, InboxEmail

logger = logging.getLogger('dz_fastapi')


# ---------------------------------------------------------------------------
# InboxEmail CRUD
# ---------------------------------------------------------------------------

async def get_inbox_email(
    session: AsyncSession, email_id: int
) -> Optional[InboxEmail]:
    result = await session.execute(
        select(InboxEmail).where(InboxEmail.id == email_id)
    )
    return result.scalars().first()


async def list_inbox_emails(
    session: AsyncSession,
    *,
    email_account_id: Optional[int] = None,
    days: int = 3,
    page: int = 1,
    page_size: int = 50,
    only_unprocessed: bool = False,
) -> Tuple[List[InboxEmail], int]:
    """Список писем с фильтрацией по ящику и глубине (дни)."""
    since = now_moscow() - timedelta(days=days)
    filters = [InboxEmail.fetched_at >= since]

    if email_account_id is not None:
        filters.append(InboxEmail.email_account_id == email_account_id)
    if only_unprocessed:
        filters.append(InboxEmail.rule_type.is_(None))

    total_result = await session.execute(
        select(func.count()).select_from(InboxEmail).where(and_(*filters))
    )
    total = total_result.scalar_one()

    items_result = await session.execute(
        select(InboxEmail)
        .where(and_(*filters))
        .order_by(InboxEmail.received_at.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return items_result.scalars().all(), total


async def exists_inbox_email(
    session: AsyncSession,
    *,
    email_account_id: int,
    uid: str,
    folder: Optional[str] = None,
) -> bool:
    """Проверка: письмо с таким uid уже есть в БД."""
    filters = [
        InboxEmail.email_account_id == email_account_id,
        InboxEmail.uid == uid,
    ]
    if folder is not None:
        filters.append(InboxEmail.folder == folder)
    result = await session.execute(
        select(func.count())
        .select_from(InboxEmail)
        .where(and_(*filters))
    )
    return result.scalar_one() > 0


async def create_inbox_email(
    session: AsyncSession,
    *,
    email_account_id: int,
    uid: Optional[str],
    folder: Optional[str],
    from_email: str,
    from_name: Optional[str],
    subject: Optional[str],
    body_preview: Optional[str],
    body_full: Optional[str],
    has_attachments: bool,
    attachment_info: list,
    received_at: Optional[datetime],
) -> InboxEmail:
    obj = InboxEmail(
        email_account_id=email_account_id,
        uid=uid,
        folder=folder,
        from_email=from_email,
        from_name=from_name,
        subject=subject,
        body_preview=body_preview,
        body_full=body_full,
        has_attachments=has_attachments,
        attachment_info=attachment_info,
        received_at=received_at,
        fetched_at=now_moscow(),
    )
    session.add(obj)
    await session.flush()
    await session.refresh(obj)
    return obj


async def update_inbox_email_rule(
    session: AsyncSession,
    *,
    email: InboxEmail,
    rule_type: str,
    rule_set_by_id: Optional[int] = None,
    auto_detected: bool = False,
) -> InboxEmail:
    email.rule_type = rule_type
    email.rule_set_at = now_moscow()
    email.rule_set_by_id = rule_set_by_id
    email.rule_auto_detected = auto_detected
    session.add(email)
    await session.flush()
    return email


async def mark_processed(
    session: AsyncSession,
    *,
    email: InboxEmail,
    result: Optional[dict] = None,
    error: Optional[str] = None,
) -> InboxEmail:
    email.processed = True
    email.processed_at = now_moscow()
    email.processing_result = result
    email.processing_error = error
    session.add(email)
    await session.flush()
    return email


async def cleanup_old_inbox_emails(
    session: AsyncSession, max_days: int = 7
) -> int:
    """Удаляет письма старше max_days дней. Возвращает кол-во удалённых."""
    cutoff = now_moscow() - timedelta(days=max_days)
    result = await session.execute(
        delete(InboxEmail).where(InboxEmail.fetched_at < cutoff)
    )
    await session.commit()
    return result.rowcount


# ---------------------------------------------------------------------------
# EmailRulePattern CRUD
# ---------------------------------------------------------------------------

async def list_rule_patterns(
    session: AsyncSession,
    *,
    email_account_id: Optional[int] = None,
    rule_type: Optional[str] = None,
    active_only: bool = True,
) -> List[EmailRulePattern]:
    filters = []
    if active_only:
        filters.append(EmailRulePattern.is_active.is_(True))
    if email_account_id is not None:
        filters.append(
            (EmailRulePattern.email_account_id == email_account_id)
            | (EmailRulePattern.email_account_id.is_(None))
        )
    if rule_type is not None:
        filters.append(EmailRulePattern.rule_type == rule_type)

    result = await session.execute(
        select(EmailRulePattern)
        .where(and_(*filters) if filters else True)
        .order_by(EmailRulePattern.times_applied.desc())
    )
    return result.scalars().all()


async def get_rule_pattern(
    session: AsyncSession, pattern_id: int
) -> Optional[EmailRulePattern]:
    result = await session.execute(
        select(EmailRulePattern).where(EmailRulePattern.id == pattern_id)
    )
    return result.scalars().first()


async def create_rule_pattern(
    session: AsyncSession,
    *,
    email_account_id: Optional[int],
    from_email_pattern: Optional[str],
    from_domain_pattern: Optional[str],
    subject_keywords: List[str],
    requires_attachments: Optional[bool],
    attachment_extensions: List[str],
    rule_type: str,
    created_by_id: Optional[int],
) -> EmailRulePattern:
    obj = EmailRulePattern(
        email_account_id=email_account_id,
        from_email_pattern=from_email_pattern,
        from_domain_pattern=from_domain_pattern,
        subject_keywords=subject_keywords,
        requires_attachments=requires_attachments,
        attachment_extensions=attachment_extensions,
        rule_type=rule_type,
        created_by_id=created_by_id,
    )
    session.add(obj)
    await session.flush()
    await session.refresh(obj)
    return obj


async def update_rule_pattern(
    session: AsyncSession,
    *,
    pattern: EmailRulePattern,
    **kwargs,
) -> EmailRulePattern:
    for key, value in kwargs.items():
        if hasattr(pattern, key) and value is not None:
            setattr(pattern, key, value)
    session.add(pattern)
    await session.flush()
    return pattern


async def increment_pattern_applied(
    session: AsyncSession, pattern: EmailRulePattern
) -> None:
    pattern.times_applied = (pattern.times_applied or 0) + 1
    session.add(pattern)
    await session.flush()


async def increment_pattern_confirmed(
    session: AsyncSession, pattern: EmailRulePattern
) -> None:
    pattern.times_confirmed = (pattern.times_confirmed or 0) + 1
    session.add(pattern)
    await session.flush()


async def delete_rule_pattern(
    session: AsyncSession, pattern_id: int
) -> bool:
    pattern = await get_rule_pattern(session, pattern_id)
    if not pattern:
        return False
    session.delete(pattern)
    await session.commit()
    return True


async def find_matching_pattern(
    session: AsyncSession,
    *,
    email_account_id: int,
    from_email: str,
    subject: str,
    has_attachments: bool,
    attachment_extensions: List[str],
) -> Optional[EmailRulePattern]:
    """
    Ищет наиболее подходящий паттерн для входящего письма.
    Возвращает первый совпавший паттерн (сортировка по times_applied desc).
    """
    patterns = await list_rule_patterns(
        session,
        email_account_id=email_account_id,
        active_only=True,
    )

    from_domain = (
        from_email.split('@')[-1].lower() if '@' in from_email else ''
    )
    subject_lower = subject.lower() if subject else ''

    for pattern in patterns:
        # Проверка from_email
        if pattern.from_email_pattern:
            if pattern.from_email_pattern.lower() != from_email.lower():
                continue

        # Проверка домена отправителя
        if pattern.from_domain_pattern:
            if pattern.from_domain_pattern.lower() != from_domain:
                continue

        # Проверка ключевых слов в теме
        keywords = pattern.subject_keywords or []
        if keywords and not all(
            kw.lower() in subject_lower for kw in keywords
        ):
            continue

        # Проверка наличия вложений
        if pattern.requires_attachments is not None:
            if pattern.requires_attachments != has_attachments:
                continue

        # Проверка расширений вложений
        ext_list = pattern.attachment_extensions or []
        if ext_list and attachment_extensions:
            if not any(ext in attachment_extensions for ext in ext_list):
                continue

        return pattern  # первый совпавший

    return None
