from sqlalchemy import (JSON, Boolean, Column, DateTime, ForeignKey, Integer,
                        String, Text)

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class InboxEmail(Base):
    """Временное хранилище входящих писем для ручной разметки правилами."""

    email_account_id = Column(
        Integer,
        ForeignKey('emailaccount.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    # Уникальный идентификатор письма на сервере (IMAP UID / Resend ID)
    uid = Column(String(255), nullable=True)
    folder = Column(String(255), nullable=True)  # INBOX, Sent, etc.

    from_email = Column(String(255), nullable=False, index=True)
    from_name = Column(String(255), nullable=True)
    subject = Column(String(1000), nullable=True, default='')
    body_preview = Column(String(500), nullable=True)   # первые ~300 символов
    body_full = Column(Text, nullable=True)

    has_attachments = Column(Boolean, default=False)
    # [{name: str, size: int, path: str | null}]
    attachment_info = Column(JSON, default=list)

    received_at = Column(DateTime(timezone=True), nullable=True)
    fetched_at = Column(DateTime(timezone=True), default=now_moscow)

    # --- Система правил ---
    # null = не размечено, 'price_list' / 'order_reply' / 'ignore'
    rule_type = Column(String(64), nullable=True, index=True)
    rule_set_at = Column(DateTime(timezone=True), nullable=True)
    rule_set_by_id = Column(
        Integer, ForeignKey('app_user.id', ondelete='SET NULL'), nullable=True
    )
    # True — правило назначено системой автоматически
    rule_auto_detected = Column(Boolean, default=False)

    processed = Column(Boolean, default=False, index=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processing_result = Column(JSON, nullable=True)
    processing_error = Column(String(1000), nullable=True)


class EmailRulePattern(Base):
    """
    Паттерн для автоматического определения правила входящего письма.
    Создаётся/обновляется когда менеджер вручную назначает правило письму.
    """

    # null = применяется ко всем почтовым ящикам
    email_account_id = Column(
        Integer,
        ForeignKey('emailaccount.id', ondelete='CASCADE'),
        nullable=True,
        index=True,
    )

    # Критерии совпадения
    from_email_pattern = Column(
        String(255), nullable=True
    )   # точный email
    from_domain_pattern = Column(
        String(255), nullable=True
    )  # домен отправителя
    # ключевые слова в теме письма (все должны присутствовать)
    subject_keywords = Column(JSON, default=list)
    # None = не важно, True/False = наличие вложений обязательно/запрещено
    requires_attachments = Column(Boolean, nullable=True)
    # ['.xlsx', '.csv', '.xls'] — расширения вложений
    attachment_extensions = Column(JSON, default=list)

    # Правило, которое нужно применить
    rule_type = Column(String(64), nullable=False)

    # Статистика
    times_applied = Column(Integer, default=0)
    times_confirmed = Column(Integer, default=0)  # вручную подтверждено

    created_by_id = Column(
        Integer, ForeignKey('app_user.id', ondelete='SET NULL'), nullable=True
    )
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )
    is_active = Column(Boolean, default=True)


class InboxForceProcessAudit(Base):
    __tablename__ = 'inbox_force_process_audit'

    inbox_email_id = Column(
        Integer,
        ForeignKey('inboxemail.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    requested_by_user_id = Column(
        Integer,
        ForeignKey('app_user.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    rule_type = Column(String(64), nullable=False)
    mode = Column(String(16), nullable=False)
    allow_reprocess = Column(Boolean, default=False, nullable=False)
    status = Column(String(64), nullable=False, index=True)
    reason_code = Column(String(128), nullable=True)
    reason_text = Column(String(1000), nullable=True)
    details = Column(JSON, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        index=True,
    )
