from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String, Text

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class EmailSendLog(Base):
    """Журнал исходящих писем: что отправили, чем закончилось, что повторится.

    Одна строка — одно письмо. Попытки дописываются в ``events``. Тело и
    вложение хранятся только пока письмо не ушло: они нужны для повтора.
    """

    created_at = Column(DateTime(timezone=True), default=now_moscow, index=True)
    updated_at = Column(DateTime(timezone=True), default=now_moscow, onupdate=now_moscow)
    to_email = Column(String(1000), nullable=False)
    from_email = Column(String(255), nullable=True)
    subject = Column(String(998), nullable=True)
    transport = Column(String(32), nullable=True)
    # Откуда письмо: supplier_order, customer_reply, pricelist, upd и т.д.
    source = Column(String(64), nullable=True, index=True)
    source_id = Column(Integer, nullable=True, index=True)
    attachment_name = Column(String(255), nullable=True)
    attachment_size = Column(Integer, nullable=True)
    # sent — ушло; retry — ждёт повтора; failed — попытки закончились
    status = Column(String(16), nullable=False, default="sent", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime(timezone=True), nullable=True, index=True)
    last_error = Column(Text, nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    events = Column(JSON, default=list)
    # Можно ли повторить письмо из журнала (есть тело, повтор не ведёт свой процесс)
    retry_allowed = Column(Boolean, nullable=False, default=True)
    # Сам повтор делает транспортный слой (а не процесс, отправивший письмо)
    auto_retry = Column(Boolean, nullable=False, default=True)
    is_html = Column(Boolean, nullable=False, default=False)
    body = Column(Text, nullable=True)
    attachment_path = Column(String(1000), nullable=True)
    duration_ms = Column(Integer, nullable=True)
