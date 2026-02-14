from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from dz_fastapi.core.db import Base


class ChatMessage(Base):
    session_id: Mapped[str] = mapped_column(String(100), index=True)

    # Info über Kunden
    client_name: Mapped[str | None] = mapped_column(String(80))
    client_phone: Mapped[str | None] = mapped_column(String(20))
    page_url: Mapped[str | None] = mapped_column(String(500))

    # Nachrichten
    message_text: Mapped[str] = mapped_column(Text)
    is_from_client: Mapped[bool] = mapped_column(Boolean, default=True)

    # Für telegram
    telegram_message_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )

    # Status
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
