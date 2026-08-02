from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class TelegramOutboxOut(BaseModel):
    id: int
    status: str
    chat_id: str
    text: Optional[str] = None
    parse_mode: Optional[str] = None
    document_name: Optional[str] = None
    document_content_type: Optional[str] = None
    caption: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[int] = None
    attempts: int = 0
    last_error: Optional[str] = None
    sent_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class RelayTelegramOutboxOut(TelegramOutboxOut):
    document_base64: Optional[str] = None
    document_error: Optional[str] = None


class TelegramOutboxMarkErrorIn(BaseModel):
    error: str
    retry: bool = True
