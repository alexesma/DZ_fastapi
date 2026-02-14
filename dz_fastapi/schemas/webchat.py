from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class SiteChatMessageIn(BaseModel):
    session_id: str = Field(..., max_length=100)
    name: Optional[str] = Field(default=None, max_length=80)
    phone: Optional[str] = Field(default=None, max_length=20)
    message: str = Field(..., min_length=1, max_length=2000)
    page: Optional[str] = Field(default=None, max_length=500)


class SiteChatMessageOut(BaseModel):
    ok: bool = True
    message_id: int


class ChatMessageResponse(BaseModel):
    id: int
    message_text: str
    is_from_client: bool
    created_at: datetime

    class Config:
        from_attributes = True


class ChatHistoryResponse(BaseModel):
    massages: list[ChatMessageResponse]
