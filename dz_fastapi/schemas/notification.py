from datetime import datetime
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict


class AppNotificationResponse(BaseModel):
    id: int
    title: str
    message: str
    level: str
    link: Optional[str] = None
    payload: Optional[dict[str, Any]] = None
    available_at: Optional[datetime] = None
    created_at: datetime
    read_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class AppNotificationListResponse(BaseModel):
    items: List[AppNotificationResponse]
    unread_count: int


class AppNotificationReadResponse(BaseModel):
    id: int
    read_at: Optional[datetime] = None


class PricelistStaleActionRequest(BaseModel):
    action: Literal["extend_one_day", "snooze_30_minutes"]


class PricelistStaleActionResponse(BaseModel):
    notification_id: int
    action: str
    available_at: Optional[datetime] = None
    override_until: Optional[datetime] = None
