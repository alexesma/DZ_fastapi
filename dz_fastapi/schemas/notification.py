from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class AppNotificationResponse(BaseModel):
    id: int
    title: str
    message: str
    level: str
    link: Optional[str] = None
    created_at: datetime
    read_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class AppNotificationListResponse(BaseModel):
    items: List[AppNotificationResponse]
    unread_count: int


class AppNotificationReadResponse(BaseModel):
    id: int
    read_at: Optional[datetime] = None
