from sqlalchemy import Column, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class AppNotificationLevel:
    INFO = 'info'
    SUCCESS = 'success'
    WARNING = 'warning'
    ERROR = 'error'


class AppNotification(Base):
    __tablename__ = 'app_notification'

    user_id = Column(
        ForeignKey('app_user.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    level = Column(
        String(16),
        nullable=False,
        default=AppNotificationLevel.INFO,
    )
    link = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    read_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship('User')
