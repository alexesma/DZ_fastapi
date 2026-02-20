from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import ForeignKey, String
from sqlalchemy.sql import func

from dz_fastapi.core.db import Base


class UserRole(str, Enum):
    ADMIN = "admin"
    MANAGER = "manager"


class UserStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    DISABLED = "disabled"


class User(Base):
    __tablename__ = "app_user"

    email = Column(String(255), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(
        SqlEnum(
            UserRole,
            name="userrole",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        default=UserRole.MANAGER,
        nullable=False,
    )
    status = Column(
        SqlEnum(
            UserStatus,
            name="userstatus",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        default=UserStatus.PENDING,
        nullable=False,
    )
    approved_by = Column(
        ForeignKey("app_user.id", ondelete="SET NULL"), nullable=True
    )
    approved_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    def approve(self, admin_id: int) -> None:
        self.status = UserStatus.ACTIVE
        self.approved_by = admin_id
        self.approved_at = datetime.now(timezone.utc)
