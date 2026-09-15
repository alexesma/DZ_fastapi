"""Runtime heartbeat for external delivery relay workers."""

from sqlalchemy import Column, DateTime, String

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class RelayHeartbeat(Base):
    __tablename__ = "relayheartbeat"

    worker_id = Column(String(128), nullable=False, unique=True, index=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_moscow)
