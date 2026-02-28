from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class EmailAccount(Base):
    name = Column(String(255), nullable=False, unique=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password = Column(String(255), nullable=False)

    imap_host = Column(String(255), nullable=True)
    imap_port = Column(Integer, default=993)
    imap_folder = Column(String(255), nullable=True)

    smtp_host = Column(String(255), nullable=True)
    smtp_port = Column(Integer, default=465)
    smtp_use_ssl = Column(Boolean, default=True)

    purposes = Column(JSON, default=[])
    is_active = Column(Boolean, default=True)

    oauth_provider = Column(String(32), nullable=True)
    oauth_refresh_token = Column(String(2048), nullable=True)
    oauth_connected_at = Column(DateTime(timezone=True), nullable=True)
    oauth_updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
        nullable=True,
    )
