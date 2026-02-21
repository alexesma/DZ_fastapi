from sqlalchemy import JSON, Boolean, Column, Integer, String

from dz_fastapi.core.db import Base


class EmailAccount(Base):
    name = Column(String(255), nullable=False, unique=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password = Column(String(255), nullable=False)

    imap_host = Column(String(255), nullable=True)
    imap_port = Column(Integer, default=993)

    smtp_host = Column(String(255), nullable=True)
    smtp_port = Column(Integer, default=465)
    smtp_use_ssl = Column(Boolean, default=True)

    purposes = Column(JSON, default=[])
    is_active = Column(Boolean, default=True)
