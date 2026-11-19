from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class EmailAccount(Base):
    name = Column(String(255), nullable=False, unique=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password = Column(String(255), nullable=False)
    # Incoming and outgoing transports can differ.  This is required for
    # Selectel, which sends mail over SMTP but does not provide an inbox.
    transport = Column(String(32), nullable=False, default="smtp")
    outbound_transport = Column(String(32), nullable=True)
    resend_api_key = Column(String(2048), nullable=True)
    resend_timeout = Column(Integer, default=20)
    resend_last_received_at = Column(DateTime(timezone=True), nullable=True)
    # Each Resend consumer needs an independent cursor.  A shared cursor let
    # the customer-order poller hide a supplier reply before the response
    # poller had a chance to process it.
    resend_supplier_response_last_received_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    resend_supplier_document_last_received_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )

    imap_host = Column(String(255), nullable=True)
    imap_port = Column(Integer, default=993)
    imap_folder = Column(String(255), nullable=True, default="INBOX")
    imap_additional_folders = Column(JSON, default=[])

    smtp_host = Column(String(255), nullable=True)
    smtp_port = Column(Integer, default=465)
    smtp_use_ssl = Column(Boolean, default=True)
    # Some transactional SMTP providers issue a login that differs from the
    # visible From address.  Falling back to ``email`` keeps all existing
    # mailbox accounts compatible.
    smtp_username = Column(String(255), nullable=True)
    # Existing SMTP accounts are consumed by the Windows relay.  A per-account
    # switch lets server-side providers (for example Selectel) send directly.
    smtp_via_relay = Column(Boolean, default=True, nullable=False)

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

    @property
    def has_password(self) -> bool:
        """Tell the UI that a password exists without returning the secret."""
        return bool(self.password)

    @property
    def has_resend_api_key(self) -> bool:
        """Tell the UI that a Resend key exists without returning the key."""
        return bool(self.resend_api_key)
