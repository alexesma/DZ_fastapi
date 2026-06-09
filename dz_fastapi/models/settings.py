from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class PriceCheckSchedule(Base):
    enabled = Column(Boolean, default=True)
    days = Column(JSON, default=[])
    times = Column(JSON, default=[])
    last_checked_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class SchedulerSetting(Base):
    key = Column(String(64), unique=True, nullable=False, index=True)
    enabled = Column(Boolean, default=True)
    days = Column(JSON, default=[])
    times = Column(JSON, default=[])
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class CustomerOrderInboxSettings(Base):
    lookback_days = Column(Integer, default=1)
    mark_seen = Column(Boolean, default=False)
    error_file_retention_days = Column(Integer, default=5)
    supplier_response_lookback_days = Column(Integer, default=14)
    supplier_response_auto_close_stale_enabled = Column(
        Boolean,
        default=True,
    )
    supplier_response_stale_days = Column(Integer, default=7)
    supplier_order_stub_enabled = Column(Boolean, default=True)
    supplier_order_stub_email = Column(
        String(255),
        default="info@dragonzap.ru",
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class DiadocIntegrationSettings(Base):
    environment = Column(String(32), nullable=False, default="staging")
    organization_id = Column(String(64), nullable=True)
    organization_name = Column(String(255), nullable=True)
    organization_inn = Column(String(32), nullable=True)
    organization_kpp = Column(String(32), nullable=True)
    seller_legal_address = Column(String(500), nullable=True)
    seller_postal_address = Column(String(500), nullable=True)
    signer_full_name = Column(String(255), nullable=True)
    signer_position = Column(String(255), nullable=True)
    signer_basis = Column(String(255), nullable=True)
    formalized_default_function = Column(
        String(64),
        nullable=False,
        default="ДОП",
    )
    box_id = Column(String(255), nullable=True)
    box_id_guid = Column(String(64), nullable=True)
    refresh_token = Column(String(4096), nullable=True)
    access_token = Column(String(4096), nullable=True)
    token_type = Column(String(32), nullable=True)
    token_scope = Column(String(512), nullable=True)
    access_token_expires_at = Column(DateTime(timezone=True), nullable=True)
    connected_user_id = Column(String(64), nullable=True)
    connected_user_name = Column(String(255), nullable=True)
    connected_at = Column(DateTime(timezone=True), nullable=True)
    inbound_sync_enabled = Column(Boolean, default=True, nullable=False)
    inbound_sync_count = Column(Integer, default=50, nullable=False)
    inbound_download_content = Column(Boolean, default=True, nullable=False)
    inbound_process_enabled = Column(Boolean, default=True, nullable=False)
    last_sync_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(String(2000), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )


class SystemMetricSnapshot(Base):
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    db_size_bytes = Column(BigInteger, nullable=True)
    disk_total_bytes = Column(BigInteger, nullable=True)
    disk_free_bytes = Column(BigInteger, nullable=True)
    mem_total_bytes = Column(BigInteger, nullable=True)
    mem_available_bytes = Column(BigInteger, nullable=True)


class ExecutionTrace(Base):
    trace_type = Column(String(32), nullable=False, index=True)
    job_key = Column(String(64), nullable=False, index=True)
    job_name = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False, index=True, default="started")
    provider_id = Column(Integer, ForeignKey("provider.id"), nullable=True, index=True)
    provider_config_id = Column(
        Integer, ForeignKey("providerpricelistconfig.id"), nullable=True, index=True
    )
    source_filename = Column(String(255), nullable=True)
    started_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False, index=True
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)
    duration_ms = Column(Integer, nullable=True)
    rss_before_mb = Column(Float, nullable=True)
    rss_after_mb = Column(Float, nullable=True)
    memory_delta_mb = Column(Float, nullable=True)
    details = Column(JSON, default=dict, nullable=False)

    provider = relationship("Provider", foreign_keys=[provider_id])
    provider_config = relationship(
        "ProviderPriceListConfig", foreign_keys=[provider_config_id]
    )


class PriceListStaleAlert(Base):
    provider_id = Column(Integer, ForeignKey("provider.id"), nullable=False)
    provider_config_id = Column(
        Integer, ForeignKey("providerpricelistconfig.id"), nullable=False
    )
    days_diff = Column(Integer, nullable=False)
    last_price_date = Column(Date, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    provider = relationship("Provider")


class PriceCheckLog(Base):
    status = Column(String(32), nullable=False)
    message = Column(String(255), nullable=True)
    checked_at = Column(DateTime(timezone=True), default=now_moscow)


class SupplierHoliday(Base):
    """Manual holiday calendar for supplier auto-refusal logic.

    Records here either:
      - Add a non-working day (is_working_day=False, default) — e.g. a public
        holiday that ``python-holidays`` does not know about, or a custom
        company day-off.
      - Override an auto-detected holiday as a working day
        (is_working_day=True)
        — e.g. when a public holiday is moved to another day and this day
        becomes a normal working day.
      - Mark a weekend as a working day (is_working_day=True) — перенос
        рабочего дня на субботу.
    """

    date = Column(Date, nullable=False, unique=True, index=True)
    description = Column(String(255), nullable=True)
    # False = non-working day (holiday); True = forced working day (override)
    is_working_day = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
