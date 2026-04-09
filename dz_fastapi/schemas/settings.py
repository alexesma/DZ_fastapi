from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PriceCheckScheduleBase(BaseModel):
    enabled: bool = True
    days: List[str] = Field(default_factory=list)
    times: List[str] = Field(default_factory=list)


class PriceCheckScheduleUpdate(PriceCheckScheduleBase):
    pass


class PriceCheckScheduleOut(PriceCheckScheduleBase):
    id: int
    last_checked_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class PriceListStaleAlertOut(BaseModel):
    id: int
    provider_id: int
    provider_config_id: int
    days_diff: int
    last_price_date: date
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class PriceCheckLogOut(BaseModel):
    id: int
    status: str
    message: Optional[str] = None
    checked_at: datetime
    model_config = ConfigDict(from_attributes=True)


class SchedulerSettingOut(BaseModel):
    id: int
    key: str
    enabled: bool = True
    days: List[str] = Field(default_factory=list)
    times: List[str] = Field(default_factory=list)
    last_run_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class SchedulerSettingUpdate(BaseModel):
    enabled: Optional[bool] = None
    days: Optional[List[str]] = None
    times: Optional[List[str]] = None


class CustomerOrderInboxSettingsOut(BaseModel):
    id: int
    lookback_days: int = 1
    mark_seen: bool = False
    error_file_retention_days: int = 5
    supplier_response_lookback_days: int = 14
    supplier_order_stub_enabled: bool = True
    supplier_order_stub_email: Optional[str] = 'info@dragonzap.ru'
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class CustomerOrderInboxSettingsUpdate(BaseModel):
    lookback_days: Optional[int] = None
    mark_seen: Optional[bool] = None
    error_file_retention_days: Optional[int] = None
    supplier_response_lookback_days: Optional[int] = None
    supplier_order_stub_enabled: Optional[bool] = None
    supplier_order_stub_email: Optional[str] = None


class SystemMetricSnapshotOut(BaseModel):
    id: int
    created_at: datetime
    db_size_bytes: Optional[int] = None
    disk_total_bytes: Optional[int] = None
    disk_free_bytes: Optional[int] = None
    mem_total_bytes: Optional[int] = None
    mem_available_bytes: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)


class MonitorDbTableOut(BaseModel):
    table: str
    size_bytes: int
    size_pretty: str


class MonitorDbOut(BaseModel):
    size_bytes: int
    size_pretty: str
    connections: int
    max_connections: int
    tables: List[MonitorDbTableOut]


class MonitorSystemOut(BaseModel):
    disk_total_bytes: Optional[int] = None
    disk_free_bytes: Optional[int] = None
    disk_used_bytes: Optional[int] = None
    mem_total_bytes: Optional[int] = None
    mem_available_bytes: Optional[int] = None
    cpu_load_1: Optional[float] = None
    cpu_load_5: Optional[float] = None
    cpu_load_15: Optional[float] = None
    uptime_seconds: Optional[float] = None


class MonitorAppOut(BaseModel):
    last_price_check_at: Optional[datetime] = None
    scheduler_last_runs: List[SchedulerSettingOut] = Field(
        default_factory=list
    )


class MonitorSummaryOut(BaseModel):
    db: MonitorDbOut
    system: MonitorSystemOut
    app: MonitorAppOut
