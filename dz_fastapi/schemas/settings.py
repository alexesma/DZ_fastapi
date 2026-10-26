from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    supplier_response_auto_close_stale_enabled: bool = True
    supplier_response_stale_days: int = 7
    supplier_order_stub_enabled: bool = True
    supplier_order_stub_email: Optional[str] = "info@dragonzap.ru"
    supplier_receipt_upd_email_enabled: bool = False
    supplier_receipt_upd_email: Optional[str] = None
    supplier_receipt_upd_email_account_id: Optional[int] = None
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class CustomerOrderInboxSettingsUpdate(BaseModel):
    lookback_days: Optional[int] = None
    mark_seen: Optional[bool] = None
    error_file_retention_days: Optional[int] = None
    supplier_response_lookback_days: Optional[int] = None
    supplier_response_auto_close_stale_enabled: Optional[bool] = None
    supplier_response_stale_days: Optional[int] = None
    supplier_order_stub_enabled: Optional[bool] = None
    supplier_order_stub_email: Optional[str] = None
    supplier_receipt_upd_email_enabled: Optional[bool] = None
    supplier_receipt_upd_email: Optional[str] = None
    supplier_receipt_upd_email_account_id: Optional[int] = None


class SystemMetricSnapshotOut(BaseModel):
    id: int
    created_at: datetime
    db_size_bytes: Optional[int] = None
    disk_total_bytes: Optional[int] = None
    disk_free_bytes: Optional[int] = None
    mem_total_bytes: Optional[int] = None
    mem_available_bytes: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)


class ExecutionTraceOut(BaseModel):
    id: int
    trace_type: str
    job_key: str
    job_name: str
    status: str
    provider_id: Optional[int] = None
    provider_config_id: Optional[int] = None
    source_filename: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    rss_before_mb: Optional[float] = None
    rss_after_mb: Optional[float] = None
    memory_delta_mb: Optional[float] = None
    details: dict = Field(default_factory=dict)
    model_config = ConfigDict(from_attributes=True)


class SupplierHolidayCreate(BaseModel):
    date: date
    description: Optional[str] = None
    is_working_day: bool = False


class SupplierHolidayOut(BaseModel):
    id: int
    date: date
    description: Optional[str] = None
    is_working_day: bool = False
    source: str = "manual"  # 'manual' | 'auto'
    created_at: Optional[datetime] = None
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


def _digits(value, lengths, label):
    if value is None:
        return None
    value = str(value).strip().replace(" ", "")
    if not value:
        return None
    if not value.isdigit() or len(value) not in lengths:
        expected = " или ".join(str(item) for item in lengths)
        raise ValueError(f"{label}: нужно {expected} цифр")
    return value


class CompanyProfileBase(BaseModel):
    is_individual_entrepreneur: bool = False
    short_name: Optional[str] = Field(default=None, max_length=255)
    full_name: Optional[str] = Field(default=None, max_length=512)
    inn: Optional[str] = None
    kpp: Optional[str] = None
    ogrn: Optional[str] = None
    ogrn_assigned_at: Optional[date] = None
    okpo: Optional[str] = None
    legal_address: Optional[str] = None
    postal_address: Optional[str] = None
    shipping_address: Optional[str] = None
    phone: Optional[str] = Field(default=None, max_length=64)
    email: Optional[str] = Field(default=None, max_length=255)
    website: Optional[str] = Field(default=None, max_length=255)
    bank_name: Optional[str] = Field(default=None, max_length=255)
    bank_bik: Optional[str] = None
    bank_account: Optional[str] = None
    bank_corr_account: Optional[str] = None
    director_position: Optional[str] = Field(default=None, max_length=255)
    director_name: Optional[str] = Field(default=None, max_length=255)
    director_basis: Optional[str] = Field(default=None, max_length=255)
    accountant_name: Optional[str] = Field(default=None, max_length=255)
    vat_payer: bool = True

    @field_validator(
        "short_name", "full_name", "legal_address", "postal_address",
        "shipping_address", "phone", "email", "website", "bank_name",
        "director_position", "director_name", "director_basis",
        "accountant_name", mode="before",
    )
    @classmethod
    def _blank_to_none(cls, value):
        if isinstance(value, str):
            value = value.strip()
        return value or None

    @field_validator("inn", mode="before")
    @classmethod
    def _inn(cls, value):
        return _digits(value, (10, 12), "ИНН")

    @field_validator("kpp", mode="before")
    @classmethod
    def _kpp(cls, value):
        return _digits(value, (9,), "КПП")

    @field_validator("ogrn", mode="before")
    @classmethod
    def _ogrn(cls, value):
        return _digits(value, (13, 15), "ОГРН/ОГРНИП")

    @field_validator("okpo", mode="before")
    @classmethod
    def _okpo(cls, value):
        return _digits(value, (8, 10, 14), "ОКПО")

    @field_validator("bank_bik", mode="before")
    @classmethod
    def _bik(cls, value):
        return _digits(value, (9,), "БИК")

    @field_validator("bank_account", "bank_corr_account", mode="before")
    @classmethod
    def _account(cls, value):
        return _digits(value, (20,), "Счёт")


class CompanyProfileUpdate(CompanyProfileBase):
    pass


class CompanyProfileOut(CompanyProfileBase):
    # True — запись ещё не сохранялась, поля предзаполнены из настроек Диадока
    prefilled: bool = False
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)
