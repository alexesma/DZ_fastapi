from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from dz_fastapi.models.order_status_mapping import ExternalStatusMatchMode


class StatusOptionOut(BaseModel):
    value: str
    label: str


class ExternalStatusMappingBase(BaseModel):
    source_key: str = Field(..., max_length=64)
    provider_id: Optional[int] = None
    raw_status: str = Field(..., max_length=255)
    match_mode: ExternalStatusMatchMode = ExternalStatusMatchMode.EXACT
    internal_order_status: Optional[str] = Field(default=None, max_length=64)
    internal_item_status: Optional[str] = Field(default=None, max_length=64)
    priority: int = Field(default=100, ge=0, le=10000)
    is_active: bool = True
    notes: Optional[str] = None


class ExternalStatusMappingCreate(ExternalStatusMappingBase):
    apply_existing: bool = True


class ExternalStatusMappingUpdate(BaseModel):
    provider_id: Optional[int] = None
    raw_status: Optional[str] = Field(default=None, max_length=255)
    match_mode: Optional[ExternalStatusMatchMode] = None
    internal_order_status: Optional[str] = Field(default=None, max_length=64)
    internal_item_status: Optional[str] = Field(default=None, max_length=64)
    priority: Optional[int] = Field(default=None, ge=0, le=10000)
    is_active: Optional[bool] = None
    notes: Optional[str] = None
    apply_existing: bool = True


class ExternalStatusMappingOut(BaseModel):
    id: int
    source_key: str
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    raw_status: str
    normalized_status: str
    match_mode: str
    internal_order_status: Optional[str] = None
    internal_item_status: Optional[str] = None
    priority: int
    is_active: bool
    notes: Optional[str] = None
    created_by_user_id: Optional[int] = None
    created_by_email: Optional[str] = None
    updated_by_user_id: Optional[int] = None
    updated_by_email: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class ExternalStatusUnmappedOut(BaseModel):
    id: int
    source_key: str
    source_label: str
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    raw_status: str
    normalized_status: str
    seen_count: int
    first_seen_at: datetime
    last_seen_at: datetime
    sample_order_id: Optional[int] = None
    sample_item_id: Optional[int] = None
    is_resolved: bool
    mapping_id: Optional[int] = None


class ExternalStatusMappingApplyResult(BaseModel):
    checked_items: int
    updated_items: int
    resolved_unmapped: int


class ExternalStatusMappingOptionsOut(BaseModel):
    sources: list[StatusOptionOut]
    match_modes: list[StatusOptionOut]
    order_statuses: list[StatusOptionOut]
    item_statuses: list[StatusOptionOut]
