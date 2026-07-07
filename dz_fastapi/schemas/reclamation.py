from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class ReclamationItemOut(BaseModel):
    id: int
    oem_number: Optional[str] = None
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    quantity: int = 1
    reason: Optional[str] = None
    item_source: str = "unknown"
    autopart_id: Optional[int] = None
    shipment_item_id: Optional[int] = None
    source_provider_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class ReclamationAttachmentOut(BaseModel):
    id: int
    kind: str
    file_name: Optional[str] = None
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class ReclamationRow(BaseModel):
    id: int
    source: str
    status: str
    reclamation_type: Optional[str] = None
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    sender_email: Optional[str] = None
    email_subject: Optional[str] = None
    email_received_at: Optional[datetime] = None
    stated_document_number: Optional[str] = None
    stated_document_date: Optional[date] = None
    recommendation: Optional[str] = None
    resolution: Optional[str] = None
    items_count: int = 0
    attachments_count: int = 0
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ReclamationDetail(BaseModel):
    id: int
    source: str
    status: str
    reclamation_type: Optional[str] = None
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    sender_email: Optional[str] = None
    source_link: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    email_received_at: Optional[datetime] = None
    stated_document_number: Optional[str] = None
    stated_document_date: Optional[date] = None
    stated_reason: Optional[str] = None
    extracted_data: dict[str, Any] = Field(default_factory=dict)
    check_result: dict[str, Any] = Field(default_factory=dict)
    recommendation: Optional[str] = None
    resolution: Optional[str] = None
    resolution_comment: Optional[str] = None
    resolved_at: Optional[datetime] = None
    return_from_customer_id: Optional[int] = None
    created_at: Optional[datetime] = None
    items: list[ReclamationItemOut] = Field(default_factory=list)
    attachments: list[ReclamationAttachmentOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class ReclamationCreateIn(BaseModel):
    customer_id: Optional[int] = None
    sender_email: Optional[str] = None
    subject: Optional[str] = Field(default=None, max_length=998)
    body: Optional[str] = None
    source_link: Optional[str] = Field(default=None, max_length=1024)


class ReclamationAssignCustomerIn(BaseModel):
    customer_id: int
    remember_email: bool = False


class ReclamationSyncResult(BaseModel):
    fetched: int = 0
    created: int = 0
    skipped: int = 0
    account_email: Optional[str] = None
    note: Optional[str] = None


class ReclamationSummary(BaseModel):
    total: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)
    without_customer: int = 0
