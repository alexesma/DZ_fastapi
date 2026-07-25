from datetime import date, datetime
from typing import Any, Literal, Optional

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


class ReclamationUpdateIn(BaseModel):
    status: Optional[str] = None
    reclamation_type: Optional[str] = None
    resolution: Optional[str] = None
    resolution_comment: Optional[str] = None


class ReclamationItemUpdateIn(BaseModel):
    item_source: Optional[str] = None
    reason: Optional[str] = None
    quantity: Optional[int] = Field(default=None, ge=1)


class ReclamationReplyIn(BaseModel):
    # kind: ack | approved | rejected | request_documents — берёт шаблон,
    # если subject/body не заданы явно
    kind: Optional[str] = None
    subject: Optional[str] = Field(default=None, max_length=998)
    body_text: Optional[str] = None


class ReclamationApplyAndReplyIn(BaseModel):
    action: Literal["approved", "rejected", "request_documents"]
    resolution_comment: Optional[str] = Field(default=None, max_length=4000)
    subject: Optional[str] = Field(default=None, max_length=998)
    body_text: Optional[str] = None


class ReclamationFrozaDecisionIn(BaseModel):
    comment: Optional[str] = Field(default=None, max_length=4000)


class ReclamationArmtekDecisionIn(BaseModel):
    comment: Optional[str] = Field(default=None, max_length=4000)


class ReclamationArmtekSyncResult(BaseModel):
    found: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    supplier_id: Optional[str] = None


class EmailOutboxOut(BaseModel):
    id: int
    status: str
    from_email: Optional[str] = None
    to_email: str
    subject: Optional[str] = None
    body_text: Optional[str] = None
    in_reply_to: Optional[str] = None
    references: Optional[str] = None
    reply_to: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[int] = None
    attempts: int = 0
    last_error: Optional[str] = None
    sent_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class OutboxMarkErrorIn(BaseModel):
    error: str
    retry: bool = True


class ReplyTemplateOut(BaseModel):
    kind: str
    subject: str
    body_text: str


class ReclamationSyncResult(BaseModel):
    fetched: int = 0
    created: int = 0
    skipped: int = 0
    account_email: Optional[str] = None
    note: Optional[str] = None
    armtek: list[dict[str, Any]] = Field(default_factory=list)
    armtek_errors: list[str] = Field(default_factory=list)


class ReclamationSummary(BaseModel):
    total: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)
    without_customer: int = 0


class ReclamationStatCustomer(BaseModel):
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    count: int = 0
    approved: int = 0
    rejected: int = 0


class ReclamationStatSupplier(BaseModel):
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    reclamations: int = 0
    items: int = 0


class ReclamationStatBrand(BaseModel):
    brand_name: Optional[str] = None
    reclamations: int = 0
    quantity: int = 0


class ReclamationStatMonth(BaseModel):
    month: str
    count: int = 0


class ReclamationStats(BaseModel):
    total: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)
    by_type: dict[str, int] = Field(default_factory=dict)
    by_resolution: dict[str, int] = Field(default_factory=dict)
    avg_resolution_days: Optional[float] = None
    top_customers: list[ReclamationStatCustomer] = Field(default_factory=list)
    top_suppliers: list[ReclamationStatSupplier] = Field(default_factory=list)
    top_brands: list[ReclamationStatBrand] = Field(default_factory=list)
    by_month: list[ReclamationStatMonth] = Field(default_factory=list)
