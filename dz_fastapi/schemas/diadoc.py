from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DiadocEnvironment(str, Enum):
    STAGING = "staging"
    PROD = "prod"


class DiadocSendMode(str, Enum):
    DRAFT = "draft"
    SEND = "send"


class DiadocShipmentDocumentFormat(str, Enum):
    NONFORMALIZED = "nonformalized"
    FORMALIZED_UTD = "formalized_utd"


class DiadocReturnDocumentFormat(str, Enum):
    FORMALIZED_UKD = "formalized_ukd"


class DiadocOAuthInitRequest(BaseModel):
    environment: DiadocEnvironment | None = None


class DiadocOAuthInitResponse(BaseModel):
    auth_url: str


class DiadocBoxOut(BaseModel):
    box_id: str
    box_id_guid: str
    title: str | None = None
    invoice_format_version: str | None = None
    encrypted_documents_allowed: bool | None = None


class DiadocOrganizationOut(BaseModel):
    org_id: str
    inn: str
    kpp: str | None = None
    full_name: str
    short_name: str | None = None
    is_active: bool | None = None
    is_test: bool | None = None
    boxes: list[DiadocBoxOut] = Field(default_factory=list)


class DiadocStatusOut(BaseModel):
    id: int
    configured: bool
    connected: bool
    environment: DiadocEnvironment
    organization_id: str | None = None
    organization_name: str | None = None
    organization_inn: str | None = None
    organization_kpp: str | None = None
    seller_legal_address: str | None = None
    seller_postal_address: str | None = None
    signer_full_name: str | None = None
    signer_position: str | None = None
    signer_basis: str | None = None
    formalized_default_function: str = "ДОП"
    box_id: str | None = None
    box_id_guid: str | None = None
    connected_user_id: str | None = None
    connected_user_name: str | None = None
    connected_at: datetime | None = None
    inbound_sync_enabled: bool = True
    inbound_sync_count: int = 50
    inbound_download_content: bool = True
    inbound_process_enabled: bool = True
    access_token_expires_at: datetime | None = None
    last_sync_at: datetime | None = None
    last_error: str | None = None


class DiadocSettingsUpdate(BaseModel):
    environment: DiadocEnvironment | None = None
    organization_id: str | None = None
    organization_name: str | None = None
    organization_inn: str | None = None
    organization_kpp: str | None = None
    seller_legal_address: str | None = Field(default=None, max_length=500)
    seller_postal_address: str | None = Field(default=None, max_length=500)
    signer_full_name: str | None = Field(default=None, max_length=255)
    signer_position: str | None = Field(default=None, max_length=255)
    signer_basis: str | None = Field(default=None, max_length=255)
    formalized_default_function: str | None = Field(
        default=None,
        max_length=64,
    )
    box_id: str | None = None
    box_id_guid: str | None = None
    inbound_sync_enabled: bool | None = None
    inbound_sync_count: int | None = Field(default=None, ge=1, le=200)
    inbound_download_content: bool | None = None
    inbound_process_enabled: bool | None = None


class DiadocInboundSyncRequest(BaseModel):
    filter_category: str = "Any.Inbound"
    count: int = 50
    after_index_key: str | None = None
    counteragent_box_id: str | None = None
    document_number: str | None = None
    from_document_date: date | None = None
    to_document_date: date | None = None
    sort_direction: str = "Descending"
    download_content: bool = True
    register_supplier_message: bool = False
    process_supplier_message: bool = False


class DiadocInboundSyncResult(BaseModel):
    total_from_api: int = 0
    synced: int = 0
    created: int = 0
    updated: int = 0
    downloaded: int = 0
    registered_supplier_messages: int = 0
    processed_supplier_messages: int = 0
    processing_skipped: int = 0
    provider_resolved: int = 0
    provider_unresolved: int = 0
    errors: list[str] = Field(default_factory=list)


class DiadocInboundDocumentRegisterIn(BaseModel):
    provider_id: int | None = None
    response_config_id: int | None = None
    download_content_if_missing: bool = True


class DiadocInboundDocumentRegisterResult(BaseModel):
    document_id: int
    provider_id: int
    supplier_order_message_id: int
    response_config_id: int | None = None
    detail: str


class DiadocInboundDocumentProcessIn(BaseModel):
    provider_id: int | None = None
    response_config_id: int | None = None
    download_content_if_missing: bool = True
    register_if_needed: bool = True


class DiadocInboundDocumentProcessResult(BaseModel):
    document_id: int
    provider_id: int
    supplier_order_message_id: int
    response_config_id: int | None = None
    receipt_ids: list[int] = Field(default_factory=list)
    already_processed: bool = False
    processed_messages: int = 0
    parsed_response_files: int = 0
    recognized_positions: int = 0
    unresolved_positions: int = 0
    unresolved_examples: list[str] = Field(default_factory=list)
    created_receipts: int = 0
    updated_receipts: int = 0
    posted_receipts: int = 0
    receipt_items_added: int = 0
    updated_items: int = 0
    skipped_messages: int = 0
    message_type: str | None = None
    import_error_details: str | None = None
    detail: str


class DiadocInboundDocumentOut(BaseModel):
    id: int
    environment: DiadocEnvironment
    box_id_guid: str
    message_id: str
    entity_id: str
    index_key: str | None = None
    counteragent_box_id: str | None = None
    file_name: str | None = None
    document_number: str | None = None
    document_date: date | None = None
    delivery_at: datetime | None = None
    sent_at: datetime | None = None
    provider_id: int | None = None
    provider_name: str | None = None
    supplier_order_message_id: int | None = None
    local_file_path: str | None = None
    status: str
    import_error_details: str | None = None
    synced_at: datetime | None = None
    registered_at: datetime | None = None
    can_register_supplier_message: bool = False
    can_process_supplier_message: bool = False
    supplier_receipt_ids: list[int] = Field(default_factory=list)


class DiadocShipmentFormalizedReadinessOut(BaseModel):
    shipment_id: int
    ready_nonformalized: bool = False
    ready_formalized: bool = False
    missing_required_fields: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    customer_id: int | None = None
    customer_name: str | None = None
    recommended_actions: list[str] = Field(default_factory=list)


class DiadocReturnFormalizedReadinessOut(BaseModel):
    return_kind: str
    document_id: int
    status: str
    ready_formalized: bool = False
    missing_required_fields: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    customer_id: int | None = None
    customer_name: str | None = None
    provider_id: int | None = None
    provider_name: str | None = None
    source_document_id: int | None = None
    source_document_number: str | None = None
    source_document_date: date | None = None


class DiadocDocumentListItem(BaseModel):
    message_id: str
    entity_id: str
    index_key: str | None = None
    file_name: str | None = None
    document_date: str | None = None
    document_number: str | None = None
    counteragent_box_id: str | None = None
    delivery_timestamp_ticks: int | None = None
    send_timestamp_ticks: int | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class DiadocDocumentListOut(BaseModel):
    total_count: int = 0
    has_more_results: bool = False
    documents: list[DiadocDocumentListItem] = Field(default_factory=list)


class DiadocDocumentQuery(BaseModel):
    filter_category: str = "Any.Inbound"
    count: int = 50
    after_index_key: str | None = None
    counteragent_box_id: str | None = None
    document_number: str | None = None
    from_document_date: date | None = None
    to_document_date: date | None = None
    sort_direction: str = "Descending"


class DiadocCounteragentOut(BaseModel):
    box_id_guid: str
    box_id: str | None = None
    full_name: str | None = None
    short_name: str | None = None
    inn: str | None = None
    kpp: str | None = None
    status: str | None = None
    event_timestamp_ticks: int | None = None
    last_event_comment: str | None = None
    message_from_counteragent: str | None = None
    message_to_counteragent: str | None = None
    mapped_provider_id: int | None = None
    mapped_provider_name: str | None = None
    mapped_customer_id: int | None = None
    mapped_customer_name: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class DiadocCounteragentListOut(BaseModel):
    total_count: int = 0
    has_more_results: bool = False
    after_index_key: str | None = None
    counteragents: list[DiadocCounteragentOut] = Field(default_factory=list)


class DiadocProviderBindingIn(BaseModel):
    counteragent_box_id: str = Field(..., min_length=1, max_length=255)
    source_system: str = "DIADOC_COUNTERAGENT_BOX"
    is_active: bool = True


class DiadocCustomerBindingIn(BaseModel):
    counteragent_box_id: str = Field(..., min_length=1, max_length=255)
    source_system: str = "DIADOC_COUNTERAGENT_BOX"
    is_active: bool = True


class DiadocOutgoingDocumentCreateIn(BaseModel):
    customer_id: int | None = None
    provider_id: int | None = None
    to_box_id_guid: str | None = None
    file_name: str = Field(..., min_length=1, max_length=500)
    content_base64: str = Field(..., min_length=1)
    signature_base64: str | None = None
    comment: str | None = Field(default=None, max_length=5000)
    need_recipient_signature: bool = False
    need_receipt: bool = True
    send_mode: DiadocSendMode = DiadocSendMode.DRAFT
    type_named_id: str = Field(default="Nonformalized", min_length=1)
    document_function: str | None = None
    document_version: str | None = None
    document_number: str | None = Field(default=None, max_length=120)
    document_date: date | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    source_type: str | None = Field(default=None, max_length=64)
    source_id: int | None = None


class DiadocOutgoingDocumentOut(BaseModel):
    id: int
    environment: DiadocEnvironment
    from_box_id_guid: str
    to_box_id_guid: str
    customer_id: int | None = None
    customer_name: str | None = None
    provider_id: int | None = None
    provider_name: str | None = None
    source_type: str | None = None
    source_id: int | None = None
    type_named_id: str
    document_function: str | None = None
    document_version: str | None = None
    file_name: str
    document_number: str | None = None
    document_date: date | None = None
    local_file_path: str
    content_sha256: str | None = None
    comment: str | None = None
    need_recipient_signature: bool = False
    need_receipt: bool = True
    is_draft: bool = True
    message_id: str | None = None
    entity_id: str | None = None
    status: str
    error_details: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    raw_response: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    sent_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class DiadocShipmentOutboundCreateIn(BaseModel):
    customer_id: int | None = None
    to_box_id_guid: str | None = None
    signature_base64: str | None = None
    comment: str | None = Field(default=None, max_length=5000)
    need_recipient_signature: bool = False
    need_receipt: bool = True
    send_mode: DiadocSendMode = DiadocSendMode.DRAFT
    document_format: DiadocShipmentDocumentFormat = (
        DiadocShipmentDocumentFormat.NONFORMALIZED
    )
    type_named_id: str = Field(default="Nonformalized", min_length=1)


class DiadocReturnOutboundCreateIn(BaseModel):
    customer_id: int | None = None
    provider_id: int | None = None
    to_box_id_guid: str | None = None
    signature_base64: str | None = None
    comment: str | None = Field(default=None, max_length=5000)
    need_recipient_signature: bool = False
    need_receipt: bool = True
    send_mode: DiadocSendMode = DiadocSendMode.DRAFT
    document_format: DiadocReturnDocumentFormat = (
        DiadocReturnDocumentFormat.FORMALIZED_UKD
    )
