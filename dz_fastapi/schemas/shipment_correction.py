"""API contract for commercial corrections of customer shipments."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class ShipmentCorrectionItemIn(BaseModel):
    shipment_item_id: int
    quantity_after: int = Field(ge=0)
    price_after: Decimal = Field(ge=0, max_digits=12, decimal_places=2)


class ShipmentCorrectionSave(BaseModel):
    shipment_id: int
    doc_number: str = Field(min_length=1, max_length=100)
    doc_date: date
    basis_number: str = Field(min_length=1, max_length=120)
    basis_date: date
    basis_description: str = Field(min_length=1, max_length=255)
    reason: str = Field(min_length=1)
    document_function: Literal["ДИС", "КСЧФ", "КСЧФДИС"] = "КСЧФДИС"
    items: list[ShipmentCorrectionItemIn] = Field(min_length=1)


class ShipmentCorrectionItemOut(BaseModel):
    id: int
    shipment_item_id: int
    original_number: int
    product_name: str
    vendor_code: str | None
    quantity_before: int
    quantity_after: int
    price_before: Decimal
    price_after: Decimal
    vat_rate: Decimal


class ShipmentCorrectionOut(BaseModel):
    id: int
    shipment_id: int
    shipment_number: str | None
    shipment_date: datetime
    customer_id: int | None
    customer_name: str | None
    doc_number: str
    doc_date: date
    basis_number: str
    basis_date: date
    basis_description: str
    reason: str
    document_function: str
    status: str
    sync_status: str
    external_id: str | None
    diadoc_outgoing_document_id: int | None
    created_at: datetime
    posted_at: datetime | None
    items: list[ShipmentCorrectionItemOut]
