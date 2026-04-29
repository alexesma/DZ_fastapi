"""Pydantic schemas for Inventory and StockMovement."""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from dz_fastapi.models.inventory import (InventoryScopeType, InventoryStatus,
                                         MovementType)


class WarehouseBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    comment: Optional[str] = None
    is_active: bool = True


class WarehouseCreate(WarehouseBase):
    pass


class WarehouseUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    comment: Optional[str] = None
    is_active: Optional[bool] = None


class WarehouseOut(WarehouseBase):
    id: int
    locations_count: int = 0

    model_config = ConfigDict(from_attributes=True)

# ─── StockByLocation ────────────────────────────────────────────────────────


class StockByLocationOut(BaseModel):
    id: int
    autopart_id: int
    storage_location_id: int
    quantity: int
    updated_at: Optional[datetime] = None
    # denormalised
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_brand: Optional[str] = None
    storage_location_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class StockByLocationUpsert(BaseModel):
    """Set (or create) stock for one autopart in one location."""
    autopart_id: int
    storage_location_id: int
    quantity: int = Field(..., ge=0)


# ─── InventorySession ────────────────────────────────────────────────────────

class InventorySessionCreate(BaseModel):
    name: str = Field(..., max_length=200)
    scope_type: InventoryScopeType = InventoryScopeType.FULL
    scope_value: Optional[str] = Field(None, max_length=100)
    notes: Optional[str] = None


class InventorySessionUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    notes: Optional[str] = None


# ─── InventoryItem ───────────────────────────────────────────────────────────

class InventoryItemCountUpdate(BaseModel):
    actual_qty: int = Field(..., ge=0)


class InventoryItemOut(BaseModel):
    id: int
    session_id: int
    autopart_id: int
    storage_location_id: int
    expected_qty: int
    actual_qty: Optional[int] = None
    discrepancy: Optional[int] = None
    counted_at: Optional[datetime] = None

    # Denormalised for convenience
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_brand: Optional[str] = None
    storage_location_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class InventorySessionOut(BaseModel):
    id: int
    name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: InventoryStatus
    scope_type: InventoryScopeType
    scope_value: Optional[str] = None
    notes: Optional[str] = None
    items: List[InventoryItemOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class InventorySessionListItem(BaseModel):
    """Lightweight session row for list views."""
    id: int
    name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: InventoryStatus
    scope_type: InventoryScopeType
    scope_value: Optional[str] = None
    item_count: int = 0
    counted_count: int = 0

    model_config = ConfigDict(from_attributes=True)


# ─── StockMovement ───────────────────────────────────────────────────────────

class StockMovementOut(BaseModel):
    id: int
    autopart_id: int
    storage_location_id: Optional[int] = None
    movement_type: MovementType
    quantity: int
    qty_before: Optional[int] = None
    qty_after: Optional[int] = None
    reference_id: Optional[int] = None
    reference_type: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime

    # Denormalised
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    storage_location_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class StockMovementCreate(BaseModel):
    autopart_id: int
    storage_location_id: Optional[int] = None
    movement_type: MovementType
    quantity: int
    notes: Optional[str] = None


# ─── Transfer ────────────────────────────────────────────────────────────────

class TransferRequest(BaseModel):
    autopart_id: int
    from_location_id: int
    to_location_id: int
    quantity: int = Field(
        ...,
        gt=0,
        description='Количество единиц для перемещения'
    )
    notes: Optional[str] = None


class TransferResult(BaseModel):
    autopart_id: int
    from_location_id: int
    to_location_id: int
    movement_out_id: int
    movement_in_id: int


# ─── StorageLocation with autopart count ────────────────────────────────────

class StorageAutoPartItem(BaseModel):
    """Autopart info as stored in a specific location."""
    autopart_id: int
    oem_number: str
    name: str
    brand_name: str
    stock_quantity: int = 0

    model_config = ConfigDict(from_attributes=True)
