"""Pydantic schemas for Inventory and StockMovement."""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from dz_fastapi.models.inventory import (InventoryScopeType, InventoryStatus,
                                         LotSourceType, MovementType,
                                         StockDocumentStatus,
                                         StockDocumentType, SyncStatus)


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


# ─── StockLot ────────────────────────────────────────────────────────────────

class StockLotOut(BaseModel):
    id: int
    autopart_id: int
    storage_location_id: Optional[int] = None
    storage_location_name: Optional[str] = None
    source_type: LotSourceType
    gtd_number: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    initial_quantity: int
    remaining_quantity: int
    source_receipt_id: Optional[int] = None
    source_receipt_item_id: Optional[int] = None
    source_document_item_id: Optional[int] = None
    external_id: Optional[str] = None
    sync_status: SyncStatus
    received_at: datetime
    created_at: datetime

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
    stock_lot_id: Optional[int] = None
    # Денормализованные поля из лота — отдаются клиенту без доп. запроса
    gtd_number: Optional[str] = None
    lot_source_type: Optional[LotSourceType] = None

    # Синхронизация с 1С
    external_id: Optional[str] = None
    operation_uid: Optional[str] = None

    # Денормализованные поля из связанных объектов
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
    operation_uid: Optional[str] = None


# ─── Transfer ────────────────────────────────────────────────────────────────

class TransferRequest(BaseModel):
    autopart_id: int
    from_location_id: int
    to_location_id: int
    quantity: int = Field(
        ...,
        gt=0,
        description='Количество единиц для перемещения',
    )
    notes: Optional[str] = None


class TransferResult(BaseModel):
    autopart_id: int
    from_location_id: int
    to_location_id: int
    movement_out_id: Optional[int] = None
    movement_in_id: Optional[int] = None


# ─── StockDocument ───────────────────────────────────────────────────────────

class StockDocumentItemCreate(BaseModel):
    autopart_id: int
    storage_location_id: Optional[int] = None
    quantity: int = Field(..., gt=0)
    gtd_number: Optional[str] = Field(None, max_length=64)
    country_code: Optional[str] = Field(None, max_length=16)
    country_name: Optional[str] = Field(None, max_length=120)
    notes: Optional[str] = None


class StockDocumentItemUpdate(BaseModel):
    autopart_id: Optional[int] = None
    storage_location_id: Optional[int] = None
    quantity: Optional[int] = Field(None, gt=0)
    gtd_number: Optional[str] = Field(None, max_length=64)
    country_code: Optional[str] = Field(None, max_length=16)
    country_name: Optional[str] = Field(None, max_length=120)
    notes: Optional[str] = None


class StockDocumentItemOut(BaseModel):
    id: int
    document_id: int
    autopart_id: int
    storage_location_id: Optional[int] = None
    quantity: int
    gtd_number: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    lot_id: Optional[int] = None
    notes: Optional[str] = None
    # Денормализованные
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_brand: Optional[str] = None
    storage_location_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class StockDocumentCreate(BaseModel):
    doc_type: StockDocumentType
    document_number: Optional[str] = Field(None, max_length=100)
    document_date: Optional[datetime] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)
    items: List[StockDocumentItemCreate] = Field(default_factory=list)


class StockDocumentUpdate(BaseModel):
    document_number: Optional[str] = Field(None, max_length=100)
    document_date: Optional[datetime] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)


class StockDocumentOut(BaseModel):
    id: int
    doc_type: StockDocumentType
    status: StockDocumentStatus
    document_number: Optional[str] = None
    document_date: datetime
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    reason: Optional[str] = None
    notes: Optional[str] = None
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    posted_at: Optional[datetime] = None
    items: List[StockDocumentItemOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class StockDocumentListItem(BaseModel):
    """Облегчённая строка для списка документов."""
    id: int
    doc_type: StockDocumentType
    status: StockDocumentStatus
    document_number: Optional[str] = None
    document_date: datetime
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    reason: Optional[str] = None
    item_count: int = 0
    created_at: datetime
    posted_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# ─── StorageLocation with autopart count ────────────────────────────────────

class StorageAutoPartItem(BaseModel):
    """Autopart info as stored in a specific location."""
    autopart_id: int
    oem_number: str
    name: str
    brand_name: str
    stock_quantity: int = 0

    model_config = ConfigDict(from_attributes=True)


# ─── Backfill ────────────────────────────────────────────────────────────────

class BackfillResult(BaseModel):
    """Результат backfill-операции opening_balance лотов."""
    lots_created: int
    locations_processed: int
    autoparts_skipped: int  # уже имели лоты — пропущены
