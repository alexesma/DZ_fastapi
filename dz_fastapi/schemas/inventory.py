"""Pydantic schemas for Inventory and StockMovement."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, condecimal

from dz_fastapi.models.inventory import (
    InventoryScopeType,
    InventoryStatus,
    LotSourceType,
    MovementType,
    ReserveStatus,
    ReturnDocumentStatus,
    ShipmentDocumentStatus,
    StockDocumentStatus,
    StockDocumentType,
    SyncStatus,
)

PriceDecimal = condecimal(max_digits=10, decimal_places=2, ge=0)


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
    quantity: int  # физический остаток
    reserved: int = 0  # зарезервировано (сумма ACTIVE резервов)
    available: int = 0  # свободный остаток = quantity − reserved
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
    sync_status: SyncStatus = SyncStatus.PENDING
    synced_at: Optional[datetime] = None

    # Денормализованные поля из связанных объектов
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_brand: Optional[str] = None
    storage_location_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class StockMovementCreate(BaseModel):
    autopart_id: int
    storage_location_id: Optional[int] = None
    movement_type: MovementType
    quantity: int
    notes: Optional[str] = None
    operation_uid: Optional[str] = None
    external_id: Optional[str] = Field(
        None,
        max_length=100,
        description="ID документа в 1С (если движение создаётся из 1С)",
    )


# ─── 1С sync schemas ─────────────────────────────────────────────────────────


class MovementSyncUpdate(BaseModel):
    """Обновление статуса синхронизации одного движения (вызов из 1С)."""

    external_id: Optional[str] = Field(
        None,
        max_length=100,
        description="ID записи в 1С",
    )
    sync_status: SyncStatus
    sync_error: Optional[str] = Field(
        None,
        max_length=500,
        description="Текст ошибки при sync_status=error",
    )


class MovementBulkSyncItem(BaseModel):
    """Один элемент пакетного обновления статуса."""

    id: int
    external_id: Optional[str] = Field(None, max_length=100)
    sync_status: SyncStatus
    sync_error: Optional[str] = Field(None, max_length=500)


class MovementBulkSyncRequest(BaseModel):
    """Пакетное подтверждение синхронизации из 1С."""

    items: List[MovementBulkSyncItem] = Field(
        ...,
        min_length=1,
        description="Список движений с их новыми статусами",
    )


class MovementBulkSyncResult(BaseModel):
    """Результат пакетного обновления."""

    updated: int
    not_found: List[int] = Field(default_factory=list)


class MovementsExportOut(BaseModel):
    """Ответ на экспорт движений для 1С."""

    total: int
    items: List[StockMovementOut]


# ─── Transfer ────────────────────────────────────────────────────────────────


class TransferRequest(BaseModel):
    autopart_id: int
    from_location_id: int
    to_location_id: int
    quantity: int = Field(
        ...,
        gt=0,
        description="Количество единиц для перемещения",
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
    external_id: Optional[str] = None
    sync_status: Optional[SyncStatus] = None
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


# ─── StockReserve ─────────────────────────────────────────────────────────────


class StockReserveCreate(BaseModel):
    autopart_id: int
    quantity: int = Field(..., gt=0)
    storage_location_id: Optional[int] = Field(
        None,
        description="Конкретная ячейка; None = резерв по всему складу",
    )
    customer_order_item_id: Optional[int] = None
    stock_order_item_id: Optional[int] = None
    expires_at: Optional[datetime] = None
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)


class StockReserveOut(BaseModel):
    id: int
    autopart_id: int
    storage_location_id: Optional[int] = None
    quantity: int
    status: ReserveStatus
    customer_order_item_id: Optional[int] = None
    stock_order_item_id: Optional[int] = None
    expires_at: Optional[datetime] = None
    released_at: Optional[datetime] = None
    notes: Optional[str] = None
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    # денормализованные
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_brand: Optional[str] = None
    storage_location_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class StockReserveCancelRequest(BaseModel):
    """Массовая отмена резервов (например, при отмене заказа)."""

    reserve_ids: List[int] = Field(..., min_length=1)
    reason: Optional[str] = None


class StockReserveCancelResult(BaseModel):
    cancelled: int
    not_found: List[int] = Field(default_factory=list)
    already_inactive: List[int] = Field(default_factory=list)


# ─── ShipmentDocument ─────────────────────────────────────────────────────────


class ShipmentDocumentItemCreate(BaseModel):
    autopart_id: int
    quantity: int = Field(..., gt=0)
    storage_location_id: Optional[int] = None
    price: Optional[PriceDecimal] = None
    reserve_id: Optional[int] = Field(
        None,
        description="Резерв, который будет снят при проведении",
    )
    notes: Optional[str] = None


class ShipmentDocumentItemUpdate(BaseModel):
    quantity: Optional[int] = Field(None, gt=0)
    storage_location_id: Optional[int] = None
    price: Optional[PriceDecimal] = None
    reserve_id: Optional[int] = None
    notes: Optional[str] = None


class ShipmentDocumentItemOut(BaseModel):
    id: int
    document_id: int
    autopart_id: int
    storage_location_id: Optional[int] = None
    quantity: int
    price: Optional[Decimal] = None
    reserve_id: Optional[int] = None
    lot_id: Optional[int] = None
    notes: Optional[str] = None
    # денормализованные
    autopart_oem: Optional[str] = None
    autopart_name: Optional[str] = None
    autopart_brand: Optional[str] = None
    storage_location_name: Optional[str] = None
    # из лота (заполняется после проведения)
    gtd_number: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class ShipmentDocumentCreate(BaseModel):
    doc_number: Optional[str] = Field(None, max_length=100)
    doc_date: Optional[datetime] = None
    customer_id: Optional[int] = None
    customer_order_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)
    items: List[ShipmentDocumentItemCreate] = Field(default_factory=list)


class ShipmentDocumentUpdate(BaseModel):
    doc_number: Optional[str] = Field(None, max_length=100)
    doc_date: Optional[datetime] = None
    customer_id: Optional[int] = None
    customer_order_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)


class ShipmentDocumentOut(BaseModel):
    id: int
    doc_number: Optional[str] = None
    doc_date: datetime
    status: ShipmentDocumentStatus
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    customer_order_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    reason: Optional[str] = None
    notes: Optional[str] = None
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    posted_at: Optional[datetime] = None
    items: List[ShipmentDocumentItemOut] = Field(default_factory=list)
    total_quantity: int = 0

    model_config = ConfigDict(from_attributes=True)


class ShipmentDocumentListItem(BaseModel):
    """Облегчённая строка для списка накладных."""

    id: int
    doc_number: Optional[str] = None
    doc_date: datetime
    status: ShipmentDocumentStatus
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    customer_order_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    reason: Optional[str] = None
    item_count: int = 0
    total_quantity: int = 0
    sync_status: SyncStatus
    created_at: datetime
    posted_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ShipmentPostResult(BaseModel):
    """Результат проведения накладной."""

    document_id: int
    movements_created: int
    reserves_released: int
    lots_consumed: List[int] = Field(
        default_factory=list,
        description="IDs лотов, затронутых при списании",
    )


# ─── Returns ────────────────────────────────────────────────────────────────


class ReturnItemCreate(BaseModel):
    shipment_item_id: Optional[int] = None
    supplier_receipt_item_id: Optional[int] = None
    customer_order_item_id: Optional[int] = None
    supplier_order_item_id: Optional[int] = None
    order_item_id: Optional[int] = None
    autopart_id: Optional[int] = None
    storage_location_id: Optional[int] = None
    lot_id: Optional[int] = None
    quantity: int = Field(..., gt=0)
    price: Optional[PriceDecimal] = None
    gtd_number: Optional[str] = Field(None, max_length=64)
    country_code: Optional[str] = Field(None, max_length=16)
    country_name: Optional[str] = Field(None, max_length=120)
    oem_number: Optional[str] = Field(None, max_length=120)
    brand_name: Optional[str] = Field(None, max_length=120)
    autopart_name: Optional[str] = Field(None, max_length=512)
    notes: Optional[str] = None


class ReturnItemUpdate(BaseModel):
    shipment_item_id: Optional[int] = None
    supplier_receipt_item_id: Optional[int] = None
    customer_order_item_id: Optional[int] = None
    supplier_order_item_id: Optional[int] = None
    order_item_id: Optional[int] = None
    autopart_id: Optional[int] = None
    storage_location_id: Optional[int] = None
    lot_id: Optional[int] = None
    quantity: Optional[int] = Field(None, gt=0)
    price: Optional[PriceDecimal] = None
    gtd_number: Optional[str] = Field(None, max_length=64)
    country_code: Optional[str] = Field(None, max_length=16)
    country_name: Optional[str] = Field(None, max_length=120)
    oem_number: Optional[str] = Field(None, max_length=120)
    brand_name: Optional[str] = Field(None, max_length=120)
    autopart_name: Optional[str] = Field(None, max_length=512)
    notes: Optional[str] = None


class ReturnItemOut(BaseModel):
    id: int
    return_from_customer_id: Optional[int] = None
    return_to_supplier_id: Optional[int] = None
    shipment_item_id: Optional[int] = None
    supplier_receipt_item_id: Optional[int] = None
    customer_order_item_id: Optional[int] = None
    supplier_order_item_id: Optional[int] = None
    order_item_id: Optional[int] = None
    autopart_id: Optional[int] = None
    storage_location_id: Optional[int] = None
    lot_id: Optional[int] = None
    quantity: int
    price: Optional[Decimal] = None
    gtd_number: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    oem_number: Optional[str] = None
    brand_name: Optional[str] = None
    autopart_name: Optional[str] = None
    notes: Optional[str] = None
    storage_location_name: Optional[str] = None
    autopart_oem: Optional[str] = None
    autopart_brand: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class ReturnFromCustomerCreate(BaseModel):
    doc_number: Optional[str] = Field(None, max_length=100)
    doc_date: Optional[datetime] = None
    customer_id: Optional[int] = None
    shipment_document_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)
    items: List[ReturnItemCreate] = Field(default_factory=list)


class ReturnFromCustomerUpdate(BaseModel):
    doc_number: Optional[str] = Field(None, max_length=100)
    doc_date: Optional[datetime] = None
    customer_id: Optional[int] = None
    shipment_document_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)


class ReturnFromCustomerOut(BaseModel):
    id: int
    doc_number: Optional[str] = None
    doc_date: datetime
    status: ReturnDocumentStatus
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    shipment_document_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    diadoc_outgoing_document_id: Optional[int] = None
    diadoc_outgoing_status: Optional[str] = None
    reason: Optional[str] = None
    notes: Optional[str] = None
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    approved_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    rejected_at: Optional[datetime] = None
    items: List[ReturnItemOut] = Field(default_factory=list)
    total_quantity: int = 0

    model_config = ConfigDict(from_attributes=True)


class ReturnFromCustomerListItem(BaseModel):
    id: int
    doc_number: Optional[str] = None
    doc_date: datetime
    status: ReturnDocumentStatus
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    shipment_document_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    reason: Optional[str] = None
    item_count: int = 0
    total_quantity: int = 0
    diadoc_outgoing_document_id: Optional[int] = None
    diadoc_outgoing_status: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    approved_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    rejected_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ReturnToSupplierCreate(BaseModel):
    doc_number: Optional[str] = Field(None, max_length=100)
    doc_date: Optional[datetime] = None
    provider_id: Optional[int] = None
    supplier_receipt_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)
    items: List[ReturnItemCreate] = Field(default_factory=list)


class ReturnToSupplierUpdate(BaseModel):
    doc_number: Optional[str] = Field(None, max_length=100)
    doc_date: Optional[datetime] = None
    provider_id: Optional[int] = None
    supplier_receipt_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    reason: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = None
    external_id: Optional[str] = Field(None, max_length=100)


class ReturnToSupplierOut(BaseModel):
    id: int
    doc_number: Optional[str] = None
    doc_date: datetime
    status: ReturnDocumentStatus
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    supplier_receipt_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    diadoc_outgoing_document_id: Optional[int] = None
    diadoc_outgoing_status: Optional[str] = None
    reason: Optional[str] = None
    notes: Optional[str] = None
    external_id: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    approved_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    rejected_at: Optional[datetime] = None
    items: List[ReturnItemOut] = Field(default_factory=list)
    total_quantity: int = 0

    model_config = ConfigDict(from_attributes=True)


class ReturnToSupplierListItem(BaseModel):
    id: int
    doc_number: Optional[str] = None
    doc_date: datetime
    status: ReturnDocumentStatus
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    supplier_receipt_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    warehouse_name: Optional[str] = None
    reason: Optional[str] = None
    item_count: int = 0
    total_quantity: int = 0
    diadoc_outgoing_document_id: Optional[int] = None
    diadoc_outgoing_status: Optional[str] = None
    sync_status: SyncStatus
    created_at: datetime
    approved_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    rejected_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# ─── 1С Sync — ShipmentDocument ──────────────────────────────────────────────


class ShipmentSyncUpdate(BaseModel):
    """Обновление статуса синхронизации одной накладной."""

    sync_status: SyncStatus
    external_id: Optional[str] = Field(None, max_length=100)
    synced_at: Optional[datetime] = None


class ShipmentBulkSyncItem(BaseModel):
    shipment_id: int
    sync_status: SyncStatus
    external_id: Optional[str] = Field(None, max_length=100)


class ShipmentBulkSyncRequest(BaseModel):
    items: List[ShipmentBulkSyncItem] = Field(..., min_length=1)


class ShipmentBulkSyncResult(BaseModel):
    updated: int
    errors: List[int] = Field(default_factory=list)  # IDs, которые не найдены


class ShipmentsExportOut(BaseModel):
    """Результат экспорта накладных для 1С."""

    total: int
    items: List[ShipmentDocumentListItem]


# ─── 1С Sync — StockDocument ──────────────────────────────────────────────────


class DocumentSyncUpdate(BaseModel):
    """Обновление статуса синхронизации одного документа оприходования/списания."""

    sync_status: SyncStatus
    external_id: Optional[str] = Field(None, max_length=100)
    synced_at: Optional[datetime] = None


class DocumentBulkSyncItem(BaseModel):
    document_id: int
    sync_status: SyncStatus
    external_id: Optional[str] = Field(None, max_length=100)


class DocumentBulkSyncRequest(BaseModel):
    items: List[DocumentBulkSyncItem] = Field(..., min_length=1)


class DocumentBulkSyncResult(BaseModel):
    updated: int
    errors: List[int] = Field(default_factory=list)


class DocumentsExportOut(BaseModel):
    """Результат экспорта документов оприходования/списания для 1С."""

    total: int
    items: List[StockDocumentListItem]
