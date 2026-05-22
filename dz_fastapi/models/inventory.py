"""
Inventory models:
  - StockByLocation      — остаток конкретной запчасти в конкретной ячейке
  - InventorySession     — сеанс инвентаризации (ручной подсчёт остатков)
  - InventoryItem        — строка подсчёта: запчасть + место + ожидаемо + фактически
  - StockLot             — партия товара с привязкой к ГТД (FIFO)
  - StockMovement        — история движения товара по местам хранения
  - StockDocument        — документ ручного оприходования / списания
  - StockDocumentItem    — строка документа ручного оприходования / списания
  - StockReserve         — резервирование товара под заказ клиента
  - ShipmentDocument     — накладная на отгрузку (аналог «Реализация» в 1С)
  - ShipmentDocumentItem — строка накладной на отгрузку
"""

from enum import StrEnum, unique

from sqlalchemy import DECIMAL, Boolean, Column, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


@unique
class InventoryStatus(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@unique
class InventoryScopeType(StrEnum):
    FULL = "full"  # весь склад
    SHELF = "shelf"  # все ячейки одного стеллажа (по префиксу)
    LOCATION = "location"  # одно конкретное место


@unique
class MovementType(StrEnum):
    RECEIPT = "receipt"  # приход (поступление от поставщика)
    SHIPMENT = "shipment"  # отгрузка клиенту
    TRANSFER_IN = "transfer_in"  # перемещение — приход
    TRANSFER_OUT = "transfer_out"  # перемещение — уход
    INVENTORY = "inventory"  # корректировка по итогам инвентаризации
    MANUAL = "manual"  # ручная правка (от StockDocument)
    WRITEOFF = "writeoff"  # списание (от StockDocument)
    CUSTOMER_RETURN = "customer_return"  # возврат от клиента
    SUPPLIER_RETURN = "supplier_return"  # возврат поставщику


@unique
class LotSourceType(StrEnum):
    """Источник создания лота — важен для 1С-синхронизации и аудита."""

    RECEIPT = "receipt"  # поступление от поставщика
    TRANSFER = "transfer"  # перемещение из другой ячейки
    MANUAL = "manual"  # ручное оприходование
    OPENING_BALANCE = "opening_balance"  # остаток на начало (backfill)
    INVENTORY_CORRECTION = "inventory_correction"  # излишек по инвентаризации
    CUSTOMER_RETURN = "customer_return"  # возврат товара от клиента


@unique
class SyncStatus(StrEnum):
    """Статус синхронизации с 1С."""

    PENDING = "pending"  # ещё не синхронизировано
    SYNCED = "synced"  # синхронизировано
    ERROR = "error"  # ошибка синхронизации


@unique
class StockDocumentType(StrEnum):
    MANUAL_RECEIPT = "manual_receipt"  # ручное оприходование
    MANUAL_WRITEOFF = "manual_writeoff"  # ручное списание


@unique
class StockDocumentStatus(StrEnum):
    DRAFT = "draft"  # черновик — не влияет на остатки
    POSTED = "posted"  # проведён — остатки изменены
    CANCELLED = "cancelled"  # отменён


class Warehouse(Base):
    """Физический склад / площадка хранения."""

    __tablename__ = "warehouse"

    name = Column(String(120), nullable=False, unique=True, index=True)
    comment = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    locations = relationship(
        "StorageLocation",
        back_populates="warehouse",
        lazy="selectin",
    )
    providers = relationship(
        "Provider",
        back_populates="default_warehouse",
        lazy="selectin",
    )
    receipts = relationship(
        "SupplierReceipt",
        back_populates="warehouse",
        lazy="selectin",
    )


class StockByLocation(Base):
    """Текущий остаток запчасти в конкретной ячейке склада."""

    __tablename__ = "stockbylocation"

    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    quantity = Column(Integer, nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")

    __table_args__ = (
        UniqueConstraint(
            "autopart_id",
            "storage_location_id",
            name="uq_stockbylocation_autopart_location",
        ),
    )


class InventorySession(Base):
    """Сеанс инвентаризации."""

    __tablename__ = "inventorysession"

    name = Column(String(200), nullable=False)
    started_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(
        SAEnum(
            InventoryStatus,
            name="inventorystatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        default=InventoryStatus.ACTIVE,
        nullable=False,
    )
    scope_type = Column(
        SAEnum(
            InventoryScopeType,
            name="inventoryscopetype",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        default=InventoryScopeType.FULL,
        nullable=False,
    )
    # для SHELF — префикс (напр. "AA"), для LOCATION — имя места (напр. "AA01")
    scope_value = Column(String(100), nullable=True)
    notes = Column(Text, nullable=True)

    items = relationship(
        "InventoryItem",
        back_populates="session",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class InventoryItem(Base):
    """Строка подсчёта в рамках сеанса инвентаризации."""

    __tablename__ = "inventoryitem"

    session_id = Column(
        Integer,
        ForeignKey("inventorysession.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="CASCADE"),
        nullable=False,
    )
    expected_qty = Column(Integer, default=0, nullable=False)
    actual_qty = Column(Integer, nullable=True)  # None = ещё не посчитано
    discrepancy = Column(Integer, nullable=True)  # actual - expected
    counted_at = Column(DateTime(timezone=True), nullable=True)

    session = relationship(
        "InventorySession",
        back_populates="items",
        lazy="noload",
    )
    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")


class StockLot(Base):
    """Партия товара — единица хранения с привязкой к ГТД.

    Создаётся при каждом поступлении (или строке поступления).
    Расходуется по принципу FIFO: сначала списывается самая старая партия.
    Хранится навсегда — обеспечивает аудиторский след для таможни/налоговой.

    Поле source_type описывает, откуда создан лот (важно для 1С-синхронизации).
    Поле external_id используется для связи с записью в 1С.
    """

    __tablename__ = "stocklot"

    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # ── Источник лота ───────────────────────────────────────────────────────
    source_type = Column(
        SAEnum(
            LotSourceType,
            name="lotsourcetype",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=LotSourceType.RECEIPT,
        index=True,
    )

    # ── ГТД и страна происхождения ──────────────────────────────────────────
    gtd_number = Column(String(64), nullable=True, index=True)
    country_code = Column(String(16), nullable=True)
    country_name = Column(String(120), nullable=True)

    # ── Количество ─────────────────────────────────────────────────────────
    initial_quantity = Column(Integer, nullable=False)  # сколько пришло
    remaining_quantity = Column(Integer, nullable=False)  # сколько осталось
    cost_price = Column(
        DECIMAL(12, 4),
        nullable=True,
        comment="Закупочная/учётная себестоимость одной единицы партии",
    )

    # ── Источник — строка поступления (для RECEIPT-лотов) ──────────────────
    source_receipt_id = Column(
        Integer,
        ForeignKey("supplierreceipt.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_receipt_item_id = Column(
        Integer,
        ForeignKey("supplierreceiptitem.id", ondelete="SET NULL"),
        nullable=True,
    )

    # ── Источник — строка ручного документа (для MANUAL/OPENING_BALANCE) ───
    source_document_item_id = Column(
        Integer,
        ForeignKey("stockdocumentitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment="ID записи в 1С для двусторонней синхронизации",
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
    )

    # ── Даты ────────────────────────────────────────────────────────────────
    received_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
        index=True,
    )
    created_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )

    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")
    source_receipt = relationship("SupplierReceipt", lazy="noload")
    movements = relationship(
        "StockMovement",
        back_populates="stock_lot",
        lazy="noload",
    )

    __table_args__ = (
        # Быстрый FIFO-запрос: по артикулу + ячейке + остаток > 0 + дата
        Index(
            "idx_stocklot_fifo",
            "autopart_id",
            "storage_location_id",
            "remaining_quantity",
            "received_at",
        ),
    )


class StockMovement(Base):
    """История движений товара по местам хранения."""

    __tablename__ = "stockmovement"

    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    movement_type = Column(
        SAEnum(
            MovementType,
            name="movementtype",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
    )
    quantity = Column(Integer, nullable=False)  # >0 приход, <0 уход
    qty_before = Column(Integer, nullable=True)  # остаток до движения
    qty_after = Column(Integer, nullable=True)  # остаток после
    reference_id = Column(Integer, nullable=True)  # id связанного документа
    reference_type = Column(String(50), nullable=True)
    # 'order'/'inventory'/etc
    notes = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )

    # ── Ссылка на лот (заполняется при расходе и при поступлении) ──────────
    stock_lot_id = Column(
        Integer,
        ForeignKey("stocklot.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment="ID записи в 1С для двусторонней синхронизации",
    )
    # operation_uid — уникальный идентификатор операции для идемпотентных
    # вызовов от 1С (один вызов = один uid, retry не создаст дубль)
    operation_uid = Column(String(64), nullable=True, unique=True, index=True)
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,  # enum already created by StockLot
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
        index=True,
        comment="Статус синхронизации с 1С",
    )
    synced_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Дата и время успешной синхронизации с 1С",
    )

    autopart = relationship("AutoPart", lazy="noload")
    storage_location = relationship("StorageLocation", lazy="noload")
    stock_lot = relationship(
        "StockLot",
        back_populates="movements",
        lazy="joined",  # нужен для отдачи gtd_number в API без доп. запроса
    )


class StockDocument(Base):
    """Документ ручного оприходования или списания товара.

    Аналог «Оприходование товаров» / «Списание товаров» в 1С.
    Статус DRAFT — черновик, не влияет на остатки.
    Статус POSTED — проведён, остатки изменены,
    созданы StockMovement и StockLot.
    """

    __tablename__ = "stockdocument"

    doc_type = Column(
        SAEnum(
            StockDocumentType,
            name="stockdocumenttype",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        index=True,
    )
    status = Column(
        SAEnum(
            StockDocumentStatus,
            name="stockdocumentstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=StockDocumentStatus.DRAFT,
    )
    document_number = Column(String(100), nullable=True, index=True)
    document_date = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    warehouse_id = Column(
        Integer,
        ForeignKey("warehouse.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reason = Column(
        String(255), nullable=True, comment="Причина оприходования / списания"
    )
    notes = Column(Text, nullable=True)

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100), nullable=True, index=True, comment="GUID документа в 1С"
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,  # enum already created by StockLot
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
    )

    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    posted_at = Column(DateTime(timezone=True), nullable=True)

    warehouse = relationship("Warehouse", lazy="joined")
    items = relationship(
        "StockDocumentItem",
        back_populates="document",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class StockDocumentItem(Base):
    """Строка документа ручного оприходования / списания."""

    __tablename__ = "stockdocumentitem"

    document_id = Column(
        Integer,
        ForeignKey("stockdocument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="SET NULL"),
        nullable=True,
    )
    quantity = Column(Integer, nullable=False)
    cost_price = Column(
        DECIMAL(12, 4),
        nullable=True,
        comment="Себестоимость единицы для ручного оприходования/корректировки",
    )

    # ГТД — заполняется при ручном оприходовании
    gtd_number = Column(String(64), nullable=True)
    country_code = Column(String(16), nullable=True)
    country_name = Column(String(120), nullable=True)

    # Ссылка на созданный лот (заполняется при проведении оприходования)
    lot_id = Column(
        Integer,
        ForeignKey("stocklot.id", ondelete="SET NULL"),
        nullable=True,
    )

    notes = Column(Text, nullable=True)

    document = relationship(
        "StockDocument", back_populates="items", lazy="noload"
    )
    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")
    # foreign_keys required: two FK paths exist between StockDocumentItem
    # and StockLot (lot_id here, and source_document_item_id on StockLot)
    lot = relationship(
        "StockLot",
        foreign_keys="StockDocumentItem.lot_id",
        lazy="noload",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Резервы
# ═══════════════════════════════════════════════════════════════════════════════


@unique
class ReserveStatus(StrEnum):
    ACTIVE = "active"  # товар зарезервирован
    RELEASED = "released"  # снят при отгрузке
    CANCELLED = "cancelled"  # отменён (заказ отменён)
    EXPIRED = "expired"  # истёк срок резерва


class StockReserve(Base):
    """Резервирование товара под конкретный заказ клиента.

    Связывает строку заказа с физическим остатком — пока резерв ACTIVE,
    единицы товара нельзя продать другому клиенту.

    Свободный остаток = StockByLocation.quantity − Σ(ACTIVE StockReserve.quantity)
    """

    __tablename__ = "stockreserve"

    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="SET NULL"),
        nullable=True,  # None = резерв без привязки к конкретной ячейке
        index=True,
    )
    quantity = Column(Integer, nullable=False)

    status = Column(
        SAEnum(
            ReserveStatus,
            name="reservestatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=ReserveStatus.ACTIVE,
        index=True,
    )

    # ── Источник резерва ────────────────────────────────────────────────────
    customer_order_item_id = Column(
        Integer,
        ForeignKey("customerorderitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    stock_order_item_id = Column(
        Integer,
        ForeignKey("stockorderitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # ── Опциональный срок действия резерва ──────────────────────────────────
    expires_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Автоматически переводить в EXPIRED после этой даты",
    )
    released_at = Column(DateTime(timezone=True), nullable=True)

    notes = Column(Text, nullable=True)

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment="ID записи в 1С",
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
        index=True,
    )

    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )

    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")
    customer_order_item = relationship("CustomerOrderItem", lazy="noload")
    stock_order_item = relationship("StockOrderItem", lazy="noload")

    __table_args__ = (
        Index(
            "idx_stockreserve_active",
            "autopart_id",
            "storage_location_id",
            "status",
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Накладная на отгрузку
# ═══════════════════════════════════════════════════════════════════════════════


@unique
class ShipmentDocumentStatus(StrEnum):
    DRAFT = "draft"  # черновик — остатки/резервы не затронуты
    POSTED = "posted"  # проведён — резервы сняты, движения созданы
    CANCELLED = "cancelled"  # отменён


class ShipmentDocument(Base):
    """Накладная на отгрузку — аналог «Реализация товаров» в 1С.

    DRAFT  → POSTED  : снимает резервы, расходует FIFO-лоты,
                       создаёт StockMovement(SHIPMENT), уменьшает остатки.
    POSTED → CANCELLED: восстанавливает остатки (обратные движения).
    """

    __tablename__ = "shipmentdocument"

    doc_number = Column(String(100), nullable=True, index=True)
    doc_date = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    status = Column(
        SAEnum(
            ShipmentDocumentStatus,
            name="shipmentdocumentstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=ShipmentDocumentStatus.DRAFT,
        index=True,
    )

    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    customer_order_id = Column(
        Integer,
        ForeignKey("customerorder.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    warehouse_id = Column(
        Integer,
        ForeignKey("warehouse.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    reason = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100), nullable=True, index=True, comment="GUID документа в 1С"
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
        index=True,
    )

    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    posted_at = Column(DateTime(timezone=True), nullable=True)

    customer = relationship("Customer", lazy="joined")
    customer_order = relationship("CustomerOrder", lazy="noload")
    warehouse = relationship("Warehouse", lazy="joined")
    items = relationship(
        "ShipmentDocumentItem",
        back_populates="document",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ShipmentDocumentItem(Base):
    """Строка накладной на отгрузку."""

    __tablename__ = "shipmentdocumentitem"

    document_id = Column(
        Integer,
        ForeignKey("shipmentdocument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        nullable=False,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="SET NULL"),
        nullable=True,
    )
    quantity = Column(Integer, nullable=False)
    price = Column(
        DECIMAL(10, 2), nullable=True, comment="Цена реализации за единицу"
    )
    cost_price = Column(
        DECIMAL(12, 4),
        nullable=True,
        comment="Снимок себестоимости за единицу на момент проведения",
    )
    cost_total = Column(
        DECIMAL(14, 2),
        nullable=True,
        comment="Суммарная себестоимость строки на момент проведения",
    )

    # ── Источник / связи ────────────────────────────────────────────────────
    reserve_id = Column(
        Integer,
        ForeignKey("stockreserve.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Резерв, который снимается при проведении",
    )
    lot_id = Column(
        Integer,
        ForeignKey("stocklot.id", ondelete="SET NULL"),
        nullable=True,
        comment="FIFO-лот, заполняется при проведении (первый затронутый лот)",
    )

    notes = Column(Text, nullable=True)

    document = relationship(
        "ShipmentDocument", back_populates="items", lazy="noload"
    )
    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")
    reserve = relationship("StockReserve", lazy="noload")
    lot = relationship("StockLot", lazy="noload")
    allocations = relationship(
        "ShipmentDocumentItemLotAllocation",
        back_populates="shipment_document_item",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ShipmentDocumentItemLotAllocation(Base):
    """Фактическое списание строки отгрузки по конкретным FIFO-лотам."""

    __tablename__ = "shipmentdocumentitemlotallocation"

    shipment_document_item_id = Column(
        Integer,
        ForeignKey("shipmentdocumentitem.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stock_lot_id = Column(
        Integer,
        ForeignKey("stocklot.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    stock_movement_id = Column(
        Integer,
        ForeignKey("stockmovement.id", ondelete="SET NULL"),
        nullable=True,
        unique=True,
        index=True,
    )
    provider_id = Column(
        Integer,
        ForeignKey("provider.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Поставщик исходной партии на момент отгрузки",
    )
    quantity = Column(Integer, nullable=False)
    unit_cost_price = Column(
        DECIMAL(12, 4),
        nullable=True,
        comment="Себестоимость единицы из конкретной партии",
    )
    total_cost_price = Column(
        DECIMAL(14, 2),
        nullable=True,
        comment="Суммарная себестоимость списания из конкретной партии",
    )
    created_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )

    shipment_document_item = relationship(
        "ShipmentDocumentItem",
        back_populates="allocations",
        lazy="noload",
    )
    stock_lot = relationship("StockLot", lazy="joined")
    stock_movement = relationship("StockMovement", lazy="joined")
    provider = relationship("Provider", lazy="joined")

    __table_args__ = (
        Index(
            "idx_shipment_item_lot_alloc_report",
            "provider_id",
            "shipment_document_item_id",
            "stock_lot_id",
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Возвраты
# ═══════════════════════════════════════════════════════════════════════════════


@unique
class ReturnDocumentStatus(StrEnum):
    CREATED = "created"
    APPROVED = "approved"
    SHIPPED = "shipped"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"


class ReturnFromCustomer(Base):
    """Возврат товара от клиента.

    CREATED  -> APPROVED  : согласован, но склад не трогаем
    APPROVED -> SHIPPED   : клиент отправил товар обратно
    SHIPPED  -> CONFIRMED : товар физически принят на склад, создаются лоты
    *        -> REJECTED  : возврат отклонён до приёмки
    """

    __tablename__ = "returnfromcustomer"

    doc_number = Column(String(100), nullable=True, index=True)
    doc_date = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    status = Column(
        SAEnum(
            ReturnDocumentStatus,
            name="returndocumentstatus",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=ReturnDocumentStatus.CREATED,
        index=True,
    )
    customer_id = Column(
        Integer,
        ForeignKey("customer.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    shipment_document_id = Column(
        Integer,
        ForeignKey("shipmentdocument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    warehouse_id = Column(
        Integer,
        ForeignKey("warehouse.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_by_user_id = Column(
        Integer,
        ForeignKey("app_user.id", ondelete="SET NULL"),
        nullable=True,
    )
    diadoc_outgoing_document_id = Column(
        Integer,
        ForeignKey("diadocoutgoingdocument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reason = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment="GUID документа возврата в 1С",
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
        index=True,
    )
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    approved_at = Column(DateTime(timezone=True), nullable=True)
    shipped_at = Column(DateTime(timezone=True), nullable=True)
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    rejected_at = Column(DateTime(timezone=True), nullable=True)

    customer = relationship("Customer", lazy="joined")
    shipment_document = relationship("ShipmentDocument", lazy="joined")
    warehouse = relationship("Warehouse", lazy="joined")
    created_by_user = relationship("User", foreign_keys=[created_by_user_id])
    diadoc_outgoing_document = relationship(
        "DiadocOutgoingDocument",
        lazy="joined",
    )
    items = relationship(
        "ReturnItem",
        back_populates="return_from_customer",
        cascade="all, delete-orphan",
        lazy="selectin",
        foreign_keys="ReturnItem.return_from_customer_id",
    )


class ReturnToSupplier(Base):
    """Возврат товара поставщику.

    CREATED  -> APPROVED : согласован
    APPROVED -> SHIPPED  : товар физически уехал, склад уменьшается
    SHIPPED  -> CONFIRMED: поставщик подтвердил получение
    CREATED/APPROVED -> REJECTED : возврат отклонён до отгрузки
    """

    __tablename__ = "returntosupplier"

    doc_number = Column(String(100), nullable=True, index=True)
    doc_date = Column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    status = Column(
        SAEnum(
            ReturnDocumentStatus,
            name="returndocumentstatus",
            create_constraint=False,
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=ReturnDocumentStatus.CREATED,
        index=True,
    )
    provider_id = Column(
        Integer,
        ForeignKey("provider.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supplier_receipt_id = Column(
        Integer,
        ForeignKey("supplierreceipt.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    warehouse_id = Column(
        Integer,
        ForeignKey("warehouse.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_by_user_id = Column(
        Integer,
        ForeignKey("app_user.id", ondelete="SET NULL"),
        nullable=True,
    )
    diadoc_outgoing_document_id = Column(
        Integer,
        ForeignKey("diadocoutgoingdocument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reason = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment="GUID документа возврата в 1С",
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name="syncstatus",
            create_constraint=False,
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
        index=True,
    )
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    approved_at = Column(DateTime(timezone=True), nullable=True)
    shipped_at = Column(DateTime(timezone=True), nullable=True)
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    rejected_at = Column(DateTime(timezone=True), nullable=True)

    provider = relationship("Provider", lazy="joined")
    supplier_receipt = relationship("SupplierReceipt", lazy="joined")
    warehouse = relationship("Warehouse", lazy="joined")
    created_by_user = relationship("User", foreign_keys=[created_by_user_id])
    diadoc_outgoing_document = relationship(
        "DiadocOutgoingDocument",
        lazy="joined",
    )
    items = relationship(
        "ReturnItem",
        back_populates="return_to_supplier",
        cascade="all, delete-orphan",
        lazy="selectin",
        foreign_keys="ReturnItem.return_to_supplier_id",
    )


class ReturnItem(Base):
    """Строка возврата.

    Одна и та же таблица используется и для возврата от клиента, и для
    возврата поставщику. Поле lot_id:
      - для возврата от клиента: лот, созданный при подтверждении
      - для возврата поставщику: первый лот, списанный при отгрузке
    """

    __tablename__ = "returnitem"

    return_from_customer_id = Column(
        Integer,
        ForeignKey("returnfromcustomer.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    return_to_supplier_id = Column(
        Integer,
        ForeignKey("returntosupplier.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    shipment_item_id = Column(
        Integer,
        ForeignKey("shipmentdocumentitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supplier_receipt_item_id = Column(
        Integer,
        ForeignKey("supplierreceiptitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    customer_order_item_id = Column(
        Integer,
        ForeignKey("customerorderitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supplier_order_item_id = Column(
        Integer,
        ForeignKey("supplierorderitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    order_item_id = Column(
        Integer,
        ForeignKey("orderitem.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    autopart_id = Column(
        Integer,
        ForeignKey("autopart.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey("storagelocation.id", ondelete="SET NULL"),
        nullable=True,
    )
    lot_id = Column(
        Integer,
        ForeignKey("stocklot.id", ondelete="SET NULL"),
        nullable=True,
    )
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=True)
    gtd_number = Column(String(64), nullable=True)
    country_code = Column(String(16), nullable=True)
    country_name = Column(String(120), nullable=True)
    oem_number = Column(String(120), nullable=True, index=True)
    brand_name = Column(String(120), nullable=True)
    autopart_name = Column(String(512), nullable=True)
    notes = Column(Text, nullable=True)

    return_from_customer = relationship(
        "ReturnFromCustomer",
        back_populates="items",
        foreign_keys=[return_from_customer_id],
        lazy="noload",
    )
    return_to_supplier = relationship(
        "ReturnToSupplier",
        back_populates="items",
        foreign_keys=[return_to_supplier_id],
        lazy="noload",
    )
    shipment_item = relationship("ShipmentDocumentItem", lazy="joined")
    supplier_receipt_item = relationship("SupplierReceiptItem", lazy="joined")
    customer_order_item = relationship("CustomerOrderItem", lazy="noload")
    supplier_order_item = relationship("SupplierOrderItem", lazy="noload")
    order_item = relationship("OrderItem", lazy="noload")
    autopart = relationship("AutoPart", lazy="joined")
    storage_location = relationship("StorageLocation", lazy="joined")
    lot = relationship("StockLot", lazy="noload")
