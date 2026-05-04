"""
Inventory models:
  - StockByLocation    — остаток конкретной запчасти в конкретной ячейке
  - InventorySession   — сеанс инвентаризации (ручной подсчёт остатков)
  - InventoryItem      — строка подсчёта: запчасть
  + место + ожидаемо + фактически
  - StockLot           — партия товара с привязкой к ГТД (FIFO)
  - StockMovement      — история движения товара по местам хранения
  - StockDocument      — документ ручного оприходования / списания
  - StockDocumentItem  — строка документа ручного оприходования / списания
"""
from enum import StrEnum, unique

from sqlalchemy import Boolean, Column, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import (ForeignKey, Index, Integer, String, Text,
                        UniqueConstraint)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


@unique
class InventoryStatus(StrEnum):
    ACTIVE = 'active'
    COMPLETED = 'completed'
    CANCELLED = 'cancelled'


@unique
class InventoryScopeType(StrEnum):
    FULL = 'full'          # весь склад
    SHELF = 'shelf'        # все ячейки одного стеллажа (по префиксу)
    LOCATION = 'location'  # одно конкретное место


@unique
class MovementType(StrEnum):
    RECEIPT = 'receipt'              # приход (поступление от поставщика)
    SHIPMENT = 'shipment'            # отгрузка клиенту
    TRANSFER_IN = 'transfer_in'      # перемещение — приход
    TRANSFER_OUT = 'transfer_out'    # перемещение — уход
    INVENTORY = 'inventory'          # корректировка по итогам инвентаризации
    MANUAL = 'manual'                # ручная правка (от StockDocument)
    WRITEOFF = 'writeoff'            # списание (от StockDocument)


@unique
class LotSourceType(StrEnum):
    """Источник создания лота — важен для 1С-синхронизации и аудита."""
    RECEIPT = 'receipt'                          # поступление от поставщика
    TRANSFER = 'transfer'                        # перемещение из другой ячейки
    MANUAL = 'manual'                            # ручное оприходование
    OPENING_BALANCE = 'opening_balance'          # остаток на начало (backfill)
    INVENTORY_CORRECTION = 'inventory_correction'  # излишек по инвентаризации


@unique
class SyncStatus(StrEnum):
    """Статус синхронизации с 1С."""
    PENDING = 'pending'    # ещё не синхронизировано
    SYNCED = 'synced'      # синхронизировано
    ERROR = 'error'        # ошибка синхронизации


@unique
class StockDocumentType(StrEnum):
    MANUAL_RECEIPT = 'manual_receipt'  # ручное оприходование
    MANUAL_WRITEOFF = 'manual_writeoff'  # ручное списание


@unique
class StockDocumentStatus(StrEnum):
    DRAFT = 'draft'        # черновик — не влияет на остатки
    POSTED = 'posted'      # проведён — остатки изменены
    CANCELLED = 'cancelled'  # отменён


class Warehouse(Base):
    """Физический склад / площадка хранения."""

    __tablename__ = 'warehouse'

    name = Column(String(120), nullable=False, unique=True, index=True)
    comment = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    locations = relationship(
        'StorageLocation',
        back_populates='warehouse',
        lazy='selectin',
    )
    providers = relationship(
        'Provider',
        back_populates='default_warehouse',
        lazy='selectin',
    )
    receipts = relationship(
        'SupplierReceipt',
        back_populates='warehouse',
        lazy='selectin',
    )


class StockByLocation(Base):
    """Текущий остаток запчасти в конкретной ячейке склада."""

    __tablename__ = 'stockbylocation'

    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey('storagelocation.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    quantity = Column(Integer, nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        default=now_moscow,
        onupdate=now_moscow,
    )

    autopart = relationship('AutoPart', lazy='joined')
    storage_location = relationship('StorageLocation', lazy='joined')

    __table_args__ = (
        UniqueConstraint(
            'autopart_id', 'storage_location_id',
            name='uq_stockbylocation_autopart_location',
        ),
    )


class InventorySession(Base):
    """Сеанс инвентаризации."""

    __tablename__ = 'inventorysession'

    name = Column(String(200), nullable=False)
    started_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(
        SAEnum(
            InventoryStatus,
            name='inventorystatus',
            values_callable=lambda enum: [item.value for item in enum],
        ),
        default=InventoryStatus.ACTIVE,
        nullable=False,
    )
    scope_type = Column(
        SAEnum(
            InventoryScopeType,
            name='inventoryscopetype',
            values_callable=lambda enum: [item.value for item in enum],
        ),
        default=InventoryScopeType.FULL,
        nullable=False,
    )
    # для SHELF — префикс (напр. "AA"), для LOCATION — имя места (напр. "AA01")
    scope_value = Column(String(100), nullable=True)
    notes = Column(Text, nullable=True)

    items = relationship(
        'InventoryItem',
        back_populates='session',
        cascade='all, delete-orphan',
        lazy='selectin',
    )


class InventoryItem(Base):
    """Строка подсчёта в рамках сеанса инвентаризации."""

    __tablename__ = 'inventoryitem'

    session_id = Column(
        Integer,
        ForeignKey('inventorysession.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey('storagelocation.id', ondelete='CASCADE'),
        nullable=False,
    )
    expected_qty = Column(Integer, default=0, nullable=False)
    actual_qty = Column(Integer, nullable=True)    # None = ещё не посчитано
    discrepancy = Column(Integer, nullable=True)   # actual - expected
    counted_at = Column(DateTime(timezone=True), nullable=True)

    session = relationship(
        'InventorySession',
        back_populates='items',
        lazy='noload',
    )
    autopart = relationship('AutoPart', lazy='joined')
    storage_location = relationship('StorageLocation', lazy='joined')


class StockLot(Base):
    """Партия товара — единица хранения с привязкой к ГТД.

    Создаётся при каждом поступлении (или строке поступления).
    Расходуется по принципу FIFO: сначала списывается самая старая партия.
    Хранится навсегда — обеспечивает аудиторский след для таможни/налоговой.

    Поле source_type описывает, откуда создан лот (важно для 1С-синхронизации).
    Поле external_id используется для связи с записью в 1С.
    """

    __tablename__ = 'stocklot'

    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey('storagelocation.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )

    # ── Источник лота ───────────────────────────────────────────────────────
    source_type = Column(
        SAEnum(
            LotSourceType,
            name='lotsourcetype',
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
    initial_quantity = Column(Integer, nullable=False)    # сколько пришло
    remaining_quantity = Column(Integer, nullable=False)  # сколько осталось

    # ── Источник — строка поступления (для RECEIPT-лотов) ──────────────────
    source_receipt_id = Column(
        Integer,
        ForeignKey('supplierreceipt.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    source_receipt_item_id = Column(
        Integer,
        ForeignKey('supplierreceiptitem.id', ondelete='SET NULL'),
        nullable=True,
    )

    # ── Источник — строка ручного документа (для MANUAL/OPENING_BALANCE) ───
    source_document_item_id = Column(
        Integer,
        ForeignKey('stockdocumentitem.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment='ID записи в 1С для двусторонней синхронизации'
    )
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name='syncstatus',
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

    autopart = relationship('AutoPart', lazy='joined')
    storage_location = relationship('StorageLocation', lazy='joined')
    source_receipt = relationship('SupplierReceipt', lazy='noload')
    movements = relationship(
        'StockMovement',
        back_populates='stock_lot',
        lazy='noload',
    )

    __table_args__ = (
        # Быстрый FIFO-запрос: по артикулу + ячейке + остаток > 0 + дата
        Index(
            'idx_stocklot_fifo',
            'autopart_id',
            'storage_location_id',
            'remaining_quantity',
            'received_at',
        ),
    )


class StockMovement(Base):
    """История движений товара по местам хранения."""

    __tablename__ = 'stockmovement'

    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey('storagelocation.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    movement_type = Column(
        SAEnum(
            MovementType,
            name='movementtype',
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
    )
    quantity = Column(Integer, nullable=False)    # >0 приход, <0 уход
    qty_before = Column(Integer, nullable=True)   # остаток до движения
    qty_after = Column(Integer, nullable=True)    # остаток после
    reference_id = Column(Integer, nullable=True)   # id связанного документа
    reference_type = Column(String(50), nullable=True)
    # 'order'/'inventory'/etc
    notes = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )

    # ── Ссылка на лот (заполняется при расходе и при поступлении) ──────────
    stock_lot_id = Column(
        Integer,
        ForeignKey('stocklot.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(
        String(100),
        nullable=True,
        index=True,
        comment='ID записи в 1С для двусторонней синхронизации'
    )
    # operation_uid — уникальный идентификатор операции для идемпотентных
    # вызовов от 1С (один вызов = один uid, retry не создаст дубль)
    operation_uid = Column(String(64), nullable=True, unique=True, index=True)

    autopart = relationship('AutoPart', lazy='noload')
    storage_location = relationship('StorageLocation', lazy='noload')
    stock_lot = relationship(
        'StockLot',
        back_populates='movements',
        lazy='joined',    # нужен для отдачи gtd_number в API без доп. запроса
    )


class StockDocument(Base):
    """Документ ручного оприходования или списания товара.

    Аналог «Оприходование товаров» / «Списание товаров» в 1С.
    Статус DRAFT — черновик, не влияет на остатки.
    Статус POSTED — проведён, остатки изменены,
    созданы StockMovement и StockLot.
    """

    __tablename__ = 'stockdocument'

    doc_type = Column(
        SAEnum(
            StockDocumentType,
            name='stockdocumenttype',
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        index=True,
    )
    status = Column(
        SAEnum(
            StockDocumentStatus,
            name='stockdocumentstatus',
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
        ForeignKey('warehouse.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    reason = Column(String(255), nullable=True,
                    comment='Причина оприходования / списания')
    notes = Column(Text, nullable=True)

    # ── Синхронизация с 1С ──────────────────────────────────────────────────
    external_id = Column(String(100), nullable=True, index=True,
                         comment='GUID документа в 1С')
    sync_status = Column(
        SAEnum(
            SyncStatus,
            name='syncstatus',
            create_constraint=False,    # enum already created by StockLot
            values_callable=lambda enum: [item.value for item in enum],
        ),
        nullable=False,
        default=SyncStatus.PENDING,
    )

    created_at = Column(DateTime(timezone=True), default=now_moscow,
                        nullable=False)
    posted_at = Column(DateTime(timezone=True), nullable=True)

    warehouse = relationship('Warehouse', lazy='joined')
    items = relationship(
        'StockDocumentItem',
        back_populates='document',
        cascade='all, delete-orphan',
        lazy='selectin',
    )


class StockDocumentItem(Base):
    """Строка документа ручного оприходования / списания."""

    __tablename__ = 'stockdocumentitem'

    document_id = Column(
        Integer,
        ForeignKey('stockdocument.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
    )
    storage_location_id = Column(
        Integer,
        ForeignKey('storagelocation.id', ondelete='SET NULL'),
        nullable=True,
    )
    quantity = Column(Integer, nullable=False)

    # ГТД — заполняется при ручном оприходовании
    gtd_number = Column(String(64), nullable=True)
    country_code = Column(String(16), nullable=True)
    country_name = Column(String(120), nullable=True)

    # Ссылка на созданный лот (заполняется при проведении оприходования)
    lot_id = Column(
        Integer,
        ForeignKey('stocklot.id', ondelete='SET NULL'),
        nullable=True,
    )

    notes = Column(Text, nullable=True)

    document = relationship('StockDocument', back_populates='items',
                            lazy='noload')
    autopart = relationship('AutoPart', lazy='joined')
    storage_location = relationship('StorageLocation', lazy='joined')
    # foreign_keys required: two FK paths exist between StockDocumentItem
    # and StockLot (lot_id here, and source_document_item_id on StockLot)
    lot = relationship(
        'StockLot',
        foreign_keys='StockDocumentItem.lot_id',
        lazy='noload',
    )
