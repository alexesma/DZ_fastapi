"""
Inventory models:
  - StockByLocation  — остаток конкретной запчасти в конкретной ячейке
  - InventorySession — сеанс инвентаризации (ручной подсчёт остатков)
  - InventoryItem    — строка подсчёта: запчасть
    + место + ожидаемо + фактически
  - StockMovement    — история движения товара по местам хранения
"""
from enum import StrEnum, unique

from sqlalchemy import Boolean, Column, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
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
    FULL = 'full'        # весь склад
    SHELF = 'shelf'      # все ячейки одного стеллажа (по префиксу)
    LOCATION = 'location'  # одно конкретное место


@unique
class MovementType(StrEnum):
    RECEIPT = 'receipt'       # приход (поступление от поставщика)
    SHIPMENT = 'shipment'     # отгрузка клиенту
    TRANSFER_IN = 'transfer_in'   # перемещение — приход
    TRANSFER_OUT = 'transfer_out'  # перемещение — уход
    INVENTORY = 'inventory'   # корректировка по итогам инвентаризации
    MANUAL = 'manual'         # ручная правка


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
        onupdate=now_moscow
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

    # id inherited from PreBase
    name = Column(String(200), nullable=False)
    started_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(
        SAEnum(InventoryStatus, name='inventorystatus'),
        default=InventoryStatus.ACTIVE,
        nullable=False,
    )
    scope_type = Column(
        SAEnum(InventoryScopeType, name='inventoryscopetype'),
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
        lazy='noload'
    )
    autopart = relationship('AutoPart', lazy='joined')
    storage_location = relationship('StorageLocation', lazy='joined')


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
        SAEnum(MovementType, name='movementtype'),
        nullable=False,
    )
    quantity = Column(Integer, nullable=False)    # >0 приход, <0 уход
    qty_before = Column(Integer, nullable=True)   # остаток до движения
    qty_after = Column(Integer, nullable=True)    # остаток после
    reference_id = Column(Integer, nullable=True)  # id связанного документа
    reference_type = Column(
        String(50),
        nullable=True
    )  # 'order'/'inventory'/etc
    notes = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True), default=now_moscow, nullable=False
    )

    autopart = relationship('AutoPart', lazy='noload')
    storage_location = relationship('StorageLocation', lazy='noload')
