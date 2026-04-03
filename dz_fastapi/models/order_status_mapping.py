from enum import StrEnum, unique

from sqlalchemy import JSON, Boolean, Column, DateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


@unique
class ExternalStatusMatchMode(StrEnum):
    EXACT = 'EXACT'
    CONTAINS = 'CONTAINS'


class ExternalStatusMapping(Base):
    __tablename__ = 'external_status_mapping'

    source_key = Column(String(64), nullable=False, index=True)
    provider_id = Column(
        Integer, ForeignKey('provider.id'), nullable=True, index=True
    )
    match_mode = Column(
        SAEnum(
            ExternalStatusMatchMode,
            values_callable=lambda enum: [e.value for e in enum],
            native_enum=False,
        ),
        nullable=False,
        default=ExternalStatusMatchMode.EXACT,
    )
    raw_status = Column(String(255), nullable=False)
    normalized_status = Column(String(255), nullable=False, index=True)
    internal_order_status = Column(String(64), nullable=True)
    internal_item_status = Column(String(64), nullable=True)
    priority = Column(Integer, nullable=False, default=100)
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    notes = Column(Text, nullable=True)
    created_by_user_id = Column(
        Integer, ForeignKey('app_user.id'), nullable=True
    )
    updated_by_user_id = Column(
        Integer, ForeignKey('app_user.id'), nullable=True
    )
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=now_moscow
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_moscow,
        onupdate=now_moscow,
    )

    provider = relationship('Provider')
    created_by_user = relationship('User', foreign_keys=[created_by_user_id])
    updated_by_user = relationship('User', foreign_keys=[updated_by_user_id])
    unresolved_statuses = relationship(
        'ExternalStatusUnmapped',
        back_populates='mapping',
    )
    order_items = relationship(
        'OrderItem',
        back_populates='external_status_mapping',
    )


class ExternalStatusUnmapped(Base):
    __tablename__ = 'external_status_unmapped'

    source_key = Column(String(64), nullable=False, index=True)
    provider_id = Column(
        Integer, ForeignKey('provider.id'), nullable=True, index=True
    )
    raw_status = Column(String(255), nullable=False)
    normalized_status = Column(String(255), nullable=False, index=True)
    seen_count = Column(Integer, nullable=False, default=1)
    first_seen_at = Column(
        DateTime(timezone=True), nullable=False, default=now_moscow
    )
    last_seen_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=now_moscow,
        onupdate=now_moscow,
    )
    sample_order_id = Column(
        Integer, ForeignKey('order.id'), nullable=True, index=True
    )
    sample_item_id = Column(
        Integer, ForeignKey('orderitem.id'), nullable=True, index=True
    )
    sample_payload = Column(JSON, nullable=True)
    is_resolved = Column(Boolean, nullable=False, default=False, index=True)
    mapping_id = Column(
        Integer,
        ForeignKey('external_status_mapping.id'),
        nullable=True,
        index=True,
    )

    provider = relationship('Provider')
    sample_order = relationship('Order')
    sample_item = relationship('OrderItem')
    mapping = relationship(
        'ExternalStatusMapping',
        back_populates='unresolved_statuses',
    )
