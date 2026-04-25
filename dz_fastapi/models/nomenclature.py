"""
Nomenclature auxiliary models:
  - HonestSignCategory  — Честный знак (маркировка РФ) lookup table
  - ApplicabilityNode   — дерево применимости
  (автомобиль или деталь → параметр)

Both linked to AutoPart via M2M association tables.
"""
from sqlalchemy import Column, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base

# ─── Association tables ─────────────────────────────────────────────────────

autopart_honest_sign_association = Table(
    'autopart_honest_sign_association',
    Base.metadata,
    Column(
        'autopart_id',
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        primary_key=True,
    ),
    Column(
        'honest_sign_category_id',
        Integer,
        ForeignKey('honestsigncategory.id', ondelete='CASCADE'),
        primary_key=True,
    ),
)

autopart_applicability_association = Table(
    'autopart_applicability_association',
    Base.metadata,
    Column(
        'autopart_id',
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        primary_key=True,
    ),
    Column(
        'applicability_node_id',
        Integer,
        ForeignKey('applicabilitynode.id', ondelete='CASCADE'),
        primary_key=True,
    ),
)


# ─── HonestSignCategory ─────────────────────────────────────────────────────

class HonestSignCategory(Base):
    """Категория Честного знака (код маркировки РФ)."""

    __tablename__ = 'honestsigncategory'

    # id inherited from PreBase
    name = Column(String(200), nullable=False, unique=True, index=True)
    code = Column(String(50), nullable=True, unique=True, index=True)
    description = Column(Text, nullable=True)

    # Back-reference (optional, for admin queries)
    autoparts = relationship(
        'AutoPart',
        secondary='autopart_honest_sign_association',
        back_populates='honest_sign_categories',
        lazy='noload',
    )


# ─── ApplicabilityNode ──────────────────────────────────────────────────────

class ApplicabilityNode(Base):
    """Узел дерева применимости.

    Примеры деревьев:
      Автомобиль → Toyota → Camry → XV70 (2018-2024) → 2.5 AT
      Деталь     → Сальник → 30x52x8
    """

    __tablename__ = 'applicabilitynode'

    # id inherited from PreBase
    name = Column(String(300), nullable=False, index=True)
    node_type = Column(
        String(50), nullable=False, default='other'
    )  # vehicle | part | other
    parent_id = Column(
        Integer,
        ForeignKey('applicabilitynode.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    description = Column(Text, nullable=True)

    # Self-referential tree — always noload to avoid MissingGreenlet in async.
    # Use explicit selectinload() in queries that need children.
    parent = relationship(
        'ApplicabilityNode',
        back_populates='children',
        foreign_keys='[ApplicabilityNode.parent_id]',
        primaryjoin='ApplicabilityNode.parent_id == remote('
                    'ApplicabilityNode.id'
                    ')',
        lazy='noload',
    )
    children = relationship(
        'ApplicabilityNode',
        back_populates='parent',
        foreign_keys='[ApplicabilityNode.parent_id]',
        primaryjoin='ApplicabilityNode.id == remote('
                    'ApplicabilityNode.parent_id'
                    ')',
        cascade='all, delete-orphan',
        lazy='noload',
    )

    # Back-reference to AutoPart
    autoparts = relationship(
        'AutoPart',
        secondary='autopart_applicability_association',
        back_populates='applicability_nodes',
        lazy='noload',
    )
