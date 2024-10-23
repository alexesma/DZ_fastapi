from sqlalchemy import (
    Column,
    String,
    Text,
    Integer,
    ForeignKey,
    Table,
    Boolean,
    UniqueConstraint,
    CheckConstraint,
    text, Index,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.constants import MAX_NAME_BRAND, MAX_LEN_WEBSITE
from dz_fastapi.core.db import Base

brand_synonyms = Table(
    'brand_synonyms',
    Base.metadata,
    Column(
        'brand_id',
        Integer,
        ForeignKey('brand.id', ondelete='CASCADE'),
        primary_key=True
    ),
    Column(
        'synonym_id',
        Integer,
        ForeignKey('brand.id', ondelete='CASCADE'),
        primary_key=True
    ),
    UniqueConstraint(
        'brand_id',
        'synonym_id',
        name='unique_brand_synonyms'
    )
)


class Brand(Base):
    name = Column(String(MAX_NAME_BRAND), nullable=False, unique=True)
    country_of_origin = Column(String(100), nullable=True)
    website = Column(String(MAX_LEN_WEBSITE), nullable=True)
    description = Column(Text, nullable=True)
    logo = Column(String(MAX_LEN_WEBSITE), nullable=True)
    main_brand = Column(Boolean, default=False)
    autoparts = relationship(
        'AutoPart',
        back_populates='brand',
        cascade='all, delete-orphan'
    )
    synonyms = relationship(
        'Brand',
        secondary='brand_synonyms',
        primaryjoin='Brand.id == brand_synonyms.c.brand_id',
        secondaryjoin='Brand.id == brand_synonyms.c.synonym_id',
        cascade='save-update, merge',
        backref='brand_synonyms'
        # lazy='subquery',
        # back_populates='synonyms',
        # viewonly=False
    )
    __table_args__ = (
        CheckConstraint(
            text("name ~ '^[a-zA-Z0-9]+(?:[ -]?[a-zA-Z0-9]+)*$'"),
            name='check_name_brand'
        ),
    )
