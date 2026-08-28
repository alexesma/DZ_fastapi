"""Сертификаты и декларации соответствия ЕАС.

Связь с номенклатурой — многие ко многим: один сертификат покрывает
сотни артикулов (на разборе прайса поставщика — до 4228 строк одним
сертификатом), а у одной позиции их может быть несколько, например при
смене поставщика или продлении.

Отдельный случай — сертификат на бренд целиком: для малоизвестных
брендов поставщик оформляет один документ на весь ассортимент, и
перечислять артикулы бессмысленно. Такой сертификат помечается
``covers_whole_brand`` и применяется ко всем позициям бренда, у которых
нет собственного.
"""
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow

autopart_certificate_association = Table(
    "autopart_certificate_association",
    Base.metadata,
    Column(
        "autopart_id",
        Integer,
        ForeignKey("autopart.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "certificate_id",
        Integer,
        ForeignKey("certificate.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class Certificate(Base):
    """Сертификат или декларация соответствия."""

    __tablename__ = "certificate"

    # Номер вида «ЕАЭС RU С-JP.АД50.В.05948/23» — он же естественный ключ.
    number = Column(String(150), nullable=False, unique=True, index=True)
    # Ссылка на карточку в реестре: именно её клиент открывает при проверке.
    url = Column(String(500), nullable=True)
    # Бренд, к которому относится документ. Клиенты сверяют формальное
    # соответствие бренда и сертификата, поэтому храним явно.
    brand_id = Column(
        Integer,
        ForeignKey("brand.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    # Документ покрывает весь ассортимент бренда, а не список артикулов.
    covers_whole_brand = Column(Boolean, default=False, nullable=False)
    valid_from = Column(Date, nullable=True)
    valid_until = Column(Date, nullable=True)
    applicant = Column(String(500), nullable=True)
    manufacturer = Column(String(500), nullable=True)
    # Описание объекта сертификации из реестра — чем ограничен документ.
    scope = Column(Text, nullable=True)
    # supplier_file | registry | manual
    source = Column(String(32), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(
        DateTime(timezone=True), default=now_moscow, onupdate=now_moscow
    )

    brand = relationship("Brand", lazy="joined")
    autoparts = relationship(
        "AutoPart",
        secondary=autopart_certificate_association,
        back_populates="certificates",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_certificate_brand_whole", "brand_id", "covers_whole_brand"),
    )


class CertificationExemptionRule(Base):
    """Наименования, по которым сертификация не требуется.

    Совпадение ищется по вхождению нормализованного шаблона в
    нормализованное наименование позиции. Нормализация приводит латинские
    буквы-двойники к кириллице: в присланных списках встречаются
    «Cальник» с латинской C и «Щyп ypoвня мacлa», которые иначе не
    совпадут ни с чем.

    Более длинный шаблон выигрывает: «ролик натяжной грм» должен
    перебивать «ролик натяжной», иначе уточнение теряется.
    """

    __tablename__ = "certificationexemptionrule"

    pattern = Column(String(255), nullable=False)
    # Нормализованная форма — по ней и сравниваем.
    normalized_pattern = Column(String(255), nullable=False, index=True)
    # False — не требует сертификации, True — требует (исключение из
    # более общего правила, например ролик ГРМ).
    certification_required = Column(Boolean, default=False, nullable=False)
    comment = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_moscow)

    __table_args__ = (
        UniqueConstraint(
            "normalized_pattern",
            "certification_required",
            name="uq_certification_exemption_pattern",
        ),
    )
