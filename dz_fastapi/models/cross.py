from sqlalchemy import (Boolean, CheckConstraint, Column, ForeignKey, Integer,
                        String, Text, UniqueConstraint)
from sqlalchemy.orm import relationship

from dz_fastapi.core.db import Base


class AutoPartCross(Base):
    """
    Справочная таблица кроссов автозапчастей.
    Используется для информации о взаимозаменяемости деталей.
    НЕ используется автоматически в прайс-листах.

    Пример: DRAGONZAP DZ12345 = TOYOTA 90915-YZZD3 = MANN W12330
    """

    source_autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    cross_brand_id = Column(
        Integer,
        ForeignKey('brand.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    cross_oem_number = Column(String(50), nullable=False, index=True)
    cross_autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='SET NULL'),
        nullable=True,
    )
    # Приоритет для сортировки (меньше = важнее)
    priority = Column(Integer, default=100)

    # Комментарий
    comment = Column(Text, nullable=True)

    # Отношения
    source_autopart = relationship(
        'AutoPart',
        foreign_keys=[source_autopart_id],
        backref='crosses_as_source',
    )

    cross_brand = relationship(
        'Brand',
        foreign_keys=[cross_brand_id],
    )

    cross_autopart = relationship(
        'AutoPart',
        foreign_keys=[cross_autopart_id],
        backref='crosses_as_cross',
    )

    __table_args__ = (
        UniqueConstraint(
            'source_autopart_id',
            'cross_brand_id',
            'cross_oem_number',
            name='uq_cross',
        ),
        CheckConstraint(
            'source_autopart_id != cross_autopart_id',
            name='check_not_self_cross',
        ),
    )


class AutoPartSubstitution(Base):
    """
    Таблица подмены для прайс-листов.
    Используется АКТИВНО при формировании прайсов для клиентов.

    Правило: когда в прайсе встречается
    source_autopart_id (например DRAGONZAP),
    добавить дополнительные строки с
    substitution_brand_id + substitution_oem_number

    Пример:
    DRAGONZAP DZ12345 (10 шт, цена 100₽)
    → добавляем в прайс:
    - TOYOTA 90915-YZZD3 (9 шт, цена 100₽)  # priority=1
    - GEELY 1234567 (8 шт, цена 100₽)       # priority=2
    """

    # Исходная деталь (обычно DRAGONZAP)
    source_autopart_id = Column(
        Integer,
        ForeignKey('autopart.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )

    # Ссылка на бренд через ID
    substitution_brand_id = Column(
        Integer,
        ForeignKey('brand.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )

    # Артикул для подмены
    substitution_oem_number = Column(String(50), nullable=False, index=True)

    # Приоритет (определяет порядок и уменьшение количества)
    # priority=1 → количество -1
    # priority=2 → количество -2
    priority = Column(Integer, default=1, nullable=False)

    # Минимальное количество исходной детали для подмены
    # Если у DRAGONZAP < 4 шт, подмена не создаётся
    min_source_quantity = Column(Integer, default=4)

    # Уменьшение количества для подмены
    # Если исходная деталь 10 шт, подмена будет: 10 - quantity_reduction
    quantity_reduction = Column(Integer, default=1)

    # Активность подмены
    is_active = Column(Boolean, default=True)

    # Применять только для определённых конфигов клиентов
    # NULL = применяется для всех
    customer_config_id = Column(
        Integer,
        ForeignKey('customerpricelistconfig.id', ondelete='CASCADE'),
        nullable=True,
        index=True,
    )

    # Комментарий
    comment = Column(Text, nullable=True)

    # Отношения
    source_autopart = relationship(
        'AutoPart',
        foreign_keys=[source_autopart_id],
        backref='substitutions',
    )

    substitution_brand = relationship(
        'Brand',
        foreign_keys=[substitution_brand_id],
    )

    customer_config = relationship(
        'CustomerPriceListConfig',
        backref='substitutions',
    )

    __table_args__ = (
        UniqueConstraint(
            'source_autopart_id',
            'substitution_brand_id',
            'substitution_oem_number',
            'customer_config_id',
            name='uq_substitution',
        ),
        CheckConstraint(
            'priority > 0',
            name='check_priority_positive',
        ),
        CheckConstraint(
            'min_source_quantity >= 0',
            name='check_min_quantity_non_negative',
        ),
        CheckConstraint(
            'quantity_reduction >= 0',
            name='check_reduction_non_negative',
        ),
    )
