# from dz_fastapi.core.db import Base
# from typing import Optional
# from enum import unique, StrEnum
#
# from sqlalchemy import (
#     Column,
#     String,
#     Integer,
#     Enum,
#     ForeignKey,
#     UniqueConstraint,
#     Table
# )
# from sqlalchemy.orm import relationship
#
#
#
# @unique
# class TYPE_AIR_FILTER(StrEnum):
#     '''
#     Типы воздушного фильтра
#     '''
#     PLASTIC_CASE = 'Plastic case'
#     RUBBER_CASE = 'Rubber case'
#     PAPER_CASE = 'Paper case'
#
#
# standard_size_autopart_association = Table(
#     'standard_size_autopart_association',
#     Base.metadata,
#     Column(
#         'standard_size_id',
#         ForeignKey('standardsize.id'),
#         nullable=True
#     ),
#     Column(
#         'autopart_id',
#         ForeignKey('autopart.id'),
#         nullable=True,
#     ),
#
#     UniqueConstraint(
#         'standard_size_id',
#         'autopart_id',
#         name='unique_standard_size_autopart'
#     )
# )
#
#
# class StandardSize(Base):
#     name = Column(String(256), nullable=False)
#     autoparts = relationship(
#         'AutoPart',
#         secondary='standard_size_autopart_association',
#         back_populates='standardsize'
#     )
#     size_type = Column(String(64))
#     __mapper_args__ = {
#         'polymorphic_identity': 'standard_size',
#         'polymorphic_on': 'size_type'
#     }
#
#
# class SealSize(StandardSize):
#     '''
#     Модель сальника
#     '''
#     id = Column(
#         Integer,
#         ForeignKey('standardsize.id'),
#         primary_key=True
#     )
#     inner_diameter = Column(
#     Integer,
#     doc='Внутренний диаметр', nullable=False
#     )
#     external_diameter = Column(
#     Integer,
#     doc='Внешний диаметр',
#     nullable=False
#     )
#     width: int = Column(
#     Integer,
#     doc='Высота основной части',
#     nullable=False
#     )
#     width_with_projection = Column(
#         Integer,
#         doc='Высота общая с выступом',
#         nullable=True
#     )
#
#     __mapper_args__ = {
#         'polymorphic_identity': 'seal_size',
#     }
#
#
# class CabinFilter(StandardSize):
#     '''
#     Модель салонного фильтра
#     '''
#     id: int = Column(
#         Integer,
#         ForeignKey('standardsize.id'),
#         primary_key=True
#     )
#     length = Column(Integer, doc='Длина', nullable=False)
#     width = Column(Integer, doc='Ширина', nullable=False)
#     height = Column(Integer, doc='Высота', nullable=False)
#
#     __mapper_args__ = {
#         'polymorphic_identity': 'cabin_filter',
#     }
#
#
# class AirFilter(StandardSize):
#     '''
#     Модель воздушного фильтра
#     '''
#     id = Column(
#         Integer,
#         ForeignKey('standardsize.id'),
#         primary_key=True
#     )
#     type_case: TYPE_AIR_FILTER = Column(
#         Enum(TYPE_AIR_FILTER),
#         default=TYPE_AIR_FILTER.PLASTIC_CASE,
#         doc='Тип корпуса фильтра'
#     )
#     length = Column(Integer, doc='Длина', nullable=False)
#     width = Column(Integer, doc='Ширина', nullable=False)
#     height = Column(Integer, doc='Высота', nullable=False)
#
#     __mapper_args__ = {
#         'polymorphic_identity': 'air_filter',
#     }
