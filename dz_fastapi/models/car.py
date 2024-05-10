# from sqlalchemy import (
#     Column,
#     String,
#     Enum,
#     Integer,
#     ForeignKey,
#     Table,
#     UniqueConstraint,
#     CheckConstraint
# )
# from sqlalchemy.orm import relationship, validates
# from datetime import datetime
#
# from enum import unique, Enum as PyEnum
# from dz_fastapi.core.constants import (
#     MAX_LIGHT_NAME_CAR_MODEL,
#     MAX_LIGHT_NAME_ENGINE,
#     ERROR_MESSAGE_FORMAT_DATE,
#     ERROR_MESSAGE_RANGE_DATE,
#     FORMAT_YEAR_FOR_CAR_1,
#     FORMAT_YEAR_FOR_CAR_2,
#     MAX_LEN_WEBSITE
# )
# from dz_fastapi.core.db import Base
#
#
# @unique
# class FuelType(PyEnum):
#     PETROL = 'Petrol'
#     DIESEL = 'Diesel'
#     ELECTRIC = 'Electric'
#
#
# class CarModel(Base):
#     brand_id = Column(Integer, ForeignKey('brand.id'), nullable=False)
#     brand = relationship('Brand')
#     name = Column(
#         String(MAX_LIGHT_NAME_CAR_MODEL),
#         nullable=False,
#         unique=True
#     )
#     engines = relationship(
#         'Engine',
#         secondary='car_model_engine_association',
#         backref='carmodels',
#         secondaryjoin="and_(Engine.id == car_model_engine_association.c.engine_id)",
#         viewonly=True
#     )
#     year_start = Column(String, nullable=True)
#     year_end = Column(String, nullable=True)
#     description = Column(String, nullable=True)
#     autoparts = relationship(
#         'AutoPart',
#         secondary='car_model_autopart_association',
#         backref='autoparts_carmodels',
#         overlaps='autoparts_carmodels'
#     )
#     image = Column(String(MAX_LEN_WEBSITE), unique=True, nullable=True)
#     __table_args__ = (
#         UniqueConstraint(
#             'brand_id',
#             'name',
#             name='unique_brand_name'
#         ),
#         CheckConstraint(
#             "name ~ '^[a-zA-Z0-9/-]+$'",
#             name='name_format'
#         ),
#     )
#
#     @validates('year_start', 'year_end')
#     def validate_year(self, key, year_str):
#         if year_str:
#             valid_formats = [FORMAT_YEAR_FOR_CAR_1, FORMAT_YEAR_FOR_CAR_2]
#             for format_year in valid_formats:
#                 try:
#                     year = datetime.strptime(year_str, format_year).year
#                     break
#                 except ValueError:
#                     continue
#             else:
#                 raise ValueError(ERROR_MESSAGE_FORMAT_DATE.format(key=key, expected_formats=valid_formats))
#             current_year = datetime.now().year
#             if year < 1980 or year > current_year:
#                 raise ValueError(ERROR_MESSAGE_RANGE_DATE.format(key=key, year=year))
#         return year_str
#
#
# class Engine(Base):
#     name = Column(
#         String(MAX_LIGHT_NAME_ENGINE),
#         nullable=False,
#         unique=True
#     )
#     fuel_type = Column(Enum(FuelType), nullable=False, default=FuelType.PETROL)
#     power = Column(Integer,  nullable=True)
#     car_models = relationship(
#         'CarModel',
#         secondary='car_model_engine_association',
#         back_populates='engines',
#         secondaryjoin="and_(CarModel.id == car_model_engine_association.c.carmodel_id)",
#         viewonly=True
#     )
#     __table_args__ = (
#         CheckConstraint(
#             "name ~ '^[a-zA-Z0-9/-]+$'",
#             name='name_format'
#         ),
#     )
#
#
# car_model_engine_association = Table(
#     'car_model_engine_association',
#     Base.metadata,
#     Column(
#         'carmodel_id',
#         ForeignKey('carmodel.id'),
#         nullable=True,
#     ),
#     Column(
#         'engine_id',
#         ForeignKey('engine.id'),
#         nullable=True,
#     ),
#     UniqueConstraint(
#         'carmodel_id',
#         'engine_id',
#         name='unique_carmodel_engine'
#     )
# )
#
#
# car_model_autopart_association = Table(
#     'car_model_autopart_association',
#     Base.metadata,
#     Column(
#         'carmodel_id',
#         ForeignKey('carmodel.id'),
#         nullable=True,
#     ),
#     Column(
#         'autopart_id',
#         ForeignKey('autopart.id'),
#         nullable=True,
#     ),
#     UniqueConstraint(
#         'carmodel_id',
#         'autopart_id',
#         name='unique_carmodel_autopart'
#     )
# )
