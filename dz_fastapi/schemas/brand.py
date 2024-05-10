from enum import StrEnum
from pydantic import BaseModel
from typing import Optional


class CountryEnum(StrEnum):
    USA = 'USA'
    UK = 'UK'
    GERMANY = 'Germany'
    CHINA = 'China'
    FRANCE = 'France'
    ITALY = 'Italy'
    JAPAN = 'Japan'
    RUSSIA = 'Russia'
    SPAIN = 'Spain'


class BrandBase(BaseModel):
    name: str
    country_of_origin: CountryEnum
    website: Optional[str] = None
    description: Optional[str] = None
    logo: Optional[str] = None
    main_brand: Optional[bool] = True


class BrandCreate(BrandBase):
    pass


class BrandUpdate(BrandBase):
    pass


class BrandCreateInDB(BrandBase):
    pass


class BrandUpdateInDB(BrandBase):
    pass


class Engine(BaseModel):
    name: str
    power: int
    fuel_type: str
