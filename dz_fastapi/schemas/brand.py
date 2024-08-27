from enum import StrEnum
from pydantic import BaseModel
from typing import Optional, List, Union


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
    main_brand: Optional[bool] = False


class BrandCreate(BrandBase):
    synonyms: Optional[List[str]] = []


class BrandUpdate(BrandBase):
    synonym_name: Optional[str] = None


class BrandCreateInDB(BrandBase):
    id: int
    synonyms: List[BrandBase] = []

    class Config:
        orm_mode = True
        from_attributes = True


class BrandUpdateInDB(BrandBase):
    pass


class SynonymCreate(BaseModel):
    names: List[str]

class SynonymResponse(BaseModel):
    id: int
    name: str

class BrandResponse(BaseModel):
    id: int
    name: str
    synonyms: List[SynonymResponse]

class Engine(BaseModel):
    name: str
    power: int
    fuel_type: str
