from enum import StrEnum
from pydantic import BaseModel, Field
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


class BrandSynonym(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True


class BrandCreate(BrandBase):
    synonyms: Optional[List[str]] = []


class BrandUpdate(BrandBase):
    name: Optional[str] = None
    country_of_origin: Optional[CountryEnum] = None
    synonym_name: Optional[str] = None


class BrandCreateInDB(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    main_brand: bool = False
    website: Optional[str] = None
    country_of_origin: Optional[str] = None
    logo: Optional[str] = None
    synonyms: List[BrandSynonym] = Field(default_factory=list)

    class Config:
        orm_mode = True
        from_attributes = True

    @classmethod
    def from_orm(cls, obj):
        # Преобразуем SQLAlchemy объект в словарь
        data = {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
        # Особая обработка для поля synonyms
        data['synonyms'] = [BrandBase.from_orm(syn) for syn in obj.synonyms]
        return cls(**data)


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
