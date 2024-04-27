from pydantic import BaseModel
from typing import Optional


class AutoPartBase(BaseModel):
    brand_id: int
    oem_number: str
    name: str
    description: Optional[str]
    width: Optional[float]
    height: Optional[float]
    length: Optional[float]
    weight: Optional[float]
    purchase_price: Optional[float]
    retail_price: Optional[float]
    wholesale_price: Optional[float]
    multiplicity: Optional[int]
    minimum_balance: Optional[int]
    min_balance_auto: Optional[bool]
    min_balance_user: Optional[bool]
    comment: Optional[str]


class AutoPartCreate(AutoPartBase):
    pass


class AutoPartCreateInDB(AutoPartBase):
    pass


class AutoPartUpdate(AutoPartBase):
    pass


class AutoPartUpdateInDB(AutoPartBase):
    pass
