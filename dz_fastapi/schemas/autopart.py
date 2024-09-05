from pydantic import BaseModel
from typing import Optional


class AutoPartBase(BaseModel):
    brand_id: int
    oem_number: str
    name: str
    description: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None
    length: Optional[float] = None
    weight: Optional[float] = None
    purchase_price: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    multiplicity: Optional[int] = None
    minimum_balance: Optional[int] = None
    min_balance_auto: Optional[bool] = None
    min_balance_user: Optional[bool] = None
    comment: Optional[str] = None
    barcode: Optional[str] = None


class AutoPartResponse(AutoPartBase):
    id: int

    class Config:
        from_attributes = True


class AutoPartCreate(AutoPartBase):
    pass


class AutoPartCreateInDB(AutoPartBase):
    pass


class AutoPartUpdate(AutoPartBase):
    pass


class AutoPartUpdateInDB(AutoPartBase):
    pass
