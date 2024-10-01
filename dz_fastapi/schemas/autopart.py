from pydantic import BaseModel, Field, StringConstraints, validator
from typing import Optional, List, Annotated
from dz_fastapi.core.constants import MAX_NAME_CATEGORY, MAX_LIGHT_NAME_LOCATION


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
    brand_id: Optional[int] = None
    oem_number: Optional[str] = None
    name: Optional[str] = None


class AutoPartCreateInDB(AutoPartBase):
    pass


class AutoPartUpdate(AutoPartBase):
    pass


class AutoPartUpdateInDB(AutoPartBase):
    pass


# Base schema with shared fields
class CategoryBase(BaseModel):
    name: str = Field(..., max_length=MAX_NAME_CATEGORY)
    comment: Optional[str] = None

# Schema for creating a new category
class CategoryCreate(CategoryBase):
    parent_id: Optional[int] = None

# Schema for updating an existing category
class CategoryUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=MAX_NAME_CATEGORY)
    comment: Optional[str] = None
    parent_id: Optional[int] = None

# Response schema
class CategoryResponse(CategoryBase):
    id: int
    name: str
    parent_id: Optional[int] = None
    children: Optional[List['CategoryResponse']] = Field(default_factory=list)
    # autoparts: List['AutoPartResponse'] = Field(default_factory=list)

    class Config:
        from_attributes = True

    @validator('children', pre=True, always=True)
    def set_children(cls, v):
        if v is None:
            return []
        elif isinstance(v, list):
            return v
        else:
            return [v]

CategoryResponse.model_rebuild()

# StorageLocation Schemas

class StorageLocationBase(BaseModel):
    name:  Annotated[
        str,
        StringConstraints(pattern='^[A-Z0-9]+$',max_length=MAX_LIGHT_NAME_LOCATION)
    ]

class StorageLocationCreate(StorageLocationBase):
    pass

class StorageLocationUpdate(BaseModel):
    name: Annotated[
        Optional[str],
        StringConstraints(pattern='^[A-Z0-9]+$', max_length=MAX_LIGHT_NAME_LOCATION)
    ] = None

class StorageLocationResponse(StorageLocationBase):
    id: int
    autoparts: List['AutoPartResponse'] = []

    class Config:
        from_attributes = True

StorageLocationResponse.model_rebuild()
