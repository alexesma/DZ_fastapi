from pydantic import BaseModel, Field, StringConstraints, validator, field_validator, ConfigDict
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


class AutoPartResponse(BaseModel):
    id: int
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
    categories: List[str] = Field(default_factory=list)
    storage_locations: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

    @field_validator('categories', mode='before')
    def get_category_names(cls, v):
        if v:
            return [category.name for category in v]
        return []

    @field_validator('storage_locations', mode='before')
    def get_storage_location_names(cls, v):
        if v:
            return [storage_location.name for storage_location in v]
        return []


class AutoPartCreate(BaseModel):
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
    category_name: Optional[str] = None
    storage_location_name: Optional[str] = None


class AutoPartCreateInDB(AutoPartBase):
    pass


class AutoPartUpdate(BaseModel):
    brand_id: Optional[int] = None
    name: Optional[str] = None
    oem_number: Optional[str] = None
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
    category_name: Optional[str] = None
    storage_location_name: Optional[str] = None


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

    model_config = ConfigDict(from_attributes=True)

    @field_validator('children', mode='before')
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
        StringConstraints(pattern='^[A-Z0-9 ]+$',max_length=MAX_LIGHT_NAME_LOCATION)
    ]

class StorageLocationCreate(StorageLocationBase):
    pass

class StorageLocationUpdate(BaseModel):
    name: Annotated[
        Optional[str],
        StringConstraints(pattern='^[A-Z0-9 ]+$', max_length=MAX_LIGHT_NAME_LOCATION)
    ] = None

class StorageLocationResponse(StorageLocationBase):
    id: int
    name: str
    autoparts: List[AutoPartResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

StorageLocationResponse.model_rebuild()
AutoPartResponse.model_rebuild()
