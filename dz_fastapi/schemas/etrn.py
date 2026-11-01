from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class CarrierIn(BaseModel):
    name: str = Field(min_length=1, max_length=500)
    inn: Optional[str] = Field(default=None, max_length=32)
    kpp: Optional[str] = Field(default=None, max_length=32)
    address: Optional[str] = Field(default=None, max_length=1000)
    phone: Optional[str] = Field(default=None, max_length=64)
    diadoc_box_id: Optional[str] = Field(default=None, max_length=64)
    comment: Optional[str] = Field(default=None, max_length=1000)
    is_active: bool = True


class DriverIn(BaseModel):
    carrier_id: Optional[int] = None
    last_name: str = Field(min_length=1, max_length=120)
    first_name: str = Field(min_length=1, max_length=120)
    middle_name: Optional[str] = Field(default=None, max_length=120)
    license_series: Optional[str] = Field(default=None, max_length=16)
    license_number: Optional[str] = Field(default=None, max_length=32)
    license_date: Optional[date] = None
    phone: Optional[str] = Field(default=None, max_length=64)
    is_active: bool = True


class VehicleIn(BaseModel):
    carrier_id: Optional[int] = None
    plate_number: str = Field(min_length=1, max_length=32)
    brand: Optional[str] = Field(default=None, max_length=120)
    vehicle_type: Optional[str] = Field(default=None, max_length=120)
    max_weight: Optional[float] = Field(default=None, ge=0, lt=1000)
    capacity: Optional[float] = Field(default=None, ge=0, lt=1000)
    trailer_number: Optional[str] = Field(default=None, max_length=32)
    ownership: str = Field(default="1", pattern="^[1-5]$")
    is_active: bool = True


class AddressIn(BaseModel):
    kind: str = Field(default="other", pattern="^(warehouse|customer|other)$")
    label: str = Field(min_length=1, max_length=255)
    customer_id: Optional[int] = None
    zip_code: str = Field(pattern=r"^\d{6}$")
    region_code: str = Field(pattern=r"^\d{2}$")
    city: Optional[str] = Field(default=None, max_length=255)
    locality: Optional[str] = Field(default=None, max_length=255)
    street: Optional[str] = Field(default=None, max_length=255)
    building: Optional[str] = Field(default=None, max_length=20)
    block: Optional[str] = Field(default=None, max_length=20)
    apartment: Optional[str] = Field(default=None, max_length=20)
    other_info: Optional[str] = Field(default=None, max_length=1000)
    is_active: bool = True


class AddressManual(BaseModel):
    label: Optional[str] = None
    zip_code: str = Field(pattern=r"^\d{6}$")
    region_code: str = Field(pattern=r"^\d{2}$")
    city: Optional[str] = None
    locality: Optional[str] = None
    street: Optional[str] = None
    building: Optional[str] = None
    block: Optional[str] = None
    apartment: Optional[str] = None
    other_info: Optional[str] = None


class CargoLine(BaseModel):
    name: str = Field(min_length=1, max_length=1000)
    places: int = Field(gt=0, lt=10000)
    gross_weight: float = Field(ge=0)
    condition: Optional[str] = None
    package_method: Optional[str] = None
    container_type: Optional[str] = None


class WarrantIn(BaseModel):
    """Необязательная МЧД: подписывает директор своей КЭП — не нужна."""

    id: str
    date: date
    number: Optional[str] = None


class WaybillDraftIn(BaseModel):
    shipment_id: Optional[int] = None
    customer_id: Optional[int] = None
    number: str = Field(min_length=1, max_length=120)
    date: date
    order_number: Optional[str] = None
    order_date: Optional[date] = None
    consignee_box_id: Optional[str] = None
    consignee_phone: Optional[str] = None
    consignee_name: Optional[str] = None
    carrier_id: Optional[int] = None
    carrier_manual: Optional[dict[str, Any]] = None
    driver_id: Optional[int] = None
    driver_manual: Optional[dict[str, Any]] = None
    vehicle_id: Optional[int] = None
    vehicle_manual: Optional[dict[str, Any]] = None
    loading_address_id: Optional[int] = None
    loading_address_manual: Optional[AddressManual] = None
    delivery_address_id: Optional[int] = None
    delivery_address_manual: Optional[AddressManual] = None
    cargo: list[CargoLine] = Field(min_length=1)
    loading_arrival_at: datetime
    loading_departure_at: datetime
    directives: Optional[str] = None
    warrant: Optional[WarrantIn] = None


class T3AcceptIn(BaseModel):
    result: str = Field(pattern="^(accepted|partial|rejected)$")
    places_received: Optional[int] = Field(default=None, ge=0)
    gross_weight_received: Optional[float] = Field(default=None, ge=0)
    cargo_status: Optional[str] = Field(default=None, max_length=10000)
    rejection_reason: Optional[str] = Field(default=None, max_length=2000)
    delivery_address_id: Optional[int] = None
    delivery_address: Optional[AddressManual] = None
    arrival_at: Optional[datetime] = None
    departure_at: Optional[datetime] = None
    carrier_box_id: Optional[str] = None
    warrant: Optional[WarrantIn] = None


class BankIn(BaseModel):
    account: Optional[str] = None
    name: Optional[str] = None
    bik: Optional[str] = None
    corr_account: Optional[str] = None


class T6SignIn(BaseModel):
    department: Optional[str] = Field(default=None, max_length=255)
    okpo: Optional[str] = Field(default=None, max_length=20)
    info: Optional[str] = Field(default=None, max_length=1000)
    bank: Optional[BankIn] = None
    warrant: Optional[WarrantIn] = None
