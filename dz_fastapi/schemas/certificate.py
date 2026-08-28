from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class CertificateBase(BaseModel):
    number: str
    url: Optional[str] = None
    brand_id: Optional[int] = None
    covers_whole_brand: bool = False
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    applicant: Optional[str] = None
    manufacturer: Optional[str] = None
    scope: Optional[str] = None


class CertificateCreate(CertificateBase):
    pass


class CertificateUpdate(BaseModel):
    number: Optional[str] = None
    url: Optional[str] = None
    brand_id: Optional[int] = None
    covers_whole_brand: Optional[bool] = None
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    applicant: Optional[str] = None
    manufacturer: Optional[str] = None
    scope: Optional[str] = None


class CertificateOut(CertificateBase):
    id: int
    brand_name: Optional[str] = None
    source: Optional[str] = None
    autopart_count: int = 0
    # Действует ли документ на сегодня: пустой срок считаем бессрочным,
    # клиент всё равно проверяет по реестру.
    is_expired: bool = False
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class CertificateListResponse(BaseModel):
    items: list[CertificateOut]
    total: int
    page: int
    page_size: int


class CertificateAutoPartRow(BaseModel):
    autopart_id: int
    oem_number: str
    name: str
    brand_name: Optional[str] = None
    regulatory_source: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class CertificateAutoPartsResponse(BaseModel):
    items: list[CertificateAutoPartRow]
    total: int
    page: int
    page_size: int


class CertificateLinkRequest(BaseModel):
    autopart_ids: list[int]


class ApplyBrandRequest(BaseModel):
    brand_id: int
    dry_run: bool = True
    only_undetermined: bool = True


class ApplyBrandResponse(BaseModel):
    certificate: str
    input_number: Optional[str] = None
    normalized: bool = False
    certificate_created: bool = False
    positions: int = 0
    linked: int = 0
