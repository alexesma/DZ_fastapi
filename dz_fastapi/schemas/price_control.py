from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class PriceControlSourceBase(BaseModel):
    provider_config_id: int
    weight_pct: float = 0.0
    min_markup_pct: float = 0.0
    locked: bool = False


class PriceControlSourceCreate(PriceControlSourceBase):
    pass


class PriceControlSourceResponse(PriceControlSourceBase):
    id: int
    provider_id: Optional[int] = None
    provider_name: Optional[str] = None
    provider_config_name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PriceControlManualItemBase(BaseModel):
    oem: str
    brand: str


class PriceControlManualItemCreate(PriceControlManualItemBase):
    pass


class PriceControlManualItemResponse(PriceControlManualItemBase):
    id: int
    model_config = ConfigDict(from_attributes=True)


class PriceControlConfigBase(BaseModel):
    customer_id: int
    pricelist_config_id: int
    is_active: bool = True
    total_daily_count: int = 100
    schedule_days: List[str] = []
    schedule_times: List[str] = []
    min_stock: Optional[int] = None
    max_delivery_days: Optional[int] = None
    delta_pct: float = 0.2
    target_cheapest_pct: float = 60.0
    site_api_key_env: Optional[str] = None
    exclude_dragonzap_non_dz: bool = False
    cooldown_hours: int = 0
    our_offer_field: Optional[str] = None
    our_offer_match: Optional[str] = None
    own_cost_markup_default: float = 20.0
    own_cost_markup_by_brand: Dict[str, float] = {}


class PriceControlConfigCreate(PriceControlConfigBase):
    sources: List[PriceControlSourceCreate] = []
    manual_items: List[PriceControlManualItemCreate] = []


class PriceControlConfigUpdate(BaseModel):
    is_active: Optional[bool] = None
    total_daily_count: Optional[int] = None
    schedule_days: Optional[List[str]] = None
    schedule_times: Optional[List[str]] = None
    min_stock: Optional[int] = None
    max_delivery_days: Optional[int] = None
    delta_pct: Optional[float] = None
    target_cheapest_pct: Optional[float] = None
    site_api_key_env: Optional[str] = None
    exclude_dragonzap_non_dz: Optional[bool] = None
    cooldown_hours: Optional[int] = None
    our_offer_field: Optional[str] = None
    our_offer_match: Optional[str] = None
    own_cost_markup_default: Optional[float] = None
    own_cost_markup_by_brand: Optional[Dict[str, float]] = None
    sources: Optional[List[PriceControlSourceCreate]] = None
    manual_items: Optional[List[PriceControlManualItemCreate]] = None


class PriceControlStateProfileResponse(BaseModel):
    id: int
    site_api_key_env: Optional[str] = None
    our_offer_field: Optional[str] = None
    our_offer_match: Optional[str] = None
    client_markup_coef: float = 1.0
    client_markup_sample_size: int = 0
    client_markup_recent_pct: List[float] = []
    cooldown_hours: int = 0
    cooldown_reset_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class PriceControlConfigResponse(PriceControlConfigBase):
    id: int
    client_markup_coef: float = 1.0
    client_markup_sample_size: int = 0
    client_markup_recent_pct: List[float] = []
    active_state_profile_id: Optional[int] = None
    state_profiles: List[PriceControlStateProfileResponse] = []
    cooldown_reset_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    sources: List[PriceControlSourceResponse] = []
    manual_items: List[PriceControlManualItemResponse] = []

    model_config = ConfigDict(from_attributes=True)


class PriceControlRunResponse(BaseModel):
    id: int
    config_id: int
    run_at: datetime
    status: str
    total_items: int

    model_config = ConfigDict(from_attributes=True)


class PriceControlSiteApiKeyOption(BaseModel):
    env_name: str
    label: str


class PriceControlRecommendationResponse(BaseModel):
    id: int
    run_id: int
    provider_config_id: Optional[int] = None
    autopart_id: Optional[int] = None
    oem: str
    brand: str
    name: Optional[str] = None
    our_price: Optional[float] = None
    competitor_price: Optional[float] = None
    competitor_qty: Optional[int] = None
    competitor_supplier: Optional[str] = None
    competitor_min_delivery: Optional[int] = None
    competitor_max_delivery: Optional[int] = None
    target_price: Optional[float] = None
    effective_client_coef: Optional[float] = None
    effective_client_pct: Optional[float] = None
    cost_price: Optional[float] = None
    min_allowed_price: Optional[float] = None
    is_cheapest: bool = False
    below_min_markup: bool = False
    below_cost: bool = False
    missing_competitor: bool = False
    missing_in_pricelist: bool = False
    suggested_action: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PriceControlSourceRecommendationResponse(BaseModel):
    id: int
    run_id: int
    provider_config_id: int
    provider_name: Optional[str] = None
    provider_config_name: Optional[str] = None
    current_markup_pct: Optional[float] = None
    suggested_markup_pct: Optional[float] = None
    coverage_pct: Optional[float] = None
    sample_size: int = 0
    note: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PriceControlApplyRecommendations(BaseModel):
    recommendation_ids: List[int]


class PriceControlApplySourceRecommendations(BaseModel):
    source_recommendation_ids: List[int]
