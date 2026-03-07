from sqlalchemy import (JSON, Boolean, Column, DateTime, Float, ForeignKey,
                        Integer, String, UniqueConstraint)

from dz_fastapi.core.db import Base
from dz_fastapi.core.time import now_moscow


class CustomerPriceListOverride(Base):
    __tablename__ = 'customerpricelistoverride'

    config_id = Column(
        Integer, ForeignKey('customerpricelistconfig.id'), nullable=False
    )
    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=False)
    price = Column(Float, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(DateTime(timezone=True), default=now_moscow)

    __table_args__ = (
        UniqueConstraint(
            'config_id', 'autopart_id', name='uq_pricelist_override_item'
        ),
    )


class PriceControlConfig(Base):
    __tablename__ = 'pricecontrolconfig'

    customer_id = Column(Integer, ForeignKey('customer.id'), nullable=False)
    pricelist_config_id = Column(
        Integer, ForeignKey('customerpricelistconfig.id'), nullable=False
    )

    is_active = Column(Boolean, default=True)
    total_daily_count = Column(Integer, default=100)

    schedule_days = Column(JSON, default=[])
    schedule_times = Column(JSON, default=[])

    min_stock = Column(Integer, nullable=True)
    max_delivery_days = Column(Integer, nullable=True)

    delta_pct = Column(Float, default=0.2)
    target_cheapest_pct = Column(Float, default=60.0)

    site_api_key_env = Column(String(128), nullable=True)
    exclude_dragonzap_non_dz = Column(Boolean, default=False)

    our_offer_field = Column(String(64), nullable=True)
    our_offer_match = Column(String(255), nullable=True)
    client_markup_coef = Column(Float, default=1.0)
    client_markup_sample_size = Column(Integer, default=0)
    client_markup_recent_coef = Column(JSON, default=[])
    cooldown_hours = Column(Integer, default=0)
    cooldown_reset_at = Column(DateTime(timezone=True), nullable=True)

    own_cost_markup_default = Column(Float, default=20.0)
    own_cost_markup_by_brand = Column(JSON, default={})

    last_run_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(DateTime(timezone=True), default=now_moscow)


class PriceControlStateProfile(Base):
    __tablename__ = 'pricecontrolstateprofile'

    config_id = Column(
        Integer, ForeignKey('pricecontrolconfig.id'), nullable=False
    )
    site_api_key_env = Column(String(128), nullable=False, default='')
    our_offer_field = Column(String(64), nullable=False, default='')
    our_offer_match = Column(String(255), nullable=False, default='')

    client_markup_coef = Column(Float, default=1.0)
    client_markup_sample_size = Column(Integer, default=0)
    client_markup_recent_coef = Column(JSON, default=[])
    cooldown_hours = Column(Integer, default=0)
    cooldown_reset_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=now_moscow)
    updated_at = Column(DateTime(timezone=True), default=now_moscow)

    __table_args__ = (
        UniqueConstraint(
            'config_id',
            'site_api_key_env',
            'our_offer_field',
            'our_offer_match',
            name='uq_pricecontrol_state_profile',
        ),
    )


class PriceControlSource(Base):
    __tablename__ = 'pricecontrolsource'

    config_id = Column(
        Integer, ForeignKey('pricecontrolconfig.id'), nullable=False
    )
    provider_config_id = Column(
        Integer, ForeignKey('providerpricelistconfig.id'), nullable=False
    )
    weight_pct = Column(Float, default=0.0)
    min_markup_pct = Column(Float, default=0.0)
    locked = Column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint(
            'config_id', 'provider_config_id',
            name='uq_pricecontrol_source'
        ),
    )


class PriceControlManualItem(Base):
    __tablename__ = 'pricecontrolmanualitem'

    config_id = Column(
        Integer, ForeignKey('pricecontrolconfig.id'), nullable=False
    )
    oem = Column(String(255), nullable=False)
    brand = Column(String(255), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'config_id', 'oem', 'brand',
            name='uq_pricecontrol_manual_item'
        ),
    )


class PriceControlRun(Base):
    __tablename__ = 'pricecontrolrun'

    config_id = Column(
        Integer, ForeignKey('pricecontrolconfig.id'), nullable=False
    )
    run_at = Column(DateTime(timezone=True), default=now_moscow)
    status = Column(String(32), default='done')
    total_items = Column(Integer, default=0)


class PriceControlRecommendation(Base):
    __tablename__ = 'pricecontrolrecommendation'

    run_id = Column(
        Integer, ForeignKey('pricecontrolrun.id'), nullable=False
    )
    provider_config_id = Column(Integer, nullable=True)
    autopart_id = Column(Integer, ForeignKey('autopart.id'), nullable=True)

    oem = Column(String(255), nullable=False)
    brand = Column(String(255), nullable=False)
    name = Column(String(255), nullable=True)

    our_price = Column(Float, nullable=True)
    competitor_price = Column(Float, nullable=True)
    competitor_qty = Column(Integer, nullable=True)
    competitor_supplier = Column(String(255), nullable=True)
    competitor_min_delivery = Column(Integer, nullable=True)
    competitor_max_delivery = Column(Integer, nullable=True)

    target_price = Column(Float, nullable=True)
    effective_client_coef = Column(Float, nullable=True)
    effective_client_pct = Column(Float, nullable=True)
    cost_price = Column(Float, nullable=True)
    min_allowed_price = Column(Float, nullable=True)

    is_cheapest = Column(Boolean, default=False)
    below_min_markup = Column(Boolean, default=False)
    below_cost = Column(Boolean, default=False)
    missing_competitor = Column(Boolean, default=False)
    missing_in_pricelist = Column(Boolean, default=False)

    suggested_action = Column(String(32), nullable=True)


class PriceControlSourceRecommendation(Base):
    __tablename__ = 'pricecontrolsource_reco'

    run_id = Column(
        Integer, ForeignKey('pricecontrolrun.id'), nullable=False
    )
    provider_config_id = Column(
        Integer, ForeignKey('providerpricelistconfig.id'), nullable=False
    )
    current_markup_pct = Column(Float, nullable=True)
    suggested_markup_pct = Column(Float, nullable=True)
    coverage_pct = Column(Float, nullable=True)
    sample_size = Column(Integer, default=0)
    note = Column(String(255), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            'run_id', 'provider_config_id',
            name='uq_pricecontrol_source_reco'
        ),
    )
