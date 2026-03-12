from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.price_control import (crud_price_control_config,
                                           crud_price_control_manual,
                                           crud_price_control_reco,
                                           crud_price_control_run,
                                           crud_price_control_source,
                                           crud_price_control_source_reco,
                                           crud_price_control_state_profile)
from dz_fastapi.models.partner import ProviderPriceListConfig
from dz_fastapi.models.price_control import PriceControlRun
from dz_fastapi.schemas.price_control import (
    PriceControlApplyRecommendations, PriceControlApplySourceRecommendations,
    PriceControlConfigCreate, PriceControlConfigResponse,
    PriceControlConfigUpdate, PriceControlManualItemResponse,
    PriceControlRecommendationResponse, PriceControlRunResponse,
    PriceControlSiteApiKeyOption, PriceControlSourceRecommendationResponse,
    PriceControlSourceResponse, PriceControlStateProfileResponse)
from dz_fastapi.services.price_control import (apply_recommendations,
                                               apply_source_recommendations,
                                               list_site_api_key_env_names,
                                               run_price_control)

router = APIRouter()


async def _provider_config_map(
    session: AsyncSession, provider_config_ids: list[int]
) -> dict[int, ProviderPriceListConfig]:
    if not provider_config_ids:
        return {}
    stmt = (
        select(ProviderPriceListConfig)
        .options(selectinload(ProviderPriceListConfig.provider))
        .where(ProviderPriceListConfig.id.in_(provider_config_ids))
    )
    rows = (await session.execute(stmt)).scalars().all()
    return {row.id: row for row in rows}


def _build_source_response(
    source, provider_config: ProviderPriceListConfig | None
) -> PriceControlSourceResponse:
    provider = provider_config.provider if provider_config else None
    return PriceControlSourceResponse(
        id=source.id,
        provider_config_id=source.provider_config_id,
        provider_id=getattr(provider, 'id', None),
        provider_name=getattr(provider, 'name', None),
        provider_config_name=getattr(provider_config, 'name_price', None),
        weight_pct=source.weight_pct or 0.0,
        min_markup_pct=source.min_markup_pct or 0.0,
        locked=bool(source.locked),
    )


def _recent_pct_from_coef(values: object) -> list[float]:
    if not isinstance(values, list):
        return []
    result = []
    for value in values:
        try:
            coef = float(value)
        except (TypeError, ValueError):
            continue
        if coef <= 0:
            continue
        result.append(round(((1 / coef) - 1) * 100, 2))
    return result


def _normalize_profile_part(value: str | None) -> str:
    return str(value or '').strip()


def _build_state_profile_response(profile) -> PriceControlStateProfileResponse:
    return PriceControlStateProfileResponse(
        id=profile.id,
        site_api_key_env=profile.site_api_key_env or None,
        our_offer_field=profile.our_offer_field or None,
        our_offer_match=profile.our_offer_match or None,
        client_markup_coef=float(profile.client_markup_coef or 1.0),
        client_markup_sample_size=int(
            profile.client_markup_sample_size or 0
        ),
        client_markup_recent_pct=_recent_pct_from_coef(
            profile.client_markup_recent_coef
        ),
        cooldown_hours=int(profile.cooldown_hours or 0),
        cooldown_reset_at=profile.cooldown_reset_at,
        updated_at=profile.updated_at,
    )


def _resolve_active_profile(config, profiles: list):
    target_site_env = _normalize_profile_part(config.site_api_key_env)
    target_offer_field = _normalize_profile_part(config.our_offer_field)
    target_offer_match = _normalize_profile_part(config.our_offer_match)
    for profile in profiles:
        if (
            _normalize_profile_part(profile.site_api_key_env)
            == target_site_env
            and _normalize_profile_part(profile.our_offer_field)
            == target_offer_field
            and _normalize_profile_part(profile.our_offer_match)
            == target_offer_match
        ):
            return profile
    return profiles[0] if profiles else None


def _build_config_response(
    config,
    sources: list[PriceControlSourceResponse],
    manual_items: list[PriceControlManualItemResponse],
    state_profiles: list[PriceControlStateProfileResponse],
    active_profile,
) -> PriceControlConfigResponse:
    source = active_profile or config
    recent_pct = _recent_pct_from_coef(source.client_markup_recent_coef or [])

    return PriceControlConfigResponse(
        id=config.id,
        customer_id=config.customer_id,
        pricelist_config_id=config.pricelist_config_id,
        is_active=bool(config.is_active),
        total_daily_count=config.total_daily_count or 0,
        client_markup_coef=float(source.client_markup_coef or 1.0),
        client_markup_sample_size=int(source.client_markup_sample_size or 0),
        client_markup_recent_pct=recent_pct,
        active_state_profile_id=getattr(active_profile, 'id', None),
        state_profiles=state_profiles,
        schedule_days=config.schedule_days or [],
        schedule_times=config.schedule_times or [],
        min_stock=config.min_stock,
        max_delivery_days=config.max_delivery_days,
        delta_pct=config.delta_pct or 0,
        target_cheapest_pct=config.target_cheapest_pct or 0,
        site_api_key_env=config.site_api_key_env,
        exclude_dragonzap_non_dz=bool(
            config.exclude_dragonzap_non_dz
        ),
        cooldown_hours=int(getattr(source, 'cooldown_hours', 0) or 0),
        our_offer_field=config.our_offer_field,
        our_offer_match=config.our_offer_match,
        own_cost_markup_default=config.own_cost_markup_default or 0,
        own_cost_markup_by_brand=config.own_cost_markup_by_brand or {},
        cooldown_reset_at=getattr(source, 'cooldown_reset_at', None),
        last_run_at=config.last_run_at,
        created_at=config.created_at,
        updated_at=config.updated_at,
        sources=sources,
        manual_items=manual_items,
    )


async def _load_config_response(
    session: AsyncSession, config_id: int
) -> PriceControlConfigResponse:
    config = await crud_price_control_config.get(session, config_id)
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    sources = await crud_price_control_source.list_by_config(
        session=session, config_id=config.id
    )
    manual_items = await crud_price_control_manual.list_by_config(
        session=session, config_id=config.id
    )
    provider_map = await _provider_config_map(
        session, [s.provider_config_id for s in sources]
    )
    source_payloads = [
        _build_source_response(s, provider_map.get(s.provider_config_id))
        for s in sources
    ]
    manual_payloads = [
        PriceControlManualItemResponse.model_validate(item)
        for item in manual_items
    ]
    profiles = await crud_price_control_state_profile.list_by_config(
        session=session, config_id=config.id
    )
    active_profile = _resolve_active_profile(config, profiles)
    profile_payloads = [
        _build_state_profile_response(profile) for profile in profiles
    ]
    return _build_config_response(
        config,
        source_payloads,
        manual_payloads,
        profile_payloads,
        active_profile,
    )


async def _sync_active_state_profile(
    session: AsyncSession, config
):
    profile = await crud_price_control_state_profile.get_or_create_active(
        session=session, config=config
    )
    profile.cooldown_hours = int(config.cooldown_hours or 0)
    if (
        not profile.client_markup_recent_coef
        and config.client_markup_recent_coef
    ):
        profile.client_markup_recent_coef = config.client_markup_recent_coef
    if (
        not profile.client_markup_sample_size
        and config.client_markup_sample_size
    ):
        profile.client_markup_sample_size = int(
            config.client_markup_sample_size
        )
    if (
        (profile.client_markup_coef is None or profile.client_markup_coef <= 0)
        and config.client_markup_coef
    ):
        profile.client_markup_coef = float(config.client_markup_coef)
    session.add(profile)
    await session.commit()
    return profile


@router.get(
    '/price-control/site-api-keys',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceControlSiteApiKeyOption],
    dependencies=[Depends(require_admin)],
)
async def list_price_control_site_api_keys():
    options = []
    for env_name in list_site_api_key_env_names():
        suffix = env_name.replace('API_CONTROL_KEY_FOR_', '')
        label = suffix if suffix else env_name
        options.append(
            PriceControlSiteApiKeyOption(env_name=env_name, label=label)
        )
    return options


@router.get(
    '/price-control/configs',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceControlConfigResponse],
    dependencies=[Depends(require_admin)],
)
async def list_price_control_configs(
    customer_id: int | None = Query(default=None),
    pricelist_config_id: int | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    if customer_id is None:
        return []
    configs = await crud_price_control_config.list_by_customer(
        session=session, customer_id=customer_id
    )
    if pricelist_config_id is not None:
        configs = [
            config
            for config in configs
            if config.pricelist_config_id == pricelist_config_id
        ]
    responses = []
    for config in configs:
        responses.append(await _load_config_response(session, config.id))
    return responses


@router.get(
    '/price-control/configs/{config_id}',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=PriceControlConfigResponse,
    dependencies=[Depends(require_admin)],
)
async def get_price_control_config(
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    return await _load_config_response(session, config_id)


@router.post(
    '/price-control/configs',
    tags=['price-control'],
    status_code=status.HTTP_201_CREATED,
    response_model=PriceControlConfigResponse,
    dependencies=[Depends(require_admin)],
)
async def create_price_control_config(
    payload: PriceControlConfigCreate,
    session: AsyncSession = Depends(get_session),
):
    existing = await crud_price_control_config.get_by_customer_pricelist(
        session=session,
        customer_id=payload.customer_id,
        pricelist_config_id=payload.pricelist_config_id,
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail='Config already exists for this pricelist',
        )
    data = payload.model_dump(exclude={'sources', 'manual_items'})
    if not data.get('schedule_times'):
        data['schedule_times'] = ['09:00']
    config = await crud_price_control_config.create(session, data)
    if payload.sources:
        await crud_price_control_source.replace_for_config(
            session=session,
            config_id=config.id,
            sources=[s.model_dump() for s in payload.sources],
        )
    if payload.manual_items:
        await crud_price_control_manual.replace_for_config(
            session=session,
            config_id=config.id,
            items=[i.model_dump() for i in payload.manual_items],
        )
    await _sync_active_state_profile(session, config)
    return await _load_config_response(session, config.id)


@router.patch(
    '/price-control/configs/{config_id}',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=PriceControlConfigResponse,
    dependencies=[Depends(require_admin)],
)
async def update_price_control_config(
    config_id: int,
    payload: PriceControlConfigUpdate,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_price_control_config.get(session, config_id)
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    data = payload.model_dump(
        exclude_unset=True, exclude={'sources', 'manual_items'}
    )
    if data:
        config = await crud_price_control_config.update(
            session=session, config=config, data=data
        )
    if payload.sources is not None:
        await crud_price_control_source.replace_for_config(
            session=session,
            config_id=config.id,
            sources=[s.model_dump() for s in payload.sources],
        )
    if payload.manual_items is not None:
        await crud_price_control_manual.replace_for_config(
            session=session,
            config_id=config.id,
            items=[i.model_dump() for i in payload.manual_items],
        )
    await _sync_active_state_profile(session, config)
    return await _load_config_response(session, config.id)


@router.get(
    '/price-control/configs/{config_id}/runs',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceControlRunResponse],
    dependencies=[Depends(require_admin)],
)
async def list_price_control_runs(
    config_id: int,
    limit: int = Query(default=20, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    runs = await crud_price_control_run.list_by_config(
        session=session, config_id=config_id, limit=limit
    )
    return [PriceControlRunResponse.model_validate(run) for run in runs]


@router.post(
    '/price-control/configs/{config_id}/run',
    tags=['price-control'],
    status_code=status.HTTP_201_CREATED,
    response_model=PriceControlRunResponse,
    dependencies=[Depends(require_admin)],
)
async def run_price_control_now(
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_price_control_config.get(session, config_id)
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    run_id = await run_price_control(session, config)
    run = await session.get(PriceControlRun, run_id)
    if not run:
        raise HTTPException(
            status_code=500, detail='Failed to load run result'
        )
    return PriceControlRunResponse.model_validate(run)


@router.post(
    '/price-control/configs/{config_id}/reset-history',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=PriceControlConfigResponse,
    dependencies=[Depends(require_admin)],
)
async def reset_price_control_history(
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_price_control_config.get(session, config_id)
    if not config:
        raise HTTPException(status_code=404, detail='Config not found')
    profile = await crud_price_control_state_profile.get_or_create_active(
        session=session, config=config
    )
    profile.cooldown_reset_at = now_moscow()
    profile.client_markup_coef = 1.0
    profile.client_markup_sample_size = 0
    profile.client_markup_recent_coef = []
    session.add(profile)
    config.cooldown_reset_at = profile.cooldown_reset_at
    config.client_markup_coef = 1.0
    config.client_markup_sample_size = 0
    config.client_markup_recent_coef = []
    session.add(config)
    await session.commit()
    return await _load_config_response(session, config.id)


@router.get(
    '/price-control/runs/{run_id}/recommendations',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceControlRecommendationResponse],
    dependencies=[Depends(require_admin)],
)
async def list_price_control_recommendations(
    run_id: int,
    session: AsyncSession = Depends(get_session),
):
    recos = await crud_price_control_reco.list_by_run(
        session=session, run_id=run_id
    )
    return [
        PriceControlRecommendationResponse.model_validate(r) for r in recos
    ]


@router.get(
    '/price-control/runs/{run_id}/source-recommendations',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    response_model=list[PriceControlSourceRecommendationResponse],
    dependencies=[Depends(require_admin)],
)
async def list_price_control_source_recommendations(
    run_id: int,
    session: AsyncSession = Depends(get_session),
):
    recos = await crud_price_control_source_reco.list_by_run(
        session=session, run_id=run_id
    )
    provider_map = await _provider_config_map(
        session, [r.provider_config_id for r in recos]
    )
    responses = []
    for reco in recos:
        provider_config = provider_map.get(reco.provider_config_id)
        provider = provider_config.provider if provider_config else None
        responses.append(
            PriceControlSourceRecommendationResponse(
                id=reco.id,
                run_id=reco.run_id,
                provider_config_id=reco.provider_config_id,
                provider_name=getattr(provider, 'name', None),
                provider_config_name=getattr(
                    provider_config, 'name_price', None
                ),
                current_markup_pct=reco.current_markup_pct,
                suggested_markup_pct=reco.suggested_markup_pct,
                coverage_pct=reco.coverage_pct,
                sample_size=reco.sample_size or 0,
                note=reco.note,
            )
        )
    return responses


@router.post(
    '/price-control/runs/{run_id}/apply',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def apply_price_control_recommendations(
    run_id: int,
    payload: PriceControlApplyRecommendations,
    session: AsyncSession = Depends(get_session),
):
    count = await apply_recommendations(
        session=session,
        run_id=run_id,
        recommendation_ids=payload.recommendation_ids,
    )
    return {'applied': count}


@router.post(
    '/price-control/runs/{run_id}/apply-sources',
    tags=['price-control'],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def apply_price_control_source_recommendations(
    run_id: int,
    payload: PriceControlApplySourceRecommendations,
    session: AsyncSession = Depends(get_session),
):
    count = await apply_source_recommendations(
        session=session,
        run_id=run_id,
        source_recommendation_ids=payload.source_recommendation_ids,
    )
    return {'applied': count}
