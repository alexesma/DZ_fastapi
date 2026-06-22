from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)
from dz_fastapi.schemas.watchlist import (
    PriceWatchItemCreate,
    PriceWatchItemOut,
    PriceWatchItemUpdate,
    PriceWatchListPage,
)

router = APIRouter()


async def _get_saved_provider_offer(item, session: AsyncSession):
    if not item.last_seen_provider_pricelist_id:
        return None
    normalized_oem = preprocess_oem_number(item.oem)
    stmt = (
        select(
            AutoPart.id.label("autopart_id"),
            AutoPart.oem_number,
            AutoPart.name.label("autopart_name"),
            Brand.name.label("brand_name"),
            Provider.id.label("supplier_id"),
            Provider.name.label("supplier_name"),
            PriceListAutoPartAssociation.price,
            PriceListAutoPartAssociation.quantity,
            PriceListAutoPartAssociation.multiplicity,
            ProviderPriceListConfig.min_delivery_day,
            ProviderPriceListConfig.max_delivery_day,
        )
        .select_from(PriceListAutoPartAssociation)
        .join(
            AutoPart,
            AutoPart.id == PriceListAutoPartAssociation.autopart_id,
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .join(
            PriceList,
            PriceList.id == PriceListAutoPartAssociation.pricelist_id,
        )
        .join(Provider, Provider.id == PriceList.provider_id)
        .outerjoin(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id == PriceList.provider_config_id,
        )
        .where(
            PriceList.id == item.last_seen_provider_pricelist_id,
            AutoPart.oem_number == normalized_oem,
            func.lower(Brand.name) == str(item.brand).strip().lower(),
            PriceListAutoPartAssociation.quantity > 0,
        )
        .limit(1)
    )
    row = (await session.execute(stmt)).mappings().first()
    if row is None:
        return None
    return {
        "source_type": "supplier",
        "key": f"provider-{item.id}-{row['supplier_id']}",
        "autopart_id": row["autopart_id"],
        "oem_number": row["oem_number"],
        "autopart_name": row["autopart_name"],
        "brand_name": row["brand_name"],
        "supplier_id": row["supplier_id"],
        "supplier_name": row["supplier_name"],
        "price": float(row["price"]),
        "quantity": int(row["quantity"]),
        "min_qnt": max(int(row["multiplicity"] or 1), 1),
        "min_delivery_day": row["min_delivery_day"],
        "max_delivery_day": row["max_delivery_day"],
        "snapshot_at": item.last_seen_provider_at,
    }


@router.get(
    "/watchlist",
    tags=["watchlist"],
    status_code=status.HTTP_200_OK,
    response_model=PriceWatchListPage,
)
async def list_watch_items(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    search: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    items, total = await crud_price_watch_item.list(
        session=session, page=page, page_size=page_size, search=search
    )
    output_items = []
    for item in items:
        payload = PriceWatchItemOut.model_validate(item)
        payload.last_seen_provider_offer = await _get_saved_provider_offer(
            item,
            session,
        )
        output_items.append(payload)
    return PriceWatchListPage(
        items=output_items,
        page=page,
        page_size=page_size,
        total=total,
    )


@router.post(
    "/watchlist",
    tags=["watchlist"],
    status_code=status.HTTP_201_CREATED,
    response_model=PriceWatchItemOut,
)
async def create_watch_item(
    payload: PriceWatchItemCreate,
    session: AsyncSession = Depends(get_session),
):
    item = await crud_price_watch_item.create(
        session=session,
        brand=payload.brand,
        oem=payload.oem,
        max_price=payload.max_price,
    )
    return PriceWatchItemOut.model_validate(item)


@router.patch(
    "/watchlist/{item_id}",
    tags=["watchlist"],
    status_code=status.HTTP_200_OK,
    response_model=PriceWatchItemOut,
)
async def update_watch_item(
    item_id: int,
    payload: PriceWatchItemUpdate,
    session: AsyncSession = Depends(get_session),
):
    values = payload.model_dump(exclude_unset=True)
    item = await crud_price_watch_item.update(
        session=session,
        item_id=item_id,
        values=values,
    )
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return PriceWatchItemOut.model_validate(item)


@router.delete(
    "/watchlist/{item_id}",
    tags=["watchlist"],
    status_code=status.HTTP_200_OK,
)
async def delete_watch_item(
    item_id: int,
    session: AsyncSession = Depends(get_session),
):
    ok = await crud_price_watch_item.delete(session=session, item_id=item_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"status": "ok"}
