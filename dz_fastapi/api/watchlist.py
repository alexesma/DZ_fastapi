from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.inventory import StockLot
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
from dz_fastapi.services.autopurchase import _resolve_autopurchase_provider_config

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


async def _load_own_stock_info(items, session: AsyncSession) -> dict:
    """Закупка, наша текущая цена и остаток для позиций watchlist.

    Позиция сопоставляется с карточкой по нормализованному OEM и бренду.
    Цена последней закупки — из последней партии StockLot (fallback —
    закупочная цена карточки), текущая цена и остаток — из активного
    прайса нашего собственного поставщика.
    """
    if not items:
        return {}
    key_by_item_id = {}
    normalized_oems = set()
    for item in items:
        normalized = preprocess_oem_number(item.oem)
        key_by_item_id[item.id] = (
            normalized,
            str(item.brand or "").strip().lower(),
        )
        normalized_oems.add(normalized)

    autopart_stmt = (
        select(
            AutoPart.id,
            AutoPart.oem_number,
            func.lower(Brand.name).label("brand_lower"),
            AutoPart.purchase_price,
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(AutoPart.oem_number.in_(normalized_oems))
    )
    autopart_by_key = {}
    for ap_id, oem, brand_lower, purchase_price in (
        await session.execute(autopart_stmt)
    ).all():
        autopart_by_key[(oem, brand_lower)] = {
            "autopart_id": int(ap_id),
            "card_purchase_price": float(purchase_price or 0) or None,
        }
    autopart_ids = [
        row["autopart_id"] for row in autopart_by_key.values()
    ]
    if not autopart_ids:
        return {}

    last_lot_by_autopart = {}
    lot_stmt = (
        select(
            StockLot.autopart_id,
            StockLot.cost_price,
            StockLot.received_at,
        )
        .where(
            StockLot.autopart_id.in_(autopart_ids),
            StockLot.cost_price.is_not(None),
            StockLot.cost_price > 0,
        )
        .order_by(StockLot.autopart_id.asc(), StockLot.received_at.desc())
        .distinct(StockLot.autopart_id)
    )
    for ap_id, cost_price, received_at in (
        await session.execute(lot_stmt)
    ).all():
        last_lot_by_autopart[int(ap_id)] = {
            "price": float(cost_price),
            "received_at": received_at,
        }

    lot_qty_by_autopart = {}
    lot_qty_stmt = (
        select(
            StockLot.autopart_id,
            func.sum(StockLot.remaining_quantity),
        )
        .where(
            StockLot.autopart_id.in_(autopart_ids),
            StockLot.remaining_quantity > 0,
        )
        .group_by(StockLot.autopart_id)
    )
    for ap_id, qty in (await session.execute(lot_qty_stmt)).all():
        lot_qty_by_autopart[int(ap_id)] = int(qty or 0)

    own_offer_by_autopart = {}
    try:
        config_row = await _resolve_autopurchase_provider_config(session)
    except ValueError:
        config_row = None
    if config_row:
        own_pricelist_id = (
            await session.execute(
                select(PriceList.id)
                .where(
                    PriceList.provider_config_id
                    == int(config_row["provider_config_id"]),
                    PriceList.is_active.is_(True),
                )
                .order_by(PriceList.date.desc(), PriceList.id.desc())
                .limit(1)
            )
        ).scalar()
        if own_pricelist_id:
            own_stmt = select(
                PriceListAutoPartAssociation.autopart_id,
                PriceListAutoPartAssociation.price,
                PriceListAutoPartAssociation.quantity,
            ).where(
                PriceListAutoPartAssociation.pricelist_id == own_pricelist_id,
                PriceListAutoPartAssociation.autopart_id.in_(autopart_ids),
            )
            for ap_id, price, quantity in (
                await session.execute(own_stmt)
            ).all():
                own_offer_by_autopart[int(ap_id)] = {
                    "price": float(price or 0) or None,
                    "quantity": int(quantity or 0),
                }

    info_by_item_id = {}
    for item_id, key in key_by_item_id.items():
        autopart = autopart_by_key.get(key)
        if not autopart:
            continue
        ap_id = autopart["autopart_id"]
        last_lot = last_lot_by_autopart.get(ap_id)
        own_offer = own_offer_by_autopart.get(ap_id)
        stock_quantity = (
            own_offer["quantity"]
            if own_offer is not None
            else lot_qty_by_autopart.get(ap_id)
        )
        info_by_item_id[item_id] = {
            "last_purchase_price": (
                last_lot["price"]
                if last_lot
                else autopart["card_purchase_price"]
            ),
            "last_purchase_at": last_lot["received_at"] if last_lot else None,
            "current_price": own_offer["price"] if own_offer else None,
            "stock_quantity": stock_quantity,
        }
    return info_by_item_id


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
    stock_info = await _load_own_stock_info(items, session)
    output_items = []
    for item in items:
        payload = PriceWatchItemOut.model_validate(item)
        payload.last_seen_provider_offer = await _get_saved_provider_offer(
            item,
            session,
        )
        item_stock = stock_info.get(item.id)
        if item_stock:
            payload.last_purchase_price = item_stock["last_purchase_price"]
            payload.last_purchase_at = item_stock["last_purchase_at"]
            payload.current_price = item_stock["current_price"]
            payload.stock_quantity = item_stock["stock_quantity"]
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
