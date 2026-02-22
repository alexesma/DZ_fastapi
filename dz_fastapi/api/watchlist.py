from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_session
from dz_fastapi.crud.watchlist import crud_price_watch_item
from dz_fastapi.schemas.watchlist import (PriceWatchItemCreate,
                                          PriceWatchItemOut,
                                          PriceWatchListPage)

router = APIRouter()


@router.get(
    '/watchlist',
    tags=['watchlist'],
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
    return PriceWatchListPage(
        items=[PriceWatchItemOut.model_validate(i) for i in items],
        page=page,
        page_size=page_size,
        total=total,
    )


@router.post(
    '/watchlist',
    tags=['watchlist'],
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


@router.delete(
    '/watchlist/{item_id}',
    tags=['watchlist'],
    status_code=status.HTTP_200_OK,
)
async def delete_watch_item(
    item_id: int,
    session: AsyncSession = Depends(get_session),
):
    ok = await crud_price_watch_item.delete(session=session, item_id=item_id)
    if not ok:
        raise HTTPException(status_code=404, detail='Item not found')
    return {'status': 'ok'}
