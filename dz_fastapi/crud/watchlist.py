from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.watchlist import PriceWatchItem


class CRUDPriceWatchItem:
    async def create(
        self, session: AsyncSession,
            brand: str,
            oem: str,
            max_price: float | None
    ) -> PriceWatchItem:
        item = PriceWatchItem(
            brand=brand.strip(),
            oem=oem.strip(),
            max_price=max_price,
        )
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return item

    async def list(
        self,
        session: AsyncSession,
        page: int = 1,
        page_size: int = 10,
        search: str | None = None,
    ):
        stmt = select(PriceWatchItem)
        count_stmt = select(func.count(PriceWatchItem.id))
        if search:
            like = f"%{search.lower()}%"
            stmt = stmt.where(
                func.lower(PriceWatchItem.oem).like(like)
                | func.lower(PriceWatchItem.brand).like(like)
            )
            count_stmt = count_stmt.where(
                func.lower(PriceWatchItem.oem).like(like)
                | func.lower(PriceWatchItem.brand).like(like)
            )

        total = (await session.execute(count_stmt)).scalar_one()
        stmt = stmt.order_by(PriceWatchItem.created_at.desc())
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        items = (await session.execute(stmt)).scalars().all()
        return items, total

    async def delete(self, session: AsyncSession, item_id: int) -> bool:
        item = await session.get(PriceWatchItem, item_id)
        if not item:
            return False
        await session.delete(item)
        await session.commit()
        return True

    async def get_all(self, session: AsyncSession):
        return (await session.execute(select(PriceWatchItem))).scalars().all()


crud_price_watch_item = CRUDPriceWatchItem()
