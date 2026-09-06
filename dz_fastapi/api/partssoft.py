import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_session
from dz_fastapi.services.partssoft_order_reconciliation import (
    MAX_RECONCILIATION_DAYS,
    reconcile_partssoft_orders,
)

router = APIRouter(prefix="/integrations/partssoft", tags=["partssoft"])


@router.post("/orders/reconcile")
async def reconcile_orders(
    days: int = Query(default=7, ge=1, le=MAX_RECONCILIATION_DAYS),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await reconcile_partssoft_orders(session, days=days)
    except aiohttp.ClientResponseError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Parts-Soft API returned HTTP {exc.status}",
        ) from exc
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Parts-Soft API is unavailable",
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
