import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_session, require_admin
from dz_fastapi.services.partssoft_order_reconciliation import (
    MAX_RECONCILIATION_DAYS,
    link_partssoft_customer,
    reconcile_partssoft_orders,
    search_local_customer_candidates,
)

router = APIRouter(prefix="/integrations/partssoft", tags=["partssoft"])


class PartsSoftCustomerLinkIn(BaseModel):
    local_customer_id: int = Field(gt=0)


@router.get("/customers/candidates")
async def customer_candidates(
    name: str = Query(default="", max_length=255),
    inn: str = Query(default="", max_length=32),
    kpp: str = Query(default="", max_length=32),
    email: str = Query(default="", max_length=255),
    search: str = Query(default="", max_length=255),
    session: AsyncSession = Depends(get_session),
):
    return await search_local_customer_candidates(
        session,
        name=name,
        inn=inn,
        kpp=kpp,
        email=email,
        search=search,
    )


@router.post(
    "/customers/{external_customer_id}/link",
    dependencies=[Depends(require_admin)],
)
async def link_customer(
    external_customer_id: int,
    payload: PartsSoftCustomerLinkIn,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await link_partssoft_customer(
            session,
            external_customer_id=external_customer_id,
            local_customer_id=payload.local_customer_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
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
