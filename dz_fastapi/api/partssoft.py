import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_session, require_admin
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import ProviderExternalReference
from dz_fastapi.models.partssoft import PartsSoftDocumentSnapshot
from dz_fastapi.services.partssoft_exchange import (
    document_sync_status,
    enqueue_all_local_products,
    process_product_outbox,
    product_outbox_status,
    sync_partssoft_documents,
)
from dz_fastapi.services.partssoft_order_reconciliation import (
    MAX_RECONCILIATION_DAYS,
    link_partssoft_customer,
    reconcile_partssoft_orders,
    search_local_customer_candidates,
    sync_partssoft_products,
    sync_partssoft_suppliers,
)
from dz_fastapi.services.partssoft_reconciliation import PARTS_SOFT_SOURCE

router = APIRouter(prefix="/integrations/partssoft", tags=["partssoft"])


class PartsSoftCustomerLinkIn(BaseModel):
    local_customer_id: int = Field(gt=0)
    merge_existing_customer: bool = False


@router.get("/products/outbox/status")
async def outbound_product_status(session: AsyncSession = Depends(get_session)):
    return await product_outbox_status(session)


@router.post(
    "/products/outbox/process",
    dependencies=[Depends(require_admin)],
)
async def process_outbound_products(
    limit: int = Query(default=25, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
):
    return await process_product_outbox(session, limit=limit)


@router.post(
    "/products/outbox/enqueue-all",
    dependencies=[Depends(require_admin)],
)
async def enqueue_all_products(session: AsyncSession = Depends(get_session)):
    return {"queued": await enqueue_all_local_products(session)}


@router.get("/documents/status")
async def partssoft_document_status(session: AsyncSession = Depends(get_session)):
    return await document_sync_status(session)


@router.get("/documents")
async def partssoft_documents(
    document_type: str | None = Query(default=None),
    import_status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(PartsSoftDocumentSnapshot)
    if document_type:
        stmt = stmt.where(PartsSoftDocumentSnapshot.document_type == document_type)
    if import_status:
        stmt = stmt.where(PartsSoftDocumentSnapshot.import_status == import_status)
    rows = list(
        (
            await session.scalars(
                stmt.order_by(PartsSoftDocumentSnapshot.document_date.desc()).limit(limit)
            )
        ).all()
    )
    return [
        {
            "id": row.id,
            "document_type": row.document_type,
            "external_document_id": row.external_document_id,
            "external_counterparty_id": row.external_counterparty_id,
            "document_number": row.document_number,
            "document_date": row.document_date,
            "external_status": row.external_status,
            "total_amount": row.total_amount,
            "vat_rate": row.vat_rate,
            "import_status": row.import_status,
            "import_error": row.import_error,
            "local_payment_invoice_id": row.local_payment_invoice_id,
            "local_supplier_receipt_id": row.local_supplier_receipt_id,
            "last_synced_at": row.last_synced_at,
        }
        for row in rows
    ]


@router.post(
    "/documents/sync",
    dependencies=[Depends(require_admin)],
)
async def sync_documents(
    days: int = Query(default=30, ge=1, le=365),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await sync_partssoft_documents(session, days=days)
    except aiohttp.ClientResponseError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Parts-Soft API returned HTTP {exc.status}",
        ) from exc
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail="Parts-Soft API is unavailable") from exc


@router.get("/suppliers/status")
async def supplier_sync_status(session: AsyncSession = Depends(get_session)):
    total = await session.scalar(
        select(func.count(ProviderExternalReference.id)).where(
            ProviderExternalReference.source_system == PARTS_SOFT_SOURCE,
            ProviderExternalReference.is_active.is_(True),
        )
    )
    return {"suppliers": int(total or 0)}


@router.post("/suppliers/sync", dependencies=[Depends(require_admin)])
async def sync_suppliers(session: AsyncSession = Depends(get_session)):
    try:
        return await sync_partssoft_suppliers(session)
    except aiohttp.ClientResponseError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Parts-Soft API returned HTTP {exc.status}",
        ) from exc
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail="Parts-Soft API is unavailable") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


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
            merge_existing_customer=payload.merge_existing_customer,
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
        return await reconcile_partssoft_orders(session, days=days, refresh_remote=True)
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


@router.get("/orders/reconcile")
async def cached_reconcile_orders(
    days: int = Query(default=7, ge=1, le=MAX_RECONCILIATION_DAYS),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await reconcile_partssoft_orders(session, days=days)
    except aiohttp.ClientResponseError as exc:
        raise HTTPException(
            status_code=502, detail=f"Parts-Soft API returned HTTP {exc.status}"
        ) from exc
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail="Parts-Soft API is unavailable") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/products/status")
async def product_sync_status(
    session: AsyncSession = Depends(get_session),
):
    total = await session.scalar(
        select(func.count(AutoPart.id)).where(AutoPart.partssoft_product_id.is_not(None))
    )
    last_synced_at = await session.scalar(select(func.max(AutoPart.partssoft_synced_at)))
    photos = await session.scalar(
        select(func.count(func.distinct(AutoPart.id)))
        .join(AutoPart.photos)
        .where(AutoPart.partssoft_product_id.is_not(None))
    )
    return {
        "products": int(total or 0),
        "products_with_photos": int(photos or 0),
        "last_synced_at": last_synced_at,
    }


@router.post(
    "/products/sync",
    dependencies=[Depends(require_admin)],
)
async def sync_products(
    full: bool = Query(default=False),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await sync_partssoft_products(session, full=full)
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
