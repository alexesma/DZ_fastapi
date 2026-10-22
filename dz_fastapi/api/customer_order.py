import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.customer_order import (
    crud_customer_order,
    crud_customer_order_config,
    crud_stock_order,
    crud_supplier_order,
)
from dz_fastapi.crud.partner import crud_customer_pricelist_config
from dz_fastapi.models.autopart import preprocess_oem_number
from dz_fastapi.models.notification import AppNotificationLevel
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    Client,
    CustomerPriceListConfig,
    Provider,
    SupplierOrderAttachment,
    SupplierReceipt,
    SupplierResponseConfig,
)
from dz_fastapi.models.user import User, UserRole
from dz_fastapi.schemas.customer_order import (
    CrossDockingDocumentStatusUpdate,
    CrossDockingLabelPrintRequest,
    CrossDockingLabelResponse,
    CustomerOrderConfigCreate,
    CustomerOrderConfigResponse,
    CustomerOrderConfigUpdate,
    CustomerOrderItemResponse,
    CustomerOrderItemStatsResponse,
    CustomerOrderItemUpdate,
    CustomerOrderManualCreate,
    CustomerOrderPriceOverridePreview,
    CustomerOrderPriceOverrideResult,
    CustomerOrderResponse,
    CustomerOrderRoutingChoice,
    CustomerOrderStatsMonthlyBucket,
    CustomerOrderStatsRecentRow,
    CustomerOrderStatsSummary,
    CustomerOrderSummaryPageResponse,
    CustomerOrderSummaryResponse,
    CustomerOrderUpdate,
    StockOrderItemPickResponse,
    StockOrderItemPickUpdate,
    StockOrderPackageContentsUpdate,
    StockOrderPackageCreate,
    StockOrderPackagePrintRequest,
    StockOrderPackageReasonRequest,
    StockOrderPackageScanRequest,
    StockOrderPackingResponse,
    StockOrderResponse,
    SupplierOrderDetailResponse,
    SupplierOrderManualCreate,
    SupplierOrderSummaryResponse,
    SupplierReceiptCandidateRow,
    SupplierReceiptCorrectionResponse,
    SupplierReceiptCorrectionSave,
    SupplierReceiptCreate,
    SupplierReceiptItemUpdate,
    SupplierReceiptManualCreate,
    SupplierReceiptManualItem,
    SupplierReceiptProviderOption,
    SupplierReceiptResponse,
    SupplierReceiptUpdate,
    SupplierResponseProcessResult,
)
from dz_fastapi.services.credit_control import CreditLimitExceeded, check_customer_credit_policy
from dz_fastapi.services.cross_docking import (
    ensure_cross_docking_labels,
    list_cross_docking_labels,
    mark_cross_docking_labels_printed,
    serialize_cross_docking_label,
    sync_posted_cross_docking_labels,
    update_cross_docking_document_status,
)
from dz_fastapi.services.customer_orders import (
    create_manual_customer_order,
    create_manual_supplier_order,
    force_accept_customer_order_price,
    force_process_partssoft_customer_order,
    forward_latest_customer_order_for_config,
    get_customer_order_price_override_preview,
    process_customer_orders,
    process_manual_customer_order,
    recheck_rejected_customer_order_item,
    retry_customer_order,
    retry_customer_order_errors_for_config,
    select_customer_order_pricelist,
    send_scheduled_supplier_orders,
    send_supplier_orders,
    soft_delete_customer_order,
    update_customer_order,
    update_customer_order_item_manual,
)
from dz_fastapi.services.holidays import get_effective_holiday_set
from dz_fastapi.services.inventory_stock import (
    dispatch_stock_order,
    sync_posted_cross_docking_assemblies,
)
from dz_fastapi.services.notifications import create_notification
from dz_fastapi.services.stock_order_packages import (
    create_stock_order_package,
    delete_stock_order_package,
    get_stock_order_packing,
    mark_stock_order_package_label_printed,
    reopen_stock_order_package,
    replace_stock_order_package_contents,
    scan_stock_order_package_item,
    seal_stock_order_package,
    serialize_stock_order_packing,
    verify_stock_order_package,
)
from dz_fastapi.services.supplier_order_responses import (
    process_supplier_response_messages,
    supplier_auto_confirm_timings,
)
from dz_fastapi.services.supplier_order_state import (
    supplier_item_refusal_sources,
    supplier_item_state,
)
from dz_fastapi.services.supplier_receipt_corrections import (
    get_correction,
    list_corrections,
    post_correction,
    save_correction_draft,
    serialize_correction,
)
from dz_fastapi.services.supplier_receipt_upd import send_supplier_receipt_upd_email
from dz_fastapi.services.supplier_workflow import (
    _auto_refuse_deadline,
    add_supplier_receipt_items,
    create_manual_supplier_receipt,
    create_supplier_receipt,
    delete_supplier_receipt,
    delete_supplier_receipt_item,
    get_supplier_receipt_detail,
    list_supplier_receipt_candidates,
    list_supplier_receipt_provider_options,
    list_supplier_receipts,
    post_supplier_receipt,
    serialize_stock_order,
    serialize_supplier_receipt,
    unpost_supplier_receipt,
    update_stock_order_item_pick,
    update_supplier_receipt,
    update_supplier_receipt_item,
)

logger = logging.getLogger("dz_fastapi")

router = APIRouter(prefix="/customer-orders", tags=["customer-orders"])


async def _validate_order_pricelist_configs(
    session: AsyncSession,
    *,
    customer_id: int,
    pricelist_config_id: int | None,
    reference_pricelist_config_id: int | None,
) -> None:
    if pricelist_config_id is not None:
        local_config = await crud_customer_pricelist_config.get_by_id(
            customer_id=customer_id,
            config_id=pricelist_config_id,
            session=session,
        )
        if local_config is None:
            raise HTTPException(
                status_code=400,
                detail="Основная конфигурация прайса не принадлежит этому клиенту",
            )
    if reference_pricelist_config_id is not None:
        reference = await crud_customer_pricelist_config.get_any_by_id(
            config_id=reference_pricelist_config_id,
            session=session,
        )
        if reference is None:
            raise HTTPException(
                status_code=400,
                detail="Выбранный прайс-эталон не найден",
            )


async def _serialize_order_config(
    session: AsyncSession,
    config,
) -> CustomerOrderConfigResponse:
    model = CustomerOrderConfigResponse.model_validate(config)
    config_ids = {
        value
        for value in (
            config.pricelist_config_id,
            config.reference_pricelist_config_id,
        )
        if value is not None
    }
    if not config_ids:
        return model
    rows = (
        await session.execute(
            select(
                CustomerPriceListConfig.id,
                CustomerPriceListConfig.name,
                CustomerPriceListConfig.customer_id,
                Client.name.label("customer_name"),
            )
            .join(Client, Client.id == CustomerPriceListConfig.customer_id)
            .where(CustomerPriceListConfig.id.in_(config_ids))
        )
    ).all()
    by_id = {row.id: row for row in rows}
    local = by_id.get(config.pricelist_config_id)
    reference = by_id.get(config.reference_pricelist_config_id)
    return model.model_copy(
        update={
            "pricelist_config_name": local.name if local else None,
            "reference_pricelist_config_name": (
                reference.name if reference else None
            ),
            "reference_pricelist_customer_id": (
                reference.customer_id if reference else None
            ),
            "reference_pricelist_customer_name": (
                reference.customer_name if reference else None
            ),
        }
    )


async def _notify_current_user(
    session: AsyncSession,
    current_user: User,
    *,
    title: str,
    message: str,
    level: str = AppNotificationLevel.INFO,
    link: str | None = None,
) -> None:
    try:
        await create_notification(
            session=session,
            user_id=current_user.id,
            title=title,
            message=message,
            level=level,
            link=link,
        )
    except Exception:
        await session.rollback()
        logger.exception(
            "Failed to create app notification for user %s",
            current_user.id,
        )


async def _send_supplier_receipt_upd_if_enabled(
    session: AsyncSession,
    current_user: User,
    receipt_id: int,
) -> None:
    try:
        result = await send_supplier_receipt_upd_email(
            session,
            receipt_id=receipt_id,
            force=False,
        )
    except Exception as exc:
        await _notify_current_user(
            session,
            current_user,
            title="УПД поставщика не отправлен",
            message=f"Документ #{receipt_id}: {exc}",
            level=AppNotificationLevel.WARNING,
            link="/documents/incoming",
        )
        return
    if result.get("sent"):
        await _notify_current_user(
            session,
            current_user,
            title="УПД поставщика отправлен",
            message=(f"Документ #{receipt_id} отправлен на " f"{result.get('to_email')}."),
            level=AppNotificationLevel.SUCCESS,
            link="/documents/incoming",
        )


def _serialize_customer_order_item_for_user(
    item,
    current_user: User,
) -> CustomerOrderItemResponse:
    model = CustomerOrderItemResponse.model_validate(item)
    if current_user.role != UserRole.ADMIN:
        return model.model_copy(
            update={
                "reject_reason_code": None,
                "reject_reason_text": None,
            }
        )
    return model


def _serialize_customer_order_for_user(
    order,
    current_user: User,
) -> CustomerOrderResponse:
    model = CustomerOrderResponse.model_validate(order)
    items = [
        _serialize_customer_order_item_for_user(item, current_user) for item in (order.items or [])
    ]
    return model.model_copy(
        update={
            "items": items,
            "pricelist_routing": (getattr(order, "external_payload", None) or {}).get(
                "pricelist_routing"
            ),
            "processing_owner": _customer_order_processing_owner(order),
        }
    )


def _customer_order_processing_owner(order) -> Optional[str]:
    persisted = str(getattr(order, "processing_owner", "") or "").strip()
    if persisted:
        return persisted
    if str(getattr(order, "external_source", "") or "").upper() != "PARTS_SOFT":
        return None
    if any(
        item.source_resolution_status == "local_workflow"
        for item in (getattr(order, "items", None) or [])
    ):
        return "LOCAL"
    return "PARTS_SOFT_SITE"


def _month_start_for_offset(months_ago: int) -> date:
    today = now_moscow().date()
    year = today.year
    month = today.month - months_ago
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def _build_month_buckets(months: int) -> list[date]:
    return [_month_start_for_offset(offset) for offset in reversed(range(months))]


def _decimal_average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    total = sum(values, Decimal("0"))
    return (total / Decimal(len(values))).quantize(Decimal("0.01"))


def _price_change_pct(
    last_price: Decimal | None,
    previous_price: Decimal | None,
) -> float | None:
    if last_price is None or previous_price is None or previous_price == 0:
        return None
    return float(((last_price - previous_price) / previous_price) * Decimal("100"))


def _build_stats_summary(rows) -> CustomerOrderStatsSummary:
    if not rows:
        return CustomerOrderStatsSummary()

    prices = [Decimal(str(row.requested_price)) for row in rows if row.requested_price is not None]
    order_ids = {row.order_id for row in rows}
    sorted_prices = [
        Decimal(str(row.requested_price)) for row in rows if row.requested_price is not None
    ]
    last_price = sorted_prices[0] if sorted_prices else None
    previous_price = sorted_prices[1] if len(sorted_prices) > 1 else None

    return CustomerOrderStatsSummary(
        orders_count=len(order_ids),
        rows_count=len(rows),
        total_requested_qty=sum(int(row.requested_qty or 0) for row in rows),
        total_ship_qty=sum(int(row.ship_qty or 0) for row in rows),
        avg_price=_decimal_average(prices),
        min_price=min(prices) if prices else None,
        max_price=max(prices) if prices else None,
        last_price=last_price,
        previous_price=previous_price,
        price_change_pct=_price_change_pct(last_price, previous_price),
        last_order_at=rows[0].received_at if rows else None,
    )


def _build_monthly_stats(
    rows,
    month_buckets: list[date],
) -> list[CustomerOrderStatsMonthlyBucket]:
    grouped: dict[date, list] = defaultdict(list)
    for row in rows:
        received_at = row.received_at
        if received_at is None:
            continue
        month_key = date(received_at.year, received_at.month, 1)
        grouped[month_key].append(row)

    result: list[CustomerOrderStatsMonthlyBucket] = []
    for month in month_buckets:
        month_rows = grouped.get(month, [])
        prices = [
            Decimal(str(row.requested_price))
            for row in month_rows
            if row.requested_price is not None
        ]
        result.append(
            CustomerOrderStatsMonthlyBucket(
                month=month,
                orders_count=len({row.order_id for row in month_rows}),
                rows_count=len(month_rows),
                total_requested_qty=sum(int(row.requested_qty or 0) for row in month_rows),
                total_ship_qty=sum(int(row.ship_qty or 0) for row in month_rows),
                avg_price=_decimal_average(prices),
                min_price=min(prices) if prices else None,
                max_price=max(prices) if prices else None,
            )
        )
    return result


def _build_recent_rows(
    rows,
    limit: int = 5,
) -> list[CustomerOrderStatsRecentRow]:
    return [
        CustomerOrderStatsRecentRow(
            order_id=row.order_id,
            customer_id=row.customer_id,
            customer_name=row.customer_name,
            order_number=row.order_number,
            received_at=row.received_at,
            requested_qty=row.requested_qty,
            requested_price=row.requested_price,
            ship_qty=row.ship_qty,
            reject_qty=row.reject_qty,
            status=row.status,
        )
        for row in rows[:limit]
    ]


@router.post(
    "/config",
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_order_config(
    payload: CustomerOrderConfigCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    await _validate_order_pricelist_configs(
        session,
        customer_id=payload.customer_id,
        pricelist_config_id=payload.pricelist_config_id,
        reference_pricelist_config_id=payload.reference_pricelist_config_id,
    )
    config = await crud_customer_order_config.upsert(
        session=session,
        customer_id=payload.customer_id,
        data=payload.model_dump(exclude={"customer_id"}),
    )
    return await _serialize_order_config(session, config)


@router.get(
    "/config/{customer_id}",
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def get_order_config(
    customer_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_customer_id(
        session=session, customer_id=customer_id
    )
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    return await _serialize_order_config(session, config)


@router.get(
    "/configs",
    response_model=List[CustomerOrderConfigResponse],
    status_code=status.HTTP_200_OK,
)
async def list_order_configs(
    customer_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    configs = await crud_customer_order_config.list_by_customer_id(
        session=session, customer_id=customer_id
    )
    return [await _serialize_order_config(session, config) for config in configs]


@router.get(
    "/configs/{config_id}",
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def get_order_config_by_id(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(session=session, config_id=config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    return await _serialize_order_config(session, config)


@router.post(
    "/configs",
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_order_config_v2(
    payload: CustomerOrderConfigCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if payload.pricelist_config_id is None:
        raise HTTPException(
            status_code=400,
            detail="pricelist_config_id is required",
        )
    await _validate_order_pricelist_configs(
        session,
        customer_id=payload.customer_id,
        pricelist_config_id=payload.pricelist_config_id,
        reference_pricelist_config_id=payload.reference_pricelist_config_id,
    )
    existing = await crud_customer_order_config.get_by_customer_and_pricelist(
        session=session,
        customer_id=payload.customer_id,
        pricelist_config_id=payload.pricelist_config_id,
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail="Order config already exists for this pricelist",
        )
    config = await crud_customer_order_config.create(
        session=session,
        customer_id=payload.customer_id,
        data=payload.model_dump(exclude={"customer_id"}),
    )
    return await _serialize_order_config(session, config)


@router.put(
    "/config/{customer_id}",
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def update_order_config(
    customer_id: int,
    payload: CustomerOrderConfigUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_customer_id(
        session=session, customer_id=customer_id
    )
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    update_data = payload.model_dump(exclude_unset=True)
    await _validate_order_pricelist_configs(
        session,
        customer_id=config.customer_id,
        pricelist_config_id=update_data.get(
            "pricelist_config_id", config.pricelist_config_id
        ),
        reference_pricelist_config_id=update_data.get(
            "reference_pricelist_config_id",
            config.reference_pricelist_config_id,
        ),
    )
    config = await crud_customer_order_config.update(
        session=session,
        config=config,
        data=update_data,
    )
    return await _serialize_order_config(session, config)


@router.patch(
    "/configs/{config_id}",
    response_model=CustomerOrderConfigResponse,
    status_code=status.HTTP_200_OK,
)
async def update_order_config_by_id(
    config_id: int,
    payload: CustomerOrderConfigUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(session=session, config_id=config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    update_data = payload.model_dump(exclude_unset=True)
    if payload.pricelist_config_id is not None:
        existing = await crud_customer_order_config.get_by_customer_and_pricelist(
            session=session,
            customer_id=config.customer_id,
            pricelist_config_id=payload.pricelist_config_id,
        )
        if existing and existing.id != config.id:
            raise HTTPException(
                status_code=409,
                detail="Order config already exists for this pricelist",
            )
    await _validate_order_pricelist_configs(
        session,
        customer_id=config.customer_id,
        pricelist_config_id=update_data.get(
            "pricelist_config_id", config.pricelist_config_id
        ),
        reference_pricelist_config_id=update_data.get(
            "reference_pricelist_config_id",
            config.reference_pricelist_config_id,
        ),
    )
    config = await crud_customer_order_config.update(
        session=session,
        config=config,
        data=update_data,
    )
    return await _serialize_order_config(session, config)


@router.delete(
    "/configs/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_order_config(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(session=session, config_id=config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    await crud_customer_order_config.delete(session=session, config=config)
    return None


def _parse_supplier_ids(raw_value: Optional[str]) -> List[int]:
    if not raw_value:
        return []
    try:
        values = [int(value.strip()) for value in raw_value.split(",")]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Некорректный поставщик") from exc
    if len(values) > 50 or any(value < 1 for value in values):
        raise HTTPException(status_code=422, detail="Некорректный поставщик")
    return list(dict.fromkeys(values))


@router.get(
    "/oem-suggestions",
    response_model=List[str],
    status_code=status.HTTP_200_OK,
)
async def suggest_customer_order_oems(
    q: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await crud_customer_order.suggest_item_oems(session, q.strip()[:100])


@router.get(
    "/",
    response_model=List[CustomerOrderResponse],
    status_code=status.HTTP_200_OK,
)
async def list_customer_orders(
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    supplier_ids: Optional[str] = None,
    item_oem: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_customer_order.list_orders(
        session=session,
        customer_id=customer_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        supplier_ids=_parse_supplier_ids(supplier_ids),
        item_oem=item_oem,
        skip=skip,
        limit=limit,
    )
    return [_serialize_customer_order_for_user(order, current_user) for order in orders]


@router.get(
    "/summary",
    response_model=List[CustomerOrderSummaryResponse],
    status_code=status.HTTP_200_OK,
)
async def list_customer_order_summary(
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    supplier_ids: Optional[str] = None,
    item_oem: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_customer_order.list_orders(
        session=session,
        customer_id=customer_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        supplier_ids=_parse_supplier_ids(supplier_ids),
        item_oem=item_oem,
        skip=skip,
        limit=limit,
    )
    return _summarize_customer_orders(
        orders, item_oem, _parse_supplier_ids(supplier_ids)
    )


def _summarize_customer_orders(
    orders, item_oem: Optional[str], supplier_ids: Optional[List[int]] = None
) -> List[CustomerOrderSummaryResponse]:
    results: List[CustomerOrderSummaryResponse] = []
    normalized_query = preprocess_oem_number(item_oem or "")

    def _money(value) -> Decimal:
        if value is None:
            return Decimal("0")
        return Decimal(str(value))

    for order in orders:
        total_sum = Decimal("0")
        stock_sum = Decimal("0")
        supplier_sum = Decimal("0")
        rejected_sum = Decimal("0")
        for item in order.items or []:
            price = item.requested_price if item.requested_price is not None else item.matched_price
            price_value = _money(price)
            requested_qty = int(item.requested_qty or 0)
            reject_qty = int(item.reject_qty or 0)

            # Для частичных отказов reject_qty может быть заполнен при
            # статусах OWN_STOCK/SUPPLIER, и его нужно включать в итог.
            if reject_qty == 0 and item.status == CUSTOMER_ORDER_ITEM_STATUS.REJECTED:
                reject_qty = requested_qty
            if reject_qty > 0:
                rejected_sum += Decimal(reject_qty) * price_value

            ship_qty = 0
            if item.status in (
                CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
                CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER,
            ):
                if item.ship_qty is not None:
                    ship_qty = int(item.ship_qty or 0)
                elif reject_qty > 0:
                    ship_qty = max(requested_qty - reject_qty, 0)
                else:
                    ship_qty = requested_qty

            if item.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK:
                stock_sum += Decimal(ship_qty) * price_value
            elif item.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER:
                supplier_sum += Decimal(ship_qty) * price_value
        total_sum = stock_sum + supplier_sum + rejected_sum
        rejected_pct = float((rejected_sum / total_sum) * 100) if total_sum > 0 else 0.0
        matching_items = [
            {
                "oem": item.oem,
                "brand": item.brand,
                "requested_qty": item.requested_qty,
            }
            for item in order.items or []
            if normalized_query
            and normalized_query in preprocess_oem_number(item.oem or "")
            and (not supplier_ids or item.supplier_id in supplier_ids)
        ]
        results.append(
            CustomerOrderSummaryResponse(
                id=order.id,
                customer_id=order.customer_id,
                customer_name=(order.customer.name if order.customer else None),
                order_number=order.order_number,
                received_at=order.received_at,
                status=order.status,
                external_source=order.external_source,
                external_order_id=order.external_order_id,
                import_origin=order.import_origin,
                processing_owner=_customer_order_processing_owner(order),
                processing_state=order.processing_state,
                identity_match_basis=order.identity_match_basis,
                recovered_at=order.recovered_at,
                total_sum=float(total_sum),
                stock_sum=float(stock_sum),
                supplier_sum=float(supplier_sum),
                rejected_sum=float(rejected_sum),
                rejected_pct=rejected_pct,
                matching_items=matching_items,
            )
        )
    return results


@router.get(
    "/summary-page",
    response_model=CustomerOrderSummaryPageResponse,
    status_code=status.HTTP_200_OK,
)
async def list_customer_order_summary_page(
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    supplier_ids: Optional[str] = None,
    item_oem: Optional[str] = None,
    skip: int = 0,
    limit: int = 20,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    selected_suppliers = _parse_supplier_ids(supplier_ids)
    filters = {
        "customer_id": customer_id,
        "status": status,
        "date_from": date_from,
        "date_to": date_to,
        "supplier_ids": selected_suppliers,
        "item_oem": item_oem,
        "exclude_errors": True,
    }
    page_size = max(1, min(limit, 100))
    orders = await crud_customer_order.list_orders(
        session=session, skip=max(0, skip), limit=page_size, **filters
    )
    total = await crud_customer_order.count_orders(session=session, **filters)
    return CustomerOrderSummaryPageResponse(
        items=_summarize_customer_orders(orders, item_oem, selected_suppliers),
        total=total,
    )


@router.get("/count", response_model=int)
async def count_customer_orders(
    customer_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    supplier_ids: Optional[str] = None,
    item_oem: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await crud_customer_order.count_orders(
        session=session,
        customer_id=customer_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        supplier_ids=_parse_supplier_ids(supplier_ids),
        item_oem=item_oem,
    )


@router.get(
    "/item-stats",
    response_model=CustomerOrderItemStatsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_customer_order_item_stats(
    kind: Literal["oem", "brand"],
    value: str,
    customer_id: int,
    months: int = 12,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    normalized_value = str(value or "").strip()
    if not normalized_value:
        raise HTTPException(status_code=400, detail="value is required")

    period_months = max(1, min(months, 24))
    month_buckets = _build_month_buckets(period_months)
    period_start = datetime.combine(
        month_buckets[0],
        datetime.min.time(),
        tzinfo=now_moscow().tzinfo,
    )

    rows = await crud_customer_order.get_stats_rows(
        session=session,
        kind=kind,
        value=normalized_value,
        date_from=period_start,
    )
    current_customer_rows = [row for row in rows if int(row.customer_id) == int(customer_id)]
    current_customer_name = next(
        (row.customer_name for row in current_customer_rows if row.customer_name),
        None,
    )

    return CustomerOrderItemStatsResponse(
        kind=kind,
        value=normalized_value,
        period_months=period_months,
        current_customer_id=customer_id,
        current_customer_name=current_customer_name,
        current_customer_summary=_build_stats_summary(current_customer_rows),
        all_customers_summary=_build_stats_summary(rows),
        current_customer_monthly=_build_monthly_stats(
            current_customer_rows,
            month_buckets,
        ),
        all_customers_monthly=_build_monthly_stats(rows, month_buckets),
        current_customer_recent=_build_recent_rows(current_customer_rows),
        all_customers_recent=_build_recent_rows(rows),
    )


@router.get(
    "/{order_id}",
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def get_customer_order(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    order = await crud_customer_order.get_by_id(session=session, order_id=order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return _serialize_customer_order_for_user(order, current_user)


@router.patch(
    "/{order_id}",
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def update_customer_order_endpoint(
    order_id: int,
    payload: CustomerOrderUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await update_customer_order(
            session,
            order_id,
            customer_id=payload.customer_id,
            order_number=payload.order_number,
            order_date=payload.order_date,
            fields_set=payload.model_fields_set,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _serialize_customer_order_for_user(order, current_user)


@router.delete(
    "/{order_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_customer_order_endpoint(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await soft_delete_customer_order(
            session,
            order_id,
            user_id=current_user.id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/manual",
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_manual_order(
    payload: CustomerOrderManualCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await create_manual_customer_order(
            session=session,
            customer_id=payload.customer_id,
            order_number=payload.order_number,
            order_date=payload.order_date,
            items=[item.model_dump() for item in payload.items],
            auto_process=payload.auto_process,
            order_config_id=payload.order_config_id,
        )
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(session=session, order_id=order.id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    response = _serialize_customer_order_for_user(order, current_user)
    credit_check = await check_customer_credit_policy(
        session,
        customer_id=order.customer_id,
    )
    if credit_check is not None and credit_check.should_warn:
        response = response.model_copy(update={"credit_warning": credit_check.to_detail()})
    return response


@router.post(
    "/{order_id}/process-manual",
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def process_manual_order_endpoint(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await process_manual_customer_order(session=session, order_id=order_id)
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(session=session, order_id=order.id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    response = _serialize_customer_order_for_user(order, current_user)
    credit_check = await check_customer_credit_policy(
        session,
        customer_id=order.customer_id,
    )
    if credit_check is not None and credit_check.should_warn:
        response = response.model_copy(update={"credit_warning": credit_check.to_detail()})
    return response


@router.post(
    "/{order_id}/process-partssoft-locally",
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def force_process_partssoft_order_endpoint(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await force_process_partssoft_customer_order(
            session=session,
            order_id=order_id,
        )
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(session=session, order_id=order.id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return _serialize_customer_order_for_user(order, current_user)


@router.patch(
    "/items/{item_id}",
    response_model=CustomerOrderItemResponse,
    status_code=status.HTTP_200_OK,
)
async def update_customer_order_item(
    item_id: int,
    payload: CustomerOrderItemUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        item = await update_customer_order_item_manual(
            session=session,
            item_id=item_id,
            status=payload.status,
            supplier_id=payload.supplier_id,
        )
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = _serialize_customer_order_item_for_user(item, current_user)
    credit_check = await check_customer_credit_policy(
        session,
        customer_id=item.order.customer_id if item.order else None,
    )
    if credit_check is not None and credit_check.should_warn:
        response = response.model_copy(update={"credit_warning": credit_check.to_detail()})
    return response


@router.post(
    "/items/{item_id}/recheck-offer",
    response_model=CustomerOrderItemResponse,
    status_code=status.HTTP_200_OK,
)
async def recheck_customer_order_item_offer(
    item_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        item = await recheck_rejected_customer_order_item(
            session, item_id, checked_by_user_id=int(current_user.id),
        )
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    response = _serialize_customer_order_item_for_user(item, current_user)
    credit_check = await check_customer_credit_policy(
        session,
        customer_id=item.order.customer_id if item.order else None,
    )
    if credit_check is not None and credit_check.should_warn:
        response = response.model_copy(update={"credit_warning": credit_check.to_detail()})
    return response


@router.get(
    "/items/{item_id}/price-override-preview",
    response_model=CustomerOrderPriceOverridePreview,
    status_code=status.HTTP_200_OK,
)
async def preview_customer_order_price_override(
    item_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_admin),
):
    try:
        return await get_customer_order_price_override_preview(session, item_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post(
    "/items/{item_id}/force-price-override",
    response_model=CustomerOrderPriceOverrideResult,
    status_code=status.HTTP_200_OK,
)
async def force_customer_order_price_override(
    item_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_admin),
):
    try:
        item, supplier_order_id, preview = await force_accept_customer_order_price(
            session,
            item_id,
            approved_by_user_id=int(current_user.id),
        )
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    credit_check = await check_customer_credit_policy(
        session,
        customer_id=item.order.customer_id if item.order else None,
    )
    return CustomerOrderPriceOverrideResult(
        item=_serialize_customer_order_item_for_user(item, current_user),
        supplier_order_id=supplier_order_id,
        preview=CustomerOrderPriceOverridePreview.model_validate(preview),
        credit_warning=(
            credit_check.to_detail()
            if credit_check is not None and credit_check.should_warn
            else None
        ),
    )


@router.post(
    "/process",
    status_code=status.HTTP_200_OK,
)
async def process_orders(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    await process_customer_orders(session)
    await _notify_current_user(
        session,
        current_user,
        title="Проверка почты завершена",
        message=("Импорт заказов клиентов завершен для всех активных" " конфигураций."),
        level=AppNotificationLevel.SUCCESS,
        link="/customer-orders",
    )
    return {"status": "ok"}


@router.post(
    "/configs/{config_id}/process",
    status_code=status.HTTP_200_OK,
)
async def process_orders_for_config(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    config = await crud_customer_order_config.get_by_id(session=session, config_id=config_id)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    await process_customer_orders(
        session,
        customer_id=config.customer_id,
        config_id=config_id,
    )
    await _notify_current_user(
        session,
        current_user,
        title="Проверка почты завершена",
        message=(
            f"Импорт заказов завершен для конфигурации #{config_id}"
            f" клиента #{config.customer_id}."
        ),
        level=AppNotificationLevel.SUCCESS,
        link="/customer-orders",
    )
    return {"status": "ok", "config_id": config_id}


@router.post(
    "/configs/{config_id}/retry-errors",
    status_code=status.HTTP_200_OK,
)
async def retry_config_errors(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await retry_customer_order_errors_for_config(
            session=session,
            config_id=config_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    retried_count = result.get("retried", 0)
    await _notify_current_user(
        session,
        current_user,
        title="Повторная обработка ошибок завершена",
        message=(
            f"Для конфигурации #{config_id} повторно обработано"
            f" {retried_count} заказов с ошибками."
        ),
        level=AppNotificationLevel.SUCCESS,
        link="/customer-orders",
    )
    return result


@router.post(
    "/configs/{config_id}/forward-latest",
    status_code=status.HTTP_200_OK,
)
async def forward_latest_order_for_config(
    config_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        forwarded_order = await forward_latest_customer_order_for_config(
            session=session,
            config_id=config_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    await _notify_current_user(
        session,
        current_user,
        title="Заказ клиента переотправлен",
        message=(
            f"Последний заказ #{forwarded_order['id']} "
            f"по конфигурации #{config_id} "
            "переотправлен на email."
        ),
        level=AppNotificationLevel.SUCCESS,
        link=f"/customer-orders/{forwarded_order['id']}",
    )
    return {
        "status": "ok",
        "config_id": config_id,
        "order_id": forwarded_order["id"],
        "order_number": forwarded_order["order_number"],
    }


@router.post(
    "/{order_id}/retry",
    response_model=CustomerOrderResponse,
    status_code=status.HTTP_200_OK,
)
async def retry_order(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await retry_customer_order(session=session, order_id=order_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(
        session=session,
        order_id=order.id,
    )
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    await _notify_current_user(
        session,
        current_user,
        title="Заказ перепроверен",
        message=f"Заказ клиента #{order.id} был перепроверен.",
        level=AppNotificationLevel.SUCCESS,
        link=f"/customer-orders/{order.id}",
    )
    return _serialize_customer_order_for_user(order, current_user)


@router.post("/{order_id}/pricelist-routing", response_model=CustomerOrderResponse)
async def choose_order_pricelist(
    order_id: int,
    payload: CustomerOrderRoutingChoice,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await select_customer_order_pricelist(
            session, order_id, payload.order_config_id, current_user.id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    order = await crud_customer_order.get_by_id(session=session, order_id=order_id)
    return _serialize_customer_order_for_user(order, current_user)


@router.get(
    "/stock/list",
    response_model=List[StockOrderResponse],
    status_code=status.HTTP_200_OK,
)
async def list_stock_orders(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    brand_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    customer_order_id: Optional[int] = None,
    storage_location_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_stock_order.list_stock_orders(
        session=session,
        date_from=date_from,
        date_to=date_to,
        brand_id=brand_id,
        customer_id=customer_id,
        customer_order_id=customer_order_id,
        storage_location_id=storage_location_id,
        skip=skip,
        limit=limit,
    )
    return [StockOrderResponse.model_validate(serialize_stock_order(order)) for order in orders]


@router.post(
    "/stock/sync-cross-docking",
    status_code=status.HTTP_200_OK,
    summary="Добавить проведённые cross-docking поступления в комплектацию",
)
async def sync_cross_docking_stock_orders(
    receipt_id: Optional[int] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Синхронизация доступна только администратору",
        )
    result = await sync_posted_cross_docking_assemblies(
        session, receipt_id=receipt_id
    )
    if receipt_id is None:
        label_result = await sync_posted_cross_docking_labels(
            session, user_id=current_user.id
        )
    else:
        label_result = await ensure_cross_docking_labels(
            session, receipt_id=receipt_id, user_id=current_user.id
        )
    result.update(label_result)
    await session.commit()
    return result


@router.patch(
    "/stock/items/{item_id}/pick",
    response_model=StockOrderItemPickResponse,
    status_code=status.HTTP_200_OK,
)
async def update_stock_order_item_pick_endpoint(
    item_id: int,
    payload: StockOrderItemPickUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await update_stock_order_item_pick(
            session=session,
            item_id=item_id,
            user=current_user,
            picked_quantity=payload.picked_quantity,
            increment=payload.increment,
            pick_comment=payload.pick_comment,
            scan_code=payload.scan_code,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StockOrderItemPickResponse(
        id=result.item.id,
        stock_order_id=result.item.stock_order_id,
        quantity=result.item.quantity,
        picked_quantity=int(result.item.picked_quantity or 0),
        picked_at=result.item.picked_at,
        picked_by_user_id=result.item.picked_by_user_id,
        picked_by_email=(result.item.picked_by_user.email if result.item.picked_by_user else None),
        pick_comment=result.item.pick_comment,
        pick_last_scan_code=result.item.pick_last_scan_code,
        stock_order_status=result.stock_order_status,
    )


@router.get(
    "/stock/orders/{order_id}/packing",
    response_model=StockOrderPackingResponse,
    status_code=status.HTTP_200_OK,
)
async def get_stock_order_packing_endpoint(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await get_stock_order_packing(
            session,
            stock_order_id=order_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StockOrderPackingResponse.model_validate(serialize_stock_order_packing(order))


@router.post(
    "/stock/orders/{order_id}/packages",
    response_model=StockOrderPackingResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_stock_order_package_endpoint(
    order_id: int,
    payload: StockOrderPackageCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await create_stock_order_package(
            session,
            stock_order_id=order_id,
            user_id=current_user.id,
            comment=payload.comment,
            pack_all_unallocated=payload.pack_all_unallocated,
        )
        await session.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StockOrderPackingResponse.model_validate(serialize_stock_order_packing(order))


@router.put(
    "/stock/packages/{package_id}/contents",
    response_model=StockOrderPackingResponse,
    status_code=status.HTTP_200_OK,
)
async def replace_stock_order_package_contents_endpoint(
    package_id: int,
    payload: StockOrderPackageContentsUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await replace_stock_order_package_contents(
            session,
            package_id=package_id,
            user_id=current_user.id,
            items=[item.model_dump() for item in payload.items],
        )
        await session.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StockOrderPackingResponse.model_validate(serialize_stock_order_packing(order))


async def _run_package_action(
    action,
    *,
    package_id: int,
    session: AsyncSession,
    current_user: User,
    **kwargs,
) -> StockOrderPackingResponse:
    try:
        order = await action(
            session,
            package_id=package_id,
            user_id=current_user.id,
            **kwargs,
        )
        await session.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StockOrderPackingResponse.model_validate(serialize_stock_order_packing(order))


@router.post(
    "/stock/packages/{package_id}/seal",
    response_model=StockOrderPackingResponse,
)
async def seal_stock_order_package_endpoint(
    package_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await _run_package_action(
        seal_stock_order_package,
        package_id=package_id,
        session=session,
        current_user=current_user,
    )


@router.post(
    "/stock/packages/{package_id}/scan",
    response_model=StockOrderPackingResponse,
)
async def scan_stock_order_package_endpoint(
    package_id: int,
    payload: StockOrderPackageScanRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await _run_package_action(
        scan_stock_order_package_item,
        package_id=package_id,
        session=session,
        current_user=current_user,
        scan_code=payload.scan_code,
    )


@router.post(
    "/stock/packages/{package_id}/verify",
    response_model=StockOrderPackingResponse,
)
async def verify_stock_order_package_endpoint(
    package_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await _run_package_action(
        verify_stock_order_package,
        package_id=package_id,
        session=session,
        current_user=current_user,
    )


@router.post(
    "/stock/packages/{package_id}/reopen",
    response_model=StockOrderPackingResponse,
)
async def reopen_stock_order_package_endpoint(
    package_id: int,
    payload: StockOrderPackageReasonRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await _run_package_action(
        reopen_stock_order_package,
        package_id=package_id,
        session=session,
        current_user=current_user,
        reason=payload.reason,
    )


@router.post(
    "/stock/packages/{package_id}/label-print",
    response_model=StockOrderPackingResponse,
)
async def print_stock_order_package_label_endpoint(
    package_id: int,
    payload: StockOrderPackagePrintRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await _run_package_action(
        mark_stock_order_package_label_printed,
        package_id=package_id,
        session=session,
        current_user=current_user,
        reason=payload.reason,
    )


@router.delete(
    "/stock/packages/{package_id}",
    response_model=StockOrderPackingResponse,
)
async def delete_stock_order_package_endpoint(
    package_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        order = await delete_stock_order_package(
            session,
            package_id=package_id,
        )
        await session.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StockOrderPackingResponse.model_validate(serialize_stock_order_packing(order))


@router.post(
    "/stock/orders/{order_id}/dispatch",
    status_code=status.HTTP_200_OK,
    summary="Создать и провести накладную из собранного складского заказа",
)
async def dispatch_stock_order_endpoint(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await dispatch_stock_order(session, stock_order_id=order_id)
        await session.commit()
        return result
    except CreditLimitExceeded as exc:
        raise HTTPException(status_code=409, detail=exc.to_detail()) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


async def _supplier_order_holidays(session: AsyncSession, orders) -> set[date]:
    years = {order.sent_at.year for order in orders if order.sent_at is not None}
    if not years:
        return set()
    years.update(year + 1 for year in list(years))
    return await get_effective_holiday_set(session, sorted(years))


async def _supplier_response_timings_for_orders(
    session: AsyncSession, orders
) -> dict[int, tuple[int, int]]:
    provider_ids = {int(order.provider_id) for order in orders}
    if not provider_ids:
        return {}
    configs = (
        await session.execute(
            select(SupplierResponseConfig).where(
                SupplierResponseConfig.provider_id.in_(provider_ids),
                SupplierResponseConfig.is_active.is_(True),
            )
        )
    ).scalars().all()
    return supplier_auto_confirm_timings(configs)


@router.get(
    "/supplier/list",
    response_model=List[SupplierOrderSummaryResponse],
    status_code=status.HTTP_200_OK,
)
async def list_supplier_orders(
    provider_id: Optional[int] = None,
    status: Optional[str] = None,
    customer_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    rejected_pct_min: Optional[float] = None,
    rejected_pct_max: Optional[float] = None,
    total_sum_min: Optional[float] = None,
    total_sum_max: Optional[float] = None,
    stock_sum_min: Optional[float] = None,
    stock_sum_max: Optional[float] = None,
    supplier_sum_min: Optional[float] = None,
    supplier_sum_max: Optional[float] = None,
    rejected_sum_min: Optional[float] = None,
    rejected_sum_max: Optional[float] = None,
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    orders = await crud_supplier_order.list_supplier_orders(
        session=session,
        provider_id=provider_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        skip=skip,
        limit=limit,
    )
    results: List[SupplierOrderSummaryResponse] = []
    holidays = await _supplier_order_holidays(session, orders)
    response_timings = await _supplier_response_timings_for_orders(session, orders)
    current_time = now_moscow()

    def _money(value) -> Decimal:
        if value is None:
            return Decimal("0")
        return Decimal(str(value))

    def _in_range(value: float, min_value, max_value) -> bool:
        if min_value is not None and value < min_value:
            return False
        if max_value is not None and value > max_value:
            return False
        return True

    for order in orders:
        customer_orders = {}
        for item in order.items or []:
            order_item = item.customer_order_item
            if order_item and order_item.order:
                customer_orders[order_item.order.id] = order_item.order

        if customer_id is not None:
            if not any(o.customer_id == customer_id for o in customer_orders.values()):
                continue

        if date_from or date_to:
            matches_date = False
            for o in customer_orders.values():
                if not o.received_at:
                    continue
                received_date = o.received_at.date()
                if date_from and received_date < date_from:
                    continue
                if date_to and received_date > date_to:
                    continue
                matches_date = True
                break
            if not matches_date:
                continue

        supplier_sum = Decimal("0")
        for item in order.items or []:
            order_item = item.customer_order_item
            if item.price is not None:
                price_value = _money(item.price)
            else:
                # Fallback to matched_price (supplier price list price),
                # never to requested_price which is the customer's price
                price_value = _money(order_item.matched_price if order_item else None)
            supplier_sum += Decimal(item.quantity) * price_value

        # Historical auto_refused_at marks a missed response deadline, not a
        # supplier refusal. Only an explicit confirmation below ordered qty
        # contributes to the refusal amount.
        rejected_sum = Decimal("0")
        rejected_by_source = {
            "supplier": Decimal("0"),
            "manager": Decimal("0"),
            "document_timeout": Decimal("0"),
            "other": Decimal("0"),
        }
        awaiting_count = 0
        overdue_count = 0
        confirmed_rejected_count = 0
        timing = response_timings.get(int(order.provider_id))
        if order.sent_at is None:
            deadline = None
        elif timing is not None:
            deadline = order.sent_at + timedelta(minutes=timing[0])
        else:
            deadline = _auto_refuse_deadline(order.sent_at, holidays)
        for item in order.items or []:
            state = supplier_item_state(
                item, sent_at=order.sent_at, deadline_at=deadline, now=current_time
            )
            rejected_qty = state.rejected_quantity
            awaiting_count += state.status in ("awaiting_response", "partially_received")
            overdue_count += state.status in ("overdue", "partially_received_overdue")
            confirmed_rejected_count += rejected_qty > 0
            if rejected_qty > 0:
                # Refusal percent/sum should be measured against the supplier
                # order value (ordered price), not response override price.
                order_item = item.customer_order_item
                if item.price is not None:
                    price_value = _money(item.price)
                else:
                    price_value = _money(order_item.matched_price if order_item else None)
                rejected_value = Decimal(rejected_qty) * price_value
                rejected_sum += rejected_value
                for source_key, source_qty in supplier_item_refusal_sources(
                    item, state
                ).items():
                    if source_key not in rejected_by_source:
                        source_key = "other"
                    rejected_by_source[source_key] += Decimal(source_qty) * price_value

        total_sum = supplier_sum
        rejected_pct = float((rejected_sum / total_sum) * 100) if total_sum > 0 else 0.0

        customer_order = None
        customer_name = None
        customer_order_number = None
        customer_received_at = None
        customer_status = None
        customer_orders_count = len(customer_orders)
        if len(customer_orders) == 1:
            customer_order = next(iter(customer_orders.values()))
            customer_name = customer_order.customer.name if customer_order.customer else None
            customer_order_number = customer_order.order_number
            customer_received_at = customer_order.received_at
            customer_status = customer_order.status
        elif len(customer_orders) > 1:
            customer_name = "Несколько"
            customer_order_number = "Несколько"
            received_list = [o.received_at for o in customer_orders.values() if o.received_at]
            if received_list:
                customer_received_at = min(received_list)

        if not _in_range(rejected_pct, rejected_pct_min, rejected_pct_max):
            continue
        if not _in_range(float(total_sum), total_sum_min, total_sum_max):
            continue
        if not _in_range(0.0, stock_sum_min, stock_sum_max):
            continue
        if not _in_range(float(supplier_sum), supplier_sum_min, supplier_sum_max):
            continue
        if not _in_range(float(rejected_sum), rejected_sum_min, rejected_sum_max):
            continue

        results.append(
            SupplierOrderSummaryResponse(
                id=order.id,
                provider_id=order.provider_id,
                provider_config_id=order.provider_config_id,
                provider_config_name=(
                    order.provider_config.name_price if order.provider_config else None
                ),
                status=order.status,
                created_at=order.created_at,
                customer_order_id=(customer_order.id if customer_order else None),
                customer_name=customer_name,
                customer_order_number=customer_order_number,
                customer_received_at=customer_received_at,
                customer_status=customer_status,
                customer_orders_count=customer_orders_count,
                total_sum=float(total_sum),
                stock_sum=0.0,
                supplier_sum=float(supplier_sum),
                rejected_sum=float(rejected_sum),
                rejected_pct=rejected_pct,
                awaiting_count=awaiting_count,
                overdue_count=overdue_count,
                confirmed_rejected_count=confirmed_rejected_count,
                rejected_by_supplier_sum=float(rejected_by_source["supplier"]),
                rejected_by_manager_sum=float(rejected_by_source["manager"]),
                rejected_by_document_timeout_sum=float(
                    rejected_by_source["document_timeout"]
                ),
                rejected_by_other_sum=float(rejected_by_source["other"]),
                last_send_attempt_at=order.last_send_attempt_at,
                last_send_error=order.last_send_error,
            )
        )

    results.sort(
        key=lambda item: item.customer_received_at or item.created_at,
        reverse=True,
    )
    return results


@router.get(
    "/supplier-receipts/providers",
    response_model=List[SupplierReceiptProviderOption],
    status_code=status.HTTP_200_OK,
)
async def list_supplier_receipt_providers_endpoint(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    customer_only: bool = True,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    rows = await list_supplier_receipt_provider_options(
        session=session,
        date_from=date_from,
        date_to=date_to,
        customer_only=customer_only,
    )
    return [SupplierReceiptProviderOption.model_validate(row) for row in rows]


@router.get(
    "/supplier-receipts/candidates",
    response_model=List[SupplierReceiptCandidateRow],
    status_code=status.HTTP_200_OK,
)
async def list_supplier_receipt_candidates_endpoint(
    provider_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    customer_only: bool = True,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    rows = await list_supplier_receipt_candidates(
        session=session,
        provider_id=provider_id,
        date_from=date_from,
        date_to=date_to,
        customer_only=customer_only,
    )
    return [SupplierReceiptCandidateRow.model_validate(row) for row in rows]


@router.post(
    "/supplier/process-responses",
    response_model=SupplierResponseProcessResult,
    status_code=status.HTTP_200_OK,
)
async def process_supplier_responses_endpoint(
    provider_id: Optional[int] = None,
    supplier_response_config_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    result = await process_supplier_response_messages(
        session=session,
        provider_id=provider_id,
        supplier_response_config_id=supplier_response_config_id,
        date_from=date_from,
        date_to=date_to,
    )
    await _notify_current_user(
        session,
        current_user,
        title="Почта поставщиков проверена",
        message=(
            "Обработано писем: "
            f'{result["processed_messages"]} '
            f'из {result["fetched_messages"]}.'
        ),
        level=AppNotificationLevel.SUCCESS,
        link="/customer-orders/receipts",
    )
    return SupplierResponseProcessResult(**result)


@router.get(
    "/supplier-receipts/corrections/receipt-options",
)
async def supplier_receipt_correction_options_endpoint(
    provider_id: Optional[int] = None,
    query: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    stmt = (
        select(SupplierReceipt.id, SupplierReceipt.provider_id, Provider.name,
               SupplierReceipt.document_number, SupplierReceipt.document_date)
        .join(Provider, Provider.id == SupplierReceipt.provider_id)
        .where(SupplierReceipt.posted_at.is_not(None))
        .order_by(SupplierReceipt.id.desc())
        .limit(50)
    )
    if provider_id is not None:
        stmt = stmt.where(SupplierReceipt.provider_id == provider_id)
    search = str(query or "").strip()
    if search:
        terms = [SupplierReceipt.document_number.ilike(f"%{search}%")]
        if search.isdigit():
            terms.append(SupplierReceipt.id == int(search))
        stmt = stmt.where(or_(*terms))
    rows = (await session.execute(stmt)).all()
    return [{
        "id": receipt_id, "provider_id": supplier_id,
        "provider_name": provider_name, "document_number": document_number,
        "document_date": document_date,
    } for receipt_id, supplier_id, provider_name, document_number, document_date in rows]


@router.get(
    "/supplier-receipts/corrections/list",
    response_model=List[SupplierReceiptCorrectionResponse],
)
async def list_supplier_receipt_corrections_endpoint(
    provider_id: Optional[int] = None,
    status: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    rows = await list_corrections(session, provider_id=provider_id, status=status)
    return [
        SupplierReceiptCorrectionResponse.model_validate(serialize_correction(row))
        for row in rows
    ]


@router.get(
    "/supplier-receipts/corrections/{correction_id}",
    response_model=SupplierReceiptCorrectionResponse,
)
async def get_supplier_receipt_correction_endpoint(
    correction_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        row = await get_correction(session, correction_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return SupplierReceiptCorrectionResponse.model_validate(serialize_correction(row))


@router.get("/supplier-receipts/corrections/{correction_id}/attachments/{attachment_id}")
async def download_supplier_receipt_correction_attachment_endpoint(
    correction_id: int,
    attachment_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        row = await get_correction(session, correction_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    attachment = (await session.execute(select(SupplierOrderAttachment).where(
        SupplierOrderAttachment.id == attachment_id,
        SupplierOrderAttachment.message_id == row.source_message_id,
    ))).scalar_one_or_none()
    if attachment is None or not os.path.isfile(attachment.file_path):
        raise HTTPException(status_code=404, detail="Файл КСФ не найден")
    return FileResponse(attachment.file_path, filename=attachment.filename)


@router.post(
    "/supplier-receipts/corrections",
    response_model=SupplierReceiptCorrectionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_supplier_receipt_correction_endpoint(
    payload: SupplierReceiptCorrectionSave,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        row = await save_correction_draft(
            session, correction_id=None, user_id=current_user.id,
            **payload.model_dump(),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptCorrectionResponse.model_validate(serialize_correction(row))


@router.put(
    "/supplier-receipts/corrections/{correction_id}",
    response_model=SupplierReceiptCorrectionResponse,
)
async def update_supplier_receipt_correction_endpoint(
    correction_id: int,
    payload: SupplierReceiptCorrectionSave,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        row = await save_correction_draft(
            session, correction_id=correction_id, user_id=current_user.id,
            **payload.model_dump(),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptCorrectionResponse.model_validate(serialize_correction(row))


@router.post(
    "/supplier-receipts/corrections/{correction_id}/post",
    response_model=SupplierReceiptCorrectionResponse,
)
async def post_supplier_receipt_correction_endpoint(
    correction_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        row = await post_correction(session, correction_id=correction_id)
    except LookupError as exc:
        await session.rollback()
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptCorrectionResponse.model_validate(serialize_correction(row))


@router.get(
    "/supplier-receipts/list",
    response_model=List[SupplierReceiptResponse],
    status_code=status.HTTP_200_OK,
)
async def list_supplier_receipts_endpoint(
    provider_id: Optional[int] = None,
    posted: Optional[bool] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    receipts = await list_supplier_receipts(
        session=session,
        provider_id=provider_id,
        posted=posted,
        date_from=date_from,
        date_to=date_to,
    )
    return [
        SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))
        for receipt in receipts
    ]


@router.get(
    "/supplier-receipts/{receipt_id}",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def get_supplier_receipt_endpoint(
    receipt_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await get_supplier_receipt_detail(session=session, receipt_id=receipt_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.get(
    "/supplier-receipts/{receipt_id}/cross-docking-labels",
    response_model=List[CrossDockingLabelResponse],
    status_code=status.HTTP_200_OK,
)
async def get_cross_docking_labels_endpoint(
    receipt_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await ensure_cross_docking_labels(
            session,
            receipt_id=receipt_id,
            user_id=current_user.id,
        )
        labels = await list_cross_docking_labels(
            session,
            receipt_id=receipt_id,
        )
        await session.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [
        CrossDockingLabelResponse.model_validate(serialize_cross_docking_label(label))
        for label in labels
    ]


@router.post(
    "/supplier-receipts/{receipt_id}/cross-docking-labels/print",
    response_model=List[CrossDockingLabelResponse],
    status_code=status.HTTP_200_OK,
)
async def print_cross_docking_labels_endpoint(
    receipt_id: int,
    payload: CrossDockingLabelPrintRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        labels = await mark_cross_docking_labels_printed(
            session,
            receipt_id=receipt_id,
            user_id=current_user.id,
            label_ids=payload.label_ids,
            reason=payload.reason,
        )
        await session.commit()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [
        CrossDockingLabelResponse.model_validate(serialize_cross_docking_label(label))
        for label in labels
    ]


@router.patch(
    "/supplier-receipts/{receipt_id}/cross-docking-document",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def update_cross_docking_document_endpoint(
    receipt_id: int,
    payload: CrossDockingDocumentStatusUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await update_cross_docking_document_status(
            session,
            receipt_id=receipt_id,
            document_pending=payload.document_pending,
            document_number=payload.document_number,
            document_date=payload.document_date,
        )
        await session.commit()
        receipt = await get_supplier_receipt_detail(
            session=session,
            receipt_id=receipt_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.post(
    "/supplier-receipts",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_supplier_receipt_endpoint(
    payload: SupplierReceiptCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await create_supplier_receipt(
            session=session,
            user=current_user,
            provider_id=payload.provider_id,
            items_payload=[item.model_dump() for item in payload.items],
            warehouse_id=payload.warehouse_id,
            post_now=payload.post_now,
            document_number=payload.document_number,
            document_date=payload.document_date,
            comment=payload.comment,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _notify_current_user(
        session,
        current_user,
        title=("Поступление проведено" if payload.post_now else "Черновик поступления создан"),
        message=(
            f"Поступление по поставщику #{payload.provider_id} "
            f"сформировано по {len(payload.items)} строкам. "
            f'Статус: {"проведено" if payload.post_now else "черновик"}.'
        ),
        level=AppNotificationLevel.SUCCESS,
        link="/customer-orders/receipts",
    )
    await _send_supplier_receipt_upd_if_enabled(
        session,
        current_user,
        receipt.id,
    )
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.post(
    "/supplier-receipts/{receipt_id}/post",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def post_supplier_receipt_endpoint(
    receipt_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await post_supplier_receipt(
            session=session,
            receipt_id=receipt_id,
            user=current_user,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _notify_current_user(
        session,
        current_user,
        title="Поступление проведено",
        message=(f"Документ поступления #{receipt_id} проведен."),
        level=AppNotificationLevel.SUCCESS,
        link="/customer-orders/receipts",
    )
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.post(
    "/supplier-receipts/{receipt_id}/send-upd-email",
    status_code=status.HTTP_200_OK,
)
async def send_supplier_receipt_upd_email_endpoint(
    receipt_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await send_supplier_receipt_upd_email(
            session,
            receipt_id=receipt_id,
            force=True,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    await _notify_current_user(
        session,
        current_user,
        title="УПД поставщика отправлен",
        message=(f"Документ #{receipt_id} отправлен на " f"{result.get('to_email')}."),
        level=AppNotificationLevel.SUCCESS,
        link="/documents/incoming",
    )
    return result


@router.post(
    "/supplier-receipts/{receipt_id}/unpost",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def unpost_supplier_receipt_endpoint(
    receipt_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await unpost_supplier_receipt(
            session=session,
            receipt_id=receipt_id,
            user=current_user,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _notify_current_user(
        session,
        current_user,
        title="Документ поступления распроведен",
        message=f"Документ поступления #{receipt_id} распроведен.",
        level=AppNotificationLevel.WARNING,
        link="/documents/incoming",
    )
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.delete(
    "/supplier-receipts/{receipt_id}",
    status_code=status.HTTP_200_OK,
)
async def delete_supplier_receipt_endpoint(
    receipt_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        await delete_supplier_receipt(
            session=session,
            receipt_id=receipt_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _notify_current_user(
        session,
        current_user,
        title="Документ поступления удален",
        message=f"Документ поступления #{receipt_id} удален.",
        level=AppNotificationLevel.WARNING,
        link="/documents/incoming",
    )
    return {"id": receipt_id, "deleted": True}


@router.post(
    "/supplier-receipts/manual",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_manual_supplier_receipt_endpoint(
    payload: SupplierReceiptManualCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await create_manual_supplier_receipt(
            session=session,
            user=current_user,
            provider_id=payload.provider_id,
            items_payload=[item.model_dump() for item in payload.items],
            warehouse_id=payload.warehouse_id,
            post_now=payload.post_now,
            document_number=payload.document_number,
            document_date=payload.document_date,
            comment=payload.comment,
        )
    except (LookupError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _send_supplier_receipt_upd_if_enabled(
        session,
        current_user,
        receipt.id,
    )
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.patch(
    "/supplier-receipts/{receipt_id}",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def update_supplier_receipt_endpoint(
    receipt_id: int,
    payload: SupplierReceiptUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await update_supplier_receipt(
            session=session,
            receipt_id=receipt_id,
            **payload.model_dump(exclude_none=True),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.post(
    "/supplier-receipts/{receipt_id}/items",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def add_supplier_receipt_items_endpoint(
    receipt_id: int,
    payload: List[SupplierReceiptManualItem],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await add_supplier_receipt_items(
            session=session,
            receipt_id=receipt_id,
            items_payload=[item.model_dump() for item in payload],
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.patch(
    "/supplier-receipt-items/{item_id}",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def update_supplier_receipt_item_endpoint(
    item_id: int,
    payload: SupplierReceiptItemUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await update_supplier_receipt_item(
            session=session,
            item_id=item_id,
            **payload.model_dump(exclude_unset=True),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.delete(
    "/supplier-receipt-items/{item_id}",
    response_model=SupplierReceiptResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_supplier_receipt_item_endpoint(
    item_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        receipt = await delete_supplier_receipt_item(session=session, item_id=item_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SupplierReceiptResponse.model_validate(serialize_supplier_receipt(receipt))


@router.get(
    "/supplier/{order_id}",
    response_model=SupplierOrderDetailResponse,
    status_code=status.HTTP_200_OK,
)
async def get_supplier_order_detail(
    order_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    order = await crud_supplier_order.get_by_id(session=session, order_id=order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    holidays = await _supplier_order_holidays(session, [order])
    response_timings = await _supplier_response_timings_for_orders(session, [order])
    timing = response_timings.get(int(order.provider_id))
    if order.sent_at is None:
        deadline = None
    elif timing is not None:
        deadline = order.sent_at + timedelta(minutes=timing[0])
    else:
        deadline = _auto_refuse_deadline(order.sent_at, holidays)
    auto_confirm_at = (
        order.sent_at + timedelta(minutes=sum(timing))
        if order.sent_at is not None and timing is not None
        else None
    )
    current_time = now_moscow()
    items = []
    for item in order.items or []:
        state = supplier_item_state(
            item, sent_at=order.sent_at, deadline_at=deadline, now=current_time
        )
        order_item = item.customer_order_item
        autopart = item.autopart
        brand_name = autopart.brand.name if autopart and autopart.brand else None
        items.append(
            {
                "id": item.id,
                "customer_order_item_id": item.customer_order_item_id,
                "quantity": item.quantity,
                "price": item.price,
                "confirmed_quantity": item.confirmed_quantity,
                "response_price": item.response_price,
                "response_comment": item.response_comment,
                "response_status_raw": item.response_status_raw,
                "response_status_normalized": item.response_status_normalized,
                "response_status_synced_at": item.response_status_synced_at,
                "oem": (
                    order_item.oem
                    if order_item
                    else (item.oem_number or (autopart.oem_number if autopart else None))
                ),
                "brand": (order_item.brand if order_item else (item.brand_name or brand_name)),
                "name": (
                    order_item.name
                    if order_item
                    else (item.autopart_name or (autopart.name if autopart else None))
                ),
                "min_delivery_day": item.min_delivery_day,
                "max_delivery_day": item.max_delivery_day,
                "received_quantity": item.received_quantity,
                "received_at": item.received_at,
                "auto_refused_at": item.auto_refused_at,
                "response_due_at": deadline,
                "auto_confirm_at": auto_confirm_at,
                "response_state": state.status,
                "response_source": state.response_source,
                "refusal_sources": supplier_item_refusal_sources(item, state),
                "response_at": state.response_at,
                "document_missing_since": item.document_missing_since,
                "document_refusal_due_at": (
                    item.document_missing_since + timedelta(hours=24)
                    if item.document_missing_since is not None else None
                ),
                "explicit_rejected_quantity": state.rejected_quantity,
                "pending_quantity": state.pending_quantity,
                "requested_qty": (order_item.requested_qty if order_item else None),
                "ship_qty": order_item.ship_qty if order_item else None,
                "reject_qty": order_item.reject_qty if order_item else None,
            }
        )
    return SupplierOrderDetailResponse(
        id=order.id,
        provider_id=order.provider_id,
        provider_name=order.provider.name if order.provider else None,
        provider_config_id=order.provider_config_id,
        provider_config_name=(order.provider_config.name_price if order.provider_config else None),
        status=order.status,
        created_at=order.created_at,
        scheduled_at=order.scheduled_at,
        sent_at=order.sent_at,
        last_send_attempt_at=order.last_send_attempt_at,
        last_send_error=order.last_send_error,
        response_status_raw=order.response_status_raw,
        response_status_normalized=order.response_status_normalized,
        response_status_synced_at=order.response_status_synced_at,
        items=items,
    )


@router.post(
    "/supplier/manual",
    response_model=SupplierOrderDetailResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_manual_supplier_order_endpoint(
    payload: SupplierOrderManualCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    try:
        created = await create_manual_supplier_order(
            session=session,
            provider_id=payload.provider_id,
            items=[item.model_dump() for item in payload.items],
            created_by_user_id=current_user.id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    order = await crud_supplier_order.get_by_id(session=session, order_id=created.id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    await _notify_current_user(
        session,
        current_user,
        title="Создан заказ поставщику",
        message=(f"Создан заказ поставщику #{order.id}" f" на {len(order.items or [])} поз."),
        level=AppNotificationLevel.SUCCESS,
        link="/orders/tracking",
    )

    items = []
    for item in order.items or []:
        autopart = item.autopart
        brand_name = autopart.brand.name if autopart and autopart.brand else None
        items.append(
            {
                "id": item.id,
                "customer_order_item_id": item.customer_order_item_id,
                "quantity": item.quantity,
                "price": item.price,
                "oem": (item.oem_number or (autopart.oem_number if autopart else None)),
                "brand": item.brand_name or brand_name,
                "name": (item.autopart_name or (autopart.name if autopart else None)),
                "min_delivery_day": item.min_delivery_day,
                "max_delivery_day": item.max_delivery_day,
                "received_quantity": item.received_quantity,
                "received_at": item.received_at,
                "requested_qty": None,
                "ship_qty": None,
                "reject_qty": None,
            }
        )
    return SupplierOrderDetailResponse(
        id=order.id,
        provider_id=order.provider_id,
        provider_name=order.provider.name if order.provider else None,
        status=order.status,
        created_at=order.created_at,
        scheduled_at=order.scheduled_at,
        sent_at=order.sent_at,
        items=items,
    )


@router.post(
    "/supplier/send",
    status_code=status.HTTP_200_OK,
)
async def send_supplier_orders_endpoint(
    supplier_order_ids: List[int],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    logger.info(
        "Manual supplier orders send requested: user_id=%s orders=%s",
        current_user.id,
        supplier_order_ids,
    )
    result = await send_supplier_orders(session=session, supplier_order_ids=supplier_order_ids)
    logger.info(
        "Manual supplier orders send finished: user_id=%s result=%s",
        current_user.id,
        result,
    )
    sent_count = result.get("sent", 0)
    error_count = result.get("failed", 0)
    await _notify_current_user(
        session,
        current_user,
        title="Отправка заказов поставщикам завершена",
        message=(f"Успешно отправлено: {sent_count}." f" С ошибкой: {error_count}."),
        level=(AppNotificationLevel.WARNING if error_count else AppNotificationLevel.SUCCESS),
        link="/customer-orders/suppliers",
    )
    return result


@router.post(
    "/supplier/send-scheduled",
    status_code=status.HTTP_200_OK,
)
async def send_scheduled_supplier_orders_endpoint(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    logger.info(
        "Scheduled supplier orders send requested: user_id=%s",
        current_user.id,
    )
    result = await send_scheduled_supplier_orders(session)
    logger.info(
        "Scheduled supplier orders send finished: user_id=%s result=%s",
        current_user.id,
        result,
    )
    sent_count = result.get("sent", 0)
    error_count = result.get("failed", 0)
    await _notify_current_user(
        session,
        current_user,
        title="Плановая отправка заказов завершена",
        message=(f"Успешно отправлено: {sent_count}." f" С ошибкой: {error_count}."),
        level=(AppNotificationLevel.WARNING if error_count else AppNotificationLevel.SUCCESS),
        link="/customer-orders/suppliers",
    )
    return result
