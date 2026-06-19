from __future__ import annotations

import io
from datetime import date, datetime, timedelta
from typing import Any, Optional

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, AutoPurchaseTopItem
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (
    CustomerOrder,
    CustomerOrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)

TOP_SOURCE_FILE = "file"
TOP_SOURCE_CURRENT = "current"


def _normalize_oem(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _is_missing_value(value: Any) -> bool:
    if value is None or value == "":
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _normalize_text(value: Any) -> str:
    if _is_missing_value(value):
        return ""
    return str(value or "").strip()


def _normalize_source(value: str | None) -> str:
    source = _normalize_text(value or TOP_SOURCE_FILE).lower()
    return source or TOP_SOURCE_FILE


def _normalize_brand_filter(value: str | None) -> str:
    return _normalize_text(value).lower()


def _pick_column(row: dict[str, Any], names: set[str]) -> Any:
    for key, value in row.items():
        normalized_key = _normalize_text(key).lower().replace(" ", "_")
        if normalized_key in names:
            return value
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    if _is_missing_value(value):
        return default
    try:
        return max(int(float(str(value).replace(",", ".").strip())), 0)
    except (TypeError, ValueError):
        return default


def _json_safe_value(value: Any) -> Any:
    if _is_missing_value(value):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _json_safe_value(value.item())
        except (TypeError, ValueError):
            return str(value)
    return value


def _json_safe_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): _json_safe_value(value)
        for key, value in dict(row or {}).items()
    }


def _serialize_top_item(
    item: AutoPurchaseTopItem,
    *,
    current_quantity: int = 0,
    in_transit_qty: int = 0,
) -> dict[str, Any]:
    target_stock_qty = int(item.target_stock_qty or 0)
    available_qty = int(current_quantity or 0) + int(in_transit_qty or 0)
    return {
        "id": int(item.id),
        "source": item.source,
        "autopart_id": item.autopart_id,
        "oem_number": item.oem_number,
        "brand_name": item.brand_name,
        "autopart_name": item.autopart_name,
        "rank": int(item.rank or 0),
        "sold_qty": int(item.sold_qty or 0),
        "target_stock_qty": target_stock_qty,
        "is_active": bool(item.is_active),
        "note": item.note,
        "current_quantity": int(current_quantity or 0),
        "in_transit_qty": int(in_transit_qty or 0),
        "gap_qty": max(target_stock_qty - available_qty, 0),
        "imported_at": item.imported_at,
        "updated_at": item.updated_at,
    }


async def _load_latest_own_stock_by_oem(
    session: AsyncSession,
) -> dict[str, int]:
    stmt = (
        select(
            AutoPart.oem_number,
            PriceListAutoPartAssociation.quantity,
        )
        .select_from(PriceListAutoPartAssociation)
        .join(PriceList, PriceList.id == PriceListAutoPartAssociation.pricelist_id)
        .join(
            ProviderPriceListConfig,
            ProviderPriceListConfig.id == PriceList.provider_config_id,
        )
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .join(AutoPart, AutoPart.id == PriceListAutoPartAssociation.autopart_id)
        .where(
            PriceList.is_active.is_(True),
            Provider.is_own_price.is_(True),
            ProviderPriceListConfig.use_for_order_insights.is_(True),
        )
        .order_by(
            PriceList.date.desc().nulls_last(),
            PriceList.id.desc(),
        )
    )
    result: dict[str, int] = {}
    for row in (await session.execute(stmt)).all():
        normalized_oem = _normalize_oem(row.oem_number)
        if not normalized_oem or normalized_oem in result:
            continue
        result[normalized_oem] = int(row.quantity or 0)
    return result


async def _resolve_autopart_id(
    session: AsyncSession,
    *,
    oem_number: str,
    brand_name: str | None,
) -> Optional[int]:
    normalized_oem = _normalize_oem(oem_number)
    if not normalized_oem:
        return None

    stmt = (
        select(AutoPart.id)
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(func.upper(AutoPart.oem_number) == normalized_oem)
        .order_by(AutoPart.id.asc())
        .limit(1)
    )
    normalized_brand = _normalize_text(brand_name).lower()
    if normalized_brand:
        stmt = stmt.where(func.lower(Brand.name) == normalized_brand)

    autopart_id = (await session.execute(stmt)).scalar_one_or_none()
    if autopart_id is not None:
        return int(autopart_id)

    fallback_stmt = (
        select(AutoPart.id)
        .where(func.upper(AutoPart.oem_number) == normalized_oem)
        .order_by(AutoPart.id.asc())
        .limit(1)
    )
    fallback_id = (await session.execute(fallback_stmt)).scalar_one_or_none()
    return int(fallback_id) if fallback_id is not None else None


async def list_autopurchase_top_items(
    session: AsyncSession,
    *,
    source: str = TOP_SOURCE_FILE,
    limit: int = 100,
    active_only: bool = True,
    brand: Optional[str] = None,
) -> dict[str, Any]:
    normalized_source = _normalize_source(source)
    normalized_limit = max(min(int(limit or 100), 1000), 1)
    normalized_brand = _normalize_brand_filter(brand)
    stmt = (
        select(AutoPurchaseTopItem)
        .where(AutoPurchaseTopItem.source == normalized_source)
        .order_by(
            AutoPurchaseTopItem.rank.asc(),
            AutoPurchaseTopItem.sold_qty.desc(),
            AutoPurchaseTopItem.id.asc(),
        )
        .limit(normalized_limit)
    )
    if active_only:
        stmt = stmt.where(AutoPurchaseTopItem.is_active.is_(True))
    if normalized_brand:
        stmt = stmt.where(
            func.lower(AutoPurchaseTopItem.brand_name).contains(normalized_brand)
        )
    rows = (await session.execute(stmt)).scalars().all()
    stock_by_oem = await _load_latest_own_stock_by_oem(session)
    return {
        "source": normalized_source,
        "total_items": len(rows),
        "rows": [
            _serialize_top_item(
                item,
                current_quantity=stock_by_oem.get(_normalize_oem(item.oem_number), 0),
            )
            for item in rows
        ],
    }


async def create_autopurchase_top_item(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
) -> dict[str, Any]:
    source = _normalize_source(payload.get("source"))
    oem_number = _normalize_oem(payload.get("oem_number"))
    if not oem_number:
        raise ValueError("Укажите OEM/артикул топ-позиции")

    brand_name = _normalize_text(payload.get("brand_name")) or None
    autopart_id = await _resolve_autopart_id(
        session,
        oem_number=oem_number,
        brand_name=brand_name,
    )
    item = AutoPurchaseTopItem(
        source=source,
        autopart_id=autopart_id,
        oem_number=oem_number,
        brand_name=brand_name,
        autopart_name=_normalize_text(payload.get("autopart_name")) or None,
        rank=_safe_int(payload.get("rank")),
        sold_qty=_safe_int(payload.get("sold_qty")),
        target_stock_qty=_safe_int(payload.get("target_stock_qty")),
        is_active=bool(payload.get("is_active", True)),
        note=_normalize_text(payload.get("note")) or None,
        imported_at=now_moscow(),
        updated_at=now_moscow(),
        raw_payload=_json_safe_payload(payload),
    )
    session.add(item)
    await session.commit()
    await session.refresh(item)
    stock_by_oem = await _load_latest_own_stock_by_oem(session)
    return _serialize_top_item(
        item,
        current_quantity=stock_by_oem.get(_normalize_oem(item.oem_number), 0),
    )


async def update_autopurchase_top_item(
    session: AsyncSession,
    *,
    item_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    item = await session.get(AutoPurchaseTopItem, int(item_id))
    if item is None:
        raise ValueError("Топ-позиция не найдена")

    if "oem_number" in payload and payload["oem_number"] is not None:
        normalized_oem = _normalize_oem(payload["oem_number"])
        if not normalized_oem:
            raise ValueError("Укажите OEM/артикул топ-позиции")
        item.oem_number = normalized_oem
    if "brand_name" in payload:
        item.brand_name = _normalize_text(payload.get("brand_name")) or None
    if "autopart_name" in payload:
        item.autopart_name = _normalize_text(payload.get("autopart_name")) or None
    if "rank" in payload and payload["rank"] is not None:
        item.rank = _safe_int(payload["rank"])
    if "sold_qty" in payload and payload["sold_qty"] is not None:
        item.sold_qty = _safe_int(payload["sold_qty"])
    if "target_stock_qty" in payload and payload["target_stock_qty"] is not None:
        item.target_stock_qty = _safe_int(payload["target_stock_qty"])
    if "is_active" in payload and payload["is_active"] is not None:
        item.is_active = bool(payload["is_active"])
    if "note" in payload:
        item.note = _normalize_text(payload.get("note")) or None

    item.autopart_id = await _resolve_autopart_id(
        session,
        oem_number=item.oem_number,
        brand_name=item.brand_name,
    )
    item.updated_at = now_moscow()
    session.add(item)
    await session.commit()
    await session.refresh(item)
    stock_by_oem = await _load_latest_own_stock_by_oem(session)
    return _serialize_top_item(
        item,
        current_quantity=stock_by_oem.get(_normalize_oem(item.oem_number), 0),
    )


async def import_autopurchase_top_items(
    session: AsyncSession,
    *,
    content: bytes,
    filename: str,
    source: str = TOP_SOURCE_FILE,
) -> dict[str, Any]:
    normalized_source = _normalize_source(source)
    lower_name = str(filename or "").lower()
    if lower_name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content))
    else:
        df = pd.read_excel(io.BytesIO(content))

    imported_count = 0
    updated_count = 0
    skipped_count = 0
    now = now_moscow()

    for idx, raw_row in enumerate(df.to_dict(orient="records"), start=1):
        oem_number = _normalize_oem(
            _pick_column(
                raw_row,
                {"oem", "артикул", "номер", "номер_детали", "part_number"},
            )
        )
        if not oem_number:
            skipped_count += 1
            continue

        brand_name = _normalize_text(
            _pick_column(raw_row, {"brand", "бренд", "make", "марка"})
        ) or None
        autopart_name = _normalize_text(
            _pick_column(raw_row, {"name", "название", "наименование"})
        ) or None
        sold_qty = _safe_int(
            _pick_column(raw_row, {"sold_qty", "sold", "продано", "qty", "количество"})
        )
        target_stock_qty = _safe_int(
            _pick_column(
                raw_row,
                {"target_stock_qty", "target", "цель", "остаток", "заказать"},
            ),
            default=sold_qty,
        )
        rank = _safe_int(_pick_column(raw_row, {"rank", "рейтинг", "место"}), idx)
        note = _normalize_text(_pick_column(raw_row, {"note", "комментарий"})) or None
        autopart_id = await _resolve_autopart_id(
            session,
            oem_number=oem_number,
            brand_name=brand_name,
        )

        existing = (
            await session.execute(
                select(AutoPurchaseTopItem).where(
                    AutoPurchaseTopItem.source == normalized_source,
                    AutoPurchaseTopItem.oem_number == oem_number,
                    AutoPurchaseTopItem.brand_name == brand_name,
                )
            )
        ).scalar_one_or_none()
        if existing:
            existing.autopart_id = autopart_id
            existing.autopart_name = autopart_name or existing.autopart_name
            existing.rank = rank
            existing.sold_qty = sold_qty
            existing.target_stock_qty = target_stock_qty
            existing.is_active = True
            existing.note = note
            existing.updated_at = now
            existing.raw_payload = _json_safe_payload(raw_row)
            updated_count += 1
        else:
            session.add(
                AutoPurchaseTopItem(
                    source=normalized_source,
                    autopart_id=autopart_id,
                    oem_number=oem_number,
                    brand_name=brand_name,
                    autopart_name=autopart_name,
                    rank=rank,
                    sold_qty=sold_qty,
                    target_stock_qty=target_stock_qty,
                    is_active=True,
                    note=note,
                    imported_at=now,
                    updated_at=now,
                    raw_payload=_json_safe_payload(raw_row),
                )
            )
            imported_count += 1

    await session.commit()
    total_active = (
        await session.execute(
            select(func.count(AutoPurchaseTopItem.id)).where(
                AutoPurchaseTopItem.source == normalized_source,
                AutoPurchaseTopItem.is_active.is_(True),
            )
        )
    ).scalar_one()
    return {
        "source": normalized_source,
        "imported_count": imported_count,
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "total_active": int(total_active or 0),
    }


async def list_current_autopurchase_top_items(
    session: AsyncSession,
    *,
    days: int = 365,
    limit: int = 100,
    brand: Optional[str] = None,
) -> dict[str, Any]:
    normalized_days = max(min(int(days or 365), 730), 1)
    normalized_limit = max(min(int(limit or 100), 1000), 1)
    normalized_brand = _normalize_brand_filter(brand)
    cutoff = now_moscow() - timedelta(days=normalized_days)
    stmt = (
        select(
            CustomerOrderItem.oem,
            CustomerOrderItem.brand,
            CustomerOrderItem.name,
            func.sum(CustomerOrderItem.requested_qty).label("sold_qty"),
            func.count(CustomerOrderItem.id).label("order_count"),
        )
        .join(CustomerOrder, CustomerOrder.id == CustomerOrderItem.order_id)
        .where(
            CustomerOrder.received_at >= cutoff,
            CustomerOrderItem.requested_qty.isnot(None),
            CustomerOrderItem.requested_qty > 0,
        )
        .group_by(
            CustomerOrderItem.oem,
            CustomerOrderItem.brand,
            CustomerOrderItem.name,
        )
        .order_by(func.sum(CustomerOrderItem.requested_qty).desc())
        .limit(normalized_limit)
    )
    if normalized_brand:
        stmt = stmt.where(
            func.lower(CustomerOrderItem.brand).contains(normalized_brand)
        )
    stock_by_oem = await _load_latest_own_stock_by_oem(session)
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate((await session.execute(stmt)).all(), start=1):
        oem_number = _normalize_oem(row.oem)
        if not oem_number:
            continue
        sold_qty = int(row.sold_qty or 0)
        current_quantity = stock_by_oem.get(oem_number, 0)
        target_stock_qty = max(int(round(sold_qty / max(normalized_days, 1) * 45)), 1)
        rows.append(
            {
                "id": -rank,
                "source": TOP_SOURCE_CURRENT,
                "autopart_id": None,
                "oem_number": oem_number,
                "brand_name": _normalize_text(row.brand) or None,
                "autopart_name": _normalize_text(row.name) or None,
                "rank": rank,
                "sold_qty": sold_qty,
                "target_stock_qty": target_stock_qty,
                "is_active": True,
                "note": f"Заказов за {normalized_days} дн: {int(row.order_count or 0)}",
                "current_quantity": current_quantity,
                "in_transit_qty": 0,
                "gap_qty": max(target_stock_qty - current_quantity, 0),
                "imported_at": now_moscow(),
                "updated_at": now_moscow(),
            }
        )
    return {
        "source": TOP_SOURCE_CURRENT,
        "total_items": len(rows),
        "rows": rows,
    }
