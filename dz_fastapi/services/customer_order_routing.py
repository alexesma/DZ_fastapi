"""Compare the latest customer pricelists without allocating or sending orders."""

from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from sqlalchemy import func, select

from dz_fastapi.models.partner import (
    CustomerPriceList,
    CustomerPriceListConfig,
    CustomerPriceListExportRow,
)


async def compare_order_pricelists(
    session,
    candidates,
    received_at,
    normalize_key,
    estimate_rejections,
):
    results = []
    fingerprints = []
    for candidate in candidates:
        config = candidate["config"]
        pricelist_config_id = (
            config.reference_pricelist_config_id
            or config.pricelist_config_id
        )
        rows = candidate.get("rows") or []
        # Different parsers must describe the same order. Dropping difficult
        # rows or changing quantities must never improve a candidate's score.
        fingerprints.append(
            Counter(
                (
                    *normalize_key(row.oem, row.brand),
                    row.requested_qty,
                    (
                        str(Decimal(str(row.requested_price)).quantize(Decimal("0.01")))
                        if row.requested_price is not None
                        else None
                    ),
                )
                for row in rows
            )
        )
        entry = {
            "order_config_id": config.id,
            "pricelist_config_id": pricelist_config_id,
            "name": f"Конфигурация #{pricelist_config_id}",
            "total": len(rows),
            "positions": 0,
            "prices": 0,
            "priced_rows": sum(row.requested_price is not None for row in rows),
            "pricelist_id": None,
            "publication_at": None,
            "parse_error": candidate.get("parse_error"),
            "filename": candidate.get("filename"),
            "evaluation_error": None,
            "rejected_pct": None,
        }
        pricelist_config = (
            await session.execute(
                select(
                    CustomerPriceListConfig.name,
                    CustomerPriceListConfig.customer_id,
                ).where(
                    CustomerPriceListConfig.id == pricelist_config_id,
                )
            )
        ).first()
        if pricelist_config:
            entry["name"] = pricelist_config.name
        results.append(entry)
        if not rows or candidate.get("parse_error"):
            continue
        if pricelist_config is None:
            entry["evaluation_error"] = "Конфигурация прайса недоступна"
            continue
        publication_at = func.coalesce(CustomerPriceList.published_at, CustomerPriceList.sent_at)
        snapshot = (
            await session.execute(
                select(CustomerPriceList.id, publication_at.label("publication_at"))
                .where(
                    CustomerPriceList.customer_id == pricelist_config.customer_id,
                    CustomerPriceList.customer_config_id == pricelist_config_id,
                    publication_at.is_not(None),
                )
                .order_by(publication_at.desc(), CustomerPriceList.id.desc())
                .limit(1)
            )
        ).first()
        if snapshot is None:
            entry["evaluation_error"] = "Нет опубликованного прайса"
            continue
        entry["pricelist_id"] = snapshot.id
        entry["publication_at"] = snapshot.publication_at.isoformat()
        wanted = {normalize_key(row.oem, row.brand)[0] for row in rows}
        exported = (
            await session.execute(
                select(
                    CustomerPriceListExportRow.advertised_oem,
                    CustomerPriceListExportRow.advertised_brand,
                    CustomerPriceListExportRow.price,
                ).where(
                    CustomerPriceListExportRow.customer_pricelist_id == snapshot.id,
                    CustomerPriceListExportRow.normalized_oem.in_(wanted),
                )
            )
        ).all()
        prices = defaultdict(set)
        for record in exported:
            prices[normalize_key(record.advertised_oem, record.advertised_brand)].add(
                Decimal(str(record.price)).quantize(Decimal("0.01"))
            )
        for row in rows:
            options = prices.get(normalize_key(row.oem, row.brand))
            if not options:
                continue
            entry["positions"] += 1
            if row.requested_price is not None:
                try:
                    price = Decimal(str(row.requested_price)).quantize(Decimal("0.01"))
                    entry["prices"] += int(price in options)
                except InvalidOperation:
                    pass
        try:
            entry.update(await estimate_rejections(session, config, rows, snapshot.id))
        except ValueError as exc:
            entry["evaluation_error"] = str(exc)

    selected = None
    reason = None
    if any(
        row["parse_error"] or row["evaluation_error"] or row["rejected_pct"] is None
        for row in results
    ):
        reason = "Не удалось оценить все конфигурации"
    elif not fingerprints or any(value != fingerprints[0] for value in fingerprints[1:]):
        reason = "Настройки разбора распознали разный состав заказа"
    elif len({candidate["config"].customer_id for candidate in candidates}) != 1:
        reason = "Конфигурации относятся к разным клиентам"
    else:
        # Compare exact ratios, not rounded percentages shown in the UI.
        ratios = [
            Fraction(row["rejected_value"]) / Fraction(row["requested_value"]) for row in results
        ]
        minimum = min(ratios)
        winners = [row for row, ratio in zip(results, ratios) if ratio == minimum]
        if minimum >= 1:
            reason = "Все конфигурации дают полный отказ"
        elif len(winners) != 1:
            reason = "Несколько конфигураций дают одинаковый минимальный процент отказа"
        else:
            selected = winners[0]
    return {
        "basis": "lowest_rejection" if selected else "review_required",
        "comparison": "latest_pricelists",
        "review_reason": reason,
        "selected_order_config_id": selected["order_config_id"] if selected else None,
        "pricelist_id": selected["pricelist_id"] if selected else None,
        "received_at": received_at.isoformat(),
        "candidates": results,
    }
