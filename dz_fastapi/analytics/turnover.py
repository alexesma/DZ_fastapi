"""Daily materialized analytics for demand, supplier movement and purchasing.

Customer orders are the demand signal until actual shipment accounting is fully
used by the business. The schema keeps shipment fields separate so both signals
can coexist when warehouse shipment accounting is enabled.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("dz_fastapi")

_DEMAND_STATUSES = ("OWN_STOCK", "SUPPLIER")
_TOP_PERCENTILE_THRESHOLD = 0.9


async def refresh_turnover_summary(session: AsyncSession) -> int:
    """Rebuild the purchasing analytics snapshot and return upserted row count."""

    # Demand from accepted customer order lines. These are orders, not confirmed
    # shipments; deleted and failed orders are deliberately excluded.
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_demand ON COMMIT DROP AS
            SELECT
                coi.autopart_id,
                sum(COALESCE(coi.ship_qty, coi.requested_qty))
                    FILTER (WHERE co.received_at >= now() - interval '30 days')::integer
                    AS ordered_qty_30d,
                sum(COALESCE(coi.ship_qty, coi.requested_qty))
                    FILTER (WHERE co.received_at >= now() - interval '90 days')::integer
                    AS ordered_qty_90d,
                count(DISTINCT co.id)
                    FILTER (WHERE co.received_at >= now() - interval '30 days')::integer
                    AS order_count_30d,
                count(DISTINCT co.customer_id)::integer AS customer_count_90d,
                count(DISTINCT date_trunc('week', co.received_at))::integer
                    AS active_weeks_90d
            FROM customerorderitem coi
            JOIN customerorder co ON co.id = coi.order_id
            WHERE coi.autopart_id IS NOT NULL
              AND coi.status = ANY(:demand_statuses)
              AND co.status::text <> 'ERROR'
              AND co.deleted_at IS NULL
              AND co.received_at >= now() - interval '90 days'
            GROUP BY coi.autopart_id
            """
        ),
        {"demand_statuses": list(_DEMAND_STATUSES)},
    )

    # Exactly one active and fresh pricelist per source configuration. Retained
    # historical pricelists must not affect a current price or supplier count.
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_current_pricelists ON COMMIT DROP AS
            WITH ranked AS (
                SELECT
                    pl.id,
                    pl.provider_id,
                    pl.provider_config_id,
                    pl.date,
                    c.name AS provider_name,
                    pc.min_delivery_day,
                    pc.max_delivery_day,
                    row_number() OVER (
                        PARTITION BY COALESCE(pl.provider_config_id, -pl.provider_id)
                        ORDER BY pl.date DESC NULLS LAST, pl.id DESC
                    ) AS rn
                FROM pricelist pl
                JOIN provider p ON p.id = pl.provider_id
                JOIN client c ON c.id = p.id
                LEFT JOIN providerpricelistconfig pc
                    ON pc.id = pl.provider_config_id
                WHERE pl.is_active IS TRUE
                  AND COALESCE(p.is_own_price, false) IS FALSE
                  AND COALESCE(p.autopurchase_blocked, false) IS FALSE
                  AND (pc.id IS NULL OR (
                        pc.is_active IS TRUE
                        AND COALESCE(pc.autopurchase_blocked, false) IS FALSE
                  ))
                  AND pl.date >= current_date
                      - GREATEST(COALESCE(pc.max_days_without_update, 3), 1)
            )
            SELECT * FROM ranked WHERE rn = 1
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_offers ON COMMIT DROP AS
            SELECT
                pla.autopart_id,
                count(DISTINCT cp.provider_id)::integer AS supplier_count,
                min(pla.price) AS min_purchase_price,
                avg(pla.price)::numeric(10, 2) AS avg_purchase_price,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY pla.price::double precision
                )::numeric(10, 2) AS median_purchase_price,
                (array_agg(cp.provider_id ORDER BY pla.price, cp.date DESC))[1]
                    AS min_price_provider_id,
                (array_agg(cp.provider_name ORDER BY pla.price, cp.date DESC))[1]
                    AS min_price_provider_name,
                (array_agg(cp.date ORDER BY pla.price, cp.date DESC))[1]
                    AS min_price_pricelist_date,
                (array_agg(
                    COALESCE(cp.max_delivery_day, cp.min_delivery_day)
                    ORDER BY pla.price, cp.date DESC
                ))[1]::double precision AS lead_time_days
            FROM _turnover_current_pricelists cp
            JOIN pricelistautopartassociation pla ON pla.pricelist_id = cp.id
            WHERE pla.quantity > 0
              AND pla.price > 0
            GROUP BY pla.autopart_id
            """
        )
    )

    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_price_history ON COMMIT DROP AS
            SELECT
                h.autopart_id,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY h.price::double precision
                ) AS median_price_90d
            FROM autopartpricehistory h
            JOIN provider p ON p.id = h.provider_id
            LEFT JOIN providerpricelistconfig pc ON pc.id = h.provider_config_id
            WHERE h.created_at >= now() - interval '90 days'
              AND h.price > 0
              AND COALESCE(p.is_own_price, false) IS FALSE
              AND (pc.id IS NULL OR pc.is_active IS TRUE)
            GROUP BY h.autopart_id
            """
        )
    )

    # One observation per source and day prevents same-day price edits from
    # pretending to be several independent stock observations.
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_daily_history ON COMMIT DROP AS
            SELECT DISTINCT ON (
                h.autopart_id,
                h.provider_id,
                COALESCE(h.provider_config_id, 0),
                h.created_at::date
            )
                h.autopart_id,
                h.provider_id,
                COALESCE(h.provider_config_id, 0) AS provider_config_key,
                c.name AS provider_name,
                h.created_at::date AS observed_on,
                h.quantity
            FROM autopartpricehistory h
            JOIN provider p ON p.id = h.provider_id
            JOIN client c ON c.id = p.id
            LEFT JOIN providerpricelistconfig pc ON pc.id = h.provider_config_id
            WHERE h.created_at >= now() - interval '30 days'
              AND COALESCE(p.is_own_price, false) IS FALSE
              AND (pc.id IS NULL OR pc.is_active IS TRUE)
            ORDER BY
                h.autopart_id,
                h.provider_id,
                COALESCE(h.provider_config_id, 0),
                h.created_at::date,
                h.created_at DESC
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_source_trends ON COMMIT DROP AS
            SELECT
                autopart_id,
                provider_id,
                provider_config_key,
                provider_name,
                regr_slope(
                    quantity,
                    extract(epoch FROM observed_on::timestamp) / 86400.0
                ) AS qty_trend_30d,
                count(*)::integer AS observed_days,
                min(observed_on) AS first_observed_on,
                max(observed_on) AS last_observed_on,
                (array_agg(quantity ORDER BY observed_on DESC))[1]::integer
                    AS latest_quantity
            FROM _turnover_daily_history
            GROUP BY autopart_id, provider_id, provider_config_key, provider_name
            HAVING count(*) >= 3
               AND max(observed_on) - min(observed_on) >= 7
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_trend ON COMMIT DROP AS
            WITH ranked AS (
                SELECT
                    st.*,
                    row_number() OVER (
                        PARTITION BY st.autopart_id
                        ORDER BY st.qty_trend_30d ASC, st.provider_id
                    ) AS detail_rn
                FROM _turnover_source_trends st
            )
            SELECT
                autopart_id,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY qty_trend_30d
                ) AS qty_trend_30d,
                count(DISTINCT provider_id)::integer AS trend_supplier_count,
                count(DISTINCT provider_id)
                    FILTER (WHERE qty_trend_30d < -0.05)::integer
                    AS declining_supplier_count,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'provider_id', provider_id,
                            'provider_name', provider_name,
                            'trend_per_day', round(qty_trend_30d::numeric, 3),
                            'observed_days', observed_days,
                            'latest_quantity', latest_quantity,
                            'first_observed_on', first_observed_on,
                            'last_observed_on', last_observed_on
                        ) ORDER BY qty_trend_30d ASC
                    ) FILTER (WHERE detail_rn <= 10),
                    '[]'::jsonb
                ) AS supplier_trends
            FROM ranked
            GROUP BY autopart_id
            """
        )
    )

    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_stock ON COMMIT DROP AS
            SELECT autopart_id, sum(GREATEST(quantity, 0))::integer AS current_stock_qty
            FROM stockbylocation
            GROUP BY autopart_id
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_reserves ON COMMIT DROP AS
            SELECT autopart_id, sum(GREATEST(quantity, 0))::integer AS reserved_qty
            FROM stockreserve
            WHERE status::text = 'active'
            GROUP BY autopart_id
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_in_transit ON COMMIT DROP AS
            SELECT
                soi.autopart_id,
                sum(GREATEST(
                    COALESCE(soi.confirmed_quantity, soi.quantity)
                    - COALESCE(soi.received_quantity, 0),
                    0
                ))::integer AS in_transit_qty
            FROM supplierorderitem soi
            JOIN supplierorder so ON so.id = soi.supplier_order_id
            WHERE soi.autopart_id IS NOT NULL
              AND so.status::text IN ('NEW', 'SCHEDULED', 'SENT')
            GROUP BY soi.autopart_id
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_backlog ON COMMIT DROP AS
            WITH backlog AS (
                SELECT coi.autopart_id, coi.requested_qty::integer AS qty
                FROM customerorderitem coi
                JOIN customerorder co ON co.id = coi.order_id
                WHERE coi.autopart_id IS NOT NULL
                  AND coi.status::text = 'NEW'
                  AND co.deleted_at IS NULL
                  AND co.status::text <> 'ERROR'
                UNION ALL
                SELECT coi.autopart_id, soi.quantity::integer
                FROM stockorderitem soi
                JOIN stockorder so ON so.id = soi.stock_order_id
                JOIN customerorderitem coi ON coi.id = soi.customer_order_item_id
                JOIN customerorder co ON co.id = coi.order_id
                WHERE coi.autopart_id IS NOT NULL
                  AND so.status::text = 'NEW'
                  AND co.deleted_at IS NULL
                UNION ALL
                SELECT
                    coi.autopart_id,
                    GREATEST(
                        COALESCE(soi.confirmed_quantity, soi.quantity)
                        - COALESCE(soi.received_quantity, 0),
                        0
                    )::integer
                FROM supplierorderitem soi
                JOIN supplierorder so ON so.id = soi.supplier_order_id
                JOIN customerorderitem coi ON coi.id = soi.customer_order_item_id
                JOIN customerorder co ON co.id = coi.order_id
                WHERE coi.autopart_id IS NOT NULL
                  AND so.status::text <> 'REMOVED'
                  AND co.deleted_at IS NULL
            )
            SELECT autopart_id, sum(GREATEST(qty, 0))::integer AS open_backlog_qty
            FROM backlog
            GROUP BY autopart_id
            """
        )
    )
    await session.execute(
        text(
            """
            CREATE TEMP TABLE _turnover_category ON COMMIT DROP AS
            SELECT
                aca.autopart_id,
                min(c.id)::integer AS category_id,
                (array_agg(c.name ORDER BY c.id))[1] AS category_name
            FROM autopart_category_association aca
            JOIN category c ON c.id = aca.category_id
            GROUP BY aca.autopart_id
            """
        )
    )

    result = await session.execute(text(_UPSERT_SQL))
    affected = result.rowcount or 0

    # Anything absent from today's source set is no longer a valid summary.
    await session.execute(
        text(
            """
            DELETE FROM autopartturnoversummary ts
            WHERE NOT EXISTS (SELECT 1 FROM _turnover_demand d WHERE d.autopart_id = ts.autopart_id)
              AND NOT EXISTS (SELECT 1 FROM _turnover_offers o WHERE o.autopart_id = ts.autopart_id)
              AND NOT EXISTS (SELECT 1 FROM _turnover_trend t WHERE t.autopart_id = ts.autopart_id)
              AND NOT EXISTS (SELECT 1 FROM _turnover_stock s WHERE s.autopart_id = ts.autopart_id)
              AND NOT EXISTS (
                  SELECT 1 FROM _turnover_in_transit i
                  WHERE i.autopart_id = ts.autopart_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM _turnover_backlog b
                  WHERE b.autopart_id = ts.autopart_id
              )
            """
        )
    )

    # Clear yesterday's classification before ranking today's population.
    await session.execute(
        text(
            """
            UPDATE autopartturnoversummary
            SET turnover_percentile = NULL, is_top = false,
                is_market_opportunity = false, demand_score = 0,
                market_score = 0, price_score = 0, recommendation_score = 0,
                safety_stock_days = 7, target_stock_qty = 0,
                recommended_order_qty = 0
            """
        )
    )
    await session.execute(text(_SCORE_SQL))
    await session.execute(text(_RANK_SQL), {"threshold": _TOP_PERCENTILE_THRESHOLD})
    await session.execute(text(_PLANNING_SQL))

    await session.commit()
    logger.info("refresh_turnover_summary: upserted %s rows", affected)
    return affected


_UPSERT_SQL = """
INSERT INTO autopartturnoversummary (
    autopart_id, sold_qty_30d, sold_qty_90d,
    daily_velocity_30d, daily_velocity_90d,
    order_count_30d, customer_count_90d, active_weeks_90d,
    shipped_qty_30d, shipped_qty_90d, shipments_data_available,
    supplier_count, min_purchase_price, avg_purchase_price,
    median_purchase_price, min_price_provider_id,
    min_price_provider_name, min_price_pricelist_date,
    price_vs_90d_pct, supplier_qty_trend_30d,
    trend_supplier_count, declining_supplier_count, supplier_trends,
    current_stock_qty, reserved_qty, free_stock_qty, in_transit_qty,
    open_backlog_qty, multiplicity, lead_time_days,
    safety_stock_days, target_stock_qty, recommended_order_qty,
    category_id, category_name, demand_score, market_score,
    price_score, recommendation_score, is_market_opportunity,
    turnover_percentile, is_top, updated_at
)
SELECT
    ap.id,
    COALESCE(d.ordered_qty_30d, 0), COALESCE(d.ordered_qty_90d, 0),
    COALESCE(d.ordered_qty_30d, 0) / 30.0,
    COALESCE(d.ordered_qty_90d, 0) / 90.0,
    COALESCE(d.order_count_30d, 0), COALESCE(d.customer_count_90d, 0),
    COALESCE(d.active_weeks_90d, 0), NULL, NULL, false,
    COALESCE(o.supplier_count, 0), o.min_purchase_price,
    o.avg_purchase_price, o.median_purchase_price, o.min_price_provider_id,
    o.min_price_provider_name, o.min_price_pricelist_date,
    CASE WHEN ph.median_price_90d > 0 AND o.min_purchase_price IS NOT NULL
         THEN ((o.min_purchase_price::double precision / ph.median_price_90d) - 1.0) * 100.0
         ELSE NULL END,
    t.qty_trend_30d, COALESCE(t.trend_supplier_count, 0),
    COALESCE(t.declining_supplier_count, 0),
    COALESCE(t.supplier_trends, '[]'::jsonb)::json,
    COALESCE(st.current_stock_qty, 0), COALESCE(r.reserved_qty, 0),
    GREATEST(COALESCE(st.current_stock_qty, 0) - COALESCE(r.reserved_qty, 0), 0),
    COALESCE(it.in_transit_qty, 0), COALESCE(bl.open_backlog_qty, 0),
    GREATEST(COALESCE(ap.multiplicity, 1), 1), o.lead_time_days,
    7, 0, 0, cat.category_id, cat.category_name,
    0, 0, 0, 0, false, NULL, false, now()
FROM autopart ap
LEFT JOIN _turnover_demand d ON d.autopart_id = ap.id
LEFT JOIN _turnover_offers o ON o.autopart_id = ap.id
LEFT JOIN _turnover_price_history ph ON ph.autopart_id = ap.id
LEFT JOIN _turnover_trend t ON t.autopart_id = ap.id
LEFT JOIN _turnover_stock st ON st.autopart_id = ap.id
LEFT JOIN _turnover_reserves r ON r.autopart_id = ap.id
LEFT JOIN _turnover_in_transit it ON it.autopart_id = ap.id
LEFT JOIN _turnover_backlog bl ON bl.autopart_id = ap.id
LEFT JOIN _turnover_category cat ON cat.autopart_id = ap.id
WHERE d.autopart_id IS NOT NULL OR o.autopart_id IS NOT NULL
   OR t.autopart_id IS NOT NULL OR st.autopart_id IS NOT NULL
   OR it.autopart_id IS NOT NULL OR bl.autopart_id IS NOT NULL
ON CONFLICT (autopart_id) DO UPDATE SET
    sold_qty_30d = EXCLUDED.sold_qty_30d,
    sold_qty_90d = EXCLUDED.sold_qty_90d,
    daily_velocity_30d = EXCLUDED.daily_velocity_30d,
    daily_velocity_90d = EXCLUDED.daily_velocity_90d,
    order_count_30d = EXCLUDED.order_count_30d,
    customer_count_90d = EXCLUDED.customer_count_90d,
    active_weeks_90d = EXCLUDED.active_weeks_90d,
    shipped_qty_30d = EXCLUDED.shipped_qty_30d,
    shipped_qty_90d = EXCLUDED.shipped_qty_90d,
    shipments_data_available = EXCLUDED.shipments_data_available,
    supplier_count = EXCLUDED.supplier_count,
    min_purchase_price = EXCLUDED.min_purchase_price,
    avg_purchase_price = EXCLUDED.avg_purchase_price,
    median_purchase_price = EXCLUDED.median_purchase_price,
    min_price_provider_id = EXCLUDED.min_price_provider_id,
    min_price_provider_name = EXCLUDED.min_price_provider_name,
    min_price_pricelist_date = EXCLUDED.min_price_pricelist_date,
    price_vs_90d_pct = EXCLUDED.price_vs_90d_pct,
    supplier_qty_trend_30d = EXCLUDED.supplier_qty_trend_30d,
    trend_supplier_count = EXCLUDED.trend_supplier_count,
    declining_supplier_count = EXCLUDED.declining_supplier_count,
    supplier_trends = EXCLUDED.supplier_trends,
    current_stock_qty = EXCLUDED.current_stock_qty,
    reserved_qty = EXCLUDED.reserved_qty,
    free_stock_qty = EXCLUDED.free_stock_qty,
    in_transit_qty = EXCLUDED.in_transit_qty,
    open_backlog_qty = EXCLUDED.open_backlog_qty,
    multiplicity = EXCLUDED.multiplicity,
    lead_time_days = EXCLUDED.lead_time_days,
    category_id = EXCLUDED.category_id,
    category_name = EXCLUDED.category_name,
    updated_at = EXCLUDED.updated_at
"""


_SCORE_SQL = """
WITH demand_ranked AS (
    SELECT ts.autopart_id,
           percent_rank() OVER (
               PARTITION BY ap.brand_id, COALESCE(ts.category_id, 0)
               ORDER BY ts.daily_velocity_30d
           ) AS demand_pct
    FROM autopartturnoversummary ts
    JOIN autopart ap ON ap.id = ts.autopart_id
    WHERE ts.daily_velocity_30d > 0
), scores AS (
    SELECT ts.autopart_id,
           COALESCE(dr.demand_pct, 0) * 80.0
               + LEAST(ts.active_weeks_90d / 8.0, 1.0) * 20.0 AS demand_score,
           CASE WHEN ts.trend_supplier_count > 0 THEN LEAST(
               100.0,
               ts.declining_supplier_count::double precision
                   / ts.trend_supplier_count * 70.0
               + LEAST(abs(LEAST(COALESCE(ts.supplier_qty_trend_30d, 0), 0)) * 10.0, 30.0)
           ) ELSE 0 END AS market_score,
           CASE
               WHEN ts.min_purchase_price IS NULL THEN 0
               WHEN ts.price_vs_90d_pct IS NULL THEN 30
               WHEN ts.price_vs_90d_pct <= -15 THEN 100
               WHEN ts.price_vs_90d_pct <= -10 THEN 85
               WHEN ts.price_vs_90d_pct <= -5 THEN 70
               WHEN ts.price_vs_90d_pct <= 0 THEN 50
               WHEN ts.price_vs_90d_pct <= 5 THEN 25
               ELSE 0
           END AS price_score
    FROM autopartturnoversummary ts
    LEFT JOIN demand_ranked dr ON dr.autopart_id = ts.autopart_id
), combined AS (
    SELECT *, CASE WHEN demand_score > 0
        THEN demand_score * 0.55 + market_score * 0.25 + price_score * 0.20
        ELSE market_score * 0.65 + price_score * 0.35
    END AS recommendation_score
    FROM scores
)
UPDATE autopartturnoversummary ts
SET demand_score = round(c.demand_score::numeric, 2),
    market_score = round(c.market_score::numeric, 2),
    price_score = round(c.price_score::numeric, 2),
    recommendation_score = round(c.recommendation_score::numeric, 2),
    is_market_opportunity = (
        ts.daily_velocity_30d = 0 AND ts.supplier_count >= 2
        AND ts.declining_supplier_count >= 2 AND c.market_score >= 50
    )
FROM combined c
WHERE c.autopart_id = ts.autopart_id
"""


_RANK_SQL = """
WITH ranked AS (
    SELECT ts.autopart_id,
           percent_rank() OVER (
               PARTITION BY ap.brand_id, COALESCE(ts.category_id, 0)
               ORDER BY ts.recommendation_score
           ) AS pct,
           count(*) OVER (
               PARTITION BY ap.brand_id, COALESCE(ts.category_id, 0)
           ) AS cohort_size
    FROM autopartturnoversummary ts
    JOIN autopart ap ON ap.id = ts.autopart_id
    WHERE ts.recommendation_score > 0
)
UPDATE autopartturnoversummary ts
SET turnover_percentile = ranked.pct,
    is_top = (
        (ts.daily_velocity_30d > 0 AND ts.active_weeks_90d >= 3
         AND ts.supplier_count >= 1
         AND ((ranked.cohort_size >= 10 AND ranked.pct >= :threshold)
              OR (ranked.cohort_size < 10 AND ts.recommendation_score >= 70)))
        OR (ts.is_market_opportunity AND ts.recommendation_score >= 70)
    )
FROM ranked
WHERE ranked.autopart_id = ts.autopart_id
"""


_PLANNING_SQL = """
WITH planning AS (
    SELECT autopart_id,
           CASE WHEN active_weeks_90d >= 8 THEN 14
                WHEN active_weeks_90d >= 4 THEN 10 ELSE 7 END AS safety_days,
           GREATEST(daily_velocity_30d, daily_velocity_90d) AS velocity
    FROM autopartturnoversummary
), targets AS (
    SELECT ts.autopart_id, p.safety_days,
           CASE WHEN p.velocity > 0 THEN CEIL(
                    p.velocity * (COALESCE(ts.lead_time_days, 7) + p.safety_days)
                )::integer
                WHEN ts.is_market_opportunity THEN ts.multiplicity
                ELSE 0 END AS target_qty
    FROM autopartturnoversummary ts
    JOIN planning p ON p.autopart_id = ts.autopart_id
)
UPDATE autopartturnoversummary ts
SET safety_stock_days = targets.safety_days,
    target_stock_qty = targets.target_qty,
    recommended_order_qty = CASE
        WHEN targets.target_qty + ts.open_backlog_qty
             - ts.free_stock_qty - ts.in_transit_qty <= 0 THEN 0
        ELSE CEIL(
            (targets.target_qty + ts.open_backlog_qty
             - ts.free_stock_qty - ts.in_transit_qty)::numeric
            / GREATEST(ts.multiplicity, 1)
        )::integer * GREATEST(ts.multiplicity, 1)
    END
FROM targets
WHERE targets.autopart_id = ts.autopart_id
"""
