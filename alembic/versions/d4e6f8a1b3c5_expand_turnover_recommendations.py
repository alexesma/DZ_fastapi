"""Expand turnover summary into purchase recommendations.

Revision ID: d4e6f8a1b3c5
Revises: c8f3a9d2e5b7
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d4e6f8a1b3c5"
down_revision: Union[str, None] = "c8f3a9d2e5b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    table = "autopartturnoversummary"
    columns = (
        sa.Column("order_count_30d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("customer_count_90d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active_weeks_90d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("shipped_qty_30d", sa.Integer(), nullable=True),
        sa.Column("shipped_qty_90d", sa.Integer(), nullable=True),
        sa.Column("shipments_data_available", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("median_purchase_price", sa.DECIMAL(10, 2), nullable=True),
        sa.Column(
            "min_price_provider_id", sa.Integer(), sa.ForeignKey("provider.id"), nullable=True
        ),
        sa.Column("min_price_provider_name", sa.String(255), nullable=True),
        sa.Column("min_price_pricelist_date", sa.Date(), nullable=True),
        sa.Column("price_vs_90d_pct", sa.Float(), nullable=True),
        sa.Column("trend_supplier_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("declining_supplier_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "supplier_trends", sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")
        ),
        sa.Column("current_stock_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reserved_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("free_stock_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("in_transit_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("open_backlog_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("multiplicity", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("lead_time_days", sa.Float(), nullable=True),
        sa.Column("safety_stock_days", sa.Integer(), nullable=False, server_default="7"),
        sa.Column("target_stock_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recommended_order_qty", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("category_id", sa.Integer(), sa.ForeignKey("category.id"), nullable=True),
        sa.Column("category_name", sa.String(255), nullable=True),
        sa.Column("demand_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("market_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("price_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("recommendation_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("is_market_opportunity", sa.Boolean(), nullable=False, server_default="false"),
    )
    for column in columns:
        op.add_column(table, column)

    op.create_index(
        "ix_autopartturnoversummary_recommendation_score",
        table,
        ["recommendation_score"],
    )
    op.create_index(
        "ix_autopartturnoversummary_is_market_opportunity",
        table,
        ["is_market_opportunity"],
    )
    # Уникальное ограничение на autopart_id уже создаёт индекс. Отдельный
    # неуникальный индекс из первоначальной миграции только дублирует его.
    op.drop_index("ix_autopartturnoversummary_autopart_id", table_name=table)

    # Полный ночной срез фильтруется по времени до группировки по позиции.
    op.create_index(
        "ix_autopartpricehistory_created_at",
        "autopartpricehistory",
        ["created_at"],
    )
    op.create_index(
        "ix_customerorder_received_at",
        "customerorder",
        ["received_at"],
    )


def downgrade() -> None:
    table = "autopartturnoversummary"
    op.drop_index("ix_customerorder_received_at", table_name="customerorder")
    op.drop_index("ix_autopartpricehistory_created_at", table_name="autopartpricehistory")
    op.create_index("ix_autopartturnoversummary_autopart_id", table, ["autopart_id"])
    op.drop_index("ix_autopartturnoversummary_is_market_opportunity", table_name=table)
    op.drop_index("ix_autopartturnoversummary_recommendation_score", table_name=table)
    for name in (
        "is_market_opportunity",
        "recommendation_score",
        "price_score",
        "market_score",
        "demand_score",
        "category_name",
        "category_id",
        "recommended_order_qty",
        "target_stock_qty",
        "safety_stock_days",
        "lead_time_days",
        "multiplicity",
        "open_backlog_qty",
        "in_transit_qty",
        "free_stock_qty",
        "reserved_qty",
        "current_stock_qty",
        "supplier_trends",
        "declining_supplier_count",
        "trend_supplier_count",
        "price_vs_90d_pct",
        "min_price_pricelist_date",
        "min_price_provider_name",
        "min_price_provider_id",
        "median_purchase_price",
        "shipments_data_available",
        "shipped_qty_90d",
        "shipped_qty_30d",
        "active_weeks_90d",
        "customer_count_90d",
        "order_count_30d",
    ):
        op.drop_column(table, name)
