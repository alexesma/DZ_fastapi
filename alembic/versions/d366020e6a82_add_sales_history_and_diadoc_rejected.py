"""sales history monthly + diadoc incoming rejected_at

Revision ID: d366020e6a82
Revises: 51c71a4c2155
Create Date: 2026-07-04 16:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d366020e6a82"
down_revision: Union[str, Sequence[str], None] = "51c71a4c2155"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "saleshistorymonthly",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("oem_number", sa.String(120), nullable=False),
        sa.Column("brand_name", sa.String(120), nullable=True),
        sa.Column(
            "quantity", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("revenue", sa.DECIMAL(14, 2), nullable=True),
        sa.Column(
            "source", sa.String(32), nullable=False, server_default="1c"
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "period",
            "oem_number",
            "brand_name",
            name="uq_sales_history_period_oem_brand",
        ),
    )
    op.create_index(
        "ix_saleshistorymonthly_period", "saleshistorymonthly", ["period"]
    )
    op.create_index(
        "ix_saleshistorymonthly_oem_number",
        "saleshistorymonthly",
        ["oem_number"],
    )

    op.add_column(
        "diadocincomingdocument",
        sa.Column(
            "rejected_at", sa.DateTime(timezone=True), nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column("diadocincomingdocument", "rejected_at")
    op.drop_index(
        "ix_saleshistorymonthly_oem_number",
        table_name="saleshistorymonthly",
    )
    op.drop_index(
        "ix_saleshistorymonthly_period", table_name="saleshistorymonthly"
    )
    op.drop_table("saleshistorymonthly")
