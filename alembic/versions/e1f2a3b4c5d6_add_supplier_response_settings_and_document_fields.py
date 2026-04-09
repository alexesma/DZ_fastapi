"""add supplier response settings and document fields

Revision ID: e1f2a3b4c5d6
Revises: c6d7e8f9a0b1, d6a8c0e2f5b1
Create Date: 2026-04-09 18:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, Sequence[str], None] = (
    "c6d7e8f9a0b1",
    "d6a8c0e2f5b1",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_response_lookback_days",
            sa.Integer(),
            nullable=True,
            server_default="14",
        ),
    )
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_order_stub_enabled",
            sa.Boolean(),
            nullable=True,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_order_stub_email",
            sa.String(length=255),
            nullable=True,
            server_default="info@dragonzap.ru",
        ),
    )

    op.add_column(
        "supplierresponseconfig",
        sa.Column(
            "file_payload_type",
            sa.String(length=16),
            nullable=False,
            server_default="response",
        ),
    )
    op.add_column(
        "supplierresponseconfig",
        sa.Column("document_number_col", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierresponseconfig",
        sa.Column("document_date_col", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierresponseconfig",
        sa.Column("gtd_col", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierresponseconfig",
        sa.Column("country_code_col", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierresponseconfig",
        sa.Column("country_name_col", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierresponseconfig",
        sa.Column("total_price_with_vat_col", sa.Integer(), nullable=True),
    )

    op.add_column(
        "supplierreceiptitem",
        sa.Column("total_price_with_vat", sa.DECIMAL(12, 2), nullable=True),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column("gtd_code", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column("country_code", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column("country_name", sa.String(length=120), nullable=True),
    )

    op.alter_column(
        "customerorderinboxsettings",
        "supplier_response_lookback_days",
        server_default=None,
    )
    op.alter_column(
        "customerorderinboxsettings",
        "supplier_order_stub_enabled",
        server_default=None,
    )
    op.alter_column(
        "customerorderinboxsettings",
        "supplier_order_stub_email",
        server_default=None,
    )
    op.alter_column(
        "supplierresponseconfig",
        "file_payload_type",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("supplierreceiptitem", "country_name")
    op.drop_column("supplierreceiptitem", "country_code")
    op.drop_column("supplierreceiptitem", "gtd_code")
    op.drop_column("supplierreceiptitem", "total_price_with_vat")

    op.drop_column("supplierresponseconfig", "total_price_with_vat_col")
    op.drop_column("supplierresponseconfig", "country_name_col")
    op.drop_column("supplierresponseconfig", "country_code_col")
    op.drop_column("supplierresponseconfig", "gtd_col")
    op.drop_column("supplierresponseconfig", "document_date_col")
    op.drop_column("supplierresponseconfig", "document_number_col")
    op.drop_column("supplierresponseconfig", "file_payload_type")

    op.drop_column("customerorderinboxsettings", "supplier_order_stub_email")
    op.drop_column("customerorderinboxsettings", "supplier_order_stub_enabled")
    op.drop_column(
        "customerorderinboxsettings", "supplier_response_lookback_days"
    )
