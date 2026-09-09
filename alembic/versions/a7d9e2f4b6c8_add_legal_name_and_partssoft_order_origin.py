"""Add legal customer name and Parts-Soft order origin fields.

Revision ID: a7d9e2f4b6c8
Revises: e4b6d8f0a123
Create Date: 2026-09-09 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a7d9e2f4b6c8"
down_revision: Union[str, Sequence[str], None] = "e4b6d8f0a123"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("customer", sa.Column("legal_name", sa.String(512), nullable=True))
    op.add_column("customerorder", sa.Column("import_origin", sa.String(64), nullable=True))
    op.add_column(
        "customerorder", sa.Column("recovered_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "customerorder",
        sa.Column(
            "external_payload",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )
    op.create_index(
        op.f("ix_customerorder_import_origin"),
        "customerorder",
        ["import_origin"],
        unique=False,
    )
    for name in ("external_offer_id", "external_provider_id", "external_warehouse_id"):
        op.add_column("customerorderitem", sa.Column(name, sa.String(128), nullable=True))
    op.add_column(
        "customerorderitem", sa.Column("source_resolution_status", sa.String(32), nullable=True)
    )
    op.add_column(
        "customerorderitem",
        sa.Column(
            "source_payload",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("customerorderitem", "source_payload")
    op.drop_column("customerorderitem", "source_resolution_status")
    for name in ("external_warehouse_id", "external_provider_id", "external_offer_id"):
        op.drop_column("customerorderitem", name)
    op.drop_index(op.f("ix_customerorder_import_origin"), table_name="customerorder")
    op.drop_column("customerorder", "external_payload")
    op.drop_column("customerorder", "recovered_at")
    op.drop_column("customerorder", "import_origin")
    op.drop_column("customer", "legal_name")
