"""Add Parts-Soft customer details and source metadata.

Revision ID: e4b6d8f0a123
Revises: d1a3c5e7f902
Create Date: 2026-09-07 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e4b6d8f0a123"
down_revision: Union[str, Sequence[str], None] = "d1a3c5e7f902"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for name, length in (
        ("company_type", 128),
        ("phone", 64),
        ("additional_phone", 64),
        ("bank_bik", 32),
        ("bank_name", 255),
        ("bank_city", 255),
        ("bank_account", 64),
        ("correspondent_account", 64),
        ("registration_source", 128),
    ):
        op.add_column("customer", sa.Column(name, sa.String(length=length), nullable=True))
    op.add_column(
        "customer",
        sa.Column("vat_rate", sa.Numeric(precision=7, scale=3), nullable=True),
    )
    op.create_index(
        op.f("ix_customer_registration_source"),
        "customer",
        ["registration_source"],
        unique=False,
    )
    op.add_column(
        "customerexternalreference",
        sa.Column(
            "external_classification",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        "customerexternalreference",
        sa.Column(
            "external_payload",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )
    op.add_column(
        "customerexternalreference",
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerexternalreference", "last_synced_at")
    op.drop_column("customerexternalreference", "external_payload")
    op.drop_column("customerexternalreference", "external_classification")
    op.drop_index(op.f("ix_customer_registration_source"), table_name="customer")
    op.drop_column("customer", "vat_rate")
    for name in (
        "registration_source",
        "correspondent_account",
        "bank_account",
        "bank_city",
        "bank_name",
        "bank_bik",
        "additional_phone",
        "phone",
        "company_type",
    ):
        op.drop_column("customer", name)
