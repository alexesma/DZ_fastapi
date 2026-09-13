"""Add provider legal and bank details from Parts-Soft.

Revision ID: e7a4c9d2b1f6
Revises: d6f9a2c4e7b1
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e7a4c9d2b1f6"
down_revision: Union[str, None] = "d6f9a2c4e7b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    provider_columns = (
        sa.Column("legal_name", sa.String(length=512), nullable=True),
        sa.Column("legal_address", sa.Text(), nullable=True),
        sa.Column("postal_address", sa.Text(), nullable=True),
        sa.Column("company_type", sa.String(length=128), nullable=True),
        sa.Column("phone", sa.String(length=64), nullable=True),
        sa.Column("additional_phone", sa.String(length=64), nullable=True),
        sa.Column("vat_rate", sa.DECIMAL(precision=7, scale=3), nullable=True),
        sa.Column("bank_bik", sa.String(length=32), nullable=True),
        sa.Column("bank_name", sa.String(length=255), nullable=True),
        sa.Column("bank_city", sa.String(length=255), nullable=True),
        sa.Column("bank_account", sa.String(length=64), nullable=True),
        sa.Column("correspondent_account", sa.String(length=64), nullable=True),
        sa.Column("credit_limit", sa.DECIMAL(precision=12, scale=2), nullable=True),
    )
    for column in provider_columns:
        op.add_column("provider", column)

    op.add_column(
        "providerexternalreference",
        sa.Column(
            "external_payload",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )
    op.add_column(
        "providerexternalreference",
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("providerexternalreference", "last_synced_at")
    op.drop_column("providerexternalreference", "external_payload")
    for column_name in (
        "credit_limit",
        "correspondent_account",
        "bank_account",
        "bank_city",
        "bank_name",
        "bank_bik",
        "vat_rate",
        "additional_phone",
        "phone",
        "company_type",
        "postal_address",
        "legal_address",
        "legal_name",
    ):
        op.drop_column("provider", column_name)
