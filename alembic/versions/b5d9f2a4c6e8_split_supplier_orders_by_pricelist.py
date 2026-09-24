"""Split supplier orders by provider price-list configuration.

Revision ID: b5d9f2a4c6e8
Revises: a4c8e7f1b2d3
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b5d9f2a4c6e8"
down_revision: Union[str, None] = "a4c8e7f1b2d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "provider",
        sa.Column(
            "split_orders_by_pricelist",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "customerorderitem",
        sa.Column("provider_config_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_customerorderitem_provider_config_id",
        "customerorderitem",
        ["provider_config_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_customerorderitem_provider_config_id",
        "customerorderitem",
        "providerpricelistconfig",
        ["provider_config_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column(
        "supplierorder",
        sa.Column("provider_config_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_supplierorder_provider_config_id",
        "supplierorder",
        ["provider_config_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_supplierorder_provider_config_id",
        "supplierorder",
        "providerpricelistconfig",
        ["provider_config_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.alter_column(
        "provider",
        "split_orders_by_pricelist",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_supplierorder_provider_config_id",
        "supplierorder",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_supplierorder_provider_config_id",
        table_name="supplierorder",
    )
    op.drop_column("supplierorder", "provider_config_id")
    op.drop_constraint(
        "fk_customerorderitem_provider_config_id",
        "customerorderitem",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_customerorderitem_provider_config_id",
        table_name="customerorderitem",
    )
    op.drop_column("customerorderitem", "provider_config_id")
    op.drop_column("provider", "split_orders_by_pricelist")
