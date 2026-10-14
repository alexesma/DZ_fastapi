"""Allow an order config to use another customer's published pricelist.

Revision ID: b4d6e8f0a2c1
Revises: a3c5e7f9b1d4
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b4d6e8f0a2c1"
down_revision: Union[str, None] = "a3c5e7f9b1d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorderconfig",
        sa.Column("reference_pricelist_config_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_customerorderconfig_reference_pricelist_config",
        "customerorderconfig",
        "customerpricelistconfig",
        ["reference_pricelist_config_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_customerorderconfig_reference_pricelist_config_id",
        "customerorderconfig",
        ["reference_pricelist_config_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_customerorderconfig_reference_pricelist_config_id",
        table_name="customerorderconfig",
    )
    op.drop_constraint(
        "fk_customerorderconfig_reference_pricelist_config",
        "customerorderconfig",
        type_="foreignkey",
    )
    op.drop_column("customerorderconfig", "reference_pricelist_config_id")
