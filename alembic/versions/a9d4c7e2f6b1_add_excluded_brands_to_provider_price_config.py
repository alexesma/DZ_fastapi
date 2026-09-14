"""Add excluded brands to provider price list config.

Revision ID: a9d4c7e2f6b1
Revises: e7a4c9d2b1f6
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a9d4c7e2f6b1"
down_revision: Union[str, None] = "e7a4c9d2b1f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column(
            "excluded_brands",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )


def downgrade() -> None:
    op.drop_column("providerpricelistconfig", "excluded_brands")
