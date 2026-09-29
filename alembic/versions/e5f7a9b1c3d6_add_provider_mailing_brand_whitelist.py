"""Add provider mailing brand whitelist.

Revision ID: e5f7a9b1c3d6
Revises: d4e6f8a1b3c5
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e5f7a9b1c3d6"
down_revision: Union[str, None] = "d4e6f8a1b3c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column(
            "mailing_included_brands",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )


def downgrade() -> None:
    op.drop_column("providerpricelistconfig", "mailing_included_brands")
