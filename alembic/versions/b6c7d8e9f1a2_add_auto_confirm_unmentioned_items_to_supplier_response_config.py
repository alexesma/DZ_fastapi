"""add auto confirm unmentioned items to supplier response config

Revision ID: b6c7d8e9f1a2
Revises: a5b6c7d8e9f0
Create Date: 2026-04-11 12:10:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b6c7d8e9f1a2"
down_revision: Union[str, Sequence[str], None] = "a5b6c7d8e9f0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "supplierresponseconfig",
        sa.Column(
            "auto_confirm_unmentioned_items",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.alter_column(
        "supplierresponseconfig",
        "auto_confirm_unmentioned_items",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("supplierresponseconfig", "auto_confirm_unmentioned_items")
