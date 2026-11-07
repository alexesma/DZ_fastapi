"""Separate inbound and outbound Diadoc sync health.

Revision ID: a8c1d4e7f920
Revises: e4f6a8b0c2d3
Create Date: 2026-11-07 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a8c1d4e7f920"
down_revision: Union[str, Sequence[str], None] = "e4f6a8b0c2d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "diadocintegrationsettings"


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column("inbound_last_sync_at", sa.DateTime(timezone=True)),
    )
    op.add_column(
        TABLE,
        sa.Column("inbound_last_error", sa.String(length=2000)),
    )
    op.add_column(
        TABLE,
        sa.Column("inbound_continuation_index_key", sa.String(length=512)),
    )
    op.add_column(
        TABLE,
        sa.Column("outbound_last_sync_at", sa.DateTime(timezone=True)),
    )
    op.add_column(
        TABLE,
        sa.Column("outbound_last_error", sa.String(length=2000)),
    )


def downgrade() -> None:
    op.drop_column(TABLE, "outbound_last_error")
    op.drop_column(TABLE, "outbound_last_sync_at")
    op.drop_column(TABLE, "inbound_last_error")
    op.drop_column(TABLE, "inbound_continuation_index_key")
    op.drop_column(TABLE, "inbound_last_sync_at")
