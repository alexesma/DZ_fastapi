"""Add external relay heartbeat.

Revision ID: d4f7a2c9e1b5
Revises: c3e8a1d5f7b2
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d4f7a2c9e1b5"
down_revision: Union[str, None] = "c3e8a1d5f7b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "relayheartbeat",
        sa.Column("worker_id", sa.String(length=128), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_relayheartbeat_worker_id",
        "relayheartbeat",
        ["worker_id"],
        unique=True,
    )
    op.create_index(
        "ix_relayheartbeat_last_seen_at",
        "relayheartbeat",
        ["last_seen_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_relayheartbeat_last_seen_at", table_name="relayheartbeat")
    op.drop_index("ix_relayheartbeat_worker_id", table_name="relayheartbeat")
    op.drop_table("relayheartbeat")
