"""add direct smtp account settings

Revision ID: e4b2c71a9d03
Revises: f7c9a2e4b681
Create Date: 2026-11-19 12:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "e4b2c71a9d03"
down_revision = "f7c9a2e4b681"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "emailaccount",
        sa.Column("outbound_transport", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "emailaccount",
        sa.Column("smtp_username", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "emailaccount",
        sa.Column(
            "smtp_via_relay",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
    )
    op.alter_column("emailaccount", "smtp_via_relay", server_default=None)


def downgrade() -> None:
    op.drop_column("emailaccount", "smtp_via_relay")
    op.drop_column("emailaccount", "smtp_username")
    op.drop_column("emailaccount", "outbound_transport")
