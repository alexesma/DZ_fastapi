"""add supplier response stale cleanup settings

Revision ID: f4e5d6c7b8a9
Revises: e9f0a1b2c3e5
Create Date: 2026-04-24 13:45:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f4e5d6c7b8a9"
down_revision: Union[str, Sequence[str], None] = "e9f0a1b2c3e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_response_auto_close_stale_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_response_stale_days",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("7"),
        ),
    )


def downgrade() -> None:
    op.drop_column(
        "customerorderinboxsettings",
        "supplier_response_stale_days",
    )
    op.drop_column(
        "customerorderinboxsettings",
        "supplier_response_auto_close_stale_enabled",
    )
