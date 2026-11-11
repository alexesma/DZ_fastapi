"""Per-provider supplier order stub (test mode for order sending).

Revision ID: e2f3a4b5c6d7
Revises: f0b1c2d3e4a5
Create Date: 2026-11-11 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "e2f3a4b5c6d7"
down_revision: Union[str, Sequence[str], None] = "f0b1c2d3e4a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "provider",
        sa.Column(
            "order_stub_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "provider",
        sa.Column("order_stub_email", sa.String(length=255), nullable=True),
    )
    # Переносим текущую глобальную настройку на каждого поставщика.
    op.execute(
        """
        UPDATE provider
        SET order_stub_enabled = COALESCE(
            (SELECT supplier_order_stub_enabled
             FROM customerorderinboxsettings
             ORDER BY id LIMIT 1),
            TRUE
        )
        """
    )


def downgrade() -> None:
    op.drop_column("provider", "order_stub_email")
    op.drop_column("provider", "order_stub_enabled")
