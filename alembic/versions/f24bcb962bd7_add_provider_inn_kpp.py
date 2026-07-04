"""add inn/kpp to provider (for Diadoc counteragent matching)

Revision ID: f24bcb962bd7
Revises: d0e1f2a3b4c5
Create Date: 2026-07-03 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f24bcb962bd7"
down_revision: Union[str, Sequence[str], None] = "d0e1f2a3b4c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "provider",
        sa.Column("inn", sa.String(32), nullable=True),
    )
    op.add_column(
        "provider",
        sa.Column("kpp", sa.String(32), nullable=True),
    )
    op.create_index("ix_provider_inn", "provider", ["inn"])
    op.create_index("ix_provider_kpp", "provider", ["kpp"])


def downgrade() -> None:
    op.drop_index("ix_provider_kpp", table_name="provider")
    op.drop_index("ix_provider_inn", table_name="provider")
    op.drop_column("provider", "kpp")
    op.drop_column("provider", "inn")
