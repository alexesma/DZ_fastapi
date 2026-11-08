"""Add cutoff date for automatic Diadoc document processing.

Revision ID: b9d2e4f6a8c1
Revises: a8c1d4e7f920
Create Date: 2026-11-08 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b9d2e4f6a8c1"
down_revision: Union[str, Sequence[str], None] = "a8c1d4e7f920"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "diadocintegrationsettings"


def upgrade() -> None:
    op.add_column(TABLE, sa.Column("inbound_process_from", sa.Date()))


def downgrade() -> None:
    op.drop_column(TABLE, "inbound_process_from")
