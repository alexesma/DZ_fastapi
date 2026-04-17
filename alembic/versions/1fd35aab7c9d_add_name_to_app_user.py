"""Add name to app_user

Revision ID: 1fd35aab7c9d
Revises: 6af711eeab42
Create Date: 2026-04-17 12:30:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1fd35aab7c9d"
down_revision: Union[str, None] = "6af711eeab42"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("app_user", sa.Column("name", sa.String(length=255)))


def downgrade() -> None:
    op.drop_column("app_user", "name")
