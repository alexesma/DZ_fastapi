"""Add imap_folder to email account

Revision ID: 3c2a9d1f5b7e
Revises: 2f6d9b1c0a8e
Create Date: 2026-02-27 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3c2a9d1f5b7e'
down_revision: Union[str, None] = '2f6d9b1c0a8e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'emailaccount', sa.Column('imap_folder', sa.String(length=255), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('emailaccount', 'imap_folder')
