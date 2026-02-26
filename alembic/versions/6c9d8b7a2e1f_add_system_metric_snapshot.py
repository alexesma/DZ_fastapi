"""Add system metric snapshot

Revision ID: 6c9d8b7a2e1f
Revises: 4b6c2a1f9e2d
Create Date: 2026-02-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6c9d8b7a2e1f'
down_revision: Union[str, None] = '4b6c2a1f9e2d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'systemmetricsnapshot',
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('db_size_bytes', sa.BigInteger(), nullable=True),
        sa.Column('disk_total_bytes', sa.BigInteger(), nullable=True),
        sa.Column('disk_free_bytes', sa.BigInteger(), nullable=True),
        sa.Column('mem_total_bytes', sa.BigInteger(), nullable=True),
        sa.Column('mem_available_bytes', sa.BigInteger(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    op.drop_table('systemmetricsnapshot')
