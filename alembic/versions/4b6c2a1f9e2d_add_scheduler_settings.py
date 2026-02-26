"""Add scheduler settings

Revision ID: 4b6c2a1f9e2d
Revises: fd57968e0964
Create Date: 2026-02-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4b6c2a1f9e2d'
down_revision: Union[str, None] = 'fd57968e0964'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'schedulersetting',
        sa.Column('key', sa.String(length=64), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False),
        sa.Column('days', sa.JSON(), nullable=True),
        sa.Column('times', sa.JSON(), nullable=True),
        sa.Column('last_run_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('key'),
    )
    op.create_index(
        op.f('ix_schedulersetting_key'),
        'schedulersetting',
        ['key'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_schedulersetting_key'), table_name='schedulersetting')
    op.drop_table('schedulersetting')
