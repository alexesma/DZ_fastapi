"""Add sync_status and synced_at to stockmovement

Revision ID: 0aa1bb2cc3dd
Revises: 3cc775719a2a
Create Date: 2026-05-04 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '0aa1bb2cc3dd'
down_revision: Union[str, None] = '3cc775719a2a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # syncstatus enum already exists (created by stocklot migration) —
    # we reference it without CREATE TYPE.
    op.add_column(
        'stockmovement',
        sa.Column(
            'sync_status',
            sa.Enum(
                'pending', 'synced', 'error',
                name='syncstatus',
                create_type=False,
            ),
            nullable=False,
            server_default='pending',
            comment='Статус синхронизации с 1С',
        ),
    )
    op.add_column(
        'stockmovement',
        sa.Column(
            'synced_at',
            sa.DateTime(timezone=True),
            nullable=True,
            comment='Дата и время успешной синхронизации с 1С',
        ),
    )
    op.create_index(
        'ix_stockmovement_sync_status',
        'stockmovement',
        ['sync_status'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_stockmovement_sync_status', table_name='stockmovement')
    op.drop_column('stockmovement', 'synced_at')
    op.drop_column('stockmovement', 'sync_status')
