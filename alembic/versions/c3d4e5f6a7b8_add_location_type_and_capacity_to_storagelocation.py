"""add location_type and capacity to storagelocation

Revision ID: c3d4e5f6a7b8
Revises: e5f6a7b8c9d0
Create Date: 2026-04-26 12:00:00.000000

"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = 'c3d4e5f6a7b8'
down_revision = 'e5f6a7b8c9d0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE locationtype AS ENUM (
                'shelf', 'pallet', 'bin', 'floor', 'other'
            );
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.add_column(
        'storagelocation',
        sa.Column(
            'location_type',
            postgresql.ENUM(
                'shelf', 'pallet', 'bin', 'floor', 'other',
                name='locationtype', create_type=False,
            ),
            nullable=True,
        ),
    )
    op.add_column(
        'storagelocation',
        sa.Column('capacity', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('storagelocation', 'capacity')
    op.drop_column('storagelocation', 'location_type')
    op.execute('DROP TYPE IF EXISTS locationtype')
