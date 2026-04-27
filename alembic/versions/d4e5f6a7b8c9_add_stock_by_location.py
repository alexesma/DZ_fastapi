"""add stock_by_location table

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-04-26 14:00:00.000000

"""

import sqlalchemy as sa

from alembic import op

revision = 'd4e5f6a7b8c9'
down_revision = 'c3d4e5f6a7b8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'stockbylocation',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('storage_location_id', sa.Integer(), nullable=False),
        sa.Column(
            'quantity',
            sa.Integer(),
            nullable=False,
            server_default='0',
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text('NOW()'),
        ),
        sa.ForeignKeyConstraint(
            ['autopart_id'], ['autopart.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['storage_location_id'], ['storagelocation.id'], ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'autopart_id', 'storage_location_id',
            name='uq_stockbylocation_autopart_location',
        ),
    )
    op.create_index(
        'ix_stockbylocation_autopart_id', 'stockbylocation', ['autopart_id']
    )
    op.create_index(
        'ix_stockbylocation_storage_location_id',
        'stockbylocation',
        ['storage_location_id'],
    )

    # ── Seed from existing M2M associations (quantity = 0) ──────────────────
    # Preserves existing autopart ↔ location links; real quantities filled
    # in via the first inventory session.
    op.execute("""
        INSERT INTO stockbylocation (
            autopart_id,
            storage_location_id,
            quantity,
            updated_at
        )
        SELECT autopart_id, storage_location_id, 0, NOW()
        FROM autopart_storage_association
        ON CONFLICT (autopart_id, storage_location_id) DO NOTHING
    """)


def downgrade() -> None:
    op.drop_index('ix_stockbylocation_storage_location_id', 'stockbylocation')
    op.drop_index('ix_stockbylocation_autopart_id', 'stockbylocation')
    op.drop_table('stockbylocation')
