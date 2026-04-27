"""add inventory and stock movement tables

Revision ID: e5f6a7b8c9d0
Revises: b2c3d4e5f6a7
Create Date: 2026-04-26 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'e5f6a7b8c9d0'
down_revision = 'b2c3d4e5f6a7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Enum types ────────────────────────────────────────────────────────────
    op.execute(
        "CREATE TYPE inventorystatus AS ENUM "
        "('active', 'completed', 'cancelled')"
    )
    op.execute(
        "CREATE TYPE inventoryscopetype AS ENUM "
        "('full', 'shelf', 'location')"
    )
    op.execute(
        "CREATE TYPE movementtype AS ENUM "
        "('receipt', 'shipment', 'transfer_in', 'transfer_out', "
        "'inventory', 'manual')"
    )

    # ── InventorySession ──────────────────────────────────────────────────────
    op.create_table(
        'inventorysession',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column(
            'started_at',
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text('NOW()'),
        ),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'status',
            sa.Enum(
                'active', 'completed', 'cancelled',
                name='inventorystatus', create_type=False,
            ),
            nullable=False,
            server_default='active',
        ),
        sa.Column(
            'scope_type',
            sa.Enum(
                'full', 'shelf', 'location',
                name='inventoryscopetype', create_type=False,
            ),
            nullable=False,
            server_default='full',
        ),
        sa.Column('scope_value', sa.String(100), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )

    # ── InventoryItem ─────────────────────────────────────────────────────────
    op.create_table(
        'inventoryitem',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('session_id', sa.Integer(), nullable=False),
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('storage_location_id', sa.Integer(), nullable=False),
        sa.Column('expected_qty', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('actual_qty', sa.Integer(), nullable=True),
        sa.Column('discrepancy', sa.Integer(), nullable=True),
        sa.Column('counted_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ['session_id'], ['inventorysession.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['autopart_id'], ['autopart.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['storage_location_id'], ['storagelocation.id'], ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_inventoryitem_session_id', 'inventoryitem', ['session_id'])
    op.create_index('ix_inventoryitem_autopart_id', 'inventoryitem', ['autopart_id'])
    op.create_index(
        'ix_inventoryitem_storage_location_id',
        'inventoryitem',
        ['storage_location_id'],
    )

    # ── StockMovement ─────────────────────────────────────────────────────────
    op.create_table(
        'stockmovement',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('storage_location_id', sa.Integer(), nullable=True),
        sa.Column(
            'movement_type',
            sa.Enum(
                'receipt', 'shipment', 'transfer_in', 'transfer_out',
                'inventory', 'manual',
                name='movementtype', create_type=False,
            ),
            nullable=False,
        ),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('qty_before', sa.Integer(), nullable=True),
        sa.Column('qty_after', sa.Integer(), nullable=True),
        sa.Column('reference_id', sa.Integer(), nullable=True),
        sa.Column('reference_type', sa.String(50), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text('NOW()'),
        ),
        sa.ForeignKeyConstraint(
            ['autopart_id'], ['autopart.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['storage_location_id'],
            ['storagelocation.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_stockmovement_autopart_id', 'stockmovement', ['autopart_id']
    )
    op.create_index(
        'ix_stockmovement_storage_location_id',
        'stockmovement',
        ['storage_location_id'],
    )
    op.create_index(
        'ix_stockmovement_created_at', 'stockmovement', ['created_at']
    )


def downgrade() -> None:
    op.drop_index('ix_stockmovement_created_at', 'stockmovement')
    op.drop_index('ix_stockmovement_storage_location_id', 'stockmovement')
    op.drop_index('ix_stockmovement_autopart_id', 'stockmovement')
    op.drop_table('stockmovement')

    op.drop_index('ix_inventoryitem_storage_location_id', 'inventoryitem')
    op.drop_index('ix_inventoryitem_autopart_id', 'inventoryitem')
    op.drop_index('ix_inventoryitem_session_id', 'inventoryitem')
    op.drop_table('inventoryitem')
    op.drop_table('inventorysession')

    op.execute('DROP TYPE IF EXISTS movementtype')
    op.execute('DROP TYPE IF EXISTS inventoryscopetype')
    op.execute('DROP TYPE IF EXISTS inventorystatus')
