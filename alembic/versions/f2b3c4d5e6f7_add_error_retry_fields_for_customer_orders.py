"""add error retry fields for customer orders

Revision ID: a1b2c3d4e5f6
Revises: e7f8a9b0c1d2
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f2b3c4d5e6f7'
down_revision = 'e7f8a9b0c1d2'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'customerorderinboxsettings',
        sa.Column(
            'error_file_retention_days',
            sa.Integer(),
            server_default='5',
        ),
    )
    op.add_column(
        'customerorder',
        sa.Column('order_config_id', sa.Integer(), nullable=True),
    )
    op.add_column(
        'customerorder',
        sa.Column('error_details', sa.String(length=500), nullable=True),
    )
    op.create_index(
        'ix_customerorder_order_config_id',
        'customerorder',
        ['order_config_id'],
        unique=False,
    )
    op.create_foreign_key(
        'fk_customerorder_order_config_id',
        'customerorder',
        'customerorderconfig',
        ['order_config_id'],
        ['id'],
    )


def downgrade():
    op.drop_constraint(
        'fk_customerorder_order_config_id',
        'customerorder',
        type_='foreignkey',
    )
    op.drop_index('ix_customerorder_order_config_id', table_name='customerorder')
    op.drop_column('customerorder', 'error_details')
    op.drop_column('customerorder', 'order_config_id')
    op.drop_column('customerorderinboxsettings', 'error_file_retention_days')
