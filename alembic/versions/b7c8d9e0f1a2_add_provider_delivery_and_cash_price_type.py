"""add provider delivery method and cash price type

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2026-03-12 19:20:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'b7c8d9e0f1a2'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name == 'postgresql':
        op.execute("ALTER TYPE type_prices ADD VALUE IF NOT EXISTS 'CASH'")

    delivery_enum = sa.Enum(
        'DELIVERED',
        'SELF_PICKUP',
        'COURIER_FOOT',
        'COURIER_CAR',
        name='provider_delivery_method',
    )
    delivery_enum.create(bind, checkfirst=True)
    op.add_column(
        'provider',
        sa.Column(
            'default_delivery_method',
            delivery_enum,
            nullable=True,
            server_default='DELIVERED',
        ),
    )


def downgrade():
    bind = op.get_bind()
    op.drop_column('provider', 'default_delivery_method')
    if bind.dialect.name == 'postgresql':
        op.execute("DROP TYPE IF EXISTS provider_delivery_method")
