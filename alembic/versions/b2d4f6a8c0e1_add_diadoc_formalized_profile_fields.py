"""add diadoc formalized profile fields

Revision ID: b2d4f6a8c0e1
Revises: ae1d9c3b4f2a
Create Date: 2026-05-12 16:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b2d4f6a8c0e1'
down_revision: Union[str, Sequence[str], None] = 'ae1d9c3b4f2a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'customer',
        sa.Column('inn', sa.String(length=32), nullable=True),
    )
    op.add_column(
        'customer',
        sa.Column('kpp', sa.String(length=32), nullable=True),
    )
    op.add_column(
        'customer',
        sa.Column('legal_address', sa.Text(), nullable=True),
    )
    op.add_column(
        'customer',
        sa.Column('postal_address', sa.Text(), nullable=True),
    )
    op.create_index(op.f('ix_customer_inn'), 'customer', ['inn'], unique=False)
    op.create_index(op.f('ix_customer_kpp'), 'customer', ['kpp'], unique=False)

    op.add_column(
        'diadocintegrationsettings',
        sa.Column('seller_legal_address', sa.String(length=500), nullable=True),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column('seller_postal_address', sa.String(length=500), nullable=True),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column('signer_full_name', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column('signer_position', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column('signer_basis', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column(
            'formalized_default_function',
            sa.String(length=64),
            nullable=False,
            server_default='ДОП',
        ),
    )
    op.alter_column(
        'diadocintegrationsettings',
        'formalized_default_function',
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column('diadocintegrationsettings', 'formalized_default_function')
    op.drop_column('diadocintegrationsettings', 'signer_basis')
    op.drop_column('diadocintegrationsettings', 'signer_position')
    op.drop_column('diadocintegrationsettings', 'signer_full_name')
    op.drop_column('diadocintegrationsettings', 'seller_postal_address')
    op.drop_column('diadocintegrationsettings', 'seller_legal_address')

    op.drop_index(op.f('ix_customer_kpp'), table_name='customer')
    op.drop_index(op.f('ix_customer_inn'), table_name='customer')
    op.drop_column('customer', 'postal_address')
    op.drop_column('customer', 'legal_address')
    op.drop_column('customer', 'kpp')
    op.drop_column('customer', 'inn')
