"""Add outgoing email account to customerpricelistconfig

Revision ID: 6a1d3b7f9e22
Revises: c3f7a9b2d1e4
Create Date: 2026-03-16 22:20:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '6a1d3b7f9e22'
down_revision = 'c3f7a9b2d1e4'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'customerpricelistconfig',
        sa.Column('outgoing_email_account_id', sa.Integer(), nullable=True),
    )
    op.create_index(
        'ix_customerpricelistconfig_outgoing_email_account_id',
        'customerpricelistconfig',
        ['outgoing_email_account_id'],
        unique=False,
    )
    op.create_foreign_key(
        'fk_customerpricelistconfig_outgoing_email_account_id',
        'customerpricelistconfig',
        'emailaccount',
        ['outgoing_email_account_id'],
        ['id'],
    )


def downgrade():
    op.drop_constraint(
        'fk_customerpricelistconfig_outgoing_email_account_id',
        'customerpricelistconfig',
        type_='foreignkey',
    )
    op.drop_index(
        'ix_customerpricelistconfig_outgoing_email_account_id',
        table_name='customerpricelistconfig',
    )
    op.drop_column('customerpricelistconfig', 'outgoing_email_account_id')
