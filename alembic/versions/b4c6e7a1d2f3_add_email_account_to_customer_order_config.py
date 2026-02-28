"""add email account to customer order config

Revision ID: b4c6e7a1d2f3
Revises: 9d2c7b1e4f6a
Create Date: 2026-02-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b4c6e7a1d2f3'
down_revision = '9d2c7b1e4f6a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'customerorderconfig',
        sa.Column('email_account_id', sa.Integer(), nullable=True),
    )
    op.create_index(
        'ix_customerorderconfig_email_account_id',
        'customerorderconfig',
        ['email_account_id'],
        unique=False,
    )
    op.create_foreign_key(
        'fk_customerorderconfig_email_account_id',
        'customerorderconfig',
        'emailaccount',
        ['email_account_id'],
        ['id'],
    )


def downgrade():
    op.drop_constraint(
        'fk_customerorderconfig_email_account_id',
        'customerorderconfig',
        type_='foreignkey',
    )
    op.drop_index(
        'ix_customerorderconfig_email_account_id',
        table_name='customerorderconfig',
    )
    op.drop_column('customerorderconfig', 'email_account_id')
