"""Store the default UNF contract separately for each supplier.

Revision ID: 9a2f4d6e8b10
Revises: 8d5f2c7a1b40
"""

import sqlalchemy as sa

from alembic import op

revision = "9a2f4d6e8b10"
down_revision = "8d5f2c7a1b40"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("provider", sa.Column("one_c_contract_name", sa.String(255)))
    op.add_column("provider", sa.Column("one_c_contract_number", sa.String(100)))
    op.add_column("provider", sa.Column("one_c_contract_date", sa.Date()))
    op.add_column("provider", sa.Column("one_c_contract_guid", sa.String(36)))


def downgrade():
    op.drop_column("provider", "one_c_contract_guid")
    op.drop_column("provider", "one_c_contract_date")
    op.drop_column("provider", "one_c_contract_number")
    op.drop_column("provider", "one_c_contract_name")
