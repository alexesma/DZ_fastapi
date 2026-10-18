"""Track rejected quantities by decision source.

Revision ID: f9d1e3a5b427
Revises: f8c0d2e4a316
"""

from alembic import op
import sqlalchemy as sa


revision = "f9d1e3a5b427"
down_revision = "f8c0d2e4a316"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("supplierorderitem", sa.Column("refusal_sources", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("supplierorderitem", "refusal_sources")
