"""Add persisted supplier order delivery diagnostics.

Revision ID: d6a8c2e4f091
Revises: c5e7f9a1b3d2
"""

from alembic import op
import sqlalchemy as sa

revision = "d6a8c2e4f091"
down_revision = "c5e7f9a1b3d2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("supplierorder", sa.Column("last_send_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("supplierorder", sa.Column("last_send_error", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("supplierorder", "last_send_error")
    op.drop_column("supplierorder", "last_send_attempt_at")
