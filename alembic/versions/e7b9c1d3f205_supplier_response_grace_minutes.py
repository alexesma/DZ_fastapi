"""Add a configurable grace period before silent supplier acceptance.

Revision ID: e7b9c1d3f205
Revises: d6a8c2e4f091
"""

from alembic import op
import sqlalchemy as sa

revision = "e7b9c1d3f205"
down_revision = "d6a8c2e4f091"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "supplierresponseconfig",
        sa.Column("auto_confirm_grace_minutes", sa.Integer(), nullable=True),
    )


def downgrade():
    op.drop_column("supplierresponseconfig", "auto_confirm_grace_minutes")
