"""Preserve supplier codes when a marked receipt is posted partially."""
import sqlalchemy as sa
from alembic import op

revision = "eb271fc438a2"
down_revision = "c3d5e7f9a1b2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("supplierreceiptitem", sa.Column("document_marking_codes", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("supplierreceiptitem", "document_marking_codes")
