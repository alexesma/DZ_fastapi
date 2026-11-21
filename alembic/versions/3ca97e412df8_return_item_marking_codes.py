"""Record the units physically scanned for a return."""
import sqlalchemy as sa

from alembic import op

revision = "3ca97e412df8"
down_revision = "eb271fc438a2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("returnitem", sa.Column("marking_codes", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("returnitem", "marking_codes")
