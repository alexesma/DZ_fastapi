"""Configure the number of business documents per UNF FTP message.

Revision ID: b8d1c2e3f4a5
Revises: a4c7e2b9d610
"""

import sqlalchemy as sa

from alembic import op

revision = "b8d1c2e3f4a5"
down_revision = "a4c7e2b9d610"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "oncenterpriseconfig",
        sa.Column("batch_size", sa.Integer(), nullable=False, server_default="1"),
    )


def downgrade():
    op.drop_column("oncenterpriseconfig", "batch_size")
