"""Add client.label_name for printed labels.

Revision ID: 5a7c3e9d1b24
Revises: 4f1b0c2d3e50
"""

import sqlalchemy as sa

from alembic import op

revision = "5a7c3e9d1b24"
down_revision = "4f1b0c2d3e50"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "client", sa.Column("label_name", sa.String(255), nullable=True)
    )


def downgrade():
    op.drop_column("client", "label_name")
