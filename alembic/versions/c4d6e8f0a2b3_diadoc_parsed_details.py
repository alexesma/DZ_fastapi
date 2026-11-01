"""Parsed details of incoming Diadoc documents.

Revision ID: c4d6e8f0a2b3
Revises: b3c5d7e9f1a2
"""

import sqlalchemy as sa

from alembic import op

revision = "c4d6e8f0a2b3"
down_revision = "b3c5d7e9f1a2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("diadocincomingdocument", sa.Column("parsed_details", sa.JSON()))


def downgrade():
    op.drop_column("diadocincomingdocument", "parsed_details")
