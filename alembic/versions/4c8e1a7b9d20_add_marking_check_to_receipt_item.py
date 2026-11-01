"""Marking code check result on supplier receipt items.

Revision ID: 4c8e1a7b9d20
Revises: 9b2d4f6a8c13
"""

import sqlalchemy as sa

from alembic import op

revision = "4c8e1a7b9d20"
down_revision = "9b2d4f6a8c13"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("supplierreceiptitem", sa.Column("marking_check", sa.JSON()))


def downgrade():
    op.drop_column("supplierreceiptitem", "marking_check")
