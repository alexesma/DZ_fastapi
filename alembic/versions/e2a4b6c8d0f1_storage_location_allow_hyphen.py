"""allow hyphen in storage location names

Revision ID: e2a4b6c8d0f1
Revises: d1f3a5b7c9e0
Create Date: 2026-11-22 18:00:00.000000
"""

from alembic import op

revision = "e2a4b6c8d0f1"
down_revision = "d1f3a5b7c9e0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("latin_characters_only", "storagelocation", type_="check")
    op.create_check_constraint(
        "latin_characters_only", "storagelocation", "name ~ '^[A-Z0-9 /-]+$'"
    )


def downgrade() -> None:
    op.drop_constraint("latin_characters_only", "storagelocation", type_="check")
    op.create_check_constraint(
        "latin_characters_only", "storagelocation", "name ~ '^[A-Z0-9 /]+$'"
    )
