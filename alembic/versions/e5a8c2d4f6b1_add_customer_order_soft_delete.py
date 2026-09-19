"""Add customer order soft deletion.

Revision ID: e5a8c2d4f6b1
Revises: d4f7a2c9e1b5
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e5a8c2d4f6b1"
down_revision: Union[str, None] = "d4f7a2c9e1b5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorder",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "customerorder",
        sa.Column("deleted_by_user_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_customerorder_deleted_by_user_id_app_user",
        "customerorder",
        "app_user",
        ["deleted_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_customerorder_deleted_at",
        "customerorder",
        ["deleted_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_customerorder_deleted_at", table_name="customerorder")
    op.drop_constraint(
        "fk_customerorder_deleted_by_user_id_app_user",
        "customerorder",
        type_="foreignkey",
    )
    op.drop_column("customerorder", "deleted_by_user_id")
    op.drop_column("customerorder", "deleted_at")
