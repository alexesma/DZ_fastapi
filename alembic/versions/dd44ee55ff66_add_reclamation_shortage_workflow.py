"""add reclamation shortage workflow

Revision ID: dd44ee55ff66
Revises: cc33dd44ee55
Create Date: 2026-07-26 15:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "dd44ee55ff66"
down_revision: Union[str, Sequence[str], None] = "cc33dd44ee55"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TYPE reclamationtype "
        "ADD VALUE IF NOT EXISTS 'shortage'"
    )
    op.execute(
        "ALTER TYPE reclamationattachmentkind "
        "ADD VALUE IF NOT EXISTS 'shortage_evidence'"
    )
    op.add_column(
        "reclamation",
        sa.Column(
            "shortage_assigned_to_user_id",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.add_column(
        "reclamation",
        sa.Column(
            "shortage_assigned_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "reclamation",
        sa.Column("shortage_status", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "reclamation",
        sa.Column(
            "shortage_confirmed_by_user_id",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.add_column(
        "reclamation",
        sa.Column(
            "shortage_confirmed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "reclamation",
        sa.Column("shortage_comment", sa.Text(), nullable=True),
    )
    op.add_column(
        "reclamation",
        sa.Column(
            "shortage_snoozed_until",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "app_notification",
        sa.Column("payload", sa.JSON(), nullable=True),
    )
    op.add_column(
        "app_notification",
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_reclamation_shortage_assigned_user",
        "reclamation",
        "app_user",
        ["shortage_assigned_to_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_reclamation_shortage_confirmed_user",
        "reclamation",
        "app_user",
        ["shortage_confirmed_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_reclamation_shortage_assigned_to_user_id",
        "reclamation",
        ["shortage_assigned_to_user_id"],
    )
    op.create_index(
        "ix_reclamation_shortage_status",
        "reclamation",
        ["shortage_status"],
    )
    op.create_index(
        "ix_app_notification_available_at",
        "app_notification",
        ["available_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_app_notification_available_at",
        table_name="app_notification",
    )
    op.drop_column("app_notification", "available_at")
    op.drop_column("app_notification", "payload")
    op.drop_index(
        "ix_reclamation_shortage_status",
        table_name="reclamation",
    )
    op.drop_index(
        "ix_reclamation_shortage_assigned_to_user_id",
        table_name="reclamation",
    )
    op.drop_constraint(
        "fk_reclamation_shortage_confirmed_user",
        "reclamation",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_reclamation_shortage_assigned_user",
        "reclamation",
        type_="foreignkey",
    )
    op.drop_column("reclamation", "shortage_comment")
    op.drop_column("reclamation", "shortage_snoozed_until")
    op.drop_column("reclamation", "shortage_confirmed_at")
    op.drop_column("reclamation", "shortage_confirmed_by_user_id")
    op.drop_column("reclamation", "shortage_status")
    op.drop_column("reclamation", "shortage_assigned_at")
    op.drop_column("reclamation", "shortage_assigned_to_user_id")
    # PostgreSQL enum values are intentionally retained on downgrade.
