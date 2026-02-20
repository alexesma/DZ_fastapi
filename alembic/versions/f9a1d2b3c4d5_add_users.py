"""add users

Revision ID: f9a1d2b3c4d5
Revises: 3f8b2c1d9a7e
Create Date: 2026-02-20 10:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f9a1d2b3c4d5"
down_revision = "3f8b2c1d9a7e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "app_user",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column(
            "role",
            sa.Enum("admin", "manager", name="userrole"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum("pending", "active", "disabled", name="userstatus"),
            nullable=False,
        ),
        sa.Column("approved_by", sa.Integer(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["approved_by"], ["app_user.id"], ondelete="SET NULL"
        ),
    )
    op.create_index("ix_app_user_email", "app_user", ["email"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_app_user_email", table_name="app_user")
    op.drop_table("app_user")
    op.execute("DROP TYPE IF EXISTS userstatus")
    op.execute("DROP TYPE IF EXISTS userrole")
