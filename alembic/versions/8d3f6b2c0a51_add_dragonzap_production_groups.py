"""add DragonZap production groups

Revision ID: 8d3f6b2c0a51
Revises: 7c2e5a1b9d40
Create Date: 2026-08-09 19:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "8d3f6b2c0a51"
down_revision: Union[str, Sequence[str], None] = "7c2e5a1b9d40"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "dragonzapproductiongroup",
        sa.Column("finished_autopart_id", sa.Integer(), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column(
            "packaging_cost",
            sa.DECIMAL(precision=12, scale=4),
            nullable=False,
            server_default="0",
        ),
        sa.Column("packaging_description", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["finished_autopart_id"],
            ["autopart.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_dragonzapproductiongroup_finished_autopart_id",
        "dragonzapproductiongroup",
        ["finished_autopart_id"],
        unique=True,
    )
    op.create_index(
        "ix_dragonzapproductiongroup_is_active",
        "dragonzapproductiongroup",
        ["is_active"],
    )

    op.create_table(
        "dragonzapproductionmaterialoverride",
        sa.Column("production_group_id", sa.Integer(), nullable=False),
        sa.Column("material_autopart_id", sa.Integer(), nullable=False),
        sa.Column(
            "priority",
            sa.Integer(),
            nullable=False,
            server_default="100",
        ),
        sa.Column(
            "is_allowed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint("priority >= 1", name="ck_dragonzap_material_priority"),
        sa.ForeignKeyConstraint(
            ["production_group_id"],
            ["dragonzapproductiongroup.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["material_autopart_id"],
            ["autopart.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "production_group_id",
            "material_autopart_id",
            name="uq_dragonzap_production_material_group_part",
        ),
    )
    op.create_index(
        "ix_dragonzapproductionmaterialoverride_production_group_id",
        "dragonzapproductionmaterialoverride",
        ["production_group_id"],
    )
    op.create_index(
        "ix_dragonzapproductionmaterialoverride_material_autopart_id",
        "dragonzapproductionmaterialoverride",
        ["material_autopart_id"],
    )
    op.create_index(
        "idx_dragonzap_production_material_allowed",
        "dragonzapproductionmaterialoverride",
        ["production_group_id", "is_allowed", "priority"],
    )


def downgrade() -> None:
    op.drop_table("dragonzapproductionmaterialoverride")
    op.drop_table("dragonzapproductiongroup")
