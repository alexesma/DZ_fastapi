"""add provider inventory policies and item rules

Revision ID: 7c2e5a1b9d40
Revises: 3f8a6c2d1b40
Create Date: 2026-08-09 17:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "7c2e5a1b9d40"
down_revision: Union[str, Sequence[str], None] = "3f8a6c2d1b40"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    provider_policy = postgresql.ENUM(
        "original_goods",
        "dragonzap_material",
        "mixed",
        name="providerinventorypolicy",
        create_type=False,
    )
    provider_policy.create(bind, checkfirst=True)
    stock_lot_role = postgresql.ENUM(
        "original_good",
        "dragonzap_material",
        "dragonzap_finished",
        name="stocklotrole",
        create_type=False,
    )

    op.add_column(
        "provider",
        sa.Column(
            "inventory_policy",
            provider_policy,
            nullable=False,
            server_default="original_goods",
        ),
    )
    op.add_column(
        "provider",
        sa.Column("inventory_policy_note", sa.Text(), nullable=True),
    )

    op.create_table(
        "providerinventoryrolerule",
        sa.Column("provider_id", sa.Integer(), nullable=False),
        sa.Column("autopart_id", sa.Integer(), nullable=False),
        sa.Column("inventory_role", stock_lot_role, nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
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
            ["provider_id"],
            ["provider.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["autopart_id"],
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
        sa.UniqueConstraint(
            "provider_id",
            "autopart_id",
            name="uq_provider_inventory_role_rule_part",
        ),
    )
    op.create_index(
        "ix_providerinventoryrolerule_provider_id",
        "providerinventoryrolerule",
        ["provider_id"],
    )
    op.create_index(
        "ix_providerinventoryrolerule_autopart_id",
        "providerinventoryrolerule",
        ["autopart_id"],
    )


def downgrade() -> None:
    op.drop_table("providerinventoryrolerule")
    op.drop_column("provider", "inventory_policy_note")
    op.drop_column("provider", "inventory_policy")

    bind = op.get_bind()
    postgresql.ENUM(name="providerinventorypolicy").drop(
        bind,
        checkfirst=True,
    )
