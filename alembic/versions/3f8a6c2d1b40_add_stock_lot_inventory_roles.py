"""add stock lot inventory roles and audit

Revision ID: 3f8a6c2d1b40
Revises: 9d7a2e4c1b60
Create Date: 2026-08-09 15:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "3f8a6c2d1b40"
down_revision: Union[str, Sequence[str], None] = "9d7a2e4c1b60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    lot_role = postgresql.ENUM(
        "original_good",
        "dragonzap_material",
        "dragonzap_finished",
        name="stocklotrole",
        create_type=False,
    )
    role_source = postgresql.ENUM(
        "system_default",
        "legacy_migration",
        "manual",
        "provider_policy",
        "item_rule",
        "production",
        "customer_return",
        name="stocklotrolesource",
        create_type=False,
    )
    lot_role.create(bind, checkfirst=True)
    role_source.create(bind, checkfirst=True)

    op.add_column("stocklot", sa.Column("inventory_role", lot_role, nullable=True))
    op.add_column("stocklot", sa.Column("role_source", role_source, nullable=True))
    op.add_column(
        "stocklot",
        sa.Column("role_rule_reference", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "stocklot",
        sa.Column("role_changed_by_user_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "stocklot",
        sa.Column("role_changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "stocklot",
        sa.Column("role_change_reason", sa.Text(), nullable=True),
    )
    op.create_foreign_key(
        "fk_stocklot_role_changed_by_user",
        "stocklot",
        "app_user",
        ["role_changed_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_stocklot_role_changed_by_user_id",
        "stocklot",
        ["role_changed_by_user_id"],
    )
    op.create_index(
        "ix_stocklot_inventory_role",
        "stocklot",
        ["inventory_role"],
    )

    op.execute(
        """
        UPDATE stocklot AS lot
        SET inventory_role = CASE
                WHEN regexp_replace(lower(coalesce(brand.name, '')), '[^a-z0-9]', '', 'g')
                     = 'dragonzap'
                    THEN 'dragonzap_finished'::stocklotrole
                ELSE 'original_good'::stocklotrole
            END,
            role_source = 'legacy_migration'::stocklotrolesource,
            role_changed_at = coalesce(lot.created_at, now()),
            role_change_reason = 'Первичная классификация существующей партии'
        FROM autopart
        LEFT JOIN brand ON brand.id = autopart.brand_id
        WHERE autopart.id = lot.autopart_id
        """
    )
    # Defensive fallback for orphaned legacy rows.
    op.execute(
        """
        UPDATE stocklot
        SET inventory_role = coalesce(
                inventory_role,
                'original_good'::stocklotrole
            ),
            role_source = coalesce(
                role_source,
                'legacy_migration'::stocklotrolesource
            ),
            role_changed_at = coalesce(role_changed_at, created_at, now()),
            role_change_reason = coalesce(
                role_change_reason,
                'Первичная классификация существующей партии'
            )
        WHERE inventory_role IS NULL OR role_source IS NULL OR role_changed_at IS NULL
        """
    )
    op.alter_column(
        "stocklot",
        "inventory_role",
        existing_type=lot_role,
        nullable=False,
        server_default="original_good",
    )
    op.alter_column(
        "stocklot",
        "role_source",
        existing_type=role_source,
        nullable=False,
        server_default="system_default",
    )
    op.alter_column(
        "stocklot",
        "role_changed_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    )

    op.create_table(
        "stocklotrolechange",
        sa.Column("stock_lot_id", sa.Integer(), nullable=False),
        sa.Column("old_role", lot_role, nullable=True),
        sa.Column("new_role", lot_role, nullable=False),
        sa.Column("source", role_source, nullable=False),
        sa.Column("rule_reference", sa.String(length=255), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("changed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["changed_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["stock_lot_id"],
            ["stocklot.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_stocklotrolechange_stock_lot_id",
        "stocklotrolechange",
        ["stock_lot_id"],
    )
    op.create_index(
        "ix_stocklotrolechange_new_role",
        "stocklotrolechange",
        ["new_role"],
    )
    op.create_index(
        "ix_stocklotrolechange_changed_by_user_id",
        "stocklotrolechange",
        ["changed_by_user_id"],
    )
    op.create_index(
        "ix_stocklotrolechange_changed_at",
        "stocklotrolechange",
        ["changed_at"],
    )
    op.create_index(
        "idx_stocklotrolechange_lot_changed",
        "stocklotrolechange",
        ["stock_lot_id", "changed_at"],
    )
    op.execute(
        """
        INSERT INTO stocklotrolechange (
            stock_lot_id,
            old_role,
            new_role,
            source,
            reason,
            changed_at
        )
        SELECT
            id,
            NULL,
            inventory_role,
            role_source,
            role_change_reason,
            role_changed_at
        FROM stocklot
        """
    )


def downgrade() -> None:
    op.drop_table("stocklotrolechange")
    op.drop_index("ix_stocklot_inventory_role", table_name="stocklot")
    op.drop_index(
        "ix_stocklot_role_changed_by_user_id",
        table_name="stocklot",
    )
    op.drop_constraint(
        "fk_stocklot_role_changed_by_user",
        "stocklot",
        type_="foreignkey",
    )
    op.drop_column("stocklot", "role_change_reason")
    op.drop_column("stocklot", "role_changed_at")
    op.drop_column("stocklot", "role_changed_by_user_id")
    op.drop_column("stocklot", "role_rule_reference")
    op.drop_column("stocklot", "role_source")
    op.drop_column("stocklot", "inventory_role")

    bind = op.get_bind()
    postgresql.ENUM(name="stocklotrolesource").drop(bind, checkfirst=True)
    postgresql.ENUM(name="stocklotrole").drop(bind, checkfirst=True)
