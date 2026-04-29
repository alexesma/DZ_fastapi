"""add warehouses and receipt warehouse fields

Revision ID: 6c7d8e9f0a1b
Revises: 4fa8b1c2d3e9
Create Date: 2026-04-29 11:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "6c7d8e9f0a1b"
down_revision: Union[str, Sequence[str], None] = "4fa8b1c2d3e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DEFAULT_WAREHOUSE_NAME = "Основной склад"
DEFAULT_WAREHOUSE_COMMENT = (
    "Склад по умолчанию для входящих документов и первичного размещения."
)
RECEIVING_SYSTEM_CODE = "RECEIVING"


def upgrade() -> None:
    op.create_table(
        "warehouse",
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    op.add_column(
        "storagelocation",
        sa.Column("warehouse_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "storagelocation",
        sa.Column("system_code", sa.String(length=50), nullable=True),
    )
    op.create_index(
        op.f("ix_storagelocation_warehouse_id"),
        "storagelocation",
        ["warehouse_id"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_storagelocation_warehouse_system_code",
        "storagelocation",
        ["warehouse_id", "system_code"],
    )
    op.create_foreign_key(
        "fk_storagelocation_warehouse_id",
        "storagelocation",
        "warehouse",
        ["warehouse_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    op.add_column(
        "provider",
        sa.Column("default_warehouse_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        op.f("ix_provider_default_warehouse_id"),
        "provider",
        ["default_warehouse_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_provider_default_warehouse_id",
        "provider",
        "warehouse",
        ["default_warehouse_id"],
        ["id"],
    )

    op.add_column(
        "supplierreceipt",
        sa.Column("warehouse_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        op.f("ix_supplierreceipt_warehouse_id"),
        "supplierreceipt",
        ["warehouse_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_supplierreceipt_warehouse_id",
        "supplierreceipt",
        "warehouse",
        ["warehouse_id"],
        ["id"],
    )

    bind = op.get_bind()
    warehouse_id = bind.execute(
        sa.text(
            """
            INSERT INTO warehouse (name, comment, is_active)
            VALUES (:name, :comment, true)
            RETURNING id
            """
        ),
        {
            "name": DEFAULT_WAREHOUSE_NAME,
            "comment": DEFAULT_WAREHOUSE_COMMENT,
        },
    ).scalar_one()

    bind.execute(
        sa.text(
            """
            UPDATE provider
            SET default_warehouse_id = :warehouse_id
            WHERE default_warehouse_id IS NULL
            """
        ),
        {"warehouse_id": warehouse_id},
    )
    bind.execute(
        sa.text(
            """
            UPDATE storagelocation
            SET warehouse_id = :warehouse_id
            WHERE warehouse_id IS NULL
            """
        ),
        {"warehouse_id": warehouse_id},
    )
    bind.execute(
        sa.text(
            """
            UPDATE supplierreceipt
            SET warehouse_id = :warehouse_id
            WHERE warehouse_id IS NULL
            """
        ),
        {"warehouse_id": warehouse_id},
    )
    bind.execute(
        sa.text(
            """
            INSERT INTO storagelocation (name, warehouse_id, system_code)
            VALUES (:name, :warehouse_id, :system_code)
            """
        ),
        {
            "name": f"WH{int(warehouse_id)} RECEIVING",
            "warehouse_id": warehouse_id,
            "system_code": RECEIVING_SYSTEM_CODE,
        },
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_supplierreceipt_warehouse_id",
        "supplierreceipt",
        type_="foreignkey",
    )
    op.drop_index(op.f("ix_supplierreceipt_warehouse_id"), table_name="supplierreceipt")
    op.drop_column("supplierreceipt", "warehouse_id")

    op.drop_constraint(
        "fk_provider_default_warehouse_id",
        "provider",
        type_="foreignkey",
    )
    op.drop_index(op.f("ix_provider_default_warehouse_id"), table_name="provider")
    op.drop_column("provider", "default_warehouse_id")

    op.drop_constraint(
        "fk_storagelocation_warehouse_id",
        "storagelocation",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_storagelocation_warehouse_system_code",
        "storagelocation",
        type_="unique",
    )
    op.drop_index(op.f("ix_storagelocation_warehouse_id"), table_name="storagelocation")
    op.drop_column("storagelocation", "system_code")
    op.drop_column("storagelocation", "warehouse_id")

    op.drop_table("warehouse")
