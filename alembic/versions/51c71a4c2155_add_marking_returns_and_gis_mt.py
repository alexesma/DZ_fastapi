"""marking: return enum values, gis mt fields and settings

Revision ID: 51c71a4c2155
Revises: 6b8c9d0e1f2a
Create Date: 2026-07-04 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "51c71a4c2155"
down_revision: Union[str, Sequence[str], None] = "6b8c9d0e1f2a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Новые значения enum: PG >= 12 позволяет ADD VALUE в транзакции
    # (использовать их в этой же миграции нельзя — и не нужно).
    op.execute(
        "ALTER TYPE markingcodestatus ADD VALUE IF NOT EXISTS "
        "'returned_to_supplier'"
    )
    for value in (
        "returned_from_customer",
        "returned_to_supplier",
        "withdrawn",
    ):
        op.execute(
            "ALTER TYPE markingmovementtype ADD VALUE IF NOT EXISTS "
            f"'{value}'"
        )

    op.add_column(
        "productmarkingcode",
        sa.Column("gis_status", sa.String(64), nullable=True),
    )
    op.add_column(
        "productmarkingcode",
        sa.Column(
            "gis_checked_at", sa.DateTime(timezone=True), nullable=True
        ),
    )

    op.create_table(
        "gismtsettings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("token", sa.String(4096), nullable=True),
        sa.Column(
            "token_expires_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("product_group", sa.String(64), nullable=True),
        sa.Column("auth_uuid", sa.String(128), nullable=True),
        sa.Column("last_error", sa.String(2000), nullable=True),
        sa.Column(
            "last_check_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("gismtsettings")
    op.drop_column("productmarkingcode", "gis_checked_at")
    op.drop_column("productmarkingcode", "gis_status")
    # Удаление значений из enum PostgreSQL не поддерживает — оставляем.
