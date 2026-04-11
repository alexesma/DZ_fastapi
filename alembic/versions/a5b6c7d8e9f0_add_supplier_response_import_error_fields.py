"""add supplier response import error fields

Revision ID: a5b6c7d8e9f0
Revises: e1f2a3b4c5d6
Create Date: 2026-04-11 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a5b6c7d8e9f0"
down_revision: Union[str, Sequence[str], None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "supplierordermessage",
        sa.Column("response_config_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierordermessage",
        sa.Column(
            "import_error_details",
            sa.String(length=500),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_supplierordermessage_response_config_id",
        "supplierordermessage",
        "supplierresponseconfig",
        ["response_config_id"],
        ["id"],
    )
    op.create_index(
        op.f("ix_supplierordermessage_response_config_id"),
        "supplierordermessage",
        ["response_config_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_supplierordermessage_response_config_id"),
        table_name="supplierordermessage",
    )
    op.drop_constraint(
        "fk_supplierordermessage_response_config_id",
        "supplierordermessage",
        type_="foreignkey",
    )
    op.drop_column("supplierordermessage", "import_error_details")
    op.drop_column("supplierordermessage", "response_config_id")
