"""add supplier receipt UPD email settings

Revision ID: bb22cc33dd44
Revises: aa11bb22cc33, f5a6b7c8d9e0, f6a7b8c9d0e1
Create Date: 2026-07-09 13:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "bb22cc33dd44"
down_revision: Union[str, Sequence[str], None] = (
    "aa11bb22cc33",
    "f5a6b7c8d9e0",
    "f6a7b8c9d0e1",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_receipt_upd_email_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_receipt_upd_email",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "customerorderinboxsettings",
        sa.Column(
            "supplier_receipt_upd_email_account_id",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_customerorderinboxsettings_receipt_upd_email_account_id",
        "customerorderinboxsettings",
        "emailaccount",
        ["supplier_receipt_upd_email_account_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_customerorderinboxsettings_receipt_upd_email_account_id",
        "customerorderinboxsettings",
        type_="foreignkey",
    )
    op.drop_column(
        "customerorderinboxsettings",
        "supplier_receipt_upd_email_account_id",
    )
    op.drop_column("customerorderinboxsettings", "supplier_receipt_upd_email")
    op.drop_column(
        "customerorderinboxsettings",
        "supplier_receipt_upd_email_enabled",
    )
