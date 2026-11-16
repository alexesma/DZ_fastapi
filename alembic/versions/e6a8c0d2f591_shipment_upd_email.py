"""Email the UPD as Excel to customers and mark it as sent.

Revision ID: e6a8c0d2f591
Revises: d5f7b9c1e480
Create Date: 2026-11-16 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e6a8c0d2f591"
down_revision: Union[str, Sequence[str], None] = "d5f7b9c1e480"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "shipmentdocument",
        sa.Column("upd_emailed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "shipmentdocument",
        sa.Column("upd_emailed_to", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "shipmentdocument",
        sa.Column(
            "upd_emailed_by_user_id",
            sa.Integer(),
            sa.ForeignKey("app_user.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "shipmentdocument",
        sa.Column("upd_email_error", sa.String(length=500), nullable=True),
    )
    op.add_column(
        "diadocintegrationsettings",
        sa.Column(
            "outbound_upd_email_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "diadocintegrationsettings",
        sa.Column("outbound_upd_email_from", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("diadocintegrationsettings", "outbound_upd_email_from")
    op.drop_column("diadocintegrationsettings", "outbound_upd_email_enabled")
    op.drop_column("shipmentdocument", "upd_email_error")
    op.drop_column("shipmentdocument", "upd_emailed_by_user_id")
    op.drop_column("shipmentdocument", "upd_emailed_to")
    op.drop_column("shipmentdocument", "upd_emailed_at")
