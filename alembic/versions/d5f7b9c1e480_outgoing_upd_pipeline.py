"""Outgoing UPD pipeline: auto drafts after posting a shipment.

Revision ID: d5f7b9c1e480
Revises: c4e6a8b0d379
Create Date: 2026-11-15 19:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d5f7b9c1e480"
down_revision: Union[str, Sequence[str], None] = "c4e6a8b0d379"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "diadocintegrationsettings",
        sa.Column(
            "outbound_auto_draft_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "diadocintegrationsettings",
        sa.Column("outbound_auto_draft_from", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "shipmentdocument",
        sa.Column("edo_auto_error", sa.String(length=1000), nullable=True),
    )
    op.add_column(
        "shipmentdocument",
        sa.Column("edo_auto_checked_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("shipmentdocument", "edo_auto_checked_at")
    op.drop_column("shipmentdocument", "edo_auto_error")
    op.drop_column("diadocintegrationsettings", "outbound_auto_draft_from")
    op.drop_column("diadocintegrationsettings", "outbound_auto_draft_enabled")
