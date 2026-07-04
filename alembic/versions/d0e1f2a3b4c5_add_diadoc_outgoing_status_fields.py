"""add diadoc outgoing docflow status fields

Revision ID: d0e1f2a3b4c5
Revises: b2d4f6a8c0e1
Create Date: 2026-07-03 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d0e1f2a3b4c5"
down_revision: Union[str, Sequence[str], None] = "b2d4f6a8c0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("docflow_status_severity", sa.String(32), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("docflow_status_text", sa.String(500), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column(
            "recipient_response_status", sa.String(120), nullable=True
        ),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("revocation_status", sa.String(120), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column(
            "delivered_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column(
            "status_checked_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("last_status_payload", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("diadocoutgoingdocument", "last_status_payload")
    op.drop_column("diadocoutgoingdocument", "status_checked_at")
    op.drop_column("diadocoutgoingdocument", "delivered_at")
    op.drop_column("diadocoutgoingdocument", "revocation_status")
    op.drop_column("diadocoutgoingdocument", "recipient_response_status")
    op.drop_column("diadocoutgoingdocument", "docflow_status_text")
    op.drop_column("diadocoutgoingdocument", "docflow_status_severity")
