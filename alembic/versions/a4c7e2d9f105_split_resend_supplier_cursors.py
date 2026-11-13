"""Split Resend cursors for supplier responses and documents.

Revision ID: a4c7e2d9f105
Revises: 9c1e3a5b7d46
Create Date: 2026-11-13 13:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a4c7e2d9f105"
down_revision: Union[str, Sequence[str], None] = "9c1e3a5b7d46"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "emailaccount",
        sa.Column(
            "resend_supplier_response_last_received_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "emailaccount",
        sa.Column(
            "resend_supplier_document_last_received_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column(
        "emailaccount",
        "resend_supplier_document_last_received_at",
    )
    op.drop_column(
        "emailaccount",
        "resend_supplier_response_last_received_at",
    )
