"""Store the selected customer-order processing route.

Revision ID: d8e4f9a2c1b7
Revises: c7d3f8a1e6b9
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d8e4f9a2c1b7"
down_revision: Union[str, None] = "c7d3f8a1e6b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorder",
        sa.Column("processing_owner", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "customerorder",
        sa.Column("processing_state", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "customerorder",
        sa.Column("identity_match_basis", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_customerorder_processing_owner",
        "customerorder",
        ["processing_owner"],
    )
    op.create_index(
        "ix_customerorder_processing_state",
        "customerorder",
        ["processing_state"],
    )

    op.execute(
        """
        UPDATE customerorder
        SET processing_owner = CASE
                WHEN EXISTS (
                    SELECT 1 FROM customerorderitem item
                    WHERE item.order_id = customerorder.id
                      AND item.source_resolution_status = 'local_workflow'
                ) THEN 'LOCAL'
                ELSE 'PARTS_SOFT_SITE'
            END,
            processing_state = CASE
                WHEN EXISTS (
                    SELECT 1 FROM customerorderitem item
                    WHERE item.order_id = customerorder.id
                      AND item.source_resolution_status = 'local_workflow'
                ) THEN 'LOCAL_PROCESSED'
                ELSE 'EXTERNAL_UNVERIFIED'
            END
        WHERE upper(coalesce(external_source, '')) = 'PARTS_SOFT'
        """
    )


def downgrade() -> None:
    op.drop_index("ix_customerorder_processing_state", table_name="customerorder")
    op.drop_index("ix_customerorder_processing_owner", table_name="customerorder")
    op.drop_column("customerorder", "identity_match_basis")
    op.drop_column("customerorder", "processing_state")
    op.drop_column("customerorder", "processing_owner")
