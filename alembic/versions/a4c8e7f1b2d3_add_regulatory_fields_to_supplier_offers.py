"""Store regulatory data on supplier offers.

Revision ID: a4c8e7f1b2d3
Revises: f2a6c4d8e1b3
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a4c8e7f1b2d3"
down_revision: Union[str, None] = "f2a6c4d8e1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pricelistautopartassociation",
        sa.Column("tnved_code", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "pricelistautopartassociation",
        sa.Column("okpd2_code", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "pricelistautopartassociation",
        sa.Column("certification_required", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "pricelistautopartassociation",
        sa.Column("eac_cert_number", sa.String(length=150), nullable=True),
    )
    op.add_column(
        "pricelistautopartassociation",
        sa.Column("eac_cert_url", sa.String(length=500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("pricelistautopartassociation", "eac_cert_url")
    op.drop_column("pricelistautopartassociation", "eac_cert_number")
    op.drop_column("pricelistautopartassociation", "certification_required")
    op.drop_column("pricelistautopartassociation", "okpd2_code")
    op.drop_column("pricelistautopartassociation", "tnved_code")
