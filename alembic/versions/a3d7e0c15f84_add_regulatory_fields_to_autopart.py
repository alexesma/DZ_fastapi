"""add regulatory fields to autopart (TNVED, OKPD2, EAC certificate)

Revision ID: a3d7e0c15f84
Revises: c7e5f9a3b102
Create Date: 2026-08-05 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3d7e0c15f84"
down_revision: Union[str, None] = "c7e5f9a3b102"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopart", sa.Column("tnved_code", sa.String(20), nullable=True)
    )
    op.add_column(
        "autopart", sa.Column("okpd2_code", sa.String(20), nullable=True)
    )
    op.add_column(
        "autopart",
        sa.Column("certification_required", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "autopart", sa.Column("eac_cert_number", sa.String(150), nullable=True)
    )
    op.add_column(
        "autopart", sa.Column("eac_cert_url", sa.String(500), nullable=True)
    )
    op.add_column(
        "autopart", sa.Column("eac_cert_valid_until", sa.Date(), nullable=True)
    )
    op.add_column(
        "autopart",
        sa.Column("regulatory_source", sa.String(32), nullable=True),
    )
    op.create_index("ix_autopart_tnved_code", "autopart", ["tnved_code"])


def downgrade() -> None:
    op.drop_index("ix_autopart_tnved_code", table_name="autopart")
    for name in (
        "regulatory_source",
        "eac_cert_valid_until",
        "eac_cert_url",
        "eac_cert_number",
        "certification_required",
        "okpd2_code",
        "tnved_code",
    ):
        op.drop_column("autopart", name)
