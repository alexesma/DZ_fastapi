"""Rounding step and validity period for publication rules, rule id on export rows.

Revision ID: e4f6a8b0c2d3
Revises: d3e5f7a9b1c2
Create Date: 2026-11-06 15:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e4f6a8b0c2d3"
down_revision: Union[str, Sequence[str], None] = "d3e5f7a9b1c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RULE = "customerpricelistpublicationrule"
EXPORT = "customerpricelistexportrow"


def upgrade() -> None:
    op.add_column(RULE, sa.Column("price_rounding_step", sa.Integer(), nullable=True))
    op.add_column(RULE, sa.Column("valid_from", sa.Date(), nullable=True))
    op.add_column(RULE, sa.Column("valid_to", sa.Date(), nullable=True))
    op.add_column(EXPORT, sa.Column("publication_rule_id", sa.Integer(), nullable=True))
    op.create_index(
        "ix_customerpricelistexportrow_publication_rule_id",
        EXPORT,
        ["publication_rule_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_customerpricelistexportrow_publication_rule_id", table_name=EXPORT)
    op.drop_column(EXPORT, "publication_rule_id")
    op.drop_column(RULE, "valid_to")
    op.drop_column(RULE, "valid_from")
    op.drop_column(RULE, "price_rounding_step")
