"""add multiple targets to customer pricelist publication rules

Revision ID: c7e5f9a3b102
Revises: b6d4e8f2a901
Create Date: 2026-08-24 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c7e5f9a3b102"
down_revision: Union[str, Sequence[str], None] = "b6d4e8f2a901"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "customerpricelistpublicationruletarget",
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("target_autopart_id", sa.Integer(), nullable=False),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.ForeignKeyConstraint(
            ["rule_id"],
            ["customerpricelistpublicationrule.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["target_autopart_id"],
            ["autopart.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "rule_id",
            "target_autopart_id",
            name="uq_customer_pricelist_publication_rule_target",
        ),
    )
    op.create_index(
        "ix_customerpricelistpublicationruletarget_rule_id",
        "customerpricelistpublicationruletarget",
        ["rule_id"],
    )
    op.create_index(
        "ix_customerpricelistpublicationruletarget_target_autopart_id",
        "customerpricelistpublicationruletarget",
        ["target_autopart_id"],
    )
    op.execute(
        """
        INSERT INTO customerpricelistpublicationruletarget
            (rule_id, target_autopart_id)
        SELECT id, target_autopart_id
        FROM customerpricelistpublicationrule
        WHERE target_autopart_id IS NOT NULL
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ix_customerpricelistpublicationruletarget_target_autopart_id",
        table_name="customerpricelistpublicationruletarget",
    )
    op.drop_index(
        "ix_customerpricelistpublicationruletarget_rule_id",
        table_name="customerpricelistpublicationruletarget",
    )
    op.drop_table("customerpricelistpublicationruletarget")
