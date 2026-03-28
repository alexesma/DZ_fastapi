"""add unique customer pricelist source constraint

Revision ID: a7b8c9d0e1f2
Revises: f2b3c4d5e6f7
Create Date: 2026-03-28 16:40:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = 'a7b8c9d0e1f2'
down_revision = 'f2b3c4d5e6f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DELETE FROM customerpricelistsource
        WHERE id IN (
            SELECT id
            FROM (
                SELECT
                    id,
                    row_number() OVER (
                        PARTITION BY customer_config_id, provider_config_id
                        ORDER BY id
                    ) AS rn
                FROM customerpricelistsource
            ) duplicates
            WHERE duplicates.rn > 1
        )
        """
    )
    op.create_unique_constraint(
        'uq_customer_pricelist_source_config_provider',
        'customerpricelistsource',
        ['customer_config_id', 'provider_config_id'],
    )


def downgrade() -> None:
    op.drop_constraint(
        'uq_customer_pricelist_source_config_provider',
        'customerpricelistsource',
        type_='unique',
    )
