"""Index normalized customer OEMs for receipt-driven source switches.

Revision ID: 6b9d2e4f7a10
Revises: 5a7c3e9d1b24
"""

import sqlalchemy as sa

from alembic import op

revision = "6b9d2e4f7a10"
down_revision = "5a7c3e9d1b24"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_customerorderitem_normalized_oem"
NORMALIZED_OEM = sa.text(
    "regexp_replace(upper(oem), '[^A-Z0-9]', '', 'g')"
)


def upgrade():
    with op.get_context().autocommit_block():
        op.create_index(
            INDEX_NAME, "customerorderitem", [NORMALIZED_OEM],
            postgresql_concurrently=True,
        )


def downgrade():
    with op.get_context().autocommit_block():
        op.drop_index(
            INDEX_NAME, table_name="customerorderitem",
            postgresql_concurrently=True,
        )
