"""Protect used honest sign categories from deletion.

Revision ID: e6a8c0d2f4b7
Revises: e5f7a9b1c3d6
"""

from typing import Sequence, Union

from alembic import op

revision: str = "e6a8c0d2f4b7"
down_revision: Union[str, None] = "e5f7a9b1c3d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "autopart_honest_sign_association"
CONSTRAINT = "autopart_honest_sign_association_honest_sign_category_id_fkey"


def upgrade() -> None:
    op.drop_constraint(CONSTRAINT, TABLE, type_="foreignkey")
    op.create_foreign_key(
        CONSTRAINT,
        TABLE,
        "honestsigncategory",
        ["honest_sign_category_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(CONSTRAINT, TABLE, type_="foreignkey")
    op.create_foreign_key(
        CONSTRAINT,
        TABLE,
        "honestsigncategory",
        ["honest_sign_category_id"],
        ["id"],
        ondelete="CASCADE",
    )
