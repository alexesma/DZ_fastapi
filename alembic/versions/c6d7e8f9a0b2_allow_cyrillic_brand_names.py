"""allow cyrillic brand names

Revision ID: c6d7e8f9a0b2
Revises: c5d6e7f8a9b1
Create Date: 2026-04-14 22:20:00.000000
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c6d7e8f9a0b2"
down_revision: Union[str, Sequence[str], None] = "c5d6e7f8a9b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("check_name_brand", "brand", type_="check")
    op.create_check_constraint(
        "check_name_brand",
        "brand",
        "name ~ '^[A-Za-zА-Яа-яЁё0-9]+(?:[ -]?[A-Za-zА-Яа-яЁё0-9]+)*$'",
    )


def downgrade() -> None:
    op.drop_constraint("check_name_brand", "brand", type_="check")
    op.create_check_constraint(
        "check_name_brand",
        "brand",
        "name ~ '^[a-zA-Z0-9]+(?:[ -]?[a-zA-Z0-9]+)*$'",
    )
