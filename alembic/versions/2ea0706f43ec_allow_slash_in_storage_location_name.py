"""Allow slash in storage location name

Revision ID: 2ea0706f43ec
Revises: 6a1f5aaec4f1
Create Date: 2025-01-29 16:45:45.501768

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2ea0706f43ec'
down_revision: Union[str, None] = '6a1f5aaec4f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint('latin_characters_only', 'storagelocation', type_='check')
    op.create_check_constraint(
        'latin_characters_only',
        'storagelocation',
        "name ~ '^[A-Z0-9 /]+$'"
    )


def downgrade() -> None:
    op.drop_constraint('latin_characters_only', 'storagelocation', type_='check')
    op.create_check_constraint(
        'latin_characters_only',
        'storagelocation',
        "name ~ '^[A-Z0-9 ]+$'"
    )
