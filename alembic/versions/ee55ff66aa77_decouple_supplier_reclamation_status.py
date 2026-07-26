"""decouple supplier request from customer reclamation status

Revision ID: ee55ff66aa77
Revises: dd44ee55ff66
Create Date: 2026-07-26 17:30:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "ee55ff66aa77"
down_revision: Union[str, Sequence[str], None] = "dd44ee55ff66"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE reclamation
        SET status = 'approved'
        WHERE status = 'waiting_supplier'
          AND resolution = 'approved'
        """
    )
    op.execute(
        """
        UPDATE reclamation
        SET status = 'rejected'
        WHERE status = 'waiting_supplier'
          AND resolution = 'rejected'
        """
    )
    op.execute(
        """
        UPDATE reclamation
        SET status = 'checked'
        WHERE status = 'waiting_supplier'
        """
    )


def downgrade() -> None:
    # Старый общий статус смешивал независимые процессы и не восстанавливается.
    pass
