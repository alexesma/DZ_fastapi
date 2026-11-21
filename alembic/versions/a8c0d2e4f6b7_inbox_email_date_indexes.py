"""indexes on inbox email dates

Revision ID: a8c0d2e4f6b7
Revises: f6a8b0c2d4e5
Create Date: 2026-11-21 23:00:00.000000
"""

from alembic import op

revision = "a8c0d2e4f6b7"
down_revision = "f6a8b0c2d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # IF NOT EXISTS: часть баз уже получила эти индексы из более ранних
    # миграций (CI поднимает схему цепочкой и упал на дубле).
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_inboxemail_received_at "
        "ON inboxemail (received_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_inboxemail_fetched_at "
        "ON inboxemail (fetched_at)"
    )


def downgrade() -> None:
    # fetched_at принадлежит исходной миграции таблицы: здесь не трогаем.
    op.execute("DROP INDEX IF EXISTS ix_inboxemail_received_at")
