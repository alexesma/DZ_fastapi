"""add registry status and check timestamp to certificate

Из прайсов поставщиков приходят только номер и ссылка, поэтому у всех
545 документов нет ни срока, ни состояния. Эти два поля заполняет сверка
с реестром: приостановленный документ формально не истёк, но выгружать
его нельзя.

Revision ID: f2b8d4e0a71c
Revises: e1a5c73d9b62
Create Date: 2026-08-29 13:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f2b8d4e0a71c"
down_revision: Union[str, None] = "e1a5c73d9b62"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "certificate", sa.Column("status", sa.String(32), nullable=True)
    )
    op.add_column(
        "certificate",
        sa.Column(
            "registry_checked_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.create_index("ix_certificate_status", "certificate", ["status"])


def downgrade() -> None:
    op.drop_index("ix_certificate_status", table_name="certificate")
    op.drop_column("certificate", "registry_checked_at")
    op.drop_column("certificate", "status")
