"""add category and deal_ref to bank transactions

Revision ID: d4e6f8a0b2c3
Revises: 3ca97e412df8
Create Date: 2026-11-21 18:00:00.000000
"""

import re

import sqlalchemy as sa

from alembic import op

revision = "d4e6f8a0b2c3"
down_revision = "3ca97e412df8"
branch_labels = None
depends_on = None

_DEAL_RE = re.compile(r"сделк\w*\s*№?\s*([A-Za-z]{2,5}-?\d{5,})", re.I)
_LABELS = {
    "deposit_open": "Депозит: открытие",
    "deposit_return": "Депозит: возврат",
    "deposit_interest": "Депозит: проценты",
}


def upgrade() -> None:
    op.add_column(
        "banktransaction",
        sa.Column("category", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "banktransaction",
        sa.Column("deal_ref", sa.String(length=64), nullable=True),
    )
    op.create_index(
        op.f("ix_banktransaction_category"),
        "banktransaction",
        ["category"],
        unique=False,
    )
    op.create_index(
        op.f("ix_banktransaction_deal_ref"),
        "banktransaction",
        ["deal_ref"],
        unique=False,
    )

    # Размечаем уже загруженные депозитные операции. Регистр кириллицы
    # зависит от локали БД, поэтому сравниваем в Python.
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id, purpose FROM banktransaction "
            "WHERE status IN ('unmatched', 'ignored') "
            "AND purpose IS NOT NULL"
        )
    ).fetchall()
    for txn_id, purpose in rows:
        text = (purpose or "").casefold()
        if "депозит" not in text:
            continue
        if "открытия депозита" in text:
            category = "deposit_open"
        elif "выплата процентов" in text:
            category = "deposit_interest"
        elif "возврат средств" in text:
            category = "deposit_return"
        else:
            continue
        match = _DEAL_RE.search(purpose or "")
        bind.execute(
            sa.text(
                "UPDATE banktransaction SET category = :category, "
                "deal_ref = :deal_ref, status = 'ignored', "
                "match_note = :note WHERE id = :id"
            ),
            {
                "category": category,
                "deal_ref": match.group(1).upper() if match else None,
                "note": "Не торговая операция: " + _LABELS[category],
                "id": txn_id,
            },
        )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_banktransaction_deal_ref"), table_name="banktransaction"
    )
    op.drop_index(
        op.f("ix_banktransaction_category"), table_name="banktransaction"
    )
    op.drop_column("banktransaction", "deal_ref")
    op.drop_column("banktransaction", "category")
