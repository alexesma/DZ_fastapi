"""bank categories, category rules and counterparty links

Revision ID: e5f7a9b1c3d4
Revises: d4e6f8a0b2c3
Create Date: 2026-11-21 21:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "e5f7a9b1c3d4"
down_revision = "d4e6f8a0b2c3"
branch_labels = None
depends_on = None

# code, name, kind, color, is_system, sort
_SEED = [
    ("tax", "Налоги и сборы", "expense", "#c0392b", True, 10),
    ("salary", "Зарплата", "expense", "#2a78d6", True, 20),
    ("rent", "Аренда", "expense", "#8e5ea2", False, 30),
    ("website", "Сайт и хостинг", "expense", "#16a085", False, 40),
    ("advertising", "Реклама", "expense", "#e67e22", False, 50),
    ("transport", "Доставка и транспорт", "expense", "#b7950b", False, 60),
    ("software", "Софт и сервисы", "expense", "#5d6d7e", False, 70),
    ("office", "Офис и хозрасходы", "expense", "#7d6608", False, 80),
    ("bank_fee", "Банковская комиссия", "expense", "#7f8c8d", True, 90),
    ("other_expense", "Прочие расходы", "expense", "#95a5a6", True, 900),
    ("bank_interest", "Проценты банка", "income", "#27ae60", True, 100),
    ("deposit_interest", "Депозит: проценты", "income", "#1e8449", True, 110),
    ("other_income", "Прочие доходы", "income", "#58d68d", True, 910),
    ("deposit_open", "Депозит: открытие", "transfer", "#aab7b8", True, 200),
    ("deposit_return", "Депозит: возврат", "transfer", "#aab7b8", True, 210),
]


def upgrade() -> None:
    categories = op.create_table(
        "bankcategory",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("color", sa.String(length=16), nullable=True),
        sa.Column("is_system", sa.Boolean(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
    )
    op.create_index(
        op.f("ix_bankcategory_code"), "bankcategory", ["code"], unique=True
    )
    op.bulk_insert(
        categories,
        [
            {
                "code": code,
                "name": name,
                "kind": kind,
                "color": color,
                "is_system": system,
                "is_active": True,
                "sort_order": sort,
            }
            for code, name, kind, color, system, sort in _SEED
        ],
    )

    op.create_table(
        "bankcategoryrule",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("category_code", sa.String(length=32), nullable=False),
        sa.Column("match_type", sa.String(length=16), nullable=False),
        sa.Column("pattern", sa.String(length=500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        op.f("ix_bankcategoryrule_category_code"),
        "bankcategoryrule",
        ["category_code"],
        unique=False,
    )

    op.create_table(
        "bankcounterpartylink",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("inn", sa.String(length=12), nullable=False),
        sa.Column(
            "customer_id",
            sa.Integer(),
            sa.ForeignKey("customer.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column(
            "provider_id",
            sa.Integer(),
            sa.ForeignKey("provider.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        op.f("ix_bankcounterpartylink_inn"),
        "bankcounterpartylink",
        ["inn"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_bankcounterpartylink_inn"), table_name="bankcounterpartylink"
    )
    op.drop_table("bankcounterpartylink")
    op.drop_index(
        op.f("ix_bankcategoryrule_category_code"),
        table_name="bankcategoryrule",
    )
    op.drop_table("bankcategoryrule")
    op.drop_index(op.f("ix_bankcategory_code"), table_name="bankcategory")
    op.drop_table("bankcategory")
