"""Add supplier email mapping actions

Revision ID: c6d7e8f9a0b1
Revises: c5d6e7f8a9b0
Create Date: 2026-04-04 21:20:00.000000

"""

from datetime import datetime, timezone
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c6d7e8f9a0b1"
down_revision: Union[str, None] = "c5d6e7f8a9b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "external_status_mapping",
        sa.Column(
            "supplier_response_action",
            sa.String(length=64),
            nullable=True,
        ),
    )

    mapping_table = sa.table(
        "external_status_mapping",
        sa.column("source_key", sa.String(length=64)),
        sa.column("provider_id", sa.Integer()),
        sa.column("match_mode", sa.String(length=32)),
        sa.column("raw_status", sa.String(length=255)),
        sa.column("normalized_status", sa.String(length=255)),
        sa.column("internal_order_status", sa.String(length=64)),
        sa.column("internal_item_status", sa.String(length=64)),
        sa.column("supplier_response_action", sa.String(length=64)),
        sa.column("priority", sa.Integer()),
        sa.column("is_active", sa.Boolean()),
        sa.column("notes", sa.Text()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    now_value = datetime.now(timezone.utc)
    op.bulk_insert(
        mapping_table,
        [
            {
                "source_key": "SUPPLIER_EMAIL",
                "provider_id": None,
                "match_mode": "EXACT",
                "raw_status": "готово",
                "normalized_status": "готово",
                "internal_order_status": None,
                "internal_item_status": None,
                "supplier_response_action": "FULL_CONFIRM",
                "priority": 100,
                "is_active": True,
                "notes": "Поставщик сообщил, что заказ готов полностью.",
                "created_at": now_value,
                "updated_at": now_value,
            },
            {
                "source_key": "SUPPLIER_EMAIL",
                "provider_id": None,
                "match_mode": "EXACT",
                "raw_status": "собрано",
                "normalized_status": "собрано",
                "internal_order_status": None,
                "internal_item_status": None,
                "supplier_response_action": "FULL_CONFIRM",
                "priority": 110,
                "is_active": True,
                "notes": "Поставщик подтвердил полную сборку заказа.",
                "created_at": now_value,
                "updated_at": now_value,
            },
            {
                "source_key": "SUPPLIER_EMAIL",
                "provider_id": None,
                "match_mode": "EXACT",
                "raw_status": "частично",
                "normalized_status": "частично",
                "internal_order_status": None,
                "internal_item_status": None,
                "supplier_response_action": "PARTIAL_CONFIRM",
                "priority": 120,
                "is_active": True,
                "notes": (
                    "Нужно проверить файл ответа "
                    "или вручную сверить позиции."
                ),
                "created_at": now_value,
                "updated_at": now_value,
            },
            {
                "source_key": "SUPPLIER_EMAIL",
                "provider_id": None,
                "match_mode": "EXACT",
                "raw_status": "нет позиции",
                "normalized_status": "нет позиции",
                "internal_order_status": None,
                "internal_item_status": None,
                "supplier_response_action": "REJECT_ALL",
                "priority": 130,
                "is_active": True,
                "notes": "Поставщик отказал по всем строкам ответа.",
                "created_at": now_value,
                "updated_at": now_value,
            },
            {
                "source_key": "SUPPLIER_EMAIL",
                "provider_id": None,
                "match_mode": "EXACT",
                "raw_status": "ожидаем",
                "normalized_status": "ожидаем",
                "internal_order_status": None,
                "internal_item_status": None,
                "supplier_response_action": "WAITING",
                "priority": 140,
                "is_active": True,
                "notes": "Ответ не финальный, количество не меняем.",
                "created_at": now_value,
                "updated_at": now_value,
            },
        ],
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            (
                "DELETE FROM external_status_mapping "
                "WHERE source_key = 'SUPPLIER_EMAIL' "
                "AND provider_id IS NULL AND raw_status IN "
                "('готово', 'собрано', 'частично', "
                "'нет позиции', 'ожидаем')"
            )
        )
    )
    op.drop_column("external_status_mapping", "supplier_response_action")
