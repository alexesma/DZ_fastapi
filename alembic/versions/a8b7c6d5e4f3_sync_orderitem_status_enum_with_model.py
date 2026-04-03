"""Sync orderitem status enum with model

Revision ID: a8b7c6d5e4f3
Revises: f3c4d5e6a7b8
Create Date: 2026-04-03 19:55:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'a8b7c6d5e4f3'
down_revision: Union[str, None] = 'f3c4d5e6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


ENUM_NAME = 'type_order_item_status'


def _has_enum_value(bind, value: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                """
                SELECT 1
                FROM pg_enum e
                JOIN pg_type t ON t.oid = e.enumtypid
                WHERE t.typname = :enum_name
                  AND e.enumlabel = :enum_value
                """
            ),
            {'enum_name': ENUM_NAME, 'enum_value': value},
        ).scalar()
    )


def upgrade() -> None:
    bind = op.get_bind()
    context = op.get_context()

    with context.autocommit_block():
        has_arrived = _has_enum_value(bind, 'ARRIVED')
        has_delivered = _has_enum_value(bind, 'DELIVERED')
        if has_arrived and not has_delivered:
            op.execute(
                "ALTER TYPE type_order_item_status "
                "RENAME VALUE 'ARRIVED' TO 'DELIVERED'"
            )

        for value in ('SENT', 'IN_PROGRESS', 'DELIVERED', 'FAILED'):
            if not _has_enum_value(bind, value):
                op.execute(
                    f"ALTER TYPE {ENUM_NAME} ADD VALUE '{value}'"
                )


def downgrade() -> None:
    bind = op.get_bind()

    op.execute(f'ALTER TYPE {ENUM_NAME} RENAME TO {ENUM_NAME}_old')

    recreated_enum = sa.Enum(
        'NEW',
        'CONFIRMED',
        'ARRIVED',
        'CANCELLED',
        'ERROR',
        name=ENUM_NAME,
    )
    recreated_enum.create(bind, checkfirst=False)

    op.execute(
        f"""
        ALTER TABLE orderitem
        ALTER COLUMN status TYPE {ENUM_NAME}
        USING (
            CASE
                WHEN status::text IN ('SENT', 'IN_PROGRESS') THEN 'CONFIRMED'
                WHEN status::text = 'FAILED' THEN 'ERROR'
                WHEN status::text = 'DELIVERED' THEN 'ARRIVED'
                ELSE status::text
            END
        )::{ENUM_NAME}
        """
    )

    op.execute(f'DROP TYPE {ENUM_NAME}_old')
