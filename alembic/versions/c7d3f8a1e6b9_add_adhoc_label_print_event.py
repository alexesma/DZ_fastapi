"""Журнал печати товарных этикеток и бирок мест хранения

Товарные этикетки и бирки мест хранения не хранятся в базе как объекты —
их собирают на лету из поиска по номенклатуре или из имени места. В
отличие от этикеток волны сборки и кросс-докинга у них не было ни следа
печати: отклеилась этикетка или принтер зажевал бумагу — узнать, кто и
когда её печатал, было неоткуда. Одна запись на каждое нажатие «Печать».

Revision ID: c7d3f8a1e6b9
Revises: b4f1e7c93a25
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c7d3f8a1e6b9"
down_revision: Union[str, None] = "b4f1e7c93a25"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "adhoclabelprintevent",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("storage_location_id", sa.Integer(), nullable=True),
        sa.Column("items", sa.JSON(), nullable=False),
        sa.Column("total_labels", sa.Integer(), nullable=False),
        sa.Column("printed_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "printed_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["autopart_id"], ["autopart.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["storage_location_id"],
            ["storagelocation.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["printed_by_user_id"], ["app_user.id"], ondelete="SET NULL"
        ),
        sa.CheckConstraint(
            "kind IN ('product', 'location')",
            name="ck_adhoc_label_print_event_kind",
        ),
    )
    op.create_index(
        "ix_adhoclabelprintevent_kind",
        "adhoclabelprintevent",
        ["kind"],
    )
    op.create_index(
        "ix_adhoclabelprintevent_autopart_id",
        "adhoclabelprintevent",
        ["autopart_id"],
    )
    op.create_index(
        "ix_adhoclabelprintevent_storage_location_id",
        "adhoclabelprintevent",
        ["storage_location_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_adhoclabelprintevent_storage_location_id",
        table_name="adhoclabelprintevent",
    )
    op.drop_index(
        "ix_adhoclabelprintevent_autopart_id",
        table_name="adhoclabelprintevent",
    )
    op.drop_index(
        "ix_adhoclabelprintevent_kind", table_name="adhoclabelprintevent"
    )
    op.drop_table("adhoclabelprintevent")
