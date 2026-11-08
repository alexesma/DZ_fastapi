"""Use Diadoc event cursors and canonical docflow state.

Revision ID: b9d2e5f8a031
Revises: a8c1d4e7f920
Create Date: 2026-11-08 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b9d2e5f8a031"
down_revision: Union[str, Sequence[str], None] = "a8c1d4e7f920"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for name in (
        "send_customer_orders",
        "send_supplier_orders",
        "send_returns",
        "send_stock_documents",
    ):
        op.add_column(
            "oncenterpriseconfig",
            sa.Column(name, sa.Boolean(), nullable=False, server_default=sa.false()),
        )
    for name, column in (
        ("docflow_is_finished", sa.Column("docflow_is_finished", sa.Boolean())),
        ("docflow_status_severity", sa.Column("docflow_status_severity", sa.String(32))),
        ("docflow_status_text", sa.Column("docflow_status_text", sa.String(500))),
        ("recipient_response_status", sa.Column("recipient_response_status", sa.String(120))),
        ("revocation_status", sa.Column("revocation_status", sa.String(120))),
        ("docflow_checked_at", sa.Column("docflow_checked_at", sa.DateTime(timezone=True))),
        ("last_docflow_payload", sa.Column("last_docflow_payload", sa.JSON())),
    ):
        op.add_column("diadocincomingdocument", column)
    op.create_index(
        "ix_diadocincomingdocument_docflow_is_finished",
        "diadocincomingdocument",
        ["docflow_is_finished"],
    )

    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("source_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("docflow_is_finished", sa.Boolean()),
    )
    op.create_index(
        "ix_diadocoutgoingdocument_docflow_is_finished",
        "diadocoutgoingdocument",
        ["docflow_is_finished"],
    )
    # Старые локальные дубли сохраняем как версии 2, 3...; новые операции
    # повторно используют существующую версию вместо создания ещё одной строки.
    op.execute(sa.text("""
        WITH ranked AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY environment, source_type, source_id
                       ORDER BY
                           CASE
                               WHEN message_id IS NOT NULL
                                AND status NOT IN ('error', 'rejected', 'revoked')
                               THEN 0 ELSE 1
                           END,
                           id DESC
                   ) AS version_no
            FROM diadocoutgoingdocument
            WHERE source_type IS NOT NULL AND source_id IS NOT NULL
        )
        UPDATE diadocoutgoingdocument AS target
        SET source_version = ranked.version_no
        FROM ranked
        WHERE target.id = ranked.id
    """))
    op.create_unique_constraint(
        "uq_diadoc_outgoing_source_version",
        "diadocoutgoingdocument",
        ["environment", "source_type", "source_id", "source_version"],
    )

    op.create_table(
        "diadoceventcursor",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("box_id_guid", sa.String(64), nullable=False),
        sa.Column("scope", sa.String(32), nullable=False, server_default="inbound"),
        sa.Column("index_key", sa.String(512)),
        sa.Column("initialized_at", sa.DateTime(timezone=True)),
        sa.Column("last_polled_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(2000)),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "environment", "box_id_guid", "scope",
            name="uq_diadoc_event_cursor_box_scope",
        ),
    )
    op.create_table(
        "diadocboxevent",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("box_id_guid", sa.String(64), nullable=False),
        sa.Column("event_id", sa.String(64), nullable=False),
        sa.Column("index_key", sa.String(512), nullable=False),
        sa.Column("event_kind", sa.String(16), nullable=False),
        sa.Column("message_id", sa.String(255)),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(24), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(2000)),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "environment", "box_id_guid", "event_id",
            name="uq_diadoc_box_event",
        ),
    )
    op.create_index("ix_diadocboxevent_box_id_guid", "diadocboxevent", ["box_id_guid"])
    op.create_index("ix_diadocboxevent_message_id", "diadocboxevent", ["message_id"])
    op.create_index("ix_diadocboxevent_status", "diadocboxevent", ["status"])
    # До появления ограничения несколько нажатий могли создать параллельные
    # SMS-задачи. Оставляем самую новую активной, остальные явно закрываем.
    op.execute(sa.text("""
        WITH ranked AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY operation, incoming_document_id
                       ORDER BY id DESC
                   ) AS position
            FROM diadoccloudsigntask
            WHERE state = 'waiting_code' AND incoming_document_id IS NOT NULL
        )
        UPDATE diadoccloudsigntask AS target
        SET state = 'error',
            error_details = 'Закрыта при миграции: создана более новая SMS-задача'
        FROM ranked
        WHERE target.id = ranked.id AND ranked.position > 1
    """))
    op.execute(sa.text("""
        WITH ranked AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY operation, outgoing_document_id
                       ORDER BY id DESC
                   ) AS position
            FROM diadoccloudsigntask
            WHERE state = 'waiting_code' AND outgoing_document_id IS NOT NULL
        )
        UPDATE diadoccloudsigntask AS target
        SET state = 'error',
            error_details = 'Закрыта при миграции: создана более новая SMS-задача'
        FROM ranked
        WHERE target.id = ranked.id AND ranked.position > 1
    """))
    op.create_index(
        "uq_diadoc_cloud_sign_active_incoming",
        "diadoccloudsigntask",
        ["operation", "incoming_document_id"],
        unique=True,
        postgresql_where=sa.text(
            "state = 'waiting_code' AND incoming_document_id IS NOT NULL"
        ),
    )
    op.create_index(
        "uq_diadoc_cloud_sign_active_outgoing",
        "diadoccloudsigntask",
        ["operation", "outgoing_document_id"],
        unique=True,
        postgresql_where=sa.text(
            "state = 'waiting_code' AND outgoing_document_id IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_diadoc_cloud_sign_active_outgoing",
        table_name="diadoccloudsigntask",
    )
    op.drop_index(
        "uq_diadoc_cloud_sign_active_incoming",
        table_name="diadoccloudsigntask",
    )
    op.drop_table("diadocboxevent")
    op.drop_table("diadoceventcursor")
    op.drop_constraint(
        "uq_diadoc_outgoing_source_version",
        "diadocoutgoingdocument",
        type_="unique",
    )
    op.drop_index(
        "ix_diadocoutgoingdocument_docflow_is_finished",
        table_name="diadocoutgoingdocument",
    )
    op.drop_column("diadocoutgoingdocument", "docflow_is_finished")
    op.drop_column("diadocoutgoingdocument", "source_version")
    op.drop_index(
        "ix_diadocincomingdocument_docflow_is_finished",
        table_name="diadocincomingdocument",
    )
    for name in (
        "last_docflow_payload",
        "docflow_checked_at",
        "revocation_status",
        "recipient_response_status",
        "docflow_status_text",
        "docflow_status_severity",
        "docflow_is_finished",
    ):
        op.drop_column("diadocincomingdocument", name)
    for name in (
        "send_stock_documents",
        "send_returns",
        "send_supplier_orders",
        "send_customer_orders",
    ):
        op.drop_column("oncenterpriseconfig", name)
