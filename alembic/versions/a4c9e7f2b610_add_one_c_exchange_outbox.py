"""add reliable 1c exchange outbox

Revision ID: a4c9e7f2b610
Revises: f3b8d6e2a410
Create Date: 2026-08-11
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a4c9e7f2b610"
down_revision = "f3b8d6e2a410"
branch_labels = None
depends_on = None


event_status = postgresql.ENUM(
    "pending",
    "in_flight",
    "succeeded",
    "error",
    name="onecexchangeeventstatus",
    create_type=False,
)
batch_status = postgresql.ENUM(
    "sent",
    "succeeded",
    "error",
    name="onecexchangebatchstatus",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    event_status.create(bind, checkfirst=True)
    batch_status.create(bind, checkfirst=True)

    op.create_table(
        "onecexchangeevent",
        sa.Column("event_uid", sa.String(36), nullable=False),
        sa.Column("entity_type", sa.String(50), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("payload_version", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("idempotency_key", sa.String(160), nullable=False),
        sa.Column("status", event_status, nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_id", sa.String(100), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_uid"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_index(
        "ix_onecexchangeevent_event_uid",
        "onecexchangeevent",
        ["event_uid"],
        unique=True,
    )
    for column in (
        "entity_type",
        "entity_id",
        "event_type",
        "status",
        "next_attempt_at",
        "external_id",
        "created_at",
        "idempotency_key",
    ):
        op.create_index(
            f"ix_onecexchangeevent_{column}",
            "onecexchangeevent",
            [column],
            unique=column == "idempotency_key",
        )
    op.create_index(
        "idx_onec_event_queue",
        "onecexchangeevent",
        ["status", "next_attempt_at", "created_at"],
    )
    op.create_index(
        "idx_onec_event_entity",
        "onecexchangeevent",
        ["entity_type", "entity_id", "created_at"],
    )

    op.create_table(
        "onecexchangebatch",
        sa.Column("batch_uid", sa.String(36), nullable=False),
        sa.Column("channel", sa.String(32), nullable=False),
        sa.Column("status", batch_status, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_uid"),
    )
    op.create_index(
        "ix_onecexchangebatch_batch_uid",
        "onecexchangebatch",
        ["batch_uid"],
        unique=True,
    )
    for column in ("channel", "status", "created_at"):
        op.create_index(
            f"ix_onecexchangebatch_{column}",
            "onecexchangebatch",
            [column],
        )
    op.create_index(
        "idx_onec_batch_channel_status",
        "onecexchangebatch",
        ["channel", "status", "created_at"],
    )

    op.create_table(
        "onecexchangebatchitem",
        sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["onecexchangebatch.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["event_id"], ["onecexchangeevent.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_id", "event_id", name="uq_onec_exchange_batch_event"),
    )
    op.create_index(
        "ix_onecexchangebatchitem_batch_id",
        "onecexchangebatchitem",
        ["batch_id"],
    )
    op.create_index(
        "ix_onecexchangebatchitem_event_id",
        "onecexchangebatchitem",
        ["event_id"],
    )


def downgrade() -> None:
    op.drop_table("onecexchangebatchitem")
    op.drop_table("onecexchangebatch")
    op.drop_table("onecexchangeevent")
    bind = op.get_bind()
    batch_status.drop(bind, checkfirst=True)
    event_status.drop(bind, checkfirst=True)
