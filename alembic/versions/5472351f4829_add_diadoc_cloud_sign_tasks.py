"""add diadoc cloud sign tasks and incoming signed_at

Revision ID: 5472351f4829
Revises: f24bcb962bd7
Create Date: 2026-07-03 14:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5472351f4829"
down_revision: Union[str, Sequence[str], None] = "f24bcb962bd7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "diadoccloudsigntask",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "environment",
            sa.String(32),
            nullable=False,
            server_default="staging",
        ),
        sa.Column("operation", sa.String(32), nullable=False),
        sa.Column(
            "state",
            sa.String(32),
            nullable=False,
            server_default="waiting_code",
        ),
        sa.Column("box_id_guid", sa.String(64), nullable=False),
        sa.Column("message_id", sa.String(255), nullable=True),
        sa.Column("entity_id", sa.String(255), nullable=True),
        sa.Column(
            "incoming_document_id",
            sa.Integer(),
            sa.ForeignKey(
                "diadocincomingdocument.id", ondelete="SET NULL"
            ),
            nullable=True,
        ),
        sa.Column(
            "outgoing_document_id",
            sa.Integer(),
            sa.ForeignKey(
                "diadocoutgoingdocument.id", ondelete="SET NULL"
            ),
            nullable=True,
        ),
        sa.Column("cloud_sign_token", sa.String(512), nullable=True),
        sa.Column("files", sa.JSON(), nullable=True),
        sa.Column("params", sa.JSON(), nullable=True),
        sa.Column("error_details", sa.String(2000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "completed_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.create_index(
        "ix_diadoccloudsigntask_incoming_document_id",
        "diadoccloudsigntask",
        ["incoming_document_id"],
    )
    op.create_index(
        "ix_diadoccloudsigntask_outgoing_document_id",
        "diadoccloudsigntask",
        ["outgoing_document_id"],
    )
    op.add_column(
        "diadocincomingdocument",
        sa.Column("signed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("diadocincomingdocument", "signed_at")
    op.drop_index(
        "ix_diadoccloudsigntask_outgoing_document_id",
        table_name="diadoccloudsigntask",
    )
    op.drop_index(
        "ix_diadoccloudsigntask_incoming_document_id",
        table_name="diadoccloudsigntask",
    )
    op.drop_table("diadoccloudsigntask")
