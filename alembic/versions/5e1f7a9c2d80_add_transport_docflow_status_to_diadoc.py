"""add transport (GIS EPD) docflow status to diadoc outgoing document

Revision ID: 5e1f7a9c2d80
Revises: 494c9703a18f
Create Date: 2026-08-02 13:10:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5e1f7a9c2d80"
down_revision: Union[str, None] = "494c9703a18f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("transport_status_named_id", sa.String(64), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("transport_status_type", sa.String(32), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("transport_status_text", sa.String(500), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("transport_mintrans_id", sa.String(120), nullable=True),
    )
    op.add_column(
        "diadocoutgoingdocument",
        sa.Column("transport_carriage_id", sa.String(120), nullable=True),
    )
    op.create_index(
        "ix_diadocoutgoingdocument_transport_mintrans_id",
        "diadocoutgoingdocument",
        ["transport_mintrans_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_diadocoutgoingdocument_transport_mintrans_id",
        table_name="diadocoutgoingdocument",
    )
    op.drop_column("diadocoutgoingdocument", "transport_carriage_id")
    op.drop_column("diadocoutgoingdocument", "transport_mintrans_id")
    op.drop_column("diadocoutgoingdocument", "transport_status_text")
    op.drop_column("diadocoutgoingdocument", "transport_status_type")
    op.drop_column("diadocoutgoingdocument", "transport_status_named_id")
