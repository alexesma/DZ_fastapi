"""add customer return UKD draft fields and VAT snapshots

Revision ID: 6a2c8e4f1b90
Revises: 5e1f7a9c2d80
Create Date: 2026-08-02 18:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "6a2c8e4f1b90"
down_revision: Union[str, None] = "5e1f7a9c2d80"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "shipmentdocumentitem",
        sa.Column(
            "vat_rate",
            sa.Numeric(5, 2),
            nullable=False,
            server_default="22.00",
        ),
    )
    op.add_column(
        "returnitem",
        sa.Column(
            "vat_rate",
            sa.Numeric(5, 2),
            nullable=False,
            server_default="22.00",
        ),
    )
    op.add_column(
        "returnitem",
        sa.Column(
            "price_includes_vat",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )

    op.add_column(
        "returnfromcustomer",
        sa.Column(
            "source_diadoc_outgoing_document_id",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("source_kind", sa.String(64), nullable=True),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("external_document_number", sa.String(120), nullable=True),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("external_document_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("source_document_number", sa.String(120), nullable=True),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("source_document_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("source_file_name", sa.String(512), nullable=True),
    )
    op.add_column(
        "returnfromcustomer",
        sa.Column("source_file_sha256", sa.String(64), nullable=True),
    )
    op.create_foreign_key(
        "fk_returnfromcustomer_source_diadoc_outgoing",
        "returnfromcustomer",
        "diadocoutgoingdocument",
        ["source_diadoc_outgoing_document_id"],
        ["id"],
        ondelete="SET NULL",
    )
    for name, columns, unique in (
        (
            "ix_returnfromcustomer_source_diadoc_outgoing_document_id",
            ["source_diadoc_outgoing_document_id"],
            False,
        ),
        ("ix_returnfromcustomer_source_kind", ["source_kind"], False),
        (
            "ix_returnfromcustomer_external_document_number",
            ["external_document_number"],
            False,
        ),
        (
            "ix_returnfromcustomer_external_document_date",
            ["external_document_date"],
            False,
        ),
        (
            "ix_returnfromcustomer_source_document_number",
            ["source_document_number"],
            False,
        ),
        (
            "ix_returnfromcustomer_source_document_date",
            ["source_document_date"],
            False,
        ),
        (
            "ix_returnfromcustomer_source_file_sha256",
            ["source_file_sha256"],
            True,
        ),
    ):
        op.create_index(name, "returnfromcustomer", columns, unique=unique)


def downgrade() -> None:
    for name in (
        "ix_returnfromcustomer_source_file_sha256",
        "ix_returnfromcustomer_source_document_date",
        "ix_returnfromcustomer_source_document_number",
        "ix_returnfromcustomer_external_document_date",
        "ix_returnfromcustomer_external_document_number",
        "ix_returnfromcustomer_source_kind",
        "ix_returnfromcustomer_source_diadoc_outgoing_document_id",
    ):
        op.drop_index(name, table_name="returnfromcustomer")
    op.drop_constraint(
        "fk_returnfromcustomer_source_diadoc_outgoing",
        "returnfromcustomer",
        type_="foreignkey",
    )
    for column in (
        "source_file_sha256",
        "source_file_name",
        "source_document_date",
        "source_document_number",
        "external_document_date",
        "external_document_number",
        "source_kind",
        "source_diadoc_outgoing_document_id",
    ):
        op.drop_column("returnfromcustomer", column)
    op.drop_column("returnitem", "price_includes_vat")
    op.drop_column("returnitem", "vat_rate")
    op.drop_column("shipmentdocumentitem", "vat_rate")
