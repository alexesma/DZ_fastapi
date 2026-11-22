"""details of imported (Diadoc UPD) shipment lines

Revision ID: d1f3a5b7c9e0
Revises: c0e2f4a6b8d9
Create Date: 2026-11-22 15:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "d1f3a5b7c9e0"
down_revision = "c0e2f4a6b8d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("shipmentdocumentitem", sa.Column("import_article", sa.String(255), nullable=True))
    op.add_column("shipmentdocumentitem", sa.Column("import_match", sa.String(16), nullable=True))
    op.add_column("shipmentdocumentitem", sa.Column("import_gtd_number", sa.String(64), nullable=True))
    op.add_column("shipmentdocumentitem", sa.Column("import_country_code", sa.String(16), nullable=True))
    op.add_column("shipmentdocumentitem", sa.Column("import_country_name", sa.String(120), nullable=True))
    op.add_column("shipmentdocumentitem", sa.Column("import_marking_codes", sa.JSON(), nullable=True))
    op.add_column("shipmentdocumentitem", sa.Column("import_vat_amount", sa.DECIMAL(14, 2), nullable=True))
    op.add_column("shipmentdocument", sa.Column("import_review", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("shipmentdocument", "import_review")
    for name in (
        "import_vat_amount",
        "import_marking_codes",
        "import_country_name",
        "import_country_code",
        "import_gtd_number",
        "import_match",
        "import_article",
    ):
        op.drop_column("shipmentdocumentitem", name)
