"""add certificates, autopart M2M and certification exemption rules

Revision ID: b6c2f8d41e37
Revises: a3d7e0c15f84
Create Date: 2026-08-05 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b6c2f8d41e37"
down_revision: Union[str, None] = "a3d7e0c15f84"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "certificate",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("number", sa.String(150), nullable=False),
        sa.Column("url", sa.String(500), nullable=True),
        sa.Column(
            "brand_id",
            sa.Integer(),
            sa.ForeignKey("brand.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "covers_whole_brand",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_until", sa.Date(), nullable=True),
        sa.Column("applicant", sa.String(500), nullable=True),
        sa.Column("manufacturer", sa.String(500), nullable=True),
        sa.Column("scope", sa.Text(), nullable=True),
        sa.Column("source", sa.String(32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("number", name="uq_certificate_number"),
    )
    op.create_index("ix_certificate_number", "certificate", ["number"])
    op.create_index("ix_certificate_brand_id", "certificate", ["brand_id"])
    op.create_index(
        "ix_certificate_brand_whole",
        "certificate",
        ["brand_id", "covers_whole_brand"],
    )

    op.create_table(
        "autopart_certificate_association",
        sa.Column(
            "autopart_id",
            sa.Integer(),
            sa.ForeignKey("autopart.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "certificate_id",
            sa.Integer(),
            sa.ForeignKey("certificate.id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )

    op.create_table(
        "certificationexemptionrule",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pattern", sa.String(255), nullable=False),
        sa.Column("normalized_pattern", sa.String(255), nullable=False),
        sa.Column(
            "certification_required",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column(
            "is_active", sa.Boolean(), nullable=False, server_default=sa.true()
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "normalized_pattern",
            "certification_required",
            name="uq_certification_exemption_pattern",
        ),
    )
    op.create_index(
        "ix_certificationexemptionrule_normalized_pattern",
        "certificationexemptionrule",
        ["normalized_pattern"],
    )


def downgrade() -> None:
    op.drop_table("certificationexemptionrule")
    op.drop_table("autopart_certificate_association")
    op.drop_index("ix_certificate_brand_whole", table_name="certificate")
    op.drop_index("ix_certificate_brand_id", table_name="certificate")
    op.drop_index("ix_certificate_number", table_name="certificate")
    op.drop_table("certificate")
