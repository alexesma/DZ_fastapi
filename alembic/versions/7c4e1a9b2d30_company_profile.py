"""Add company profile (our legal details for documents).

Revision ID: 7c4e1a9b2d30
Revises: 6b9d2e4f7a10
"""

import sqlalchemy as sa

from alembic import op

revision = "7c4e1a9b2d30"
down_revision = "6b9d2e4f7a10"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "companyprofile",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "is_individual_entrepreneur", sa.Boolean(), nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("short_name", sa.String(255)),
        sa.Column("full_name", sa.String(512)),
        sa.Column("inn", sa.String(12)),
        sa.Column("kpp", sa.String(9)),
        sa.Column("ogrn", sa.String(15)),
        sa.Column("ogrn_assigned_at", sa.Date()),
        sa.Column("okpo", sa.String(14)),
        sa.Column("legal_address", sa.Text()),
        sa.Column("postal_address", sa.Text()),
        sa.Column("shipping_address", sa.Text()),
        sa.Column("phone", sa.String(64)),
        sa.Column("email", sa.String(255)),
        sa.Column("website", sa.String(255)),
        sa.Column("bank_name", sa.String(255)),
        sa.Column("bank_bik", sa.String(9)),
        sa.Column("bank_account", sa.String(20)),
        sa.Column("bank_corr_account", sa.String(20)),
        sa.Column("director_position", sa.String(255)),
        sa.Column("director_name", sa.String(255)),
        sa.Column("director_basis", sa.String(255)),
        sa.Column("accountant_name", sa.String(255)),
        sa.Column(
            "vat_payer", sa.Boolean(), nullable=False, server_default=sa.true()
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )


def downgrade():
    op.drop_table("companyprofile")
