"""add supplier response config table

Revision ID: d6a8c0e2f5b1
Revises: cb8f1a2d3e45
Create Date: 2026-04-09 12:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd6a8c0e2f5b1'
down_revision = 'cb8f1a2d3e45'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'supplierresponseconfig',
        sa.Column('provider_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column(
            'is_active',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column('inbox_email_account_id', sa.Integer(), nullable=True),
        sa.Column('sender_emails', sa.JSON(), nullable=True),
        sa.Column(
            'response_type',
            sa.String(length=16),
            nullable=False,
            server_default='file',
        ),
        sa.Column(
            'process_shipping_docs',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column('file_format', sa.String(length=16), nullable=True),
        sa.Column('filename_pattern', sa.String(length=255), nullable=True),
        sa.Column(
            'shipping_doc_filename_pattern',
            sa.String(length=255),
            nullable=True,
        ),
        sa.Column(
            'start_row',
            sa.Integer(),
            nullable=False,
            server_default='1',
        ),
        sa.Column('oem_col', sa.Integer(), nullable=True),
        sa.Column('brand_col', sa.Integer(), nullable=True),
        sa.Column('qty_col', sa.Integer(), nullable=True),
        sa.Column('status_col', sa.Integer(), nullable=True),
        sa.Column('comment_col', sa.Integer(), nullable=True),
        sa.Column('price_col', sa.Integer(), nullable=True),
        sa.Column('confirm_keywords', sa.JSON(), nullable=True),
        sa.Column('reject_keywords', sa.JSON(), nullable=True),
        sa.Column(
            'value_after_article_type',
            sa.String(length=16),
            nullable=False,
            server_default='both',
        ),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['inbox_email_account_id'],
            ['emailaccount.id'],
        ),
        sa.ForeignKeyConstraint(
            ['provider_id'],
            ['provider.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_supplierresponseconfig_provider_id',
        'supplierresponseconfig',
        ['provider_id'],
        unique=False,
    )
    op.create_index(
        'ix_supplierresponseconfig_inbox_email_account_id',
        'supplierresponseconfig',
        ['inbox_email_account_id'],
        unique=False,
    )


def downgrade():
    op.drop_index(
        'ix_supplierresponseconfig_inbox_email_account_id',
        table_name='supplierresponseconfig',
    )
    op.drop_index(
        'ix_supplierresponseconfig_provider_id',
        table_name='supplierresponseconfig',
    )
    op.drop_table('supplierresponseconfig')
