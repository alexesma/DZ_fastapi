"""add diadoc incoming documents

Revision ID: 8c4f2e1a9b7d
Revises: 6b2a1f4e8d9c
Create Date: 2026-05-11 12:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '8c4f2e1a9b7d'
down_revision: Union[str, Sequence[str], None] = '6b2a1f4e8d9c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'diadocincomingdocument',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('environment', sa.String(length=32), nullable=False),
        sa.Column('box_id_guid', sa.String(length=64), nullable=False),
        sa.Column('message_id', sa.String(length=255), nullable=False),
        sa.Column('entity_id', sa.String(length=255), nullable=False),
        sa.Column('index_key', sa.String(length=255), nullable=True),
        sa.Column(
            'counteragent_box_id',
            sa.String(length=255),
            nullable=True,
        ),
        sa.Column('file_name', sa.String(length=500), nullable=True),
        sa.Column('document_number', sa.String(length=120), nullable=True),
        sa.Column('document_date', sa.Date(), nullable=True),
        sa.Column(
            'delivery_timestamp_ticks', sa.BigInteger(), nullable=True
        ),
        sa.Column('send_timestamp_ticks', sa.BigInteger(), nullable=True),
        sa.Column(
            'delivery_at', sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'provider_id',
            sa.Integer(),
            sa.ForeignKey('provider.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'supplier_order_message_id',
            sa.Integer(),
            sa.ForeignKey('supplierordermessage.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column('local_file_path', sa.String(length=1024), nullable=True),
        sa.Column('content_sha256', sa.String(length=64), nullable=True),
        sa.Column(
            'status',
            sa.String(length=32),
            nullable=False,
            server_default='synced',
        ),
        sa.Column(
            'import_error_details',
            sa.String(length=2000),
            nullable=True,
        ),
        sa.Column('raw_metadata', sa.JSON(), nullable=True),
        sa.Column('synced_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'registered_at', sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            'environment',
            'box_id_guid',
            'message_id',
            'entity_id',
            name='uq_diadoc_incoming_document_source',
        ),
    )
    op.create_index(
        'ix_diadocincomingdocument_box_id_guid',
        'diadocincomingdocument',
        ['box_id_guid'],
    )
    op.create_index(
        'ix_diadocincomingdocument_counteragent_box_id',
        'diadocincomingdocument',
        ['counteragent_box_id'],
    )
    op.create_index(
        'ix_diadocincomingdocument_document_number',
        'diadocincomingdocument',
        ['document_number'],
    )
    op.create_index(
        'ix_diadocincomingdocument_document_date',
        'diadocincomingdocument',
        ['document_date'],
    )
    op.create_index(
        'ix_diadocincomingdocument_provider_id',
        'diadocincomingdocument',
        ['provider_id'],
    )
    op.create_index(
        'ix_diadocincomingdocument_supplier_order_message_id',
        'diadocincomingdocument',
        ['supplier_order_message_id'],
    )
    op.create_index(
        'ix_diadocincomingdocument_content_sha256',
        'diadocincomingdocument',
        ['content_sha256'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_diadocincomingdocument_content_sha256',
        table_name='diadocincomingdocument',
    )
    op.drop_index(
        'ix_diadocincomingdocument_supplier_order_message_id',
        table_name='diadocincomingdocument',
    )
    op.drop_index(
        'ix_diadocincomingdocument_provider_id',
        table_name='diadocincomingdocument',
    )
    op.drop_index(
        'ix_diadocincomingdocument_document_date',
        table_name='diadocincomingdocument',
    )
    op.drop_index(
        'ix_diadocincomingdocument_document_number',
        table_name='diadocincomingdocument',
    )
    op.drop_index(
        'ix_diadocincomingdocument_counteragent_box_id',
        table_name='diadocincomingdocument',
    )
    op.drop_index(
        'ix_diadocincomingdocument_box_id_guid',
        table_name='diadocincomingdocument',
    )
    op.drop_table('diadocincomingdocument')
