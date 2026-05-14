"""add diadoc scheduler settings and outgoing documents

Revision ID: ae1d9c3b4f2a
Revises: 8c4f2e1a9b7d
Create Date: 2026-05-11 18:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'ae1d9c3b4f2a'
down_revision: Union[str, Sequence[str], None] = '8c4f2e1a9b7d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'diadocintegrationsettings',
        sa.Column(
            'inbound_sync_enabled',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column(
            'inbound_sync_count',
            sa.Integer(),
            nullable=False,
            server_default='50',
        ),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column(
            'inbound_download_content',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        'diadocintegrationsettings',
        sa.Column(
            'inbound_process_enabled',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )

    op.create_table(
        'customerexternalreference',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'customer_id',
            sa.Integer(),
            sa.ForeignKey('customer.id', ondelete='CASCADE'),
            nullable=False,
        ),
        sa.Column('source_system', sa.String(length=32), nullable=False),
        sa.Column('external_customer_id', sa.BigInteger(), nullable=True),
        sa.Column('external_customer_name', sa.String(length=255), nullable=True),
        sa.Column(
            'is_active',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            'source_system',
            'external_customer_id',
            name='uq_customer_external_reference_source_customer',
        ),
    )
    op.create_index(
        'ix_customerexternalreference_customer_id',
        'customerexternalreference',
        ['customer_id'],
    )
    op.create_index(
        'ix_customerexternalreference_source_system',
        'customerexternalreference',
        ['source_system'],
    )
    op.create_index(
        'ix_customerexternalreference_external_customer_id',
        'customerexternalreference',
        ['external_customer_id'],
    )

    op.create_table(
        'diadocoutgoingdocument',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('environment', sa.String(length=32), nullable=False),
        sa.Column('from_box_id_guid', sa.String(length=64), nullable=False),
        sa.Column('to_box_id_guid', sa.String(length=64), nullable=False),
        sa.Column(
            'customer_id',
            sa.Integer(),
            sa.ForeignKey('customer.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'provider_id',
            sa.Integer(),
            sa.ForeignKey('provider.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column('source_type', sa.String(length=64), nullable=True),
        sa.Column('source_id', sa.Integer(), nullable=True),
        sa.Column('type_named_id', sa.String(length=120), nullable=False),
        sa.Column('document_function', sa.String(length=120), nullable=True),
        sa.Column('document_version', sa.String(length=120), nullable=True),
        sa.Column('file_name', sa.String(length=500), nullable=False),
        sa.Column('document_number', sa.String(length=120), nullable=True),
        sa.Column('document_date', sa.Date(), nullable=True),
        sa.Column('local_file_path', sa.String(length=1024), nullable=False),
        sa.Column('content_sha256', sa.String(length=64), nullable=True),
        sa.Column('comment', sa.String(length=5000), nullable=True),
        sa.Column(
            'need_recipient_signature',
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            'need_receipt',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column(
            'is_draft',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column('message_id', sa.String(length=255), nullable=True),
        sa.Column('entity_id', sa.String(length=255), nullable=True),
        sa.Column(
            'status',
            sa.String(length=32),
            nullable=False,
            server_default='draft',
        ),
        sa.Column('error_details', sa.String(length=2000), nullable=True),
        sa.Column('metadata', sa.JSON(), nullable=True),
        sa.Column('raw_response', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        'ix_diadocoutgoingdocument_from_box_id_guid',
        'diadocoutgoingdocument',
        ['from_box_id_guid'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_to_box_id_guid',
        'diadocoutgoingdocument',
        ['to_box_id_guid'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_customer_id',
        'diadocoutgoingdocument',
        ['customer_id'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_provider_id',
        'diadocoutgoingdocument',
        ['provider_id'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_source_type',
        'diadocoutgoingdocument',
        ['source_type'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_source_id',
        'diadocoutgoingdocument',
        ['source_id'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_document_number',
        'diadocoutgoingdocument',
        ['document_number'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_document_date',
        'diadocoutgoingdocument',
        ['document_date'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_content_sha256',
        'diadocoutgoingdocument',
        ['content_sha256'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_message_id',
        'diadocoutgoingdocument',
        ['message_id'],
    )
    op.create_index(
        'ix_diadocoutgoingdocument_entity_id',
        'diadocoutgoingdocument',
        ['entity_id'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_diadocoutgoingdocument_entity_id',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_message_id',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_content_sha256',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_document_date',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_document_number',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_source_id',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_source_type',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_provider_id',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_to_box_id_guid',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_customer_id',
        table_name='diadocoutgoingdocument',
    )
    op.drop_index(
        'ix_diadocoutgoingdocument_from_box_id_guid',
        table_name='diadocoutgoingdocument',
    )
    op.drop_table('diadocoutgoingdocument')

    op.drop_index(
        'ix_customerexternalreference_external_customer_id',
        table_name='customerexternalreference',
    )
    op.drop_index(
        'ix_customerexternalreference_source_system',
        table_name='customerexternalreference',
    )
    op.drop_index(
        'ix_customerexternalreference_customer_id',
        table_name='customerexternalreference',
    )
    op.drop_table('customerexternalreference')

    op.drop_column('diadocintegrationsettings', 'inbound_process_enabled')
    op.drop_column('diadocintegrationsettings', 'inbound_download_content')
    op.drop_column('diadocintegrationsettings', 'inbound_sync_count')
    op.drop_column('diadocintegrationsettings', 'inbound_sync_enabled')
