"""add inbox_email and email_rule_pattern tables

Revision ID: b2c3d4e5f6a1
Revises: 3c2a9d1f5b7e, c6d7e8f9a0b1, c7d8e9f0a1b2, d6a8c0e2f5b1
Create Date: 2026-04-13 12:01:00.000000

"""
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a1'
down_revision: Union[str, None] = (
    '3c2a9d1f5b7e',
    'c6d7e8f9a0b1',
    'c7d8e9f0a1b2',
    'd6a8c0e2f5b1',
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- inboxemail ---
    op.create_table(
        'inboxemail',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email_account_id', sa.Integer(), nullable=False),
        sa.Column('uid', sa.String(length=255), nullable=True),
        sa.Column('folder', sa.String(length=255), nullable=True),
        sa.Column('from_email', sa.String(length=255), nullable=False),
        sa.Column('from_name', sa.String(length=255), nullable=True),
        sa.Column('subject', sa.String(length=1000), nullable=True),
        sa.Column('body_preview', sa.String(length=500), nullable=True),
        sa.Column('body_full', sa.Text(), nullable=True),
        sa.Column('has_attachments', sa.Boolean(), nullable=True),
        sa.Column('attachment_info', sa.JSON(), nullable=True),
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('rule_type', sa.String(length=64), nullable=True),
        sa.Column('rule_set_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('rule_set_by_id', sa.Integer(), nullable=True),
        sa.Column('rule_auto_detected', sa.Boolean(), nullable=True),
        sa.Column('processed', sa.Boolean(), nullable=True),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('processing_result', sa.JSON(), nullable=True),
        sa.Column('processing_error', sa.String(length=1000), nullable=True),
        sa.ForeignKeyConstraint(
            ['email_account_id'],
            ['emailaccount.id'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['rule_set_by_id'],
            ['app_user.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_inboxemail_email_account_id',
        'inboxemail',
        ['email_account_id'],
    )
    op.create_index(
        'ix_inboxemail_from_email',
        'inboxemail',
        ['from_email'],
    )
    op.create_index(
        'ix_inboxemail_rule_type',
        'inboxemail',
        ['rule_type'],
    )
    op.create_index(
        'ix_inboxemail_processed',
        'inboxemail',
        ['processed'],
    )
    op.create_index(
        'ix_inboxemail_fetched_at',
        'inboxemail',
        ['fetched_at'],
    )

    # --- emailrulepattern ---
    op.create_table(
        'emailrulepattern',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email_account_id', sa.Integer(), nullable=True),
        sa.Column('from_email_pattern', sa.String(length=255), nullable=True),
        sa.Column('from_domain_pattern', sa.String(length=255), nullable=True),
        sa.Column('subject_keywords', sa.JSON(), nullable=True),
        sa.Column('requires_attachments', sa.Boolean(), nullable=True),
        sa.Column('attachment_extensions', sa.JSON(), nullable=True),
        sa.Column('rule_type', sa.String(length=64), nullable=False),
        sa.Column('times_applied', sa.Integer(), nullable=True),
        sa.Column('times_confirmed', sa.Integer(), nullable=True),
        sa.Column('created_by_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(
            ['email_account_id'],
            ['emailaccount.id'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['created_by_id'],
            ['app_user.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_emailrulepattern_email_account_id',
        'emailrulepattern',
        ['email_account_id'],
    )


def downgrade() -> None:
    op.drop_index('ix_emailrulepattern_email_account_id', table_name='emailrulepattern')
    op.drop_table('emailrulepattern')

    op.drop_index('ix_inboxemail_fetched_at', table_name='inboxemail')
    op.drop_index('ix_inboxemail_processed', table_name='inboxemail')
    op.drop_index('ix_inboxemail_rule_type', table_name='inboxemail')
    op.drop_index('ix_inboxemail_from_email', table_name='inboxemail')
    op.drop_index('ix_inboxemail_email_account_id', table_name='inboxemail')
    op.drop_table('inboxemail')
