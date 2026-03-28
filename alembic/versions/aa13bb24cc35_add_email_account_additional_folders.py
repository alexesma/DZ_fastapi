"""add email account additional folders

Revision ID: aa13bb24cc35
Revises: c9d0e1f2a3b4
Create Date: 2026-03-28 22:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa13bb24cc35'
down_revision = 'c9d0e1f2a3b4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'emailaccount',
        sa.Column('imap_additional_folders', sa.JSON(), nullable=True),
    )
    op.add_column(
        'providerlastemailuid',
        sa.Column('folder_last_uids', sa.JSON(), nullable=True),
    )
    op.add_column(
        'providerconfiglastemailuid',
        sa.Column('folder_last_uids', sa.JSON(), nullable=True),
    )
    op.add_column(
        'customerorderconfig',
        sa.Column('folder_last_uids', sa.JSON(), nullable=True),
    )
    op.execute(
        """
        UPDATE emailaccount
        SET imap_folder = 'INBOX'
        WHERE imap_folder IS NULL OR btrim(imap_folder) = ''
        """
    )
    op.execute(
        """
        UPDATE providerlastemailuid
        SET folder_last_uids = '{}'::json
        WHERE folder_last_uids IS NULL
        """
    )
    op.execute(
        """
        UPDATE providerconfiglastemailuid
        SET folder_last_uids = '{}'::json
        WHERE folder_last_uids IS NULL
        """
    )
    op.execute(
        """
        UPDATE customerorderconfig
        SET folder_last_uids = '{}'::json
        WHERE folder_last_uids IS NULL
        """
    )
    op.alter_column(
        'emailaccount',
        'imap_folder',
        existing_type=sa.String(length=255),
        server_default='INBOX',
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        'emailaccount',
        'imap_folder',
        existing_type=sa.String(length=255),
        server_default=None,
        existing_nullable=True,
    )
    op.drop_column('customerorderconfig', 'folder_last_uids')
    op.drop_column('providerconfiglastemailuid', 'folder_last_uids')
    op.drop_column('providerlastemailuid', 'folder_last_uids')
    op.drop_column('emailaccount', 'imap_additional_folders')
