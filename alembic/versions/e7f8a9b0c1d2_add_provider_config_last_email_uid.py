"""Add provider-config last email UID tracking

Revision ID: e7f8a9b0c1d2
Revises: d1e2f3a4b5c6
Create Date: 2026-03-27 21:30:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'e7f8a9b0c1d2'
down_revision = 'd1e2f3a4b5c6'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'providerconfiglastemailuid',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('provider_config_id', sa.Integer(), nullable=False),
        sa.Column('last_uid', sa.Integer(), nullable=False, server_default='0'),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ['provider_config_id'],
            ['providerpricelistconfig.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider_config_id'),
    )
    op.execute(
        """
        INSERT INTO providerconfiglastemailuid
            (provider_config_id, last_uid, updated_at)
        SELECT
            cfg.id,
            legacy.last_uid,
            legacy.updated_at
        FROM providerpricelistconfig AS cfg
        JOIN providerlastemailuid AS legacy
          ON legacy.provider_id = cfg.provider_id
        """
    )
    op.alter_column(
        'providerconfiglastemailuid',
        'last_uid',
        server_default=None,
    )


def downgrade():
    op.drop_table('providerconfiglastemailuid')
