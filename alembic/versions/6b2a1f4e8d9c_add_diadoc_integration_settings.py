"""add diadoc integration settings

Revision ID: 6b2a1f4e8d9c
Revises: 3cc775719a2a, f7a8b9c0d1e2
Create Date: 2026-05-11 11:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '6b2a1f4e8d9c'
down_revision: Union[str, Sequence[str], None] = (
    '3cc775719a2a',
    'f7a8b9c0d1e2',
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'diadocintegrationsettings',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'environment',
            sa.String(length=32),
            nullable=False,
            server_default='staging',
        ),
        sa.Column('organization_id', sa.String(length=64), nullable=True),
        sa.Column('organization_name', sa.String(length=255), nullable=True),
        sa.Column('organization_inn', sa.String(length=32), nullable=True),
        sa.Column('organization_kpp', sa.String(length=32), nullable=True),
        sa.Column('box_id', sa.String(length=255), nullable=True),
        sa.Column('box_id_guid', sa.String(length=64), nullable=True),
        sa.Column('refresh_token', sa.String(length=4096), nullable=True),
        sa.Column('access_token', sa.String(length=4096), nullable=True),
        sa.Column('token_type', sa.String(length=32), nullable=True),
        sa.Column('token_scope', sa.String(length=512), nullable=True),
        sa.Column(
            'access_token_expires_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column('connected_user_id', sa.String(length=64), nullable=True),
        sa.Column(
            'connected_user_name', sa.String(length=255), nullable=True
        ),
        sa.Column('connected_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_sync_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_error', sa.String(length=2000), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table('diadocintegrationsettings')
