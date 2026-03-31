"""create app notification

Revision ID: b1c2d3e4f5a6
Revises: aa13bb24cc35
Create Date: 2026-03-31 10:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b1c2d3e4f5a6'
down_revision = 'aa13bb24cc35'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'app_notification',
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(length=255), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('level', sa.String(length=16), nullable=False),
        sa.Column('link', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('read_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['app_user.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_app_notification_user_id'),
        'app_notification',
        ['user_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_app_notification_user_id'), table_name='app_notification')
    op.drop_table('app_notification')
