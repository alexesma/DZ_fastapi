"""add reject reason to customer order item

Revision ID: c9d0e1f2a3b4
Revises: b8c9d0e1f2a3
Create Date: 2026-03-28 20:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c9d0e1f2a3b4'
down_revision = 'b8c9d0e1f2a3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'customerorderitem',
        sa.Column('reject_reason_code', sa.String(length=64), nullable=True),
    )
    op.add_column(
        'customerorderitem',
        sa.Column('reject_reason_text', sa.String(length=500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('customerorderitem', 'reject_reason_text')
    op.drop_column('customerorderitem', 'reject_reason_code')
