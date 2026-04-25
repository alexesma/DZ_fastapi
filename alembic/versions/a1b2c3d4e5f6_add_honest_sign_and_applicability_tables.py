"""add honest_sign and applicability relational tables

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-04-25 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'f1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── HonestSignCategory ────────────────────────────────────────────────
    op.create_table(
        'honestsigncategory',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column('code', sa.String(50), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', name='uq_honestsigncategory_name'),
        sa.UniqueConstraint('code', name='uq_honestsigncategory_code'),
    )
    op.create_index('ix_honestsigncategory_name', 'honestsigncategory', ['name'])
    op.create_index('ix_honestsigncategory_code', 'honestsigncategory', ['code'])

    # ── ApplicabilityNode ─────────────────────────────────────────────────
    op.create_table(
        'applicabilitynode',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('name', sa.String(300), nullable=False),
        sa.Column('node_type', sa.String(50), nullable=False, server_default='other'),
        sa.Column('parent_id', sa.Integer(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ['parent_id'], ['applicabilitynode.id'], ondelete='SET NULL'
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_applicabilitynode_name', 'applicabilitynode', ['name'])
    op.create_index('ix_applicabilitynode_parent_id', 'applicabilitynode', ['parent_id'])

    # ── M2M: autopart ↔ HonestSignCategory ───────────────────────────────
    op.create_table(
        'autopart_honest_sign_association',
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('honest_sign_category_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['autopart_id'], ['autopart.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['honest_sign_category_id'],
            ['honestsigncategory.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('autopart_id', 'honest_sign_category_id'),
    )

    # ── M2M: autopart ↔ ApplicabilityNode ────────────────────────────────
    op.create_table(
        'autopart_applicability_association',
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('applicability_node_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['autopart_id'], ['autopart.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['applicability_node_id'],
            ['applicabilitynode.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('autopart_id', 'applicability_node_id'),
    )


def downgrade() -> None:
    op.drop_table('autopart_applicability_association')
    op.drop_table('autopart_honest_sign_association')
    op.drop_index('ix_applicabilitynode_parent_id', 'applicabilitynode')
    op.drop_index('ix_applicabilitynode_name', 'applicabilitynode')
    op.drop_table('applicabilitynode')
    op.drop_index('ix_honestsigncategory_code', 'honestsigncategory')
    op.drop_index('ix_honestsigncategory_name', 'honestsigncategory')
    op.drop_table('honestsigncategory')
