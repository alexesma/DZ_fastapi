"""Reserve marking codes until the GIS MT withdrawal is confirmed."""

from alembic import op

revision = "da180ce6f4b1"
down_revision = "e4b2c71a9d03"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE markingcodestatus ADD VALUE IF NOT EXISTS 'withdrawal_pending'")


def downgrade():
    # An older application cannot safely manage codes already reserved for withdrawal.
    op.execute(
        """
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM productmarkingcode WHERE status::text = 'withdrawal_pending') THEN
                RAISE EXCEPTION 'Resolve pending GIS withdrawals before downgrading';
            END IF;
        END $$
    """
    )
