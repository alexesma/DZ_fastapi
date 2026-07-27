"""Allow one reclamation email for several legal entities.

Revision ID: ff66aa77bb88
Revises: ee55ff66aa77
"""

from typing import Sequence, Union

from alembic import op

revision: str = "ff66aa77bb88"
down_revision: Union[str, Sequence[str], None] = "ee55ff66aa77"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_customer_reclamation_email",
        "customerreclamationemail",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_customer_reclamation_customer_email",
        "customerreclamationemail",
        ["customer_id", "email"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_customer_reclamation_customer_email",
        "customerreclamationemail",
        type_="unique",
    )
    # This intentionally fails when an email is shared by several customers:
    # silently deleting legal-entity bindings during rollback is unsafe.
    op.create_unique_constraint(
        "uq_customer_reclamation_email",
        "customerreclamationemail",
        ["email"],
    )
