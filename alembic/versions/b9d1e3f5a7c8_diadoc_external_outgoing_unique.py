"""unique external outgoing Diadoc documents

Revision ID: b9d1e3f5a7c8
Revises: a8c0d2e4f6b7
Create Date: 2026-11-22 10:00:00.000000
"""

from alembic import op

revision = "b9d1e3f5a7c8"
down_revision = "a8c0d2e4f6b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Параллельные запуски импорта успели завести документы дважды.
    op.execute(
        """
        DELETE FROM diadocoutgoingdocument a
        USING diadocoutgoingdocument b
        WHERE a.source_type = 'diadoc_external'
          AND b.source_type = 'diadoc_external'
          AND a.environment = b.environment
          AND a.from_box_id_guid = b.from_box_id_guid
          AND a.entity_id = b.entity_id
          AND a.id > b.id
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_diadoc_outgoing_external_entity "
        "ON diadocoutgoingdocument (environment, from_box_id_guid, entity_id) "
        "WHERE source_type = 'diadoc_external'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_diadoc_outgoing_external_entity")
