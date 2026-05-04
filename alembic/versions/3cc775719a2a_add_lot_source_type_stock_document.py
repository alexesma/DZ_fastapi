"""Add LotSourceType, SyncStatus, StockDocument models; extend StockLot/StockMovement

Adds:
  - lotsourcetype enum + stocklot.source_type column
  - syncstatus enum + stocklot.sync_status, stocklot.external_id,
    stocklot.source_document_item_id
  - stockmovement.external_id, stockmovement.operation_uid
  - movementtype: add WRITEOFF value
  - stockdocumenttype enum
  - stockdocumentstatus enum
  - stockdocument table
  - stockdocumentitem table

Uses raw SQL for table/column DDL to avoid SQLAlchemy's enum creation events
firing on types that already exist (created by create_all on app startup).

Revision ID: 3cc775719a2a
Revises: a91915cdcc87
Create Date: 2026-05-01 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op

revision: str = '3cc775719a2a'
down_revision: Union[str, None] = 'a91915cdcc87'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Enum types (idempotent) ───────────────────────────────────────────────

    op.execute("""
        DO $$ BEGIN
            CREATE TYPE lotsourcetype AS ENUM (
                'receipt', 'transfer', 'manual',
                'opening_balance', 'inventory_correction'
            );
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    op.execute("""
        DO $$ BEGIN
            CREATE TYPE syncstatus AS ENUM ('pending', 'synced', 'error');
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    op.execute("""
        DO $$ BEGIN
            CREATE TYPE stockdocumenttype AS ENUM (
                'manual_receipt', 'manual_writeoff'
            );
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    op.execute("""
        DO $$ BEGIN
            CREATE TYPE stockdocumentstatus AS ENUM (
                'draft', 'posted', 'cancelled'
            );
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    # Add WRITEOFF to existing movementtype enum
    op.execute(
        "ALTER TYPE movementtype ADD VALUE IF NOT EXISTS 'writeoff'"
    )

    # ── stockdocument table ──────────────────────────────────────────────────
    # Raw SQL: avoids SQLAlchemy's before_create enum-recreation event

    op.execute("""
        CREATE TABLE IF NOT EXISTS stockdocument (
            id          SERIAL PRIMARY KEY,
            doc_type    stockdocumenttype    NOT NULL,
            status      stockdocumentstatus  NOT NULL DEFAULT 'draft',
            document_number VARCHAR(100),
            document_date   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            warehouse_id    INTEGER REFERENCES warehouse(id) ON DELETE SET NULL,
            reason      VARCHAR(255),
            notes       TEXT,
            external_id VARCHAR(100),
            sync_status syncstatus NOT NULL DEFAULT 'pending',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            posted_at   TIMESTAMPTZ
        )
    """)

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockdocument_doc_type "
        "ON stockdocument (doc_type)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockdocument_document_number "
        "ON stockdocument (document_number)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockdocument_external_id "
        "ON stockdocument (external_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockdocument_warehouse_id "
        "ON stockdocument (warehouse_id)"
    )

    # ── stockdocumentitem table ──────────────────────────────────────────────

    op.execute("""
        CREATE TABLE IF NOT EXISTS stockdocumentitem (
            id                  SERIAL PRIMARY KEY,
            document_id         INTEGER NOT NULL
                                REFERENCES stockdocument(id) ON DELETE CASCADE,
            autopart_id         INTEGER NOT NULL
                                REFERENCES autopart(id) ON DELETE CASCADE,
            storage_location_id INTEGER
                                REFERENCES storagelocation(id) ON DELETE SET NULL,
            quantity            INTEGER NOT NULL,
            gtd_number          VARCHAR(64),
            country_code        VARCHAR(16),
            country_name        VARCHAR(120),
            lot_id              INTEGER
                                REFERENCES stocklot(id) ON DELETE SET NULL,
            notes               TEXT
        )
    """)

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockdocumentitem_document_id "
        "ON stockdocumentitem (document_id)"
    )

    # ── Extend stocklot ──────────────────────────────────────────────────────

    op.execute("""
        ALTER TABLE stocklot
            ADD COLUMN IF NOT EXISTS
                source_type lotsourcetype NOT NULL DEFAULT 'receipt',
            ADD COLUMN IF NOT EXISTS
                external_id VARCHAR(100),
            ADD COLUMN IF NOT EXISTS
                sync_status syncstatus NOT NULL DEFAULT 'pending',
            ADD COLUMN IF NOT EXISTS
                source_document_item_id INTEGER
                    REFERENCES stockdocumentitem(id) ON DELETE SET NULL
    """)

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stocklot_source_type "
        "ON stocklot (source_type)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stocklot_external_id "
        "ON stocklot (external_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stocklot_source_document_item_id "
        "ON stocklot (source_document_item_id)"
    )

    # ── Extend stockmovement ─────────────────────────────────────────────────

    op.execute("""
        ALTER TABLE stockmovement
            ADD COLUMN IF NOT EXISTS external_id  VARCHAR(100),
            ADD COLUMN IF NOT EXISTS operation_uid VARCHAR(64) UNIQUE
    """)

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockmovement_external_id "
        "ON stockmovement (external_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_stockmovement_operation_uid "
        "ON stockmovement (operation_uid)"
    )


def downgrade() -> None:
    # ── stockmovement ────────────────────────────────────────────────────────
    op.execute(
        "DROP INDEX IF EXISTS ix_stockmovement_operation_uid"
    )
    op.execute(
        "DROP INDEX IF EXISTS ix_stockmovement_external_id"
    )
    op.execute(
        "ALTER TABLE stockmovement "
        "DROP COLUMN IF EXISTS operation_uid, "
        "DROP COLUMN IF EXISTS external_id"
    )

    # ── stocklot ─────────────────────────────────────────────────────────────
    op.execute(
        "DROP INDEX IF EXISTS ix_stocklot_source_document_item_id"
    )
    op.execute(
        "DROP INDEX IF EXISTS ix_stocklot_external_id"
    )
    op.execute(
        "DROP INDEX IF EXISTS ix_stocklot_source_type"
    )
    op.execute(
        "ALTER TABLE stocklot "
        "DROP COLUMN IF EXISTS source_document_item_id, "
        "DROP COLUMN IF EXISTS sync_status, "
        "DROP COLUMN IF EXISTS external_id, "
        "DROP COLUMN IF EXISTS source_type"
    )

    # ── tables ───────────────────────────────────────────────────────────────
    op.execute("DROP TABLE IF EXISTS stockdocumentitem")
    op.execute("DROP TABLE IF EXISTS stockdocument")

    # ── enum types ───────────────────────────────────────────────────────────
    op.execute("DROP TYPE IF EXISTS stockdocumentstatus")
    op.execute("DROP TYPE IF EXISTS stockdocumenttype")
    op.execute("DROP TYPE IF EXISTS syncstatus")
    op.execute("DROP TYPE IF EXISTS lotsourcetype")
    # Note: cannot remove enum values from movementtype in PostgreSQL
