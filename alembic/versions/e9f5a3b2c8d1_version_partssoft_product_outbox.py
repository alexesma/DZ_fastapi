"""Version Parts-Soft product outbox changes.

Revision ID: e9f5a3b2c8d1
Revises: d8e4f9a2c1b7
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e9f5a3b2c8d1"
down_revision: Union[str, None] = "d8e4f9a2c1b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _replace_queue_functions(*, versioned: bool) -> None:
    version_column = ", change_version" if versioned else ""
    version_value = ", 1" if versioned else ""
    version_update = (
        ", change_version = partssoftproductoutbox.change_version + 1"
        if versioned
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION queue_partssoft_product_change() RETURNS trigger AS $$
        DECLARE
            target_id integer;
            external_id bigint;
            target_operation varchar(16);
        BEGIN
            target_id := COALESCE(NEW.id, OLD.id);
            external_id := COALESCE(NEW.partssoft_product_id, OLD.partssoft_product_id);
            target_operation := CASE WHEN TG_OP = 'DELETE' THEN 'delete' ELSE 'upsert' END;
            INSERT INTO partssoftproductoutbox (
                autopart_id, external_product_id, operation, status, attempts,
                available_at, created_at, updated_at{version_column}
            ) VALUES (
                target_id, external_id, target_operation, 'pending', 0,
                now(), now(), now(){version_value}
            )
            ON CONFLICT (autopart_id) DO UPDATE SET
                external_product_id = COALESCE(
                    EXCLUDED.external_product_id,
                    partssoftproductoutbox.external_product_id
                ),
                operation = EXCLUDED.operation,
                status = 'pending',
                attempts = 0,
                last_error = NULL,
                available_at = now(),
                locked_at = NULL,
                sent_at = NULL,
                updated_at = now(){version_update};
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION queue_partssoft_photo_change() RETURNS trigger AS $$
        DECLARE
            target_autopart_id integer;
            external_id bigint;
        BEGIN
            target_autopart_id := CASE
                WHEN TG_OP = 'DELETE' THEN OLD.autopart_id
                ELSE NEW.autopart_id
            END;
            SELECT partssoft_product_id INTO external_id
            FROM autopart WHERE id = target_autopart_id;
            INSERT INTO partssoftproductoutbox (
                autopart_id, external_product_id, operation, status, attempts,
                available_at, created_at, updated_at{version_column}
            ) VALUES (
                target_autopart_id, external_id, 'upsert', 'pending', 0,
                now(), now(), now(){version_value}
            )
            ON CONFLICT (autopart_id) DO UPDATE SET
                external_product_id = COALESCE(
                    EXCLUDED.external_product_id,
                    partssoftproductoutbox.external_product_id
                ),
                operation = 'upsert',
                status = 'pending',
                attempts = 0,
                last_error = NULL,
                available_at = now(),
                locked_at = NULL,
                sent_at = NULL,
                updated_at = now(){version_update};
            RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql
        """
    )


def upgrade() -> None:
    op.add_column(
        "partssoftproductoutbox",
        sa.Column(
            "change_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )
    _replace_queue_functions(versioned=True)


def downgrade() -> None:
    _replace_queue_functions(versioned=False)
    op.drop_column("partssoftproductoutbox", "change_version")
