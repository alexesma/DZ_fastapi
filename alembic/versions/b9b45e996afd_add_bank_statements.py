"""Add bank statement import: BankAccount, BankStatement, BankTransaction

Revision ID: b9b45e996afd
Revises: 4f7e4ffc217a
Create Date: 2026-05-12 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b9b45e996afd'
down_revision: Union[str, None] = '4f7e4ffc217a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Enum types ─────────────────────────────────────────────────────────────
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE bankstatementformat AS ENUM (
                'tochka_csv','1c_exchange','alfabank_csv','sberbank_csv','unknown'
            );
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE banktxndirection AS ENUM ('incoming','outgoing');
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE banktxnstatus AS ENUM ('unmatched','matched','ignored');
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )

    # ── BankAccount ────────────────────────────────────────────────────────────
    op.create_table(
        'bankaccount',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('account_number', sa.String(20), nullable=False),
        sa.Column('bank_name', sa.String(255), nullable=False),
        sa.Column('bik', sa.String(9), nullable=True),
        sa.Column('corr_account', sa.String(20), nullable=True),
        sa.Column('currency', sa.String(3), nullable=False, server_default='RUB'),
        sa.Column('description', sa.String(255), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('account_number'),
    )
    op.create_index('ix_bankaccount_account_number', 'bankaccount', ['account_number'], unique=True)

    # ── BankStatement ──────────────────────────────────────────────────────────
    op.create_table(
        'bankstatement',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('bank_account_id', sa.Integer(), nullable=True),
        sa.Column('period_from', sa.Date(), nullable=False),
        sa.Column('period_to', sa.Date(), nullable=False),
        sa.Column('opening_balance', sa.DECIMAL(15, 2), nullable=True),
        sa.Column('closing_balance', sa.DECIMAL(15, 2), nullable=True),
        sa.Column('total_incoming', sa.DECIMAL(15, 2), nullable=True),
        sa.Column('total_outgoing', sa.DECIMAL(15, 2), nullable=True),
        sa.Column(
            'format',
            postgresql.ENUM(
                'tochka_csv', '1c_exchange', 'alfabank_csv', 'sberbank_csv', 'unknown',
                name='bankstatementformat',
                create_type=False,
            ),
            nullable=False,
            server_default='unknown',
        ),
        sa.Column('filename', sa.String(255), nullable=True),
        sa.Column('uploaded_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('txn_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('matched_count', sa.Integer(), nullable=False, server_default='0'),
        sa.ForeignKeyConstraint(['bank_account_id'], ['bankaccount.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_bankstatement_bank_account_id', 'bankstatement', ['bank_account_id'])

    # ── BankTransaction ────────────────────────────────────────────────────────
    op.create_table(
        'banktransaction',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('statement_id', sa.Integer(), nullable=False),
        sa.Column('doc_number', sa.String(50), nullable=True),
        sa.Column('doc_date', sa.Date(), nullable=True),
        sa.Column('value_date', sa.Date(), nullable=False),
        sa.Column(
            'direction',
            postgresql.ENUM(
                'incoming', 'outgoing',
                name='banktxndirection',
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column('amount', sa.DECIMAL(15, 2), nullable=False),
        sa.Column('vat_amount', sa.DECIMAL(15, 2), nullable=True),
        sa.Column('currency', sa.String(3), nullable=False, server_default='RUB'),
        sa.Column('counterparty_name', sa.String(500), nullable=True),
        sa.Column('counterparty_inn', sa.String(12), nullable=True),
        sa.Column('counterparty_kpp', sa.String(9), nullable=True),
        sa.Column('counterparty_account', sa.String(20), nullable=True),
        sa.Column('counterparty_bank', sa.String(255), nullable=True),
        sa.Column('counterparty_bik', sa.String(9), nullable=True),
        sa.Column('purpose', sa.Text(), nullable=True),
        sa.Column('balance_after', sa.DECIMAL(15, 2), nullable=True),
        sa.Column(
            'status',
            postgresql.ENUM(
                'unmatched', 'matched', 'ignored',
                name='banktxnstatus',
                create_type=False,
            ),
            nullable=False,
            server_default='unmatched',
        ),
        sa.Column('customer_payment_id', sa.Integer(), nullable=True),
        sa.Column('supplier_payment_id', sa.Integer(), nullable=True),
        sa.Column('match_note', sa.String(500), nullable=True),
        sa.ForeignKeyConstraint(['statement_id'], ['bankstatement.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['customer_payment_id'], ['customerpayment.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['supplier_payment_id'], ['supplierpayment.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_banktransaction_statement_id', 'banktransaction', ['statement_id'])
    op.create_index('ix_banktransaction_counterparty_inn', 'banktransaction', ['counterparty_inn'])
    op.create_index('ix_banktransaction_status', 'banktransaction', ['status'])
    op.create_index('ix_banktransaction_customer_payment_id', 'banktransaction', ['customer_payment_id'])
    op.create_index('ix_banktransaction_supplier_payment_id', 'banktransaction', ['supplier_payment_id'])


def downgrade() -> None:
    op.drop_table('banktransaction')
    op.drop_table('bankstatement')
    op.drop_table('bankaccount')
    op.execute('DROP TYPE IF EXISTS banktxnstatus')
    op.execute('DROP TYPE IF EXISTS banktxndirection')
    op.execute('DROP TYPE IF EXISTS bankstatementformat')
