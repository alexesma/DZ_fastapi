"""Импорт банковских выписок: общий код для ручной загрузки и почты.

Банк присылает выписку письмом (Точка: ``informer@tochka.com``, вложение —
zip с csv/pdf/xlsx/txt; Альфа-Банк: ``Esupport@alfa-bank.info``, реестр
эквайринга csv). Парсеры уже есть в ``bank_statement_parser``; здесь —
распаковка вложений, выбор лучшего файла, защита от повторов и запись в БД.
"""

import io
import logging
import os
import zipfile
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.finance import BankAccount, BankStatement, BankTransaction
from dz_fastapi.services.bank_statement_parser import ParsedStatement, detect_and_parse

logger = logging.getLogger("dz_fastapi")

# Отправители писем с выписками -> банк (для сообщений и логов).
BANK_STATEMENT_SENDERS = {
    "informer@tochka.com": "Точка",
    "esupport@alfa-bank.info": "Альфа-Банк",
}


def _extra_senders() -> dict[str, str]:
    raw = os.getenv("BANK_STATEMENT_EXTRA_SENDERS", "")
    result = {}
    for item in raw.replace(";", ",").split(","):
        email = item.strip().lower()
        if email:
            result[email] = "Банк"
    return result


def bank_for_sender(from_email: Optional[str]) -> Optional[str]:
    """Название банка, если письмо пришло от известного отправителя."""
    email = (from_email or "").strip().lower()
    if not email:
        return None
    if "<" in email and ">" in email:
        email = email.split("<", 1)[1].split(">", 1)[0].strip()
    return BANK_STATEMENT_SENDERS.get(email) or _extra_senders().get(email)


# Ограничения при распаковке (письма приходят снаружи).
MAX_ARCHIVE_ENTRIES = 20
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_TOTAL_BYTES = 50 * 1024 * 1024
STATEMENT_EXTENSIONS = (".txt", ".csv")


@dataclass
class StatementFile:
    name: str
    content: bytes


def _file_priority(name: str) -> int:
    """Чем меньше, тем лучше: формат 1С самый полный и однозначный."""
    lowered = name.lower()
    if "kl_to_1c" in lowered:
        return 0
    if lowered.endswith(".txt"):
        return 1
    if lowered.endswith(".csv"):
        return 2
    return 9


def extract_statement_files(
    attachments: Iterable[tuple[str, bytes]],
) -> list[StatementFile]:
    """Достаёт из вложений файлы выписок, лучшие форматы первыми.

    Zip распаковывается в памяти с жёсткими лимитами; вложенные архивы,
    pdf и xlsx пропускаются (парсера для них нет, а содержимое дублируется
    в txt/csv того же архива).
    """
    found: list[StatementFile] = []
    total = 0
    for name, payload in attachments:
        name = name or ""
        lowered = name.lower()
        if not payload:
            continue
        if lowered.endswith(".zip"):
            try:
                archive = zipfile.ZipFile(io.BytesIO(payload))
            except zipfile.BadZipFile:
                logger.warning("Bank statement archive is not a zip: %s", name)
                continue
            with archive:
                infos = archive.infolist()
                if len(infos) > MAX_ARCHIVE_ENTRIES:
                    logger.warning(
                        "Bank statement archive %s has too many entries: %s",
                        name,
                        len(infos),
                    )
                    continue
                for info in infos:
                    inner = os.path.basename(info.filename)
                    if info.is_dir() or not inner:
                        continue
                    if not inner.lower().endswith(STATEMENT_EXTENSIONS):
                        continue
                    if info.file_size > MAX_FILE_BYTES:
                        continue
                    total += info.file_size
                    if total > MAX_TOTAL_BYTES:
                        break
                    data = archive.read(info)
                    found.append(StatementFile(inner, data))
        elif lowered.endswith(STATEMENT_EXTENSIONS):
            if len(payload) > MAX_FILE_BYTES:
                continue
            total += len(payload)
            if total > MAX_TOTAL_BYTES:
                break
            found.append(StatementFile(os.path.basename(name), payload))
    found.sort(key=lambda item: _file_priority(item.name))
    return found


async def find_duplicate_statement(
    session: AsyncSession,
    *,
    bank_account_id: Optional[int],
    parsed: ParsedStatement,
) -> Optional[BankStatement]:
    """Та же выписка уже загружена: счёт, период и итоги совпадают."""
    if bank_account_id is None or not parsed.period_from:
        return None
    result = await session.execute(
        select(BankStatement).where(
            BankStatement.bank_account_id == bank_account_id,
            BankStatement.period_from == parsed.period_from,
            BankStatement.period_to == (parsed.period_to or parsed.period_from),
            BankStatement.txn_count == len(parsed.transactions),
            BankStatement.total_incoming == parsed.total_incoming,
            BankStatement.total_outgoing == parsed.total_outgoing,
        )
    )
    return result.scalars().first()


async def resolve_bank_account(
    session: AsyncSession,
    parsed: ParsedStatement,
    *,
    bank_account_id: Optional[int] = None,
) -> Optional[BankAccount]:
    if bank_account_id:
        return await session.get(BankAccount, bank_account_id)
    if not parsed.account_number:
        return None
    result = await session.execute(
        select(BankAccount).where(
            BankAccount.account_number == parsed.account_number
        )
    )
    account = result.scalar_one_or_none()
    if account is None:
        account = BankAccount(
            account_number=parsed.account_number,
            bank_name=parsed.bank_name or "Неизвестный банк",
            bik=parsed.bik,
        )
        session.add(account)
        await session.flush()
    return account


async def store_parsed_statement(
    session: AsyncSession,
    parsed: ParsedStatement,
    *,
    filename: Optional[str],
    bank_account: Optional[BankAccount],
    source: str = "upload",
    inbox_email_id: Optional[int] = None,
) -> BankStatement:
    statement = BankStatement(
        bank_account_id=bank_account.id if bank_account else None,
        period_from=parsed.period_from or date.today(),
        period_to=parsed.period_to or date.today(),
        opening_balance=parsed.opening_balance,
        closing_balance=parsed.closing_balance,
        total_incoming=parsed.total_incoming,
        total_outgoing=parsed.total_outgoing,
        format=parsed.format,
        filename=filename,
        txn_count=len(parsed.transactions),
        matched_count=0,
        source=source,
        inbox_email_id=inbox_email_id,
    )
    session.add(statement)
    await session.flush()
    for pt in parsed.transactions:
        session.add(
            BankTransaction(
                statement_id=statement.id,
                doc_number=pt.doc_number,
                doc_date=pt.doc_date,
                value_date=pt.value_date,
                direction=pt.direction,
                amount=pt.amount,
                vat_amount=pt.vat_amount,
                currency=pt.currency,
                purpose=pt.purpose,
                balance_after=pt.balance_after,
                counterparty_name=pt.counterparty_name,
                counterparty_inn=pt.counterparty_inn,
                counterparty_kpp=pt.counterparty_kpp,
                counterparty_account=pt.counterparty_account,
                counterparty_bank=pt.counterparty_bank,
                counterparty_bik=pt.counterparty_bik,
            )
        )
    await session.commit()
    await session.refresh(statement)
    return statement


async def import_statement_from_email(
    session: AsyncSession,
    *,
    attachments: Iterable[tuple[str, bytes]],
    bank_name: str,
    inbox_email_id: Optional[int] = None,
) -> dict:
    """Импортирует выписку из вложений письма банка.

    Возвращает словарь с итогом для ``InboxEmail.processing_result``:
    ``status`` — imported / duplicate / no_operations / no_files / error.
    """
    from dz_fastapi.services.bank_reconciliation import auto_match_statement

    files = extract_statement_files(attachments)
    result: dict = {"action": "bank_statement", "bank": bank_name}
    if not files:
        result.update(
            status="no_files",
            note="В письме нет файлов выписки (txt/csv, в том числе в zip).",
        )
        return result

    summary_only_errors: list[str] = []
    for item in files:
        parsed = detect_and_parse(item.content, filename=item.name)
        if not parsed.transactions:
            # Неизвестный формат либо сводка без операций (эквайринг).
            summary_only_errors.extend(parsed.errors[:1])
            continue

        account = await resolve_bank_account(session, parsed)
        duplicate = await find_duplicate_statement(
            session,
            bank_account_id=account.id if account else None,
            parsed=parsed,
        )
        if duplicate is not None:
            result.update(
                status="duplicate",
                statement_id=duplicate.id,
                filename=item.name,
                note="Такая выписка уже загружена.",
            )
            return result

        statement = await store_parsed_statement(
            session,
            parsed,
            filename=item.name,
            bank_account=account,
            source="email",
            inbox_email_id=inbox_email_id,
        )
        await auto_match_statement(session, statement)
        await session.refresh(statement)
        result.update(
            status="imported",
            statement_id=statement.id,
            filename=item.name,
            period_from=statement.period_from.isoformat(),
            period_to=statement.period_to.isoformat(),
            txn_count=statement.txn_count,
            matched_count=statement.matched_count,
            total_incoming=str(statement.total_incoming or 0),
            total_outgoing=str(statement.total_outgoing or 0),
        )
        return result

    if summary_only_errors:
        result.update(status="no_operations", note=summary_only_errors[0])
    else:
        result.update(
            status="error",
            note="Не удалось определить формат файлов выписки.",
        )
    return result
