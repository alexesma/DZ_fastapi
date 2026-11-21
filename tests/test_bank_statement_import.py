import io
import zipfile

import pytest
from sqlalchemy import select

from dz_fastapi.models.finance import BankStatement, BankTransaction
from dz_fastapi.services.bank_statement_import import (
    bank_for_sender,
    extract_statement_files,
    import_statement_from_email,
)

ACCOUNT = "40702810500000000001"


def _statement_1c(number: str = "1", amount: str = "1000.00") -> bytes:
    text = (
        "1CClientBankExchange\n"
        "ВерсияФормата=1.03\n"
        "Кодировка=Windows\n"
        "Отправитель=Система интернет банк\n"
        "ДатаСоздания=20.11.2026\n"
        "ДатаНачала=19.11.2026\n"
        "ДатаКонца=19.11.2026\n"
        f"РасчСчет={ACCOUNT}\n"
        "СекцияРасчСчет\n"
        "ДатаНачала=19.11.2026\n"
        "ДатаКонца=19.11.2026\n"
        f"РасчСчет={ACCOUNT}\n"
        "НачальныйОстаток=0.00\n"
        f"ВсегоПоступило={amount}\n"
        "ВсегоСписано=0.00\n"
        f"КонечныйОстаток={amount}\n"
        "КонецРасчСчет\n"
        "СекцияДокумент=Платежное поручение\n"
        f"Номер={number}\n"
        "Дата=19.11.2026\n"
        f"Сумма={amount}\n"
        f"ПлательщикСчет=40702810500000000099\n"
        "Плательщик1=ООО ТЕСТ\n"
        "ПлательщикИНН=7700000001\n"
        "ПлательщикБИК=044525593\n"
        f"ПолучательСчет={ACCOUNT}\n"
        "Получатель1=ООО АВТОПАРТС\n"
        "ПолучательИНН=7700000002\n"
        "ПолучательБИК=044525104\n"
        "ДатаПоступило=19.11.2026\n"
        "НазначениеПлатежа=Оплата по счету 1 без НДС\n"
        "КонецДокумента\n"
        "КонецФайла\n"
    )
    return text.encode("cp1251")


def _zip_of(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def test_bank_for_sender_recognises_known_banks():
    assert bank_for_sender("informer@tochka.com") == "Точка"
    assert bank_for_sender("Esupport@alfa-bank.info") == "Альфа-Банк"
    assert bank_for_sender("no-reply@alfabank.ru") == "Альфа-Банк"
    assert bank_for_sender("Банк <informer@tochka.com>") == "Точка"
    assert bank_for_sender("someone@example.com") is None
    assert bank_for_sender(None) is None


def test_extract_statement_files_prefers_1c_and_skips_pdf():
    archive = _zip_of(
        {
            "stmt (19.11.2026).pdf": b"%PDF",
            "stmt (19.11.2026).csv": b"a;b",
            "kl_to_1c_stmt.txt": _statement_1c(),
        }
    )

    files = extract_statement_files([("Архив.zip", archive)])

    assert [item.name for item in files] == [
        "kl_to_1c_stmt.txt",
        "stmt (19.11.2026).csv",
    ]


def test_extract_statement_files_rejects_oversized_archives():
    archive = _zip_of({f"f{i}.txt": b"x" for i in range(30)})

    assert extract_statement_files([("many.zip", archive)]) == []
    assert extract_statement_files([("broken.zip", b"not a zip")]) == []


@pytest.mark.asyncio
async def test_import_statement_from_email_creates_and_dedupes(test_session):
    attachments = [("Архив.zip", _zip_of({"kl_to_1c_stmt.txt": _statement_1c()}))]

    first = await import_statement_from_email(
        test_session, attachments=attachments, bank_name="Точка"
    )
    second = await import_statement_from_email(
        test_session, attachments=attachments, bank_name="Точка"
    )

    assert first["status"] == "imported"
    assert first["txn_count"] == 1
    assert second["status"] == "duplicate"
    assert second["statement_id"] == first["statement_id"]
    statements = (
        await test_session.execute(select(BankStatement))
    ).scalars().all()
    assert len(statements) == 1
    assert statements[0].source == "email"
    transactions = (
        await test_session.execute(select(BankTransaction))
    ).scalars().all()
    assert len(transactions) == 1


@pytest.mark.asyncio
async def test_import_statement_from_email_reports_missing_files(test_session):
    result = await import_statement_from_email(
        test_session,
        attachments=[("scan.pdf", b"%PDF")],
        bank_name="Точка",
    )

    assert result["status"] == "no_files"


@pytest.mark.asyncio
async def test_import_statement_from_email_handles_summary_without_operations(
    test_session,
):
    summary = (
        '"Наименование банка ""Альфа-Банк""";;\n'
        "Наименование предприятия ООО ТЕСТ;;\n"
        "За период 20.11.2026 по 20.11.2026;;\n"
        "Сумма транзакций on-line;;\n"
    ).encode("cp1251")

    result = await import_statement_from_email(
        test_session,
        attachments=[("80017196.323.csv", summary)],
        bank_name="Альфа-Банк",
    )

    assert result["status"] in {"no_operations", "error"}
    statements = (
        await test_session.execute(select(BankStatement))
    ).scalars().all()
    assert statements == []


@pytest.mark.asyncio
async def test_inbox_email_from_bank_is_imported_automatically(test_session):
    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.models.email_account import EmailAccount
    from dz_fastapi.models.inbox_email import InboxEmail
    from dz_fastapi.services import inbox_email as inbox_service
    from dz_fastapi.services.email import _FetchedAttachment, _FetchedInboxMessage

    account = EmailAccount(
        name="Info",
        email="info@example.com",
        password="x",
        purposes=["orders_in"],
        is_active=True,
    )
    test_session.add(account)
    await test_session.flush()
    archive = _zip_of({"kl_to_1c_stmt.txt": _statement_1c()})
    email = InboxEmail(
        email_account_id=account.id,
        uid="10",
        folder="INBOX",
        from_email="informer@tochka.com",
        subject='Выписка ООО "АВТОПАРТС" за 19.11.2026',
        body_preview="x",
        has_attachments=True,
        attachment_info=[{"name": "Архив.zip", "size": len(archive)}],
        received_at=now_moscow(),
        fetched_at=now_moscow(),
    )
    test_session.add(email)
    await test_session.flush()
    fetched = _FetchedInboxMessage(
        uid="10",
        from_="informer@tochka.com",
        subject=email.subject,
        attachments=[_FetchedAttachment("Архив.zip", archive)],
    )

    handled = await inbox_service.auto_detect_and_process(
        test_session, inbox_email=email, fetched_msg=fetched, account=account
    )

    assert handled is True
    await test_session.refresh(email)
    assert email.rule_type == "bank_statement"
    assert email.processed is True
    assert email.processing_result["status"] == "imported"
    statements = (
        await test_session.execute(select(BankStatement))
    ).scalars().all()
    assert len(statements) == 1
    assert statements[0].inbox_email_id == email.id


def test_fetch_prioritises_bank_mail_over_newer_messages(monkeypatch):
    from types import SimpleNamespace

    from dz_fastapi.services import inbox_email as inbox_service

    # Заголовки приходят от новых к старым (reverse=True).
    headers = [
        SimpleNamespace(uid="9", from_="price@a.ru"),
        SimpleNamespace(uid="8", from_="price@b.ru"),
        SimpleNamespace(uid="7", from_="price@c.ru"),
        SimpleNamespace(uid="3", from_="informer@tochka.com"),
        SimpleNamespace(uid="2", from_="price@d.ru"),
        SimpleNamespace(uid="1", from_="no-reply@alfabank.ru"),
    ]
    fetched_uids: list[str] = []

    class FakeMailbox:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def login(self, *args):
            return self

        folder = SimpleNamespace(set=lambda name: None)

        def fetch(self, criteria, **kwargs):
            if kwargs.get("headers_only"):
                return iter(headers)
            fetched_uids.append(str(criteria).split()[-1])
            return iter([])

    monkeypatch.setattr(
        inbox_service, "_create_mailbox", lambda *a, **k: FakeMailbox()
    )

    inbox_service._fetch_inbox_imap_sync(
        "h", "e", "p", "INBOX", full_fetch_limit=2
    )

    # Письма банков (в порядке заголовков) идут первыми, затем два самых
    # свежих из остальных.
    assert fetched_uids == ["3", "1", "9", "8"]


def test_1c_statement_detects_our_bank_by_bik():
    from dz_fastapi.services.bank_statement_parser import detect_and_parse

    parsed = detect_and_parse(
        _statement_1c(), filename="kl_to_1c_stmt.txt"
    )

    # ПолучательБИК=044525104 — наш банк (мы получатель) -> Точка.
    assert parsed.bik == "044525104"
    assert parsed.bank_name == "Банк Точка"


def test_bank_bik_is_taken_from_filename_when_missing():
    from dz_fastapi.services.bank_statement_parser import bik_from_filename

    assert (
        bik_from_filename("kl_to_1c_40702810501500063689_044525104_13.11.txt")
        == "044525104"
    )
    assert bik_from_filename("Выписка_4070.txt") is None


def test_name_and_inn_normalisation():
    from dz_fastapi.services.bank_reconciliation import normalize_inn, normalize_name

    assert normalize_name('ООО "Эксист-закупка"') == "эксист закупка"
    assert normalize_name("ЭЛЕМЕНТ (клиент)") == "элемент"
    assert (
        normalize_name('ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ЭЛЕМЕНТ"')
        == "элемент"
    )
    assert normalize_inn("000000000000") == ""
    assert normalize_inn(" 7723 706 908 ") == "7723706908"


def test_non_trade_operations_are_classified():
    from types import SimpleNamespace

    from dz_fastapi.services.bank_reconciliation import classify_non_trade

    def txn(purpose, name="X", inn="1"):
        return SimpleNamespace(
            purpose=purpose, counterparty_name=name, counterparty_inn=inn
        )

    assert (
        classify_non_trade(txn("Перечисление средств для открытия депозита"))
        == "Депозит: открытие"
    )
    assert (
        classify_non_trade(txn("Единый налоговый платеж. НДС не облагается"))
        == "Налоги и сборы"
    )
    assert classify_non_trade(txn("Заработная плата (аванс) за ноябрь")) == "Зарплата"
    assert (
        classify_non_trade(txn("Выплата процентов", inn="9721194461"))
        == "Проценты банка"
    )
    # Торговые платежи и «без налога (НДС)» служебными не считаются.
    assert (
        classify_non_trade(txn("Оплата за автозапчасти. Без налога (НДС)"))
        is None
    )
    assert classify_non_trade(txn("Оплата по счёту 5 без налога (НДС)")) is None


@pytest.mark.asyncio
async def test_auto_match_finds_customer_by_name_and_remembers_inn(
    test_session,
):
    from dz_fastapi.models.finance import BankTxnStatus
    from dz_fastapi.models.partner import Customer

    customer = Customer(name="АВТОПАРТС ТЕСТ")
    test_session.add(customer)
    await test_session.commit()

    # Плательщик в выписке — «ООО ТЕСТ» с ИНН, которого у клиента нет.
    customer.name = "Тест"
    await test_session.commit()

    attachments = [("kl_to_1c_stmt.txt", _statement_1c())]
    result = await import_statement_from_email(
        test_session, attachments=attachments, bank_name="Точка"
    )
    assert result["status"] == "imported"

    await test_session.refresh(customer)
    assert customer.inn == "7700000001"
    transaction = (
        await test_session.execute(select(BankTransaction))
    ).scalars().one()
    assert transaction.status == BankTxnStatus.MATCHED
    assert transaction.match_note.startswith("Клиент: Тест")
    # Счётчик выписки не должен отставать от фактического статуса операций.
    statement = (
        await test_session.execute(select(BankStatement))
    ).scalars().one()
    assert statement.matched_count == 1


@pytest.mark.asyncio
async def test_resolve_bank_account_heals_unknown_bank_name(test_session):
    from dz_fastapi.models.finance import BankAccount
    from dz_fastapi.services.bank_statement_import import resolve_bank_account
    from dz_fastapi.services.bank_statement_parser import ParsedStatement

    test_session.add(
        BankAccount(account_number=ACCOUNT, bank_name="Неизвестный банк")
    )
    await test_session.commit()
    parsed = ParsedStatement(
        format="1c_exchange",
        period_from=None,
        period_to=None,
        account_number=ACCOUNT,
        bank_name=None,
        bik=None,
        opening_balance=None,
        closing_balance=None,
        total_incoming=None,
        total_outgoing=None,
    )

    account = await resolve_bank_account(
        test_session, parsed, bank_hint="Альфа-Банк"
    )

    assert account.bank_name == "Альфа-Банк"


def test_deposit_operations_are_classified_with_deal_number():
    from types import SimpleNamespace

    from dz_fastapi.services.bank_reconciliation import classify_operation

    def txn(purpose, direction):
        return SimpleNamespace(
            purpose=purpose,
            counterparty_name="ООО Банк Точка",
            counterparty_inn="9721194461",
            direction=direction,
        )

    opening = classify_operation(
        txn(
            "Перечисление средств для открытия депозита по депозитной "
            "сделке № UNV-0000012503044 от 16.11.2026",
            "outgoing",
        )
    )
    returned = classify_operation(
        txn(
            "Возврат средств по депозитной сделке № UNV-0000012480827 "
            "от 13.11.2026 согласно заявлению",
            "incoming",
        )
    )
    interest = classify_operation(
        txn(
            "Выплата процентов по депозитной сделке № UNV-0000012480827 "
            "от 13.11.2026 за период",
            "incoming",
        )
    )

    assert (opening.category, opening.deal_ref) == (
        "deposit_open",
        "UNV-0000012503044",
    )
    assert (returned.category, returned.deal_ref) == (
        "deposit_return",
        "UNV-0000012480827",
    )
    # Проценты по депозиту — не «проценты банка» на остаток.
    assert interest.category == "deposit_interest"


@pytest.mark.asyncio
async def test_deposit_summary_pairs_deals_and_computes_rate(test_session):
    from datetime import date
    from decimal import Decimal

    from dz_fastapi.models.finance import BankTxnStatus
    from dz_fastapi.services.bank_reconciliation import build_deposit_summary

    statement = BankStatement(
        period_from=date(2026, 11, 13),
        period_to=date(2026, 11, 14),
        format="1c_exchange",
        txn_count=3,
        matched_count=0,
    )
    test_session.add(statement)
    await test_session.flush()

    def add(category, day, amount, direction, deal="UNV-0000012480827"):
        test_session.add(
            BankTransaction(
                statement_id=statement.id,
                value_date=day,
                direction=direction,
                amount=Decimal(amount),
                status=BankTxnStatus.IGNORED,
                category=category,
                deal_ref=deal,
            )
        )

    add("deposit_open", date(2026, 11, 13), "100000.00", "outgoing")
    add("deposit_return", date(2026, 11, 14), "100000.00", "incoming")
    add("deposit_interest", date(2026, 11, 14), "36.50", "incoming")
    # Незакрытая сделка другого дня.
    add(
        "deposit_open",
        date(2026, 11, 20),
        "50000.00",
        "outgoing",
        deal="UNV-0000012999999",
    )
    await test_session.commit()

    summary = await build_deposit_summary(test_session)

    assert summary["totals"]["interest_total"] == Decimal("36.50")
    assert summary["totals"]["deals_total"] == 2
    assert summary["totals"]["deals_open"] == 1
    assert summary["totals"]["open_principal"] == Decimal("50000.00")
    closed = next(d for d in summary["deals"] if d["status"] == "closed")
    assert closed["days"] == 1
    # 36.50 / 100000 / 1 * 365 * 100 = 13.32 % годовых
    assert closed["rate_percent"] == Decimal("13.32")
    assert summary["totals"]["average_rate_percent"] == Decimal("13.32")

    only_later = await build_deposit_summary(
        test_session, date_from=date(2026, 11, 18)
    )
    assert only_later["totals"]["deals_total"] == 1
    assert only_later["totals"]["interest_total"] == Decimal("0")
