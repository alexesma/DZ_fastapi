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
