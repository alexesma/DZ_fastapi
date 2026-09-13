"""Поставщик по адресу отправителя ищется без учёта регистра.

Адрес отправителя из письма приводится к нижнему регистру, а в карточке
поставщика хранится так, как его вписали руками. У ALYANS в карточке
стояло «Alyans-7@list.ru», а письма приходили с «alyans-7@list.ru» —
поставщик не находился, письмо пропускалось как чужое, и прайс не
обновлялся почти пять месяцев. Ошибок при этом не было ни одной: с точки
зрения системы письмо было не от поставщика.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.services.email import get_emails


@pytest.mark.asyncio
async def test_provider_found_regardless_of_address_case(
    test_session, created_providers
):
    """Ровно случай ALYANS: в карточке заглавные буквы."""
    поставщик = created_providers[0]
    поставщик.email_incoming_price = "Alyans-7@List.RU"
    await test_session.commit()

    найден = await crud_provider.get_by_email_incoming_price(
        session=test_session, email="alyans-7@list.ru"
    )

    assert найден is not None
    assert найден.id == поставщик.id


@pytest.mark.asyncio
async def test_provider_found_with_stray_spaces(
    test_session, created_providers
):
    """Пробелы по краям адреса тоже не должны прятать поставщика.

    Через модель такой адрес не пройдёт — там стоит проверка, — но в
    базу он мог попасть прямым запросом или из старой схемы.
    """
    поставщик = created_providers[0]
    await test_session.execute(
        text(
            "UPDATE provider SET email_incoming_price = :адрес "
            "WHERE id = :id"
        ),
        {"адрес": "  alyans-7@list.ru  ", "id": поставщик.id},
    )
    await test_session.commit()

    найден = await crud_provider.get_by_email_incoming_price(
        session=test_session, email="alyans-7@list.ru"
    )

    assert найден is not None
    assert найден.id == поставщик.id


@pytest.mark.asyncio
async def test_empty_address_finds_nobody(test_session, created_providers):
    """Пустой адрес не должен вытягивать случайного поставщика."""
    найден = await crud_provider.get_by_email_incoming_price(
        session=test_session, email="   "
    )
    assert найден is None


def _письмо(uid: str, отправитель: str, тема: str, вложение: str):
    return SimpleNamespace(
        uid=uid,
        from_=отправитель,
        subject=тема,
        attachments=[SimpleNamespace(filename=вложение, payload=b"x")],
        date=datetime(2026, 9, 11, 10, 0, 0),
    )


def _почта(monkeypatch, письма, поставщик, конфигурации, скачивать=True):
    account = SimpleNamespace(
        id=6,
        email="price@dragonzap.ru",
        transport="smtp",
        imap_host="imap.example",
        password="secret",
        imap_folder="INBOX",
        imap_port=993,
    )

    async def активные(session, purpose):
        return [account]

    def ящик(*args, **kwargs):
        return list(письма)

    async def поставщик_по_почте(session, email):
        # Настоящее сравнение: письмо приносит адрес в нижнем регистре.
        хранится = (поставщик.email_incoming_price or "").strip().lower()
        return поставщик if хранится == (email or "").strip().lower() else None

    async def конфиги(provider_id, session, only_active):
        return конфигурации

    async def водяной_знак(
        provider_id, session, provider_config_id=None, folder=None
    ):
        return 0

    async def скачать(msg, provider, provider_conf, session):
        return f"/tmp/{provider_conf.id}-{msg.uid}.xls" if скачивать else None

    for путь, значение in (
        ("crud_email_account.get_active_by_purpose", активные),
        ("_fetch_mailbox_messages", ящик),
        ("crud_provider.get_by_email_incoming_price", поставщик_по_почте),
        ("crud_provider_pricelist_config.get_configs", конфиги),
        ("_get_last_uid_compat", водяной_знак),
        ("download_new_price_provider", скачать),
    ):
        monkeypatch.setattr(f"dz_fastapi.services.email.{путь}", значение)


def _альянс(адрес_в_карточке: str):
    поставщик = SimpleNamespace(
        id=934,
        name="ALYANS",
        email_incoming_price=адрес_в_карточке,
    )
    конфигурация = SimpleNamespace(
        id=38,
        provider_id=934,
        name_mail="Остатки товаров",
        name_price="alyprice",
        filename_pattern=None,
        file_url=None,
        incoming_email_account_id=6,
    )
    return поставщик, [конфигурация]


@pytest.mark.asyncio
async def test_pricelist_loads_when_card_address_differs_in_case(monkeypatch):
    """Тот самый случай ALYANS: регистр в карточке не совпадал."""
    поставщик, конфигурации = _альянс("Alyans-7@list.ru")
    _почта(
        monkeypatch,
        [
            _письмо(
                "500",
                "alyans-7@list.ru",
                "Остатки товаров",
                "alyprice.xls",
            )
        ],
        поставщик,
        конфигурации,
    )

    результат = await get_emails(session=None)

    assert len(результат) == 1, "письмо снова пропущено как чужое"
    _, файл, конфигурация = результат[0]
    assert конфигурация.id == 38
    assert файл.endswith("38-500.xls")


@pytest.mark.asyncio
async def test_unknown_senders_are_counted_in_diagnostics(monkeypatch):
    """Письма от нераспознанных отправителей должны быть видны.

    Прежде такое письмо исчезало бесследно: разбор причин показывал
    пустоту, потому что до отбора конфигураций дело не доходило.
    """
    поставщик, конфигурации = _альянс("someone-else@example.com")
    _почта(
        monkeypatch,
        [
            _письмо("500", "alyans-7@list.ru", "Остатки товаров", "a.xls"),
            _письмо("501", "alyans-7@list.ru", "Остатки товаров", "b.xls"),
            _письмо("502", "stranger@example.org", "Реклама", "c.pdf"),
        ],
        поставщик,
        конфигурации,
    )
    диагностика: dict = {}

    результат = await get_emails(session=None, diagnostics=диагностика)

    assert результат == []
    учтено = {
        строка["email"]: строка["emails"]
        for строка in диагностика["unknown_senders"]
    }
    assert учтено == {
        "alyans-7@list.ru": 2,
        "stranger@example.org": 1,
    }


@pytest.mark.asyncio
async def test_no_unknown_senders_when_all_recognised(monkeypatch):
    поставщик, конфигурации = _альянс("alyans-7@list.ru")
    _почта(
        monkeypatch,
        [
            _письмо(
                "500",
                "alyans-7@list.ru",
                "Остатки товаров",
                "alyprice.xls",
            )
        ],
        поставщик,
        конфигурации,
    )
    диагностика: dict = {}

    await get_emails(session=None, diagnostics=диагностика)

    assert диагностика["unknown_senders"] == []
