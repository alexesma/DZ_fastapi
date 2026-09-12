"""Перестановка колонок в прайсе поставщика не должна проходить молча.

Кунцево переставило в файле `Chery_CS` колонки цены и количества. Разбор
стал брать цену как остаток, остатки вышли дробными, а колонка
`quantity` в базе целочисленная — запись падала, прайс не создавался,
водяной знак письма не двигался. Система каждый день считала, что
загружать нечего: конфигурация простояла четыре с половиной месяца, и
ни ошибки, ни следа в журнале при этом не было.

Здесь закреплены три свойства, которые это исключают: дробный остаток
округляется, а не теряет весь файл; массовая дробность выносится
отдельным предупреждением; шаг загрузки оставляет след по каждой
конфигурации.
"""
from datetime import datetime
from io import BytesIO
from types import SimpleNamespace

import pandas as pd
import pytest

from dz_fastapi.services.email import get_emails
from dz_fastapi.services.pricelist_guard import (
    describe_rounded_quantities,
    guard_automatic_provider_pricelist,
)
from dz_fastapi.services.process import _prepare_pricelist_data

# Разметка сегодняшнего файла Chery_CS: цена второй, количество третьим.
ЗАГОЛОВКИ = ['Артикул', 'Наименование', 'Цена', 'Количество', 'Бренд']
ВЕРНЫЕ_КОЛОНКИ = {'price_col': 2, 'qty_col': 3}
ПЕРЕПУТАННЫЕ_КОЛОНКИ = {'price_col': 3, 'qty_col': 2}


def _файл(строки: list[list]) -> bytes:
    """Собирает .xlsx с тремя строками шапки, как присылает Кунцево."""
    ширина = len(ЗАГОЛОВКИ)
    пусто = [None] * ширина
    подпись = ['Прайс-лист от 12 сентября 2026 г.'] + [None] * (ширина - 1)
    таблица = pd.DataFrame(
        [пусто, подпись, пусто, ЗАГОЛОВКИ, *строки]
    )
    буфер = BytesIO()
    таблица.to_excel(буфер, index=False, header=False)
    return буфер.getvalue()


def _разобрать(содержимое: bytes, **колонки) -> tuple[list[dict], dict]:
    строки, статистика, _ = _prepare_pricelist_data(
        file_extension='xlsx',
        file_content=содержимое,
        start_row=4,
        oem_col=0,
        brand_col=4,
        name_col=1,
        multiplicity_col=None,
        **колонки,
    )
    return строки, статистика


# Цены с копейками и целые остатки — так выглядит файл на самом деле.
РЕАЛЬНЫЕ_СТРОКИ = [
    [f'ART{номер:04d}', f'Деталь {номер}', 100.17 + номер, 10 + номер, 'Chery']
    for номер in range(20)
]


def test_swapped_columns_keep_all_rows():
    """Главное: файл больше не теряется целиком."""
    строки, статистика = _разобрать(
        _файл(РЕАЛЬНЫЕ_СТРОКИ), **ПЕРЕПУТАННЫЕ_КОЛОНКИ
    )

    assert len(строки) == len(РЕАЛЬНЫЕ_СТРОКИ)
    assert статистика['rows_clean'] == len(РЕАЛЬНЫЕ_СТРОКИ)
    # Остаток пришёл из колонки цены, поэтому округлён у каждой строки.
    assert статистика['rows_quantity_rounded'] == len(РЕАЛЬНЫЕ_СТРОКИ)
    assert статистика['quantity_rounded_share'] == 1.0
    assert all(isinstance(строка['quantity'], int) for строка in строки)


def test_swapped_columns_are_reported():
    _, статистика = _разобрать(
        _файл(РЕАЛЬНЫЕ_СТРОКИ), **ПЕРЕПУТАННЫЕ_КОЛОНКИ
    )

    текст = describe_rounded_quantities(статистика)

    assert текст is not None
    assert '20 из 20' in текст
    assert 'перепутаны колонки' in текст


def test_correct_columns_round_nothing():
    строки, статистика = _разобрать(_файл(РЕАЛЬНЫЕ_СТРОКИ), **ВЕРНЫЕ_КОЛОНКИ)

    assert статистика['rows_quantity_rounded'] == 0
    assert статистика['quantity_rounded_share'] == 0.0
    assert describe_rounded_quantities(статистика) is None
    assert строки[0]['quantity'] == 10
    assert строки[0]['price'] == pytest.approx(100.17)


def test_single_fractional_quantity_is_rounded_without_warning():
    """Дробный остаток бывает законным — из-за одного не шумим."""
    строки_файла = [list(строка) for строка in РЕАЛЬНЫЕ_СТРОКИ]
    строки_файла[0][3] = 3.4

    строки, статистика = _разобрать(_файл(строки_файла), **ВЕРНЫЕ_КОЛОНКИ)

    assert статистика['rows_quantity_rounded'] == 1
    assert статистика['quantity_rounded_share'] == 0.05
    assert describe_rounded_quantities(статистика) is None
    остатки = {строка['oem_number']: строка['quantity'] for строка in строки}
    assert остатки['ART0000'] == 3


@pytest.mark.parametrize(
    'статистика',
    [
        None,
        {},
        {'quantity_rows_checked': 0, 'rows_quantity_rounded': 0},
        {'quantity_rows_checked': 100, 'rows_quantity_rounded': 0},
        # Ровно на пороге снизу: 9 из 100 — ещё молчим.
        {'quantity_rows_checked': 100, 'rows_quantity_rounded': 9},
    ],
)
def test_no_warning_below_threshold(статистика):
    assert describe_rounded_quantities(статистика) is None


def test_warning_from_threshold():
    текст = describe_rounded_quantities(
        {'quantity_rows_checked': 100, 'rows_quantity_rounded': 10}
    )
    assert текст is not None
    assert '10 из 100 (10%)' in текст


@pytest.mark.asyncio
async def test_guard_reports_rounding_without_blocking(monkeypatch):
    """Округление само не блокирует прайс, но попадает в разбор причин.

    Блокировка остаётся за проверкой цен: дробные остатки законны, и
    ежедневно придерживать из-за них прайс было бы хуже молчания.
    """

    async def нет_прошлого(session, config_id):
        return None, {}

    async def нет_синонимов(session):
        return {}

    monkeypatch.setattr(
        'dz_fastapi.services.pricelist_guard._load_previous_price_map',
        нет_прошлого,
    )
    monkeypatch.setattr(
        'dz_fastapi.services.pricelist_guard._load_canonical_brand_name_map',
        нет_синонимов,
    )

    результат = await guard_automatic_provider_pricelist(
        session=None,
        provider=SimpleNamespace(id=932, name='CHERY KUNTSEVO'),
        provider_config=SimpleNamespace(
            id=35, name_price='price_kuntsevo_Chery_CS'
        ),
        items=[{'brand': 'Chery', 'oem': 'ART0001', 'price': 360}],
        source_filename='price_kuntsevo_Chery_CS12.09.2026.xlsx',
        parse_stats={
            'quantity_rows_checked': 10556,
            'rows_quantity_rounded': 10447,
        },
    )

    assert результат.blocked is False
    assert len(результат.reasons) == 1
    assert '10447 из 10556' in результат.reasons[0]
    assert результат.metrics['rows_quantity_rounded'] == 10447
    assert результат.metrics['quantity_rows_checked'] == 10556


def _письмо(uid: str, тема: str, вложение: str, час: int):
    return SimpleNamespace(
        uid=uid,
        from_='chery.1c@kuntsevo.com',
        subject=тема,
        attachments=[SimpleNamespace(filename=вложение, payload=b'x')],
        date=datetime(2026, 9, 12, час, 0, 0),
    )


def _почта_кунцево(monkeypatch, письма, последний_uid=0, скачивать=True):
    """Подставляет ящик с письмами Кунцево и две его конфигурации."""
    account = SimpleNamespace(
        id=6,
        email='price@dragonzap.ru',
        transport='smtp',
        imap_host='imap.example',
        password='secret',
        imap_folder='INBOX',
        imap_port=993,
    )
    provider = SimpleNamespace(
        id=932,
        name='CHERY KUNTSEVO',
        email_incoming_price='Chery.1c@kuntsevo.com',
    )
    конфигурации = [
        SimpleNamespace(
            id=35,
            provider_id=932,
            name_mail=None,
            name_price='price_kuntsevo_Chery_CS',
            filename_pattern=None,
            file_url=None,
            incoming_email_account_id=6,
        ),
        SimpleNamespace(
            id=36,
            provider_id=932,
            name_mail='Chery Кунцево',
            name_price='price_kuntsevo__Chery',
            filename_pattern=None,
            file_url=None,
            incoming_email_account_id=6,
        ),
    ]

    async def активные(session, purpose):
        return [account]

    def ящик(*args, **kwargs):
        return list(письма)

    async def поставщик_по_почте(session, email):
        return provider

    async def конфиги(provider_id, session, only_active):
        return конфигурации

    async def водяной_знак(
        provider_id, session, provider_config_id=None, folder=None
    ):
        return последний_uid

    async def скачать(msg, provider, provider_conf, session):
        if not скачивать:
            return None
        return f'/tmp/{provider_conf.id}-{msg.uid}.xlsx'

    for путь, значение in (
        ('crud_email_account.get_active_by_purpose', активные),
        ('_fetch_mailbox_messages', ящик),
        ('crud_provider.get_by_email_incoming_price', поставщик_по_почте),
        ('crud_provider_pricelist_config.get_configs', конфиги),
        ('_get_last_uid_compat', водяной_знак),
        ('download_new_price_provider', скачать),
    ):
        monkeypatch.setattr(f'dz_fastapi.services.email.{путь}', значение)
    return provider, конфигурации


@pytest.mark.asyncio
async def test_diagnostics_name_config_left_without_file(monkeypatch):
    """Тот самый случай: письмо для 35 не пришло, и это должно быть видно."""
    только_второе = [
        _письмо(
            '302294',
            'Chery Кунцево остатки 12.09.26',
            'price_kuntsevo__Chery 12.09.2026.xls',
            10,
        )
    ]
    _почта_кунцево(monkeypatch, только_второе)
    диагностика: dict = {}

    результат = await get_emails(session=None, diagnostics=диагностика)

    assert len(результат) == 1
    assert диагностика['downloaded'] == 1
    assert диагностика['considered_configs'] == 2
    проблемы = диагностика['problems']
    assert [строка['config_id'] for строка in проблемы] == [35]
    assert проблемы[0]['outcome'] == 'no_matching_email'
    assert проблемы[0]['config'] == 'price_kuntsevo_Chery_CS'
    assert проблемы[0]['provider'] == 'CHERY KUNTSEVO'


@pytest.mark.asyncio
async def test_diagnostics_stay_empty_when_both_configs_load(monkeypatch):
    оба = [
        _письмо(
            '302294',
            'Chery Кунцево остатки 12.09.26',
            'price_kuntsevo__Chery 12.09.2026.xls',
            10,
        ),
        _письмо(
            '302293',
            '12.09.26',
            'price_kuntsevo_Chery_CS12.09.2026.xlsx',
            9,
        ),
    ]
    _почта_кунцево(monkeypatch, оба)
    диагностика: dict = {}

    результат = await get_emails(session=None, diagnostics=диагностика)

    assert len(результат) == 2
    assert диагностика['downloaded'] == 2
    assert диагностика['problems'] == []


@pytest.mark.asyncio
async def test_diagnostics_separate_old_emails_from_missing_ones(monkeypatch):
    """Уже загруженное письмо и отсутствующее — разные причины."""
    оба = [
        _письмо(
            '302294',
            'Chery Кунцево остатки 12.09.26',
            'price_kuntsevo__Chery 12.09.2026.xls',
            10,
        ),
        _письмо(
            '302293',
            '12.09.26',
            'price_kuntsevo_Chery_CS12.09.2026.xlsx',
            9,
        ),
    ]
    _почта_кунцево(monkeypatch, оба, последний_uid=999999)
    диагностика: dict = {}

    результат = await get_emails(session=None, diagnostics=диагностика)

    assert результат == []
    причины = {
        строка['config_id']: строка['outcome']
        for строка in диагностика['problems']
    }
    assert причины == {
        35: 'only_already_loaded_emails',
        36: 'only_already_loaded_emails',
    }


@pytest.mark.asyncio
async def test_diagnostics_mark_failed_download(monkeypatch):
    только_второе = [
        _письмо(
            '302294',
            'Chery Кунцево остатки 12.09.26',
            'price_kuntsevo__Chery 12.09.2026.xls',
            10,
        )
    ]
    _почта_кунцево(monkeypatch, только_второе, скачивать=False)
    диагностика: dict = {}

    await get_emails(session=None, diagnostics=диагностика)

    причины = {
        строка['config_id']: строка['outcome']
        for строка in диагностика['problems']
    }
    assert причины[36] == 'download_failed'


@pytest.mark.asyncio
async def test_diagnostics_are_optional(monkeypatch):
    """Без аргумента поведение прежнее — его вызывают и из других мест."""
    _почта_кунцево(
        monkeypatch,
        [
            _письмо(
                '302294',
                'Chery Кунцево остатки 12.09.26',
                'price_kuntsevo__Chery 12.09.2026.xls',
                10,
            )
        ],
    )

    результат = await get_emails(session=None)

    assert len(результат) == 1


@pytest.mark.asyncio
async def test_intake_problems_explain_silent_config(test_session):
    """Причина молчания конфигурации собирается из журнала запусков."""
    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.models.settings import ExecutionTrace
    from dz_fastapi.services.monitoring import provider_config_intake_problems

    сейчас = now_moscow()
    test_session.add(
        ExecutionTrace(
            trace_type='scheduler_job',
            job_key='download_price_provider',
            job_name='Download price provider',
            status='success',
            started_at=сейчас,
            details={
                'email_processing_summary': {
                    'download_diagnostics': {
                        'considered_configs': 2,
                        'downloaded': 1,
                        'problems': [
                            {
                                'config_id': 35,
                                'provider_id': 932,
                                'provider': 'CHERY KUNTSEVO',
                                'config': 'price_kuntsevo_Chery_CS',
                                'outcome': 'no_matching_email',
                                'emails_seen': 2,
                                'emails_matched': 0,
                                'skipped_old_uid': 0,
                                'pattern': 'price_kuntsevo_Chery_CS',
                                'name_mail': None,
                            },
                            {
                                'config_id': 77,
                                'provider_id': 111,
                                'config': 'чужой поставщик',
                                'outcome': 'download_failed',
                                'emails_seen': 1,
                                'emails_matched': 1,
                                'skipped_old_uid': 0,
                            },
                        ],
                    }
                }
            },
        )
    )
    await test_session.commit()

    строки = await provider_config_intake_problems(test_session, 932)

    assert [строка['provider_config_id'] for строка in строки] == [35]
    строка = строки[0]
    assert строка['outcome'] == 'no_matching_email'
    assert 'шаблоном' in строка['message']
    assert строка['emails_seen'] == 2
    assert строка['emails_matched'] == 0
    assert строка['filename_pattern'] == 'price_kuntsevo_Chery_CS'
    assert строка['detected_at'] is not None


@pytest.mark.asyncio
async def test_intake_problems_keep_latest_run_only(test_session):
    """Показываем последнюю причину, а не всю историю."""
    from datetime import timedelta

    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.models.settings import ExecutionTrace
    from dz_fastapi.services.monitoring import provider_config_intake_problems

    сейчас = now_moscow()

    def запуск(исход, когда):
        return ExecutionTrace(
            trace_type='scheduler_job',
            job_key='download_price_provider',
            job_name='Download price provider',
            status='success',
            started_at=когда,
            details={
                'email_processing_summary': {
                    'download_diagnostics': {
                        'problems': [
                            {
                                'config_id': 41,
                                'provider_id': 937,
                                'config': 'Cosmo.xlsx',
                                'outcome': исход,
                            }
                        ]
                    }
                }
            },
        )

    test_session.add(запуск('download_failed', сейчас - timedelta(hours=2)))
    test_session.add(запуск('only_already_loaded_emails', сейчас))
    await test_session.commit()

    строки = await provider_config_intake_problems(test_session, 937)

    assert len(строки) == 1
    assert строки[0]['outcome'] == 'only_already_loaded_emails'


@pytest.mark.asyncio
async def test_intake_problems_include_rounding_warning(
    test_session, created_providers, created_pricelist_config
):
    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.models.settings import ExecutionTrace
    from dz_fastapi.services.monitoring import provider_config_intake_problems

    провайдер = created_providers[0]
    test_session.add(
        ExecutionTrace(
            trace_type='provider_pricelist',
            job_key='process_provider_pricelist',
            job_name='Process provider pricelist',
            status='warning',
            provider_id=провайдер.id,
            provider_config_id=created_pricelist_config.id,
            started_at=now_moscow(),
            details={
                'provider_config_name': 'price_kuntsevo_Chery_CS',
                'quantity_rounding_warning': (
                    'Дробных остатков 10447 из 10556 (99%) — они округлены.'
                ),
            },
        )
    )
    await test_session.commit()

    строки = await provider_config_intake_problems(test_session, провайдер.id)

    assert len(строки) == 1
    строка = строки[0]
    assert строка['provider_config_id'] == created_pricelist_config.id
    assert строка['outcome'] == 'quantity_rounded'
    assert '10447 из 10556' in строка['rounding_warning']


@pytest.mark.asyncio
async def test_intake_problems_endpoint(
    async_client, test_session, created_providers
):
    from dz_fastapi.core.time import now_moscow
    from dz_fastapi.models.settings import ExecutionTrace

    провайдер = created_providers[0]
    test_session.add(
        ExecutionTrace(
            trace_type='scheduler_job',
            job_key='download_price_provider',
            job_name='Download price provider',
            status='success',
            started_at=now_moscow(),
            details={
                'email_processing_summary': {
                    'download_diagnostics': {
                        'problems': [
                            {
                                'config_id': 35,
                                'provider_id': провайдер.id,
                                'config': 'price_kuntsevo_Chery_CS',
                                'outcome': 'no_matching_email',
                                'emails_seen': 2,
                                'emails_matched': 0,
                                'skipped_old_uid': 0,
                            }
                        ]
                    }
                }
            },
        )
    )
    await test_session.commit()

    ответ = await async_client.get(
        f'/providers/{провайдер.id}/pricelist-intake-problems/'
    )

    assert ответ.status_code == 200
    данные = ответ.json()
    assert len(данные) == 1
    assert данные[0]['provider_config_id'] == 35
    assert данные[0]['outcome'] == 'no_matching_email'
    assert данные[0]['message']


@pytest.mark.asyncio
async def test_intake_problems_endpoint_404_for_unknown_provider(async_client):
    ответ = await async_client.get('/providers/999999/pricelist-intake-problems/')
    assert ответ.status_code == 404
