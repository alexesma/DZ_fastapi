import logging
from datetime import datetime
from io import BytesIO
from typing import List, Optional

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import and_, select

from dz_fastapi.core.constants import ANALYSIS_EMAIL
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.partner import crud_pricelist
from dz_fastapi.models.autopart import AutoPart, AutoPartPriceHistory
from dz_fastapi.models.partner import PriceList, Provider
from dz_fastapi.services.email import send_email_with_attachment

logger = logging.getLogger('dz_fastapi')


async def get_time_series_for_autopart_df(
    session: AsyncSession,
    autopart_id: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    provider_id: Optional[int] = None,
) -> pd.DataFrame:
    """
    Возвращает pandas.DataFrame c историческими
    данными для конкретной запчасти (autopart_id):
      - created_at: дата/время фиксации цены
      - provider_id: ID поставщика
      - provider_name: название поставщика
      - price: цена
      - quantity: остаток (из прайс-листа)

    Параметры:
      session: AsyncSession SQLAlchemy
      autopart_id: ID запчасти
      date_from, date_to: необязательные ограничения по дате.
    """
    q = (
        select(
            AutoPartPriceHistory.created_at,
            AutoPartPriceHistory.price,
            AutoPartPriceHistory.quantity,
            AutoPartPriceHistory.provider_id,
            Provider.name,
        )
        .join(Provider, Provider.id == AutoPartPriceHistory.provider_id)
        .where(AutoPartPriceHistory.autopart_id == autopart_id)
    )
    if date_from is not None:
        q = q.where(AutoPartPriceHistory.created_at >= date_from)
    if date_to is not None:
        q = q.where(AutoPartPriceHistory.created_at <= date_to)
    if provider_id is not None:
        q = q.where(AutoPartPriceHistory.provider_id == provider_id)

    # Сортируем, чтобы DataFrame был в хронологическом порядке
    q = q.order_by(
        AutoPartPriceHistory.created_at, AutoPartPriceHistory.provider_id
    )

    # Выполняем запрос
    rows = (await session.execute(q)).all()

    # Превращаем в DataFrame
    df = pd.DataFrame(
        rows,
        columns=[
            'created_at',
            'price',
            'quantity',
            'provider_id',
            'provider_name',
        ],
    )
    # Убедимся, что колонка дат в формате datetime
    df['created_at'] = pd.to_datetime(df['created_at'])

    return df


def create_excel_report(changes: list[dict]) -> BytesIO:
    """
    changes — список словарей вида:
      {
         'autopart_oem': str,
         'brand': str,
         'change_type': str,
         'old_value': Any,
         'new_value': Any,
         'diff_pct': Optional[str]
      }
    Возвращает BytesIO с Excel-файлом.
    """
    df = pd.DataFrame(changes)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name="Changes", index=False)
    output.seek(0)

    return output


async def analyze_new_pricelist(new_pl: PriceList, session: AsyncSession):
    """
    Сравнить новый прайс-лист (new_pl) с предыдущим для того же поставщика
    и вывести/вернуть результаты:
      - новые позиции (нет в старом)
      - изменение цены (в %)
      - изменение количества (в %)
    """
    logger.debug('Зашли в функцию analyze_new_pricelist')
    provider_id = new_pl.provider_id

    # 1) Найти все прайс-листы этого поставщика
    all_pls = await crud_pricelist.get_last_pricelists_by_provider(
        provider_id=provider_id, session=session
    )
    logger.debug(f'Прайс-листов поставщика {len(all_pls)}')

    # Если у нас меньше 2 прайс-листов, "старого" нет
    if len(all_pls) < 2:
        logger.info(
            f'Нет предыдущего прайс-листа для провайдера '
            f'{new_pl.provider.name}, ничего сравнивать.'
        )
        return
    old_pl = all_pls[-2]
    # 2) Создаём карты (dict) autopart_id => (price, quantity)
    old_map = {}
    for assoc in old_pl.autopart_associations:
        old_map[assoc.autopart_id] = (float(assoc.price), assoc.quantity)
    logger.debug(f'Старый прайс кол-во позиций: {len(old_map)}')
    new_map = {}
    for assoc in new_pl.autopart_associations:
        new_map[assoc.autopart_id] = (float(assoc.price), assoc.quantity)
    logger.debug(f'Новый прайс кол-во позиций: {len(new_map)}')
    # 3) Найдём новые позиции: (в новом, нет в старом)
    new_positions = set(new_map.keys()) - set(old_map.keys())
    # Список изменений, чтобы потом записать в Excel
    changes_list = []
    for autopart_id in new_positions:
        (new_price, new_qty) = new_map[autopart_id]
        autopart = await crud_autopart.get_autopart_by_id(
            session=session, autopart_id=autopart_id
        )
        logger.info(
            f'[ANALYSIS] Новая позиция autopart_name={autopart.name},'
            f'autopart_brand={autopart.brand.name}, '
            f'price={new_price}, qty={new_qty}'
        )
        changes_list.append(
            {
                'autopart_oem': autopart.oem_number,
                'brand': autopart.brand.name,
                'change_type': 'new_position',
                'old_value': None,
                'new_value': f'price={new_price}, qty={new_qty}',
                'diff_pct': None,
            }
        )

    # 4) Для общих позиций сравним price и quantity
    common_positions = set(new_map.keys()) & set(old_map.keys())
    for autopart_id in common_positions:
        (old_price, old_qty) = old_map[autopart_id]
        (new_price, new_qty) = new_map[autopart_id]
        autopart = await crud_autopart.get_autopart_by_id(
            session=session, autopart_id=autopart_id
        )
        price_diff = new_price - old_price
        price_diff_pct = 0.0
        if old_price != 0:
            price_diff_pct = (price_diff / old_price) * 100

        qty_diff = new_qty - old_qty
        qty_diff_pct = 0.0
        if old_qty != 0:
            qty_diff_pct = (qty_diff / old_qty) * 100

        if (
            abs(price_diff_pct) > 0.01
        ):  # чтобы отсеять микроскопические изменения
            msg = (
                f'[ANALYSIS] Новая позиция autopart_name={autopart.name},'
                f'autopart_brand={autopart.brand.name}, '
                f'Price changed {old_price} -> {new_price} '
                f'({price_diff_pct:.2f}%)'
            )
            logger.info(msg)
            changes_list.append(
                {
                    'autopart_oem': autopart.oem_number,
                    'brand': autopart.brand.name,
                    'change_type': 'price_changed',
                    'old_value': old_price,
                    'new_value': new_price,
                    'diff_pct': f'{price_diff_pct:.2f}%',
                }
            )

        if abs(qty_diff_pct) > 0.0001:
            msg = (
                f'[ANALYSIS] Новая позиция autopart_name={autopart.name},'
                f'autopart_brand={autopart.brand.name}, '
                f'Qty changed {old_qty} -> {new_qty} ({qty_diff_pct:.2f}%)'
            )
            logger.info(msg)
            changes_list.append(
                {
                    'autopart_oem': autopart.oem_number,
                    'brand': autopart.brand.name,
                    'change_type': 'qty_changed',
                    'old_value': old_qty,
                    'new_value': new_qty,
                    'diff_pct': f'{qty_diff_pct:.2f}%',
                }
            )

    # 5) В конце вывести Excel
    if changes_list:
        excel_file = create_excel_report(changes_list)
        logger.info(f'Excel report created, size={len(excel_file.getvalue())}')
        subject = (
            f'[ANALYSIS] Поставщик = {new_pl.provider.name}'
            f'| Прайс = {new_pl.config.name_price}'
        )
        filename = 'analysis_report.xlsx'
        send_email_with_attachment(
            to_email=ANALYSIS_EMAIL,
            subject=subject,
            body='Добрый день, высылаем Вам анализ нового прайса',
            attachment_filename=filename,
            attachment_bytes=excel_file.getvalue(),
        )
    else:
        logger.info('No changes detected, no Excel report generated.')

    return


async def analyze_autopart_popularity(
    session: AsyncSession,
    provider_id: int,
    date_start: datetime = datetime(2022, 1, 1),
    date_finish: datetime = datetime.now(),
) -> pd.DataFrame:
    """
    Анализирует позиции по истории изменений прайс-листов.
    Возвращает DataFrame со всеми позициями,
    ранжированными по перспективности.
    """
    # 1) Загружаем историю
    query = (
        select(
            AutoPartPriceHistory.autopart_id,
            AutoPartPriceHistory.quantity,
            AutoPartPriceHistory.created_at,
            AutoPart.oem_number,
            AutoPart.name,
        )
        .join(AutoPart, AutoPart.id == AutoPartPriceHistory.autopart_id)
        .where(
            and_(
                AutoPartPriceHistory.provider_id == provider_id,
                AutoPartPriceHistory.created_at >= date_start,
                AutoPartPriceHistory.created_at <= date_finish,
            )
        )
        .order_by(
            AutoPartPriceHistory.autopart_id, AutoPartPriceHistory.created_at
        )
    )
    result = (await session.execute(query)).all()

    df = pd.DataFrame(
        result,
        columns=[
            'autopart_id',
            'quantity',
            'created_at',
            'oem_number',
            'name',
        ],
    )

    # 2. Группируем по запчасти и считаем "продажи" как уменьшение количества
    def count_quantity_drops(group):
        group = group.sort_values('created_at')
        group['qty_diff'] = group['quantity'].diff()
        drops = group[group['qty_diff'] < 0]
        return pd.Series(
            {
                'total_qty_sold': -drops['qty_diff'].sum(),
                'times_qty_dropped': drops.shape[0],
                'last_seen': group['created_at'].max(),
                'last_quantity': group['quantity'].iloc[-1],
                'name': group['name'].iloc[-1],
                'oem_number': group['oem_number'].iloc[-1],
            }
        )

    result_df = (
        df.groupby('autopart_id').apply(count_quantity_drops).reset_index()
    )

    result_df.sort_values(by='total_qty_sold', ascending=False, inplace=True)

    return result_df


def create_autopart_analysis_excel(df: pd.DataFrame) -> BytesIO:
    report_df = df[
        [
            'oem_number',
            'name',
            'total_qty_sold',
            'times_qty_dropped',
            'last_quantity',
            'last_seen',
        ]
    ].rename(
        columns={
            'oem_number': 'OEM номер',
            'name': 'Название',
            'total_qty_sold': 'Продано всего (шт)',
            'times_qty_dropped': 'Сколько раз покупали',
            'last_quantity': 'Остаток последний',
            'last_seen': 'Последний раз в прайсе',
        }
    )

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        report_df.to_excel(
            writer, sheet_name='Популярные позиции', index=False
        )

        # Форматирование столбцов
        worksheet = writer.sheets['Популярные позиции']
        for column_cells in worksheet.columns:
            max_length = max(
                len(str(cell.value or '')) for cell in column_cells
            )
            worksheet.column_dimensions[
                column_cells[0].column_letter
            ].width = (max_length + 3)

    output.seek(0)
    return output


async def analyze_autopart_allprices(
    session: AsyncSession,
    autoparts: List[AutoPart],
    date_start: datetime = datetime(2022, 1, 1),
    date_finish: datetime = datetime.now(),
) -> pd.DataFrame:

    autopart_ids = [ap.id for ap in autoparts]

    # 1) Загружаем историю
    query = (
        select(
            AutoPartPriceHistory.created_at,
            AutoPartPriceHistory.price,
            Provider.name.label('provider'),
        )
        .join(Provider, Provider.id == AutoPartPriceHistory.provider_id)
        .where(
            and_(
                AutoPartPriceHistory.autopart_id.in_(autopart_ids),
                AutoPartPriceHistory.created_at >= date_start,
                AutoPartPriceHistory.created_at <= date_finish,
            )
        )
        .order_by(AutoPartPriceHistory.created_at)
    )
    result = await session.execute(query)
    rows = result.all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail='Нет данных по этому артикулу в указанный период',
        )

    df = pd.DataFrame(rows, columns=['created_at', 'price', 'provider'])
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    return df
