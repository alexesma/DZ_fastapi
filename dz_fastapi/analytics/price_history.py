import logging
from datetime import datetime
from io import BytesIO
from typing import Optional

import pandas as pd
from sqlalchemy import select

from dz_fastapi.core.constants import ANALYSIS_EMAIL
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.partner import crud_pricelist
from dz_fastapi.models.autopart import AutoPartPriceHistory
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
    provider_id = new_pl.provider.id

    # 1) Найти все прайс-листы этого поставщика
    all_pls = await crud_pricelist.get_pricelists_by_provider(
        provider_id=provider_id, session=session
    )
    logger.debug(f'Прайс-листов поставщика {all_pls}')

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

            # 5) Если хотим в конце вывести Excel
        if changes_list:
            excel_file = create_excel_report(changes_list)
            logger.info(
                f'Excel report created, size={len(excel_file.getvalue())}'
            )
            subject = f'[ANALYSIS] for new price {provider_id}'
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
