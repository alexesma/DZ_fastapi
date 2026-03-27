import logging
from datetime import datetime
from io import BytesIO
from typing import Any, List, Optional

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import and_, select

from dz_fastapi.core.db import AsyncSession
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import crud_pricelist
from dz_fastapi.models.autopart import AutoPart, AutoPartPriceHistory
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import PriceList, Provider

logger = logging.getLogger('dz_fastapi')


def _get_previous_pricelist(
    new_pl: PriceList,
    recent_pricelists: list[PriceList],
) -> Optional[PriceList]:
    for pricelist in recent_pricelists:
        if pricelist.id != new_pl.id:
            return pricelist
    return None


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


def _build_pricelist_map(pricelist: PriceList) -> dict[int, dict[str, Any]]:
    return {
        assoc.autopart_id: {
            'price': float(assoc.price),
            'quantity': int(assoc.quantity),
        }
        for assoc in pricelist.autopart_associations
    }


async def _get_autopart_details(
    session: AsyncSession, autopart_ids: set[int]
) -> dict[int, dict[str, Any]]:
    if not autopart_ids:
        return {}

    stmt = (
        select(
            AutoPart.id,
            AutoPart.oem_number,
            AutoPart.name,
            Brand.name.label('brand_name'),
        )
        .join(Brand, Brand.id == AutoPart.brand_id)
        .where(AutoPart.id.in_(autopart_ids))
    )
    rows = (await session.execute(stmt)).all()
    return {
        row.id: {
            'oem_number': row.oem_number,
            'name': row.name,
            'brand': row.brand_name,
        }
        for row in rows
    }


async def build_pricelist_change_summary(
    new_pl: PriceList,
    old_pl: PriceList,
    session: AsyncSession,
    top_n: int = 20,
) -> dict[str, Any]:
    old_map = _build_pricelist_map(old_pl)
    new_map = _build_pricelist_map(new_pl)
    logger.debug('Старый прайс кол-во позиций: %s', len(old_map))
    logger.debug('Новый прайс кол-во позиций: %s', len(new_map))

    new_positions = set(new_map.keys()) - set(old_map.keys())
    removed_positions = set(old_map.keys()) - set(new_map.keys())
    common_positions = set(new_map.keys()) & set(old_map.keys())

    changed_price_count = 0
    changed_quantity_count = 0
    turnover_candidates: list[dict[str, Any]] = []
    price_change_candidates: list[dict[str, Any]] = []

    for autopart_id in common_positions:
        old_item = old_map[autopart_id]
        new_item = new_map[autopart_id]

        old_price = old_item['price']
        new_price = new_item['price']
        old_qty = old_item['quantity']
        new_qty = new_item['quantity']

        price_diff = new_price - old_price
        if old_price:
            price_diff_pct = (price_diff / old_price) * 100
        else:
            price_diff_pct = 0.0

        qty_diff = new_qty - old_qty

        if abs(price_diff_pct) > 0.01:
            changed_price_count += 1
            price_change_candidates.append(
                {
                    'autopart_id': autopart_id,
                    'old_price': old_price,
                    'new_price': new_price,
                    'price_diff': price_diff,
                    'price_diff_pct': price_diff_pct,
                    'old_quantity': old_qty,
                    'new_quantity': new_qty,
                }
            )

        if qty_diff != 0:
            changed_quantity_count += 1
            if qty_diff < 0:
                turnover_candidates.append(
                    {
                        'autopart_id': autopart_id,
                        'old_quantity': old_qty,
                        'new_quantity': new_qty,
                        'quantity_drop': abs(qty_diff),
                        'old_price': old_price,
                        'new_price': new_price,
                    }
                )

    turnover_candidates.sort(
        key=lambda item: (
            item['quantity_drop'],
            item['old_quantity'],
            item['autopart_id'],
        ),
        reverse=True,
    )
    price_change_candidates.sort(
        key=lambda item: (
            abs(item['price_diff_pct']),
            abs(item['price_diff']),
            item['autopart_id'],
        ),
        reverse=True,
    )

    top_turnover = turnover_candidates[:top_n]
    top_price_changes = price_change_candidates[:top_n]
    autopart_details = await _get_autopart_details(
        session,
        {
            item['autopart_id']
            for item in [*top_turnover, *top_price_changes]
        },
    )

    def enrich(
        items: list[dict[str, Any]],
        extra_fields: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        enriched_items = []
        for item in items:
            details = autopart_details.get(item['autopart_id'], {})
            payload = {
                'autopart_id': item['autopart_id'],
                'oem_number': details.get('oem_number'),
                'brand': details.get('brand'),
                'name': details.get('name'),
            }
            for field in extra_fields:
                payload[field] = item.get(field)
            enriched_items.append(payload)
        return enriched_items

    return {
        'latest_pricelist_id': new_pl.id,
        'latest_pricelist_date': new_pl.date,
        'previous_pricelist_id': old_pl.id,
        'previous_pricelist_date': old_pl.date,
        'latest_positions_count': len(new_map),
        'previous_positions_count': len(old_map),
        'new_positions_count': len(new_positions),
        'removed_positions_count': len(removed_positions),
        'changed_price_count': changed_price_count,
        'changed_quantity_count': changed_quantity_count,
        'top_turnover_positions': enrich(
            top_turnover,
            (
                'old_quantity',
                'new_quantity',
                'quantity_drop',
                'old_price',
                'new_price',
            ),
        ),
        'sharpest_price_changes': enrich(
            top_price_changes,
            (
                'old_price',
                'new_price',
                'price_diff',
                'price_diff_pct',
                'old_quantity',
                'new_quantity',
            ),
        ),
    }


async def get_pricelist_change_summary(
    session: AsyncSession,
    provider_id: int,
    provider_config_id: int,
    top_n: int = 20,
) -> dict[str, Any]:
    recent_pricelists = await crud_pricelist.get_last_pricelists_by_provider(
        provider_id=provider_id,
        provider_config_id=provider_config_id,
        session=session,
    )
    logger.debug('Прайс-листов поставщика %s', len(recent_pricelists))
    if not recent_pricelists:
        return {
            'ready': False,
            'latest_pricelist_id': None,
            'latest_pricelist_date': None,
            'previous_pricelist_id': None,
            'previous_pricelist_date': None,
            'latest_positions_count': 0,
            'previous_positions_count': 0,
            'new_positions_count': 0,
            'removed_positions_count': 0,
            'changed_price_count': 0,
            'changed_quantity_count': 0,
            'top_turnover_positions': [],
            'sharpest_price_changes': [],
            'note': 'Для этой конфигурации еще не загружено ни одного прайса.',
        }

    new_pl = recent_pricelists[0]
    old_pl = _get_previous_pricelist(new_pl, recent_pricelists)
    if old_pl is None:
        return {
            'ready': False,
            'latest_pricelist_id': new_pl.id,
            'latest_pricelist_date': new_pl.date,
            'previous_pricelist_id': None,
            'previous_pricelist_date': None,
            'latest_positions_count': len(_build_pricelist_map(new_pl)),
            'previous_positions_count': 0,
            'new_positions_count': 0,
            'removed_positions_count': 0,
            'changed_price_count': 0,
            'changed_quantity_count': 0,
            'top_turnover_positions': [],
            'sharpest_price_changes': [],
            'note': 'Нужны минимум два прайса для сравнения.',
        }

    summary = await build_pricelist_change_summary(
        new_pl=new_pl,
        old_pl=old_pl,
        session=session,
        top_n=top_n,
    )
    summary['ready'] = True
    summary['note'] = None
    return summary


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

    summary = await get_pricelist_change_summary(
        session=session,
        provider_id=provider_id,
        provider_config_id=new_pl.provider_config_id,
    )
    if not summary['ready']:
        logger.info(
            'Недостаточно данных для анализа '
            'provider_id=%s provider_config_id=%s: %s',
            new_pl.provider_id,
            new_pl.provider_config_id,
            summary['note'],
        )
        return summary

    logger.info(
        'Сводный анализ прайса provider_id=%s provider_config_id=%s: '
        'новых=%s удаленных=%s изменено цен=%s изменено остатков=%s',
        new_pl.provider_id,
        new_pl.provider_config_id,
        summary['new_positions_count'],
        summary['removed_positions_count'],
        summary['changed_price_count'],
        summary['changed_quantity_count'],
    )
    return summary


async def analyze_autopart_popularity(
    session: AsyncSession,
    provider_id: int,
    date_start: datetime = datetime(2022, 1, 1),
    date_finish: datetime = now_moscow(),
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
    date_finish: datetime = now_moscow(),
) -> pd.DataFrame:

    if not autoparts:
        raise HTTPException(
            status_code=404,
            detail='Запчасти по этому артикулу не найдены',
        )

    autopart_ids = [ap.id for ap in autoparts]

    # 1) Загружаем историю
    query = (
        select(
            AutoPartPriceHistory.created_at,
            AutoPartPriceHistory.price,
            AutoPartPriceHistory.quantity,
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

    df = pd.DataFrame(
        rows,
        columns=['created_at', 'price', 'quantity', 'provider'],
    )
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    return df
