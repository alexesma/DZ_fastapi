import logging
import unicodedata
from typing import List

import pandas as pd

from dz_fastapi.models.partner import CustomerPriceListAutoPartAssociation

logger = logging.getLogger('dz_fastapi')


def individual_markups(
    individual_markups: dict, df: pd.DataFrame
) -> pd.DataFrame:
    for provider_id, markup in individual_markups.items():
        multiplier = markup / 100 + 1
        df.loc[df['provider_id'] == int(provider_id), 'price'] *= multiplier
    return df


def price_intervals(price_intervals: dict, df: pd.DataFrame) -> pd.DataFrame:
    for interval in price_intervals:
        min_price = float(interval.min_price)
        max_price = float(interval.max_price)
        coefficient = float(interval.coefficient) / 100 + 1
        df.loc[
            (df['price'] >= min_price) & (df['price'] <= max_price), 'price'
        ] *= coefficient
    return df


def brand_filters(brand_filters: dict, df: pd.DataFrame) -> pd.DataFrame:
    if brand_filters.get('type') == 'exclude':
        df = df[~df['brand_id'].isin(brand_filters.get('brands', []))]
    elif brand_filters.get('type') == 'include':
        df = df[df['brand_id'].isin(brand_filters.get('brands', []))]
    return df


def position_filters(position_filters: dict, df: pd.DataFrame) -> pd.DataFrame:
    if position_filters.get('type') == 'exclude':
        df = df[~df['autopart_id'].isin(position_filters.get('autoparts', []))]
    elif position_filters.get('type') == 'include':
        df = df[df['autopart_id'].isin(position_filters.get('autoparts', []))]
    return df


def supplier_quantity_filters(
    supplier_quantity_filters: dict, df: pd.DataFrame
) -> pd.DataFrame:
    combined_mask = pd.Series(False, index=df.index)
    for supplier_filter in supplier_quantity_filters:
        provider_id = supplier_filter.provider_id
        min_qty = supplier_filter.min_quantity
        max_qty = supplier_filter.max_quantity
        mask = (
            (df['provider_id'] == provider_id)
            & (df['quantity'] >= min_qty)
            & (df['quantity'] <= max_qty)
        )
        combined_mask |= mask
    return df[combined_mask]


def position_exclude(
    provider_id: int, excluded_autoparts: list[int], df: pd.DataFrame
) -> pd.DataFrame:
    """
    Исключает указанные позиции автозапчастей
    для заданного поставщика из DataFrame.

    :param provider_id: ID поставщика, для которого нужно исключить позиции.
    :param excluded_autoparts: Список ID автозапчастей для исключения.
    :param df: DataFrame с данными прайс-листа.
    :return: Отфильтрованный DataFrame.
    """
    mask = ~(
        (df['provider_id'] == provider_id)
        & (df['autopart_id'].isin(excluded_autoparts))
    )
    return df[mask]


def prepare_excel_data(
    associations: List[CustomerPriceListAutoPartAssociation],
) -> pd.DataFrame:
    """
    Преобразует список ассоциаций автозапчастей
    в DataFrame для экспорта в Excel.

    :param associations: Список ассоциаций
    (CustomerPriceListAutoPartAssociation).
    :return: DataFrame с подготовленными данными.
    """
    excel_data = []
    for assoc in associations:
        autopart = assoc.autopart
        excel_data.append(
            {
                'Производитель': (
                    autopart.brand.name if autopart.brand else None
                ),
                'Наименование': autopart.name,
                'Артикул': autopart.oem_number,
                'Количество': assoc.quantity,
                'Цена': assoc.price,
            }
        )
    return pd.DataFrame(excel_data)


async def compare_pricelists(old_pl, new_pl, qty_diff_threshold: int = 3):
    """
    Сравнить два прайса (старый old_pl и новый new_pl),
    посчитать различия по цене и количеству, залогировать изменения.

    qty_diff_threshold - если разница в штуках меньше этого значения,
                         можно считать изменение количества несущественным.
    """
    # Получаем словарь { (oem_number, brand):
    # (price, quantity) } для старого прайс-листа
    old_map = {}
    for old_assoc in old_pl.autoparts:
        key = (old_assoc.autopart.oem_number, old_assoc.autopart.brand)
        old_map[key] = (old_assoc.price, old_assoc.quantity)

    # Аналогично - для нового
    new_map = {}
    for new_assoc in new_pl.autoparts:
        key = (new_assoc.autopart.oem_number, new_assoc.autopart.brand)
        new_map[key] = (new_assoc.price, new_assoc.quantity)

    # Пройдём по всем позициям из нового прайса
    for key, (new_price, new_qty) in new_map.items():
        old_price_qty = old_map.get(key)
        if old_price_qty:
            (old_price, old_qty) = old_price_qty

            # === Сравнение цены ===
            if old_price != 0:
                price_diff = new_price - old_price
                price_diff_pct = (price_diff / old_price) * 100
            else:
                # Если старой цены не было (0), определяем,
                # как интерпретировать
                price_diff_pct = (
                    100.0  # условно считаем, что изменилась на 100%
                )

            # === Сравнение остатков ===
            qty_diff = new_qty - old_qty
            # Если разница в штуках меньше порога - считаем «несущественным»
            if abs(qty_diff) < qty_diff_threshold:
                qty_diff_pct = 0  # или считаем 0, или просто игнорируем
            else:
                # Но если хотим процент, аналогично:
                if old_qty != 0:
                    qty_diff_pct = (qty_diff / old_qty) * 100
                else:
                    qty_diff_pct = 100.0

            # Логируем, если что-то существенно изменилось
            if price_diff_pct != 0 or qty_diff_pct != 0:
                logger.info(
                    f'[PriceListCompare] OEM={key[0]}, Brand={key[1]}. '
                    f'OldPrice={old_price}, NewPrice={new_price}, '
                    f'ΔPrice={price_diff_pct:.2f}%. '
                    f'OldQty={old_qty}, NewQty={new_qty}, '
                    f'ΔQty={qty_diff_pct:.2f}%.'
                )
        else:
            # Позиция новая, раньше её не было
            logger.info(
                f'[PriceListCompare] New item: OEM={key[0]}, '
                f'Brand={key[1]} - Price={new_price}, Qty={new_qty}'
            )

    # Если хотите, можно проверить также позиции,
    # которые исчезли в новом прайсе
    # (которые были в old_map, но нет в new_map)
    for key, (old_price, old_qty) in old_map.items():
        if key not in new_map:
            logger.info(
                f'[PriceListCompare] Item removed from '
                f'new PL: OEM={key[0]}, Brand={key[1]}. '
                f'OldPrice={old_price}, OldQty={old_qty}'
            )


def normalize_str(name: str) -> str:
    return unicodedata.normalize('NFC', name)
