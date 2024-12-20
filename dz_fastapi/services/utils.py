from typing import List

import pandas as pd


from dz_fastapi.models.partner import CustomerPriceListAutoPartAssociation


def individual_markups(
        individual_markups: dict,
        df: pd.DataFrame
) -> pd.DataFrame:
    for provider_id, markup in individual_markups.items():
        multiplier = markup / 100 + 1
        df.loc[
            df['provider_id'] == int(provider_id),
            'price'
        ] *= multiplier
    return df


def price_intervals(
        price_intervals: dict,
        df: pd.DataFrame
) -> pd.DataFrame:
    for interval in price_intervals:
        min_price = float(interval.min_price)
        max_price = float(interval.max_price)
        coefficient = float(interval.coefficient) / 100 + 1
        df.loc[
            (df['price'] >= min_price) & (df['price'] <= max_price),
            'price'
        ] *= coefficient
    return df


def brand_filters(
        brand_filters: dict,
        df: pd.DataFrame
) -> pd.DataFrame:
    if brand_filters.get('type') == 'exclude':
        df = df[
            ~df['brand_id'].isin(
                brand_filters.get('brands', [])
            )
        ]
    elif brand_filters.get('type') == 'include':
        df = df[
            df['brand_id'].isin(
                brand_filters.get('brands', [])
            )
        ]
    return df


def position_filters(
        position_filters: dict,
        df: pd.DataFrame
) -> pd.DataFrame:
    if position_filters.get('type') == 'exclude':
        df = df[
            ~df['autopart_id'].isin(
                position_filters.get('autoparts', [])
            )
        ]
    elif position_filters.get('type') == 'include':
        df = df[
            df['autopart_id'].isin(
                position_filters.get('autoparts', [])
            )
        ]
    return df


def supplier_quantity_filters(
        supplier_quantity_filters: dict,
        df: pd.DataFrame
) -> pd.DataFrame:
    combined_mask = pd.Series(False, index=df.index)
    for supplier_filter in supplier_quantity_filters:
        provider_id = supplier_filter.provider_id
        min_qty = supplier_filter.min_quantity
        max_qty = supplier_filter.max_quantity
        mask = (df['provider_id'] == provider_id) & \
               (df['quantity'] >= min_qty) & (df['quantity'] <= max_qty)
        combined_mask |= mask
    return df[combined_mask]


def position_exclude(
        provider_id: int,
        excluded_autoparts: list[int],
        df: pd.DataFrame
) -> pd.DataFrame:
    """
    Исключает указанные позиции автозапчастей для заданного поставщика из DataFrame.

    :param provider_id: ID поставщика, для которого нужно исключить позиции.
    :param excluded_autoparts: Список ID автозапчастей для исключения.
    :param df: DataFrame с данными прайс-листа.
    :return: Отфильтрованный DataFrame.
    """
    mask = ~((df['provider_id'] == provider_id) & (df['autopart_id'].isin(excluded_autoparts)))
    return df[mask]


def prepare_excel_data(
        associations: List[CustomerPriceListAutoPartAssociation]
) -> pd.DataFrame:
    """
    Преобразует список ассоциаций автозапчастей в DataFrame для экспорта в Excel.

    :param associations: Список ассоциаций (CustomerPriceListAutoPartAssociation).
    :return: DataFrame с подготовленными данными.
    """
    excel_data = []
    for assoc in associations:
        autopart = assoc.autopart
        excel_data.append({
            'Производитель': autopart.brand.name if autopart.brand else None,
            'Наименование': autopart.name,
            'Артикул': autopart.oem_number,
            'Количество': assoc.quantity,
            'Цена': assoc.price
        })
    return pd.DataFrame(excel_data)
