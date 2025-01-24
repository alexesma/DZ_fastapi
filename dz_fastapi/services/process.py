import asyncio
import logging
from datetime import date, datetime
from functools import partial
from io import BytesIO, StringIO
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import HTTPException
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.analytics.price_history import analyze_new_pricelist
from dz_fastapi.core.constants import (BRILLIANCE_OEM, CUMMINS_OEM, FAW_OEM,
                                       GEELY_NOT_OEM, INDICATOR_BYD,
                                       INDICATOR_BYD_FIRST_FIVE,
                                       INDICATOR_BYD_FIRST_THREE,
                                       INDICATOR_CHANGAN_END_THREE,
                                       INDICATOR_CHANGAN_FIRST_FOUR,
                                       INDICATOR_CHANGAN_FIRST_SEVEN,
                                       INDICATOR_CHANGAN_FIRST_THREE,
                                       INDICATOR_CHANGAN_FIRST_TWO,
                                       INDICATOR_CHERY_10_11_POSITION,
                                       INDICATOR_CHERY_FIRST_THREE,
                                       INDICATOR_CHERY_FIRST_THREE_LEN_10,
                                       INDICATOR_CHERY_FULL,
                                       INDICATOR_CHERY_GW_FIRST_THREE,
                                       INDICATOR_CHERY_GW_FIRST_TWO,
                                       INDICATOR_CHERY_GW_FULL,
                                       INDICATOR_DONGFENG_FULL,
                                       INDICATOR_END_IS_NOT_LIFAN,
                                       INDICATOR_FAW_OTHER_PATTERNS,
                                       INDICATOR_FAW_PREFIXES, INDICATOR_FOTON,
                                       INDICATOR_GEELY_FIRST_THREE,
                                       INDICATOR_GEELY_FIRST_TWO,
                                       INDICATOR_HAIMA_FULL, INDICATOR_HAVAL,
                                       INDICATOR_JAC, INDICATOR_LIFAN_END_FIVE,
                                       INDICATOR_LIFAN_END_FOUR,
                                       INDICATOR_LIFAN_END_THREE,
                                       INDICATOR_LIFAN_END_TWO,
                                       INDICATOR_LIFAN_FIRST_THREE,
                                       INDICATOR_LIFAN_FIRST_THREE_2,
                                       INDICATOR_LIFAN_LEN_NINE,
                                       INDICATOR_LIFAN_LEN_SEVEN,
                                       INDICATOR_LIFAN_LEN_TEN,
                                       INDICATOR_LIFAN_WHISOUT,
                                       INDICATOR_LIFAN_WHISOUT_FIRST,
                                       MAX_PRICE_LISTS, ORIGINAL_BRANDS)
from dz_fastapi.crud.partner import (crud_customer_pricelist,
                                     crud_customer_pricelist_config,
                                     crud_pricelist, crud_provider,
                                     crud_provider_pricelist_config)
from dz_fastapi.models.partner import Customer, CustomerPriceList, Provider
from dz_fastapi.schemas.autopart import (AutoPartCreatePriceList,
                                         AutoPartResponse)
from dz_fastapi.schemas.partner import (AutoPartInPricelist,
                                        CustomerPriceListCreate,
                                        CustomerPriceListResponse,
                                        PriceListAutoPartAssociationCreate,
                                        PriceListCreate)
from dz_fastapi.services.email import send_email_with_attachment
from dz_fastapi.services.utils import position_exclude, prepare_excel_data

logger = logging.getLogger('dz_fastapi')


async def process_provider_pricelist(
    provider: Provider,
    file_content: bytes,
    file_extension: str,
    use_stored_params: bool,
    start_row: Optional[int],
    oem_col: Optional[int],
    brand_col: Optional[int],
    name_col: Optional[int],
    qty_col: Optional[int],
    price_col: Optional[int],
    session: AsyncSession,
):
    logger.debug(
        f'Зашли в process_provider_pricelist '
        f'provider name = {provider.name} '
        f'file_extension = {file_extension} '
        f'use_stored_params = {use_stored_params}'
    )

    if use_stored_params:
        existing_config = (
            await crud_provider_pricelist_config.get_config_or_none(
                provider_id=provider.id, session=session
            )
        )
        if not existing_config:
            raise HTTPException(
                status_code=400,
                detail='No stored parameters found for this provider.',
            )

        start_row = existing_config.start_row
        oem_col = existing_config.oem_col
        brand_col = existing_config.brand_col
        name_col = existing_config.name_col
        qty_col = existing_config.qty_col
        price_col = existing_config.price_col
    else:
        if None in (start_row, oem_col, qty_col, price_col):
            raise HTTPException(
                status_code=400, detail='Missing required parameters.'
            )

    # Load the file into a DataFrame
    if file_extension in ['xlsx', 'xls']:
        try:
            if file_extension in 'xls':
                df = pd.read_excel(
                    BytesIO(file_content),
                    header=None,
                    engine='xlrd'
                )
            else:  # xlsx
                df = pd.read_excel(
                    BytesIO(file_content),
                    header=None,
                    engine='openpyxl'
                )
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise HTTPException(status_code=400, detail='Invalid Excel file.')
    elif file_extension == 'csv':
        try:
            df = pd.read_csv(
                StringIO(file_content.decode('utf-8')), header=None
            )
        except Exception as e:
            logger.error(f'Error reading CSV file: {e}')
            raise HTTPException(status_code=400, detail='Invalid CSV file.')
    else:
        raise HTTPException(status_code=400, detail='Unsupported file type')

    try:
        data_df = df.iloc[start_row:]
        required_columns = {
            'oem_number': oem_col,
            'brand': brand_col,
            'name': name_col,
            'quantity': qty_col,
            'price': price_col,
        }
        required_columns = {
            k: v for k, v in required_columns.items() if v is not None
        }

        data_df = data_df.loc[:, list(required_columns.values())]
        data_df.columns = list(required_columns.keys())
        logger.debug(f'file df = {data_df}')
    except KeyError as e:
        raise HTTPException(
            status_code=400, detail=f'Invalid column indices provided: {e}'
        )

    try:
        data_df.dropna(
            subset=['oem_number', 'quantity', 'price'], inplace=True
        )
        data_df['oem_number'] = data_df['oem_number'].astype(str).str.strip()
        if 'name' in data_df.columns:
            data_df['name'] = data_df['name'].astype(str).str.strip()
        if 'brand' in data_df.columns:
            data_df['brand'] = data_df['brand'].astype(str).str.strip()
        data_df['quantity'] = pd.to_numeric(
            data_df['quantity'], errors='coerce'
        )
        data_df['price'] = pd.to_numeric(data_df['price'], errors='coerce')
        data_df.dropna(subset=['quantity', 'price'], inplace=True)
        # Удаляем (или игнорируем) записи со слишком большой ценой
        MAX_PRICE = 99999999.99
        before_count = len(data_df)
        data_df = data_df[data_df['price'] <= MAX_PRICE]
        after_count = len(data_df)
        logger.debug(
            f'Removed {before_count - after_count} '
            f'rows due to exceeding price {MAX_PRICE}'
        )

        # и отрицательные цены:
        data_df = data_df[data_df['price'] >= 0]
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}")
        raise HTTPException(
            status_code=400, detail='Error during data cleaning.'
        )

    autoparts_data = data_df.to_dict(orient='records')

    pricelist_in = PriceListCreate(provider_id=provider.id, autoparts=[])

    for item in autoparts_data:
        logger.debug(f'Processing item: {item}')

        try:
            autopart_data = AutoPartCreatePriceList(
                oem_number=item['oem_number'],
                brand=item.get('brand'),
                name=item.get('name'),
            )
            logger.debug(f'Created AutoPartCreatePriceList: {autopart_data}')
        except KeyError as ke:
            logger.error(f"Missing key in item: {ke}")
            raise HTTPException(
                status_code=400, detail=f'Missing key in item: {ke}'
            )

        autopart_assoc = PriceListAutoPartAssociationCreate(
            autopart=autopart_data,
            quantity=int(item['quantity']),
            price=float(item['price']),
        )
        pricelist_in.autoparts.append(autopart_assoc)

    # Create the price list
    try:
        pricelist = await crud_pricelist.create(
            obj_in=pricelist_in, session=session
        )
        # Получили Pydantic-ответ с .id
        created_id = pricelist.id
        # А теперь достаём полноценный ORM-объект (со всеми relationships)
        pl_orm = await crud_pricelist.get(session=session, obj_id=created_id)

        await analyze_new_pricelist(pl_orm, session=session)
        return pricelist
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(
            f'Unexpected error occurred while creating PriceList: {e}'
        )
        raise HTTPException(
            status_code=500,
            detail='Unexpected error during PriceList creation',
        )


def starts_with_any(s, prefixes):
    return any(s.startswith(pref) for pref in prefixes)


def contains_any(s, substrings):
    return any(sub in s for sub in substrings)


def is_chery_haval_gw(oem_original):
    return (
        oem_original[:2] in INDICATOR_CHERY_GW_FIRST_TWO
        or oem_original[:3] in INDICATOR_CHERY_GW_FIRST_THREE
        or oem_original in INDICATOR_CHERY_GW_FULL
    )


def is_faw(oem_original):
    # FAW
    # Условие для определения FAW:
    # - начинается на один из INDICATOR_FAW_PREFIXES или
    # - попадает в FAW_OEM или
    # - содержит любой из INDICATOR_FAW_OTHER_PATTERNS
    return (
        starts_with_any(oem_original, INDICATOR_FAW_PREFIXES)
        or oem_original in FAW_OEM
        or contains_any(oem_original, INDICATOR_FAW_OTHER_PATTERNS)
    )


def is_dongfeng(oem_original):
    return oem_original in INDICATOR_DONGFENG_FULL


def is_haima(oem_original):
    return oem_original in INDICATOR_HAIMA_FULL


def is_lifan_simple(oem_original):
    # Простое условие для Лифан по одному из критериев
    # Второй критерий для LIFAN: Если длина 8 и
    # первые 3 символа в INDICATOR_LIFAN_FIRST_THREE_2
    return (
        len(oem_original) == 8
        and oem_original[:3] in INDICATOR_LIFAN_FIRST_THREE_2
    )


def is_changan(oem_original):
    return (
        oem_original[:3] in INDICATOR_CHANGAN_FIRST_THREE
        or (
            len(oem_original) == 15
            and oem_original[:4] in INDICATOR_CHANGAN_FIRST_FOUR
        )
        or (
            len(oem_original) == 8
            and oem_original[:2] in INDICATOR_CHANGAN_FIRST_TWO
        )
        or (
            len(oem_original) == 14
            and oem_original[:7] in INDICATOR_CHANGAN_FIRST_SEVEN
        )
        or (
            len(oem_original) == 10
            and oem_original[-3:] in INDICATOR_CHANGAN_END_THREE
        )
    )


def is_chery(oem_original):
    # CHERY Определяется сложными условиями
    # Разбиваем на несколько отдельных проверок:
    cond1 = (
        oem_original[:3] in INDICATOR_CHERY_FIRST_THREE
        and len(oem_original) > 8
    )
    cond2 = oem_original in INDICATOR_CHERY_FULL
    cond3 = (
        len(oem_original) >= 11
        and oem_original[9:11] in INDICATOR_CHERY_10_11_POSITION
    )
    cond4 = (
        len(oem_original) == 10
        and oem_original[:3] in INDICATOR_CHERY_FIRST_THREE_LEN_10
        and oem_original[7:] not in INDICATOR_HAVAL
    )
    return cond1 or cond2 or cond3 or cond4


def is_lifan(oem_original):
    # LIFAN Определяется набором сложных условий
    cond1 = (
        len(oem_original) == 8
        and (oem_original not in INDICATOR_LIFAN_WHISOUT)
        and (oem_original[-1] not in INDICATOR_END_IS_NOT_LIFAN)
        and (oem_original[:1] not in INDICATOR_LIFAN_WHISOUT_FIRST)
    )
    cond2 = (
        len(oem_original) == 10
        and oem_original[-2:] in INDICATOR_LIFAN_END_TWO
    )
    cond3 = (
        len(oem_original) == 10
        and oem_original[-3:] in INDICATOR_LIFAN_END_THREE
    )
    cond4 = (
        len(oem_original) == 7
        and oem_original[:3] in INDICATOR_LIFAN_LEN_SEVEN
    )  # Исправлено [:2] на [:3]
    cond5 = (
        len(oem_original) == 9 and oem_original[:4] in INDICATOR_LIFAN_LEN_NINE
    )
    cond6 = oem_original[:3] in INDICATOR_LIFAN_FIRST_THREE
    cond7 = (
        len(oem_original) == 12
        and oem_original[-5:] in INDICATOR_LIFAN_END_FIVE
    )
    cond8 = (
        len(oem_original) == 11
        and oem_original[-4:] in INDICATOR_LIFAN_END_FOUR
    )
    cond9 = (
        len(oem_original) == 13
        and oem_original[-5:] in INDICATOR_LIFAN_END_FIVE
    )
    cond10 = (
        len(oem_original) == 10 and oem_original[:3] in INDICATOR_LIFAN_LEN_TEN
    )  # Исправил на [:3] для единообразия,
    # хотя можно [:2], но в списке по 3 символа.
    return (
        cond1
        or cond2
        or cond3
        or cond4
        or cond5
        or cond6
        or cond7
        or cond8
        or cond9
        or cond10
    )


def is_byd(oem_original):
    cond1 = (
        oem_original[:3] in INDICATOR_BYD_FIRST_THREE
        and len(oem_original) != 11
    )
    cond2 = oem_original in INDICATOR_BYD
    cond3 = (
        oem_original[:5] in INDICATOR_BYD_FIRST_FIVE
        and len(oem_original) == 10
    )
    return cond1 or cond2 or cond3


def is_geely(oem_original):
    cond1 = oem_original[:3] in INDICATOR_GEELY_FIRST_THREE
    cond2 = (
        len(oem_original) == 10
        and oem_original.isdigit()
        and (oem_original not in GEELY_NOT_OEM)
    )
    cond3 = (len(oem_original) in [11, 12, 13]) and oem_original.isdigit()
    cond4 = (
        len(oem_original) == 11
        and oem_original[:2] in INDICATOR_GEELY_FIRST_TWO
    )
    return cond1 or cond2 or cond3 or cond4


def is_jac(oem_original):
    return oem_original in INDICATOR_JAC


def is_foton(oem_original):
    return oem_original in INDICATOR_FOTON


def is_brilliance(oem_original):
    return oem_original in BRILLIANCE_OEM


def is_cummins(oem_original):
    return oem_original in CUMMINS_OEM


def assign_brand(oem_original):
    # 1. CHERY & HAVAL GW
    if is_chery_haval_gw(oem_original):
        return ['CHERY', 'HAVAL']

    # 2. FAW
    if is_faw(oem_original):
        return ['FAW']

    # 3. DONGFENG
    if is_dongfeng(oem_original):
        return ['DONGFENG']

    # 4. HAIMA
    if is_haima(oem_original):
        return ['HAIMA']

    # 5. Часть логики LIFAN (простое правило)
    if is_lifan_simple(oem_original):
        return ['LIFAN']

    # 6. CHANGAN
    if is_changan(oem_original):
        return ['CHANGAN']

    # 7. CHERY
    if is_chery(oem_original):
        return ['CHERY']

    # 8. LIFAN
    if is_lifan(oem_original):
        return ['LIFAN']

    # 9. BYD
    if is_byd(oem_original):
        return ['BYD']

    # 10. GEELY
    if is_geely(oem_original):
        return ['GEELY']

    # 11. JAC
    if is_jac(oem_original):
        return ['JAC']

    # 12. FOTON
    if is_foton(oem_original):
        return ['FOTON']

    # 13. BRILLIANCE
    if is_brilliance(oem_original):
        return ['BRILLIANCE']

    # 14. CUMMINS
    if is_cummins(oem_original):
        return ['CUMMINS']

    # 15. Если ни одно условие не выполнилось - HAVAL
    return ['HAVAL']


# def assign_brand(oem_original):
#
#     if oem_original[:2] in (
#             INDICATOR_CHERY_GW_FIRST_TWO
#     ) or oem_original[:3] in (
#         INDICATOR_CHERY_GW_FIRST_THREE
#     ) or oem_original in (
#         INDICATOR_CHERY_GW_FULL
#     ):
#         return ['CHERY', 'HAVAL']
#     elif any(
#             oem_original.startswith(
#                 prefix
#             ) for prefix in INDICATOR_FAW_PREFIXES
#     ) or (
#             oem_original in FAW_OEM
#     ):
#         return ['FAW']
#     elif any(
#             pattern in oem_original
#             for pattern in INDICATOR_FAW_OTHER_PATTERNS
#     ):
#         return ['FAW']
#     elif oem_original in INDICATOR_DONGFENG_FULL:
#         return ['DONGFENG']
#     elif oem_original in INDICATOR_HAIMA_FULL:
#         return ['HAIMA']
#     elif (
#     len(oem_original) == 8 and oem_original[:3]
#     in INDICATOR_LIFAN_FIRST_THREE_2
#     ):
#         return ['LIFAN']
#     elif (oem_original[:3] in INDICATOR_CHANGAN_FIRST_THREE) or (
#         len(
#             oem_original
#         ) == 15 and oem_original[:4] in INDICATOR_CHANGAN_FIRST_FOUR
#     ) or (
#             len(
#             oem_original
#             ) == 8 and oem_original[:2] in INDICATOR_CHANGAN_FIRST_TWO
#     ) or (
#             len(
#                 oem_original
#             ) == 14 and oem_original[:7] in INDICATOR_CHANGAN_FIRST_SEVEN
#     ) or (
#         len(oem_original) == 10 and oem_original[-3:]
#         in INDICATOR_CHANGAN_END_THREE
#     ):
#         return ['CHANGAN']
#     elif oem_original[:3] in (
#             INDICATOR_CHERY_FIRST_THREE
#     ) and len(oem_original) > 8 or oem_original in (
#         INDICATOR_CHERY_FULL
#     ) or (
#         len(
#             oem_original
#         ) >= 11 and oem_original[9:11] in INDICATOR_CHERY_10_11_POSITION
#     ) or (
#         len(
#             oem_original
#         ) == 10 and oem_original[:3]
#         in INDICATOR_CHERY_FIRST_THREE_LEN_10
#         and oem_original[7:] not in INDICATOR_HAVAL
#     ):
#         return ['CHERY']
#     elif (
#             len(oem_original) == 8 and (oem_original not in (
#         INDICATOR_LIFAN_WHISOUT
#     ) and oem_original[-1] not in INDICATOR_END_IS_NOT_LIFAN
#     ) and (oem_original[:1] not in INDICATOR_LIFAN_WHISOUT_FIRST)
#     ) or (len(oem_original) == 10 and oem_original[-2:] in (
#             INDICATOR_LIFAN_END_TWO
#     )) or (len(oem_original) == 10 and oem_original[-3:] in (
#             INDICATOR_LIFAN_END_THREE
#     )) or (len(oem_original) == 7 and oem_original[:2] in (
#             INDICATOR_LIFAN_LEN_SEVEN
#     )) or (len(oem_original) == 9 and oem_original[:4] in (
#             INDICATOR_LIFAN_LEN_NINE
#     )) or (oem_original[:3] in INDICATOR_LIFAN_FIRST_THREE
#     ) or (len(oem_original) == 12 and oem_original[-5:] in (
#             INDICATOR_LIFAN_END_FIVE
#     )) or (len(oem_original) == 11 and oem_original[-4:] in
#            INDICATOR_LIFAN_END_FOUR
#     ) or (len(oem_original) == 13 and oem_original[-5:] in
#           INDICATOR_LIFAN_END_FIVE
#     ) or (
#         len(oem_original) == 10 and oem_original[:2]
#         in INDICATOR_LIFAN_LEN_TEN
#     ):
#         return ['LIFAN']
#     elif (
#             oem_original[:3]
#             in INDICATOR_BYD_FIRST_THREE and len(oem_original) != 11
#     ) or (
#             oem_original in INDICATOR_BYD
#     ) or (
#             oem_original[:5]
#             in INDICATOR_BYD_FIRST_FIVE and len(oem_original) == 10
#     ):
#         return ['BYD']
#     elif oem_original[:3] in (
#             INDICATOR_GEELY_FIRST_THREE
#     ) or (len(oem_original) == 10
#     and oem_original.isdigit()
#     and (oem_original not in GEELY_NOT_OEM)) or (
#             (len(oem_original) == 12
#             or len(oem_original) == 11
#             or len(oem_original) == 13)
#             and oem_original.isdigit()
#     ) or (len(oem_original) == 11
#     and oem_original[:2] in INDICATOR_GEELY_FIRST_TWO):
#         return ['GEELY']
#     elif oem_original in INDICATOR_JAC:
#         return ['JAC']
#     elif oem_original in INDICATOR_FOTON:
#         return ['FOTON']
#     elif oem_original in BRILLIANCE_OEM:
#         return ['BRILLIANCE']
#     elif oem_original in CUMMINS_OEM:
#         return ['CUMMINS']
#     else:
#         return ['HAVAL']


async def add_origin_brand_from_dz(
    price_zzap: pd.DataFrame, session: AsyncSession
) -> pd.DataFrame:
    # Создаем копию DataFrame для предотвращения изменения оригинала
    price_zzap = price_zzap.copy()

    # Добавляем префикс 'Оригинал ' к названию для оригинальных брендов
    mask_original = price_zzap['Производитель'].isin(ORIGINAL_BRANDS)
    price_zzap.loc[mask_original, 'Наименование'] = (
        '>>Оригинал<< ' + price_zzap.loc[mask_original, 'Наименование']
    )

    # Обработка записей с брендом 'DRAGONZAP'
    dz_items = price_zzap.loc[
        price_zzap['Производитель'] == 'DRAGONZAP'
    ].copy()

    # Добавляем префикс 'Неоригинал ' к названию для новых брендов
    dz_items['Наименование'] = '>>Неоригинал<< ' + dz_items['Наименование']
    dz_items['Артикул'] = dz_items['Артикул'].apply(
        lambda x: x[2:] if 'DZ' in x else x
    )

    # Применяем функцию assign_brand для получения новых брендов
    dz_items['assigned_brands'] = dz_items['Артикул'].apply(assign_brand)

    # Разворачиваем список брендов в отдельные строки
    dz_items = dz_items.explode('assigned_brands')

    # Обновляем поле 'brand' с новым брендом
    dz_items['Производитель'] = dz_items['assigned_brands']

    # Удаляем временное поле 'assigned_brands'
    dz_items = dz_items.drop(columns=['assigned_brands'])

    # Получаем уникальные названия новых брендов
    # new_brands = dz_items['Brand'].unique().tolist()
    #
    # # Получаем записи брендов из базы данных
    # brand_records = await brand_crud.get_brands_by_names(new_brands, session)
    #
    # # Создаем словарь соответствия названий брендов и их ID
    # brand_id_map = {brand.name: brand.id for brand in brand_records}
    #
    # # Присваиваем brand_id новым записям
    # dz_items['brand_id'] = dz_items['Brand'].map(brand_id_map)
    #
    # # Проверяем, есть ли бренды без brand_id
    # missing_brands = dz_items[dz_items['brand_id'].isna()]['brand'].unique()
    # if len(missing_brands) > 0:
    #     logger.warning(f"Missing brand_id for brands: {missing_brands}")

    # # Automatically create missing brands
    # await create_missing_brands(missing_brands, session)

    # # Fetch brand records again
    # brand_records = await brand_crud.get_brands_by_names(new_brands, session)
    # brand_id_map = {brand.name: brand.id for brand in brand_records}
    #
    # # Update brand_id for dz_items
    # dz_items['brand_id'] = dz_items['brand'].map(brand_id_map)

    # Объединяем оригинальный DataFrame с новыми записями
    price_zzap = pd.concat([price_zzap, dz_items], ignore_index=True)

    return price_zzap


async def send_pricelist(
    df_excel: pd.DataFrame,
    customer: Customer,
    subject: str,
    body: str,
    attachment_filename: str,
):
    output = BytesIO()
    wb = Workbook()
    ws = wb.active

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.cell(row=1, column=5).value = f"Сформирован {current_time}"
    ws.cell(row=1, column=5).font = Font(name="Arial", size=7)
    ws.cell(row=1, column=5).alignment = Alignment(
        horizontal="center", vertical="center"
    )

    # Write headers on the second row
    logger.debug('Write headers on the second row')
    for col_num, column_title in enumerate(df_excel.columns, start=1):
        cell = ws.cell(row=2, column=col_num)
        cell.value = column_title
        cell.font = Font(name="Arial", size=10, bold=True)
        cell.fill = PatternFill(
            start_color="D9EAD3", end_color="D9EAD3", fill_type="solid"
        )
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # Write data rows starting from the third row
    logger.debug('Write data rows starting from the third row')
    for row_num, row_data in enumerate(
        df_excel.itertuples(index=False), start=3
    ):
        for col_num, cell_value in enumerate(row_data, start=1):
            cell = ws.cell(row=row_num, column=col_num)
            cell.value = cell_value
            cell.font = Font(name="Arial", size=10)

    wb.save(output)
    logger.debug(
        'Workbook saved successfully. ' 'Size: %s bytes',
        len(output.getvalue()),
    )
    output.seek(0)

    # Send email with the Excel attachment
    logger.debug('Send email with the Excel attachment')
    to_email = customer.email_outgoing_price
    subject = subject
    body = body

    attachment_bytes = output.getvalue()
    attachment_filename = attachment_filename

    # Send the email asynchronously
    logger.debug('Send the email asynchronously')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        partial(
            send_email_with_attachment,
            to_email=to_email,
            subject=subject,
            body=body,
            attachment_bytes=attachment_bytes,
            attachment_filename=attachment_filename,
        ),
    )
    logger.debug('Final send email')


async def process_customer_pricelist(
    customer: Customer, request: CustomerPriceListCreate, session: AsyncSession
) -> CustomerPriceListResponse:

    config = await crud_customer_pricelist_config.get_by_id(
        config_id=request.config_id, customer_id=customer.id, session=session
    )
    if not config:
        raise HTTPException(
            status_code=400,
            detail='No pricelist configuration found for the customer',
        )
    # Получаю все прайс листы клиента
    all_prices = await crud_customer_pricelist.get_all_pricelist(
        session=session, customer_id=customer.id
    )
    # Проверяю превышает ли кол-во прайсов MAX_PRICE_LISTS,
    # и если да то удаляем излишек
    if len(all_prices) > MAX_PRICE_LISTS:
        await crud_customer_pricelist.delete_older_pricelists(
            session=session, customer_id=customer.id, max_count=MAX_PRICE_LISTS
        )

    combined_data = []

    for pricelist_id in request.items:
        associations = await crud_pricelist.fetch_pricelist_data(
            pricelist_id, session
        )
        if not associations:
            continue

        df = await crud_pricelist.transform_to_dataframe(
            associations=associations, session=session
        )
        logger.debug(f'Transform file to dataframe {df}')

        df = crud_customer_pricelist.apply_coefficient(df, config)
        combined_data.append(df)

    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)

        # Deduplicate: keep the lowest price for each autopart
        final_df = final_df.sort_values(
            by=['oem_number', 'brand', 'price']
        ).drop_duplicates(subset=['oem_number', 'brand'], keep='first')
    else:
        final_df = pd.DataFrame()

    logger.debug(f'Final DataFrame before creating associations:\n{final_df}')
    # Apply exclusions

    if not final_df.empty:
        if request.excluded_supplier_positions:
            for (
                provider_id,
                excluded_autoparts,
            ) in request.excluded_supplier_positions.items():
                final_df = position_exclude(
                    provider_id=provider_id,
                    excluded_autoparts=excluded_autoparts,
                    df=final_df,
                )
        customer_autoparts_data = final_df.to_dict('records')
    else:
        raise HTTPException(
            status_code=400, detail='No autoparts to include in the pricelist'
        )

    customer_pricelist = CustomerPriceList(
        customer=customer, date=request.date or date.today(), is_active=True
    )
    session.add(customer_pricelist)
    await session.flush()

    associations = await crud_customer_pricelist.create_associations(
        customer_pricelist_id=customer_pricelist.id,
        autoparts_data=customer_autoparts_data,
        session=session,
    )

    # Prepare data for Excel file
    df_excel = prepare_excel_data(associations=associations)

    if config.additional_filters.get('ZZAP'):
        logger.debug('Зашел в get additional_filters')
        provider_diller = await crud_provider.get_provider_or_none(
            provider='AVTODIN KAMA', session=session
        )
        if not provider_diller:
            logger.error('Provider AVTODIN KAMA not found.')
            raise ValueError('Provider AVTODIN KAMA not found.')
        pricelist_ids = await crud_pricelist.get_pricelist_ids_by_provider(
            provider_id=provider_diller.id, session=session
        )
        if not pricelist_ids:
            logger.error(
                f'No pricelists found for provider {provider_diller.name}.'
            )
            raise ValueError(
                f'No pricelists found for provider {provider_diller.name}.'
            )
        associations = await crud_pricelist.fetch_pricelist_data(
            pricelist_ids[-1], session
        )
        df_diller = await crud_pricelist.transform_to_dataframe(
            associations=associations, session=session
        )
        logger.debug(f'Transform file to dataframe {df_diller}')
        df_diller_rename = df_diller.rename(
            columns={
                'brand': 'Производитель',
                'name': 'Наименование',
                'oem_number': 'Артикул',
                'quantity': 'Количество',
                'price': 'Цена',
            }
        )
        # 1. Предположим, что и df_excel, и
        # df_diller имеют колонку "brand_id" и "price".
        #    Если у вас "brand" (строка), замените ниже 'brand_id' на 'brand'.

        # 2. Соединяем df_excel и df_diller по "brand_id".
        #    how='left' чтобы к df_excel присоединить цены диллера (если есть).
        df_merged = pd.merge(
            df_excel,
            df_diller_rename,
            on=['Производитель', 'Артикул'],
            how='left',
            suffixes=('', '_diller'),  # Чтобы колонки не конфликтовали
        )

        # Теперь в df_merged есть колонки:
        # "price" (из df_excel) и "price_diller" (из df_diller)
        # Проверяем условие: если df_excel['price'] < df_diller['price'] * 1.2
        # и brand_id совпадает, тогда повышаем цену.

        # 3. Формируем маску (true, где нужно повысить)
        mask = (df_merged['Цена_diller'].notna()) & (
            df_merged['Цена'] < df_merged['Цена_diller'] * 1.2
        )
        # 4. Применяем
        df_merged.loc[mask, 'Цена'] = (
            np.ceil(df_merged.loc[mask, 'Цена_diller'] * 1.2 / 10) * 10
        )

        # 5. Возвращаем df_merged к исходному набору колонок df_excel,
        #    например если в df_excel были колонки:
        #    ['brand_id','price','name','...']
        #    или же просто присваиваем обратно df_excel = df_merged
        #    (учтите, что df_merged может иметь лишние колонки, напр.
        #    'price_diller').
        df_excel = df_merged[df_excel.columns]

        df_excel = await add_origin_brand_from_dz(
            price_zzap=df_excel, session=session
        )
        logger.debug(f'Измененный файл для ZZAP: {df_excel}')
    await session.commit()
    logger.debug('Calling send_pricelist')
    await send_pricelist(
        customer=customer,
        df_excel=df_excel,
        subject=f'Прайс лист {customer_pricelist.date}',
        body='Добрый день, высылаем Вам наш прайс-лист',
        attachment_filename='zzap_kross.xlsx',
    )
    logger.debug('Finished send_pricelist')

    autoparts_response = []
    for assoc in associations:
        autopart = AutoPartResponse.model_validate(
            assoc.autopart, from_attributes=True
        )
        autopart_in_pricelist = AutoPartInPricelist(
            autopart_id=assoc.autopart_id,
            quantity=assoc.quantity,
            price=float(assoc.price),
            autopart=autopart,
        )
        autoparts_response.append(autopart_in_pricelist)

    response = CustomerPriceListResponse(
        id=customer_pricelist.id,
        date=customer_pricelist.date,
        customer_id=customer.id,
        autoparts=autoparts_response,
    )
    return response
