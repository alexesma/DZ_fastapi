import logging

import pandas as pd
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartSubstitution

logger = logging.getLogger('dz_fastapi')


async def apply_substitutions(
    df: pd.DataFrame,
    session: AsyncSession,
    config_id: int,
) -> pd.DataFrame:
    """
    Применение подмены DRAGONZAP позиций на оригинальные артикулы.

    Args:
        df: DataFrame с колонками brand, oem_number, name, price, quantity
        session: DB session
        config_id: ID конфигурации клиента

    Returns:
        DataFrame с добавленными строками подмены
    """
    df = df.copy()

    # Переименовываем колонки для удобства
    column_mapping = {
        'brand': 'Производитель',
        'oem_number': 'Артикул',
        'name': 'Наименование',
        'price': 'Цена',
        'quantity': 'Количество'
    }

    # Проверяем какие колонки уже есть
    if 'Производитель' not in df.columns:
        df = df.rename(columns=column_mapping)

    # 1. Найти все DRAGONZAP позиции
    dz_items = df[df['Производитель'] == 'DRAGONZAP'].copy()

    if dz_items.empty:
        return df

    logger.debug(f'Found {len(dz_items)} DRAGONZAP items to process')

    # 2. Получить brand_id для DRAGONZAP
    result = await session.execute(
        select(Brand.id).where(Brand.name == 'DRAGONZAP')
    )
    dz_brand_id = result.scalar_one_or_none()

    if not dz_brand_id:
        logger.warning('DRAGONZAP brand not found in database')
        return df

    # 3. Убрать префикс DZ из артикула для поиска
    dz_items['clean_oem'] = dz_items['Артикул'].apply(
        lambda x: x[2:] if str(x).startswith('DZ') else str(x)
    )

    # 4. Получить все автозапчасти DRAGONZAP из базы
    oem_numbers = dz_items['clean_oem'].unique().tolist()

    result = await session.execute(
        select(AutoPart.id, AutoPart.oem_number)
        .where(
            AutoPart.brand_id == dz_brand_id,
            AutoPart.oem_number.in_(oem_numbers)
        )
    )
    autoparts_map = {oem: ap_id for ap_id, oem in result.all()}

    if not autoparts_map:
        logger.warning('No DRAGONZAP autoparts found in database')
        return df

    logger.debug(f'Found {len(autoparts_map)} DRAGONZAP autoparts in DB')

    # 5. Получить все подмены с JOIN к Brand для получения имени бренда
    autopart_ids = list(autoparts_map.values())

    result = await session.execute(
        select(
            AutoPartSubstitution,
            Brand.name.label('brand_name'),
        )
        .join(Brand, AutoPartSubstitution.substitution_brand_id == Brand.id)
        .where(
            and_(
                AutoPartSubstitution.source_autopart_id.in_(autopart_ids),
                AutoPartSubstitution.is_active.is_(True),
                or_(
                    AutoPartSubstitution.customer_config_id == config_id,
                    AutoPartSubstitution.customer_config_id.is_(None),
                ),
            ),
        )
        .order_by(AutoPartSubstitution.priority)
    )

    substitutions_with_brands = result.all()

    if not substitutions_with_brands:
        logger.info('No active substitutions found')
        return df

    logger.debug(
        f'Found {len(substitutions_with_brands)} active substitutions'
    )

    # 6. Создать маппинг: OEM → список подмен
    substitutions_map = {}
    for sub, brand_name in substitutions_with_brands:
        # Найти OEM по autopart_id
        oem = next(
            (
                oem
                for oem, ap_id in autoparts_map.items()
                if ap_id == sub.source_autopart_id
            ),
            None,
        )
        if oem:
            if oem not in substitutions_map:
                substitutions_map[oem] = []
            substitutions_map[oem].append(
                {
                    'brand': brand_name,
                    'oem': sub.substitution_oem_number,
                    'priority': sub.priority,
                    'min_quantity': sub.min_source_quantity,
                    'reduction': sub.quantity_reduction,
                }
            )

    # 7. Создать новые строки для каждой подмены
    new_rows = []

    for idx, row in dz_items.iterrows():
        clean_oem = row['clean_oem']
        substitutions_list = substitutions_map.get(clean_oem, [])

        if not substitutions_list:
            continue

        base_quantity_raw = row['Количество']
        try:
            base_quantity = int(pd.to_numeric(base_quantity_raw))
        except Exception:
            logger.warning(
                f'Invalid quantity "{base_quantity_raw}" for OEM {clean_oem}; '
                'skipping substitutions'
            )
            continue

        # Проверяем минимальное количество
        min_qty = min(sub['min_quantity'] for sub in substitutions_list)
        if base_quantity < min_qty:
            logger.debug(
                f'Skipping substitution for {clean_oem}: '
                f'quantity {base_quantity} < min {min_qty}'
            )
            continue

        for sub in substitutions_list:
            new_row = row.copy()
            new_row['Производитель'] = sub['brand']
            new_row['Артикул'] = sub['oem']

            # Уменьшить количество
            if base_quantity > sub['min_quantity']:
                new_quantity = base_quantity - sub['reduction']
                new_row['Количество'] = max(1, new_quantity)
            else:
                new_row['Количество'] = base_quantity

            new_rows.append(new_row)

    # 8. Объединить оригинальный DataFrame с новыми строками
    if new_rows:
        logger.info(f'Created {len(new_rows)} substitution rows')
        new_df = pd.DataFrame(new_rows)
        # Удаляем временную колонку из новых строк
        if 'clean_oem' in new_df.columns:
            new_df = new_df.drop(columns=['clean_oem'])
        df = pd.concat([df, new_df], ignore_index=True)

    # 9. Удалить временную колонку из основного df
    if 'clean_oem' in df.columns:
        df = df.drop(columns=['clean_oem'])

    return df
