import logging
import os
from collections import defaultdict
from datetime import timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from dz_fastapi.core.constants import (DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
                                       LIMIT_ORDER,
                                       PERCENTAGE_DEVIATION_ORDER_PRICE,
                                       URL_DZ_SEARCH)
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.autopart import (crud_autopart,
                                      crud_autopart_price_history,
                                      crud_autopart_restock_decision)
from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.http.dz_site_client import DZSiteClient
from dz_fastapi.services.email import send_email_with_attachment
from dz_fastapi.services.telegram import send_file_to_telegram

KEY = os.getenv('KEY_FOR_WEBSITE')
logger = logging.getLogger('dz_fastapi')


async def get_autoparts_below_min_balance(
    session: AsyncSession, threshold_percent: float = 0.5
) -> Dict[int, Tuple[float, float, str, str, str]]:
    '''
    Получить автозапчасти и их текущий остаток из PriceList id=1
    сравнить с autopart.minimum_balance
    :param session:
    :param threshold_percent процент после которого в заказ формируется
    не min_balance а 50% от него default 0.5(50%)
    :return:
    вернуть слловарь ключ autopart_id значение 0: min_balance,
    1:quantity_for_order, 2: oem_number, 3: autopart_name,
    4: brand_name
    '''

    logger.debug('Зашли в функцию get_autoparts_below_min_balance')
    autopart_for_order = (
        await crud_autopart.get_autoparts_with_minimum_balance(
            session=session, threshold_percent=threshold_percent
        )
    )
    logger.debug('Вышли из функции get_autoparts_below_min_balance')
    return autopart_for_order


async def fetch_supplier_offers(
    autopart_ids: list[int], session: AsyncSession
) -> Dict[int, List[Dict[str, Any]]]:
    '''
    получения данных поставщиков
    :param autopart_ids: список id автозапчастей
    :param session: сессия БД
    :return:
    '''
    logger.debug('Зашли в функцию fetch_supplier_offers')
    autopart_suppliers = (
        await crud_autopart_restock_decision.get_prices_suppliers(
            autopart_ids=autopart_ids, session=session
        )
    )

    grouped_offers = defaultdict(list)
    for price_assoc, pricelist, provider in autopart_suppliers:
        autopart_id = price_assoc.autopart_id
        grouped_offers[autopart_id].append(
            {
                'price_assoc': price_assoc,
                'provider': provider,
                'price': float(price_assoc.price),
                'quantity': price_assoc.quantity,
                'sup_logo': (
                    provider.abbreviations[0].abbreviation
                    if provider.abbreviations
                    else ''
                ),
                'min_delivery_day': (
                    pricelist.config_id.min_delivery_day
                    if pricelist.config_id
                    else 1
                ),
                'max_delivery_day': (
                    pricelist.config_id.max_delivery_day
                    if pricelist.config_id
                    else 2
                ),
            }
        )

    logger.debug('Вышли из функции fetch_supplier_offers')
    return grouped_offers


async def get_historical_min_price(
    autopart_ids: list[int],
    session: AsyncSession,
    months_back: int = DEPTH_MONTHS_HISTORY_PRICE_FOR_ORDER,
) -> Dict[int, float]:
    '''
    Возвращает минимальную цену каждой
    автозапчасти из списка за последние месяцы.

    :param autopart_ids: список ID автозапчастей
    :param session: сессия БД
    :param months_back: количество месяцев назад для поиска
    :return: словарь {autopart_id: min_price}
    '''
    logger.debug('Зашли в функцию get_historical_min_price')
    date_threshold = now_moscow() - timedelta(days=30 * months_back)
    rows = await crud_autopart_price_history.get_autoparts(
        autopart_ids=autopart_ids,
        date_threshold=date_threshold,
        session=session,
    )
    logger.debug('Вышли из функции get_historical_min_price')

    return {autopart_id: float(min_price) for autopart_id, min_price in rows}


async def get_requests_for_our_site(
    oem_number: str,
    brand: str,
    qty: int,
    min_price: float,
    session: AsyncSession,
) -> Optional[Dict[str, Any]]:
    logger.debug(
        f'Зашли в get_requests_for_our_site с параметрами '
        f'oem_number = {oem_number}, brand = {brand}, qty = {qty}, '
        f'min_price = {min_price}'
    )

    best_offer: Optional[Dict[str, Any]] = None
    async with DZSiteClient(
        base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
    ) as client:
        response = await client.get_offers(
            oem=oem_number, brand=brand, without_cross=True
        )
    logger.debug(f'Ответ на сайте: {response}')
    for item in response:
        try:
            offer_price = float(item['cost'])
            offer_quantity = int(item['qnt'])
            offer_min_quantity = int(item.get('min_qnt', 1))
            abbreviation = item.get('sup_logo')
            detail_name = item.get('detail_name')
            min_delivery_day = int(item.get('min_delivery_day', 1))
            max_delivery_day = int(item.get('max_delivery_day', 2))
            provider = (
                await crud_provider.get_or_create_provider_by_abbreviation(
                    abbreviation=abbreviation, session=session
                )
            )
            # Ищем подходящее предложение
            # (достаточно количества и цена ниже максимальной):
            logger.debug(
                f'Поставщик = {abbreviation} |'
                f'Кол-во в предложении = {offer_quantity} |'
                f'Необходимое кол-во = {qty} |'
                f'Цена в предложении = {offer_price}'
            )
            if offer_quantity >= qty and offer_price <= min_price:
                if best_offer is None or offer_price < best_offer['price']:
                    if offer_min_quantity >= qty:
                        qnt_order = offer_min_quantity
                    elif qty % offer_min_quantity == 0:
                        qnt_order = qty
                    else:
                        qnt_order = offer_min_quantity * (
                            qty // offer_min_quantity + 1
                        )

                    best_offer = {
                        'price': offer_price,
                        'quantity': qnt_order,
                        'supplier_id': provider.id,
                        'supplier_name': provider.name,
                        'pricelist_id': item.get('pricelist_id', 0),
                        'hash_key': item['hash_key'],
                        'system_hash': item['system_hash'],
                        'oem': oem_number,
                        'qnt': offer_quantity,
                        'min_qnt': offer_min_quantity,
                        'make_name': item.get('make_name', brand),
                        'detail_name': detail_name,
                        'min_delivery_day': min_delivery_day,
                        'max_delivery_day': max_delivery_day,
                        'sup_logo': abbreviation,
                        'comment': '',
                    }

        except (KeyError, ValueError, TypeError) as error:
            logger.warning(
                f'Некорректные данные по предложению: {item}, ошибка: {error}'
            )
            continue
    if best_offer is None:
        logger.debug(
            f'Нет подходящих предложений с сайта по {brand} {oem_number}'
        )
    return best_offer


async def evaluate_supplier_offers(
    ids_autoparts_for_order: Dict[int, Tuple[float, float, str, str, str]],
    autoparts_in_prices: Dict[int, List[Dict[str, Any]]],
    historical_min_prices: Dict[int, float],
    session: AsyncSession,
    budget_limit: int = LIMIT_ORDER,
) -> Dict[int, Dict[str, Any]]:
    '''
    оценки предложений
    :param session:
    :return:
    '''
    logger.debug('Зашли в функцию evaluate_supplier_offers')

    decisions = {}
    total_spent = 0

    for autopart_id, (
        min_balance,
        quantity_needed,
        oem_number,
        detail_name,
        make_name,
    ) in ids_autoparts_for_order.items():

        hist_min_price = historical_min_prices.get(autopart_id)
        if hist_min_price is None:
            logger.warning(
                f'Нет исторических данных по цене'
                f' для позиции id = {autopart_id}'
                f'установили hist_min_price = 999999'
            )
            hist_min_price = 999999

        max_acceptable_price = (
            hist_min_price * PERCENTAGE_DEVIATION_ORDER_PRICE
        )

        offers = autoparts_in_prices.get(autopart_id, [])
        suitable_offer = None

        if offers:
            for offer in sorted(offers, key=lambda x: x['price']):
                if (
                    offer['price'] <= max_acceptable_price
                    and offer['quantity'] >= quantity_needed
                ):
                    suitable_offer = {
                        'price': offer['price'],
                        'quantity': quantity_needed,
                        'supplier_id': offer['provider'].id,
                        'supplier_name': offer['provider'].name,
                        'qnt': offer['quantity'],
                        'min_delivery_day': offer['min_delivery_day'],
                        'max_delivery_day': offer['max_delivery_day'],
                        'sup_logo': offer['sup_logo'],
                    }
                    break

        if suitable_offer is None:
            autopart = await crud_autopart.get_autopart_by_id(
                autopart_id=autopart_id, session=session
            )
            suitable_offer = await get_requests_for_our_site(
                oem_number=autopart.oem_number,
                brand=autopart.brand.name,
                qty=quantity_needed,
                min_price=max_acceptable_price,
                session=session,
            )

            if suitable_offer is None:
                logger.warning(
                    f'Не удалось найти подходящее '
                    f'предложение для позиции {autopart_id}'
                )
                continue

        total_cost = suitable_offer['price'] * suitable_offer['quantity']

        # Проверка лимита бюджета:
        if total_spent + total_cost > budget_limit:
            logger.warning(
                f'Превышение бюджета при добавлении позиции '
                f'{autopart_id}. Текущий расход: {total_spent}, '
                f'стоимость позиции: {total_cost}'
            )
            continue

        decisions[autopart_id] = {
            'supplier_id': suitable_offer['supplier_id'],
            'supplier_name': suitable_offer['supplier_name'],
            'price': suitable_offer['price'],
            'quantity': suitable_offer['quantity'],
            'total_cost': total_cost,
            'historical_min_price': hist_min_price,
            'min_qnt': suitable_offer.get('min_qnt', 1),
            'qnt': suitable_offer.get('qnt'),
            'min_delivery_day': suitable_offer.get('min_delivery_day'),
            'max_delivery_day': suitable_offer.get('max_delivery_day'),
            'sup_logo': suitable_offer.get('sup_logo'),
            'hash_key': suitable_offer.get('hash_key'),
            'system_hash': suitable_offer.get('system_hash'),
            'oem_number': oem_number,
            'detail_name': detail_name,
            'make_name': make_name,
        }

        total_spent += total_cost

    logger.debug('Вышли из функции evaluate_supplier_offers')
    return decisions


async def save_restock_decision(offers: {}, session: AsyncSession) -> None:
    '''
    сохранения решений
    :param session:
    :return:
    '''
    logger.debug('Зашли в функции save_restock_decision')
    await crud_autopart_restock_decision.save_restock_decision(
        decisions=offers, session=session
    )
    logger.debug('Вышли из функции save_restock_decision')


async def generate_restock_report(
    decisions: Dict[int, Dict],
    session: AsyncSession,
) -> pd.DataFrame:
    report = []
    # Получаем список всех автозапчастей за один запрос
    autopart_ids = list(decisions.keys())
    autoparts = await crud_autopart.get_autopart_by_ids(
        session=session, autopart_ids=autopart_ids
    )
    # Преобразуем список в словарь для быстрого доступа
    autopart_dict = {autopart.id: autopart for autopart in autoparts}
    for autopart_id, offer in decisions.items():
        offer_autopart = {}
        autopart = autopart_dict.get(autopart_id)
        if not autopart:
            continue  # Пропустить, если вдруг автозапчасть не найдена

        price_provider = offer['price']
        hist_min_price = offer['historical_min_price']
        deviation = ((price_provider - hist_min_price) / hist_min_price) * 100
        offer_autopart['OEM Номер'] = autopart.oem_number
        offer_autopart['Производитель'] = autopart.brand.name
        offer_autopart['Наименование'] = autopart.name
        offer_autopart['Цена оптовая сейчас'] = autopart.wholesale_price
        offer_autopart['min_balance'] = autopart.minimum_balance
        offer_autopart['Нужно заказать'] = offer['quantity']
        offer_autopart['Предложение (цена)'] = price_provider
        offer_autopart['Истор. мин. цена'] = hist_min_price
        offer_autopart['Отклонение %'] = f'{deviation:.2f} %'
        offer_autopart['Поставщик'] = offer['supplier_name']
        offer_autopart['Общая сумма'] = offer['total_cost']
        report.append(offer_autopart)
    return pd.DataFrame(report)


async def export_and_send_restock_report(
    report_df: pd.DataFrame, email_to: str, telegram_chat_id: str
) -> None:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        report_df.to_excel(writer, index=False, sheet_name='Restock Report')
    buffer.seek(0)

    file_bytes = buffer.read()
    filename = 'restock_report.xlsx'

    send_email_with_attachment(
        to_email=email_to,
        subject='Отчет по пополнению остатков',
        body='Прикреплен отчет с предложениями по заказу.',
        attachment_filename=filename,
        attachment_bytes=file_bytes,
    )

    await send_file_to_telegram(
        chat_id=telegram_chat_id,
        file_bytes=file_bytes,
        file_name=filename,
        caption='📦 Отчет по пополнению остатков',
    )


async def process_restock_pipeline(
    session: AsyncSession,
    budget_limit: int,
    months_back: int,
    email_to: str,
    telegram_chat_id: str,
    autoparts: Dict[int, Tuple[float, float]] = None,
    threshold_percent: float = 0.5,
) -> None:
    '''
    Объединяющая функция для формирования
    заказа и отправки его на почту и телеграмм
    :param session:
    :param budget_limit сумма для которого будет формироваться заказ
    :param months_back глубина поиска исторической минимальной цены
    :param autoparts можно передать словарь
    {id автопартс: [минимальный баланс, кол-во для заказа]}
    или по умолчанию программа будет брать позиции у которых есть
    min_balance и текущие кол-во меньше его
    :param threshold_percent процент после которого в заказ формируется
    не min_balance а 50% от него default 0.5(50%)
    :param email_to адрес куда будет отправлен отчет о заказе
    :param telegram_chat_id id куда будет отправлен отчет в телеграмм
    :return:
    '''
    logger.debug('Зашли в функцию process_restock_pipeline')
    if autoparts is None:
        autoparts = await get_autoparts_below_min_balance(
            threshold_percent=threshold_percent, session=session
        )
    autopart_ids = autoparts.keys()
    logger.debug(f'autoparts ids : {autopart_ids}')
    supplier_prices = await fetch_supplier_offers(
        autopart_ids=autopart_ids, session=session
    )
    logger.debug(f'supplier_prices: {supplier_prices}')

    historical_min_price = await get_historical_min_price(
        months_back=months_back, autopart_ids=autopart_ids, session=session
    )
    logger.debug(f'historical_min_price: {historical_min_price}')
    supplier_offers = await evaluate_supplier_offers(
        ids_autoparts_for_order=autoparts,
        autoparts_in_prices=supplier_prices,
        budget_limit=budget_limit,
        historical_min_prices=historical_min_price,
        session=session,
    )
    logger.debug(f'supplier_offers: {supplier_offers}')
    if not supplier_offers:
        logger.warning('Нет предложений для добавления в отчет.')
        return

    await save_restock_decision(offers=supplier_offers, session=session)
    restock_report = await generate_restock_report(
        decisions=supplier_offers, session=session
    )
    await export_and_send_restock_report(
        email_to=email_to,
        telegram_chat_id=telegram_chat_id,
        report_df=restock_report,
    )


'''
Временный метод не знаю куда его приклееть
'''


async def process_and_add_to_basket(
    oem_number: str,
    brand: str,
    qty: int,
    min_price: float,
    session: AsyncSession,
):
    best_offer = await get_requests_for_our_site(
        oem_number=oem_number,
        brand=brand,
        qty=qty,
        min_price=min_price,
        session=session,
    )

    if not best_offer:
        logger.info('Нет подходящих предложений')
        return False

    async with DZSiteClient(
        base_url=URL_DZ_SEARCH, api_key=KEY, verify_ssl=False
    ) as client:
        added = await client.add_autopart_in_basket(
            oem=best_offer['oem'],
            make_name=best_offer['make_name'],
            detail_name=best_offer['detail_name'],
            qnt=best_offer['quantity'],
            comment=best_offer['comment'],
            min_delivery_day=best_offer['min_delivery_day'],
            max_delivery_day=best_offer['max_delivery_day'],
            api_hash=best_offer['hash_key'],
        )

        if added:
            logger.info('Позиция успешно добавлена в корзину')
            return True
        else:
            logger.error('Ошибка при добавлении позиции в корзину')
            return False
