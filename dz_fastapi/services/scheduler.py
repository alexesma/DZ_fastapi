# scheduler.py
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import aiofiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import (CONFIG_DATA_CUSTOMER,
                                       CONFIG_DATA_PROVIDER, CUSTOMER,
                                       CUSTOMER_IN, PROVIDER_IN)
from dz_fastapi.crud.partner import (crud_customer,
                                     crud_customer_pricelist_config,
                                     crud_pricelist, crud_provider,
                                     crud_provider_pricelist_config)
from dz_fastapi.schemas.partner import (CustomerCreate,
                                        CustomerPriceListConfigCreate,
                                        CustomerPriceListCreate,
                                        ProviderCreate,
                                        ProviderPriceListConfigCreate)
from dz_fastapi.services.email import get_emails
from dz_fastapi.services.process import (process_customer_pricelist,
                                         process_provider_pricelist)

logger = logging.getLogger('dz_fastapi')
EMAIL_NAME_ORDER = os.getenv('EMAIL_NAME_ORDERS')
EMAIL_PASSWORD_ORDER = os.getenv('EMAIL_PASSWORD_ORDERS')
EMAIL_HOST_ORDER = os.getenv('EMAIL_HOST_ORDERS')


def start_scheduler(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.configure(
        timezone='UTC', job_defaults={'coalesce': True, 'max_instances': 1}
    )

    # Добавляем задачи в планировщик
    scheduler.add_job(
        func=download_price_provider_task,
        trigger='cron',  # или 'interval'
        args=[app],
        id='download_price_provider',
        name='Download price provider',
        minute='*/5',  # каждые 5 минут
        jitter=5,
        replace_existing=True,
        # hour='9',
    )

    scheduler.add_job(
        func=cleanup_old_pricelists_task,
        trigger='cron',
        args=[app],
        id='cleanup_old_pricelists',
        name='Cleanup old pricelists keep last 5',
        hour=2,
        minute=30,
        replace_existing=True,
    )

    # scheduler.add_job(
    #     func=,
    #     trigger='cron',
    #     args=[app],
    #     id='download_all_price_providers',
    #     name='Download prices over providers',
    #     minute='*/5',  # каждые 5 минут
    #     # hour='9',  # каждый день в 9 утра
    # )

    scheduler.start()
    logger.info('Scheduler started.')
    return scheduler


@asynccontextmanager
async def new_session_from_app(app: FastAPI):
    session_factory = app.state.session_factory
    async with session_factory() as s:
        yield s


async def _process_one(item, app: FastAPI, sem: asyncio.Semaphore):
    provider, filepath, provider_conf = item
    async with sem:
        try:
            async with new_session_from_app(app) as session:
                file_extension = filepath.split('.')[-1].lower()
                async with aiofiles.open(filepath, 'rb') as f:
                    file_content = await f.read()
                logger.info(
                    f'Скачан прайс для провайдера {provider.id} '
                    f'({provider.name}), размер: {len(file_content)} байт'
                )
                await process_provider_pricelist(
                    provider=provider,
                    file_content=file_content,
                    file_extension=file_extension,
                    provider_list_conf=provider_conf,
                    use_stored_params=True,
                    start_row=None,
                    oem_col=None,
                    brand_col=None,
                    name_col=None,
                    qty_col=None,
                    price_col=None,
                    session=session,
                )
                logger.info(
                    f'Успешно обработан прайс для провайдера {provider.id}'
                )

                if provider.name == PROVIDER_IN['name']:
                    await send_price_list_task(app)
        except Exception as e:
            logger.error(
                f'Ошибка обработки прайса для провайдера {provider.id}: {e}',
                exc_info=True,
            )
            raise


async def send_price_list_task(app: FastAPI):
    # logger.info('Starting send_price_list_task')
    # async_session_factory = get_async_session()
    logger.info('Starting send_price_list_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            customer_in_model = CustomerCreate(**CUSTOMER_IN)

            customer = await crud_customer.get_customer_or_none(
                customer=CUSTOMER, session=session
            )
            if not customer:
                customer = await crud_customer.create(
                    obj_in=customer_in_model, session=session
                )
            config_in_model = CustomerPriceListConfigCreate(
                **CONFIG_DATA_CUSTOMER
            )
            configs = await crud_customer_pricelist_config.get_by_customer_id(
                customer_id=customer.id, session=session
            )
            if not configs:
                config = await crud_customer_pricelist_config.create_config(
                    customer_id=customer.id,
                    config_in=config_in_model,
                    session=session,
                )
            else:
                config = configs[-1]

            provider = await crud_provider.get_provider_or_none(
                provider=PROVIDER_IN['name'], session=session
            )
            if not provider:
                logger.error(f"Provider {PROVIDER_IN['name']} not found.")
                raise ValueError(f"Provider {PROVIDER_IN['name']} not found.")
            pricelist_ids = await crud_pricelist.get_pricelist_ids_by_provider(
                provider_id=provider.id, session=session
            )
            if not pricelist_ids:
                logger.error(
                    f'No pricelists found for provider {provider.name}.'
                )
                raise ValueError(
                    f'No pricelists found for provider {provider.name}.'
                )
            logger.debug(f'Using pricelist_ids[-1]: {pricelist_ids[-1]}')
            # Создаем или получаем объект запроса
            request = CustomerPriceListCreate(
                customer_id=customer.id,
                config_id=config.id,
                items=[pricelist_ids[-1]],
            )

            await process_customer_pricelist(
                customer=customer, request=request, session=session
            )

            logger.info(
                f'Pricelist created and sent for customer {customer.name}'
            )
        except Exception as e:
            logger.error(
                f'Error process. pricelist for customer {customer.name}: {e}',
                exc_info=True,
            )


async def process_new_provider_emails(session: AsyncSession, app: FastAPI):
    """
    Обрабатывает все новые письма за сегодня,
    скачивая файлы для провайдеров и далее
    запускает функцию обработки прайслеста для каждого провайдера.
    """
    logger.info('Начинаем обработку писем провайдеров...')
    start_time = time.perf_counter()
    downloaded = await get_emails(session=session)

    email_time = time.perf_counter()
    logger.info(
        f'get_emails() выполнена за {email_time - start_time:.2f} секунд'
    )

    if not downloaded:
        logger.info('Новых писем для обработки не найдено.')
        return
    sem = asyncio.Semaphore(2)
    process_start = time.perf_counter()
    tasks = [
        asyncio.create_task(_process_one(item, app, sem))
        for item in downloaded
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    process_end = time.perf_counter()
    logger.info(
        f'Обработка прайса выполнена '
        f'за {process_end - process_start:.2f} секунд'
    )
    successful = 0
    errors = 0
    for result in results:
        if isinstance(result, Exception):
            errors += 1
            logger.error(
                f'Ошибка обработки прайс-листа: {result}', exc_info=True
            )
        else:
            successful += 1

    total_time = time.perf_counter() - start_time
    logger.info(
        f'process_new_provider_emails завершена за {total_time:.2f} секунд. '
        f'Успешно: {successful}, Ошибок: {errors}'
    )


async def download_price_provider_task(app: FastAPI):
    logger.info('Starting download_price_provider_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            # Проверяю наличие поставщика
            provider = await crud_provider.get_provider_or_none(
                provider=PROVIDER_IN['name'], session=session
            )
            if not provider:
                provider_in_model = ProviderCreate(**PROVIDER_IN)
                provider = await crud_provider.create(
                    obj_in=provider_in_model, session=session
                )
                config_in_model = ProviderPriceListConfigCreate(
                    **CONFIG_DATA_PROVIDER
                )
                await crud_provider_pricelist_config.create(
                    provider_id=provider.id,
                    config_in=config_in_model,
                    session=session,
                )
                logger.info(f'Created initial provider with id: {provider.id}')
            await process_new_provider_emails(session, app)
            logger.info('Completed download_price_provider_task')
        except Exception as e:
            logger.error(f'Error in download_price_provider_task: {e}')


async def cleanup_old_pricelists_task(app: FastAPI):
    logger.info('Starting cleanup_old_pricelists_task')
    async_session_factory = app.state.session_factory
    async with async_session_factory() as session:
        try:
            total_deleted = 0
            while True:
                cleanup = crud_pricelist.cleanup_old_pricelists_keep_last_n
                deleted = await cleanup(
                    session=session,
                    keep_last_n=5,
                    batch_size=500,
                )
                total_deleted += deleted
                if deleted == 0:
                    break
            logger.info(
                f'Cleanup finished. Deleted pricelists: {total_deleted}'
            )
        except Exception as e:
            logger.error(
                f'Error in cleanup_old_pricelists_task: {e}',
                exc_info=True
            )
            await session.rollback()
