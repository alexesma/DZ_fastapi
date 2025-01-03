# scheduler.py
import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.email import download_price_provider
from dz_fastapi.crud.partner import (
    crud_customer_pricelist,
    crud_customer_pricelist_config,
    crud_provider,
    crud_customer, crud_pricelist, crud_provider_pricelist_config
)
from dz_fastapi.schemas.partner import (
    CustomerPriceListCreate,
    ProviderCreate,
    ProviderPriceListConfigCreate,
    CustomerCreate,
    CustomerPriceListConfigCreate
)
from dz_fastapi.core.config import settings
from dz_fastapi.services.process import process_customer_pricelist, process_provider_pricelist
from dz_fastapi.core.constants import (
    PROVIDER,
    CUSTOMER,
    CONFIG_DATA_PROVIDER,
    PROVIDER_IN,
    CUSTOMER_IN,
    CONFIG_DATA_CUSTOMER
)

logger = logging.getLogger('dz_fastapi')

def start_scheduler(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.configure(timezone="UTC")

    # Добавляем задачи в планировщик
    scheduler.add_job(
        func=download_price_provider_task,
        trigger='cron',  # или 'interval'
        args=[app],
        id='download_price_provider',
        name='Download price provider',
        minute='*/5'  # каждые 5 минут
        # hour='9',
    )

    # scheduler.add_job(
    #     func=send_price_list_task,
    #     trigger='cron',
    #     args=[app],
    #     id='send_price_list',
    #     name='Send price list to customers',
    #     # minute='*/5'  # каждые 5 минут
    #     hour='9',  # каждый день в 9 утра
    # )

    scheduler.start()
    logger.info('Scheduler started.')


async def download_price_provider_task(app: FastAPI):
    logger.info('Starting download_price_provider_task')
    async_session_factory = get_async_session()
    async with async_session_factory() as session:
        try:
            provider_in_model = ProviderCreate(**PROVIDER_IN)
            provider = await crud_provider.get_provider_or_none(
                provider=PROVIDER_IN['name'],
                session=session
            )
            if not provider:
                provider = await crud_provider.create(
                    obj_in=provider_in_model,
                    session=session
                )
            config_in_model = ProviderPriceListConfigCreate(**CONFIG_DATA_PROVIDER)
            config = await crud_provider_pricelist_config.get_config_or_none(
                provider_id=provider.id,
                session=session
            )
            if not config:
                config = await crud_provider_pricelist_config.create(
                    provider_id=provider.id,
                    config_in=config_in_model,
                    session=session
                )

            filepath = await download_price_provider(
                provider_id=provider.id,
                session=session
            )
            if not filepath:
                logger.error(f'Failed to download file for provider_id: {provider.id}')
                raise ValueError('download_price_provider returned None')
            file_extension = filepath.split('.')[-1].lower()
            with open(filepath, "rb") as f:
                file_content = f.read()
            logger.info(
                f'Successfully downloaded price for provider {provider.id}'
            )
            await process_provider_pricelist(
                provider_id=provider.id,
                file_content=file_content,
                file_extension=file_extension,
                use_stored_params=True,
                start_row=None,
                oem_col=None,
                brand_col=None,
                name_col=None,
                qty_col=None,
                price_col=None,
                session=session
            )
            await send_price_list_task(app)
            return {'detail': f'Downloaded and processed provider price list for provider_id: {provider.id}'}
        except Exception as e:
            logger.error(f'Error in download_price_provider_task: {e}')


async def send_price_list_task(app: FastAPI):
    logger.info('Starting send_price_list_task')
    async_session_factory = get_async_session()
    async with async_session_factory() as session:
        try:
            customer_in_model = CustomerCreate(**CUSTOMER_IN)

            customer = await crud_customer.get_customer_or_none(
                customer=CUSTOMER,
                session=session
            )
            if not customer:
                customer = await crud_customer.create(
                    obj_in=customer_in_model,
                    session=session
                )
            config_in_model = CustomerPriceListConfigCreate(**CONFIG_DATA_CUSTOMER)
            configs = await crud_customer_pricelist_config.get_by_customer_id(
                customer_id=customer.id,
                session=session
            )
            if not configs:
                config = await crud_customer_pricelist_config.create_config(
                    customer_id=customer.id,
                    config_in=config_in_model,
                    session=session
                )
            config = configs[-1]

            provider = await crud_provider.get_provider_or_none(
                provider=PROVIDER_IN['name'],
                session=session
            )
            if not provider:
                logger.error(f'Provider "{PROVIDER_IN['name']}" not found.')
                raise ValueError(f'Provider "{PROVIDER_IN['name']}" not found.')
            pricelist_ids = await crud_pricelist.get_pricelist_ids_by_provider(
                provider_id=provider.id,
                session=session
            )
            if not pricelist_ids:
                logger.error(f'No pricelists found for provider {provider.name}.')
                raise ValueError(f'No pricelists found for provider {provider.name}.')
            logger.debug(f'Using pricelist_ids[-1]: {pricelist_ids[-1]}')
            # Создаем или получаем объект запроса
            request = CustomerPriceListCreate(
                customer_id=customer.id,
                config_id=config.id,
                items=[pricelist_ids[-1]],
            )

            response = await process_customer_pricelist(
                customer=customer,
                request=request,
                session=session
            )

            logger.info(
                f'Pricelist created and sent for customer {customer.name}'
            )
        except Exception as e:
            logger.error(
                f'Error processing pricelist for customer {customer.name}: {e}'
            )
