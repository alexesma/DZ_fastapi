import asyncio
import logging
import os
from typing import List, Optional

from fastapi import (APIRouter, BackgroundTasks, Body, Depends, File, Form,
                     HTTPException, Query, UploadFile, status)
from httpx import Response
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from dz_fastapi.analytics.price_history import (analyze_autopart_popularity,
                                                create_autopart_analysis_excel)
from dz_fastapi.api.validators import change_brand_name, change_customer_name
from dz_fastapi.core.constants import (
    BODY_MAIL_ANALYTIC_PRICE_PROVIDER,
    FILENAME_EXCEL_MAIL_ANALYTIC_PRICE_PROVIDER,
    SUBJECT_MAIL_ANALYTIC_PRICE_PROVIDER)
from dz_fastapi.core.db import get_session
from dz_fastapi.crud.partner import (crud_customer, crud_customer_pricelist,
                                     crud_customer_pricelist_config,
                                     crud_customer_pricelist_source,
                                     crud_pricelist, crud_provider,
                                     crud_provider_abbreviation,
                                     crud_provider_pricelist_config)
from dz_fastapi.models.partner import (Customer, CustomerPriceList,
                                       CustomerPriceListAutoPartAssociation,
                                       CustomerPriceListConfig, PriceList,
                                       Provider, ProviderPriceListConfig)
from dz_fastapi.schemas.autopart import AutoPartResponse
from dz_fastapi.schemas.partner import (AutoPartInPricelist,
                                        CustomerAllPriceListResponse,
                                        CustomerCreate,
                                        CustomerPriceListConfigCreate,
                                        CustomerPriceListConfigResponse,
                                        CustomerPriceListConfigSummary,
                                        CustomerPriceListConfigUpdate,
                                        CustomerPriceListCreate,
                                        CustomerPriceListItem,
                                        CustomerPriceListResponse,
                                        CustomerPriceListResponseShort,
                                        CustomerPriceListSourceCreate,
                                        CustomerPriceListSourceResponse,
                                        CustomerPriceListSourceUpdate,
                                        CustomerResponse,
                                        CustomerResponseShort, CustomerUpdate,
                                        PaginatedProvidersResponse,
                                        PriceListDeleteRequest,
                                        PriceListPaginationResponse,
                                        PriceListProcessStats,
                                        PriceListResponse, PriceListSummary,
                                        ProviderAbbreviationOut,
                                        ProviderCreate, ProviderPageResponse,
                                        ProviderPriceListConfigCreate,
                                        ProviderPriceListConfigOption,
                                        ProviderPriceListConfigOut,
                                        ProviderPriceListConfigUpdate,
                                        ProviderResponse, ProviderUpdate)
from dz_fastapi.services.email import (download_price_provider,
                                       send_email_with_attachment)
from dz_fastapi.services.process import (check_start_and_finish_date,
                                         parse_exclude_positions_file,
                                         process_customer_pricelist,
                                         process_provider_pricelist)

logger = logging.getLogger('dz_fastapi')
EMAIL_NAME_ANALYTIC = os.getenv('EMAIL_NAME_ANALYTIC')


router = APIRouter()


@router.post(
    '/providers/',
    tags=['providers'],
    status_code=status.HTTP_201_CREATED,
    summary='Создание поставщика',
    response_model=ProviderResponse,
)
async def create_provider(
    provider_in: ProviderCreate, session: AsyncSession = Depends(get_session)
):
    new_name = await change_brand_name(brand_name=provider_in.name)

    if not new_name or not new_name.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail='Name became empty after normalization. '
            'Please use Latin letters, numbers, spaces and .,_&()-',
        )
    provider_in.name = new_name
    existing_provider = await crud_provider.get_provider_or_none(
        provider=provider_in.name, session=session
    )
    if existing_provider:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Provider with name {provider_in.name} already exists.',
        )

    provider = await crud_provider.create(obj_in=provider_in, session=session)
    return ProviderResponse.model_validate(provider)


@router.get(
    '/providers/',
    tags=['providers'],
    status_code=status.HTTP_200_OK,
    summary='Список поставщиков',
    response_model=PaginatedProvidersResponse,
)
async def get_all_providers(
    session: AsyncSession = Depends(get_session),
    page: int = Query(1, ge=1, description='Номер страницы'),
    page_size: int = Query(
        10, ge=1, le=100, description='Количество элементов на странице'
    ),
    search: Optional[str] = Query(
        None, description='Поиск по названию поставщика'
    ),
):
    '''
    Получить список всех поставщиков с пагинацией и поиском.

    :param session:
    :param page: номер страницы (начинается с 1)
    :param page_size: количество поставщиков на странице (1-100)
    :param search: поиск по названию поставщика (необязательно)
    :return:
        - items: список поставщиков
        - page: текущая страница
        - page_size: размер страницы
        - total: общее количество поставщиков
        - pages: общее количество страниц
    '''

    providers = await crud_provider.get_all(
        session=session,
        page=page,
        page_size=page_size,
        search=search,
    )
    return providers


@router.get(
    '/providers/{provider_id}/',
    tags=['providers'],
    status_code=status.HTTP_200_OK,
    summary='Поставщик по id',
    response_model=ProviderResponse,
)
async def get_provider(
    provider_id: int, session: AsyncSession = Depends(get_session)
):
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail='Provider not found')
    return ProviderResponse.model_validate(provider)


@router.get(
    '/providers/{provider_id}/full',
    tags=['providers'],
    status_code=status.HTTP_200_OK,
    summary='Поставщик по id',
    response_model=ProviderPageResponse,
)
async def get_provider_full(
    provider_id: int, session: AsyncSession = Depends(get_session)
):
    result = await crud_provider.get_full_by_id(
        provider_id=provider_id, session=session
    )
    if result is None:
        raise HTTPException(status_code=404, detail='Provider not found')
    return result


@router.get(
    '/provider-configs/',
    tags=['providers'],
    status_code=status.HTTP_200_OK,
    summary='Список всех конфигураций прайсов поставщиков',
    response_model=List[ProviderPriceListConfigOption],
)
async def get_provider_config_options(
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(ProviderPriceListConfig, Provider)
        .join(Provider, Provider.id == ProviderPriceListConfig.provider_id)
        .order_by(Provider.name.asc(), ProviderPriceListConfig.id.asc())
    )
    rows = (await session.execute(stmt)).all()
    return [
        ProviderPriceListConfigOption(
            id=config.id,
            provider_id=provider.id,
            provider_name=provider.name,
            name_price=config.name_price,
            is_own_price=bool(provider.is_own_price),
        )
        for config, provider in rows
    ]


@router.delete(
    '/providers/{provider_id}/',
    tags=['providers'],
    summary='Удаление поставщика',
    status_code=status.HTTP_200_OK,
    response_model=ProviderResponse,
)
async def delete_provider(
    provider_id: int, session: AsyncSession = Depends(get_session)
):
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail='Provider not found')

    return await crud_provider.remove(provider, session, commit=True)


@router.patch(
    '/providers/{provider_id}/',
    tags=['providers'],
    summary='Обновление поставщика',
    status_code=status.HTTP_200_OK,
    response_model=ProviderResponse,
)
async def update_provider(
    provider_id: int,
    provider_in: ProviderUpdate = Body(...),
    session: AsyncSession = Depends(get_session),
):
    updated_provider = await crud_provider.update_provider(
        provider_id=provider_id, obj_in=provider_in, session=session
    )
    return ProviderResponse.model_validate(updated_provider)


@router.post(
    '/customers/',
    tags=['customers'],
    status_code=status.HTTP_201_CREATED,
    summary='Создание покупателя',
    response_model=CustomerResponse,
)
async def create_customer(
    customer_in: CustomerCreate, session: AsyncSession = Depends(get_session)
):
    normalized_name = await change_customer_name(customer_in.name)
    if not normalized_name or not normalized_name.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail='Name must not be empty after normalization.',
        )
    customer_in.name = normalized_name
    existing_customer = await crud_customer.get_customer_or_none(
        customer=customer_in.name, session=session
    )
    if existing_customer:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Customer with name {customer_in.name} already exists.',
        )
    try:
        customer = await crud_customer.create(
            obj_in=customer_in, session=session
        )
    except IntegrityError as e:
        error_message = str(e.orig)
        if (
            'duplicate key value violates '
            'unique constraint "client_name_key"'
        ) in error_message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f'Customer with name {customer_in.name} already exists.'
                ),
            )
        elif (
            'duplicate key value violates '
            'unique constraint "ix_client_email_contact"'
        ) in error_message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f'Customer with email '
                    f'{customer_in.email_contact} already exists.'
                ),
            )
        elif (
            'duplicate key value violates '
            'unique constraint "ix_customer_email_outgoing_price"'
        ) in error_message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f'Customer with email '
                    f'{customer_in.email_outgoing_price} already exists.'
                ),
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail='Unexpected database error occurred.',
            )
    return CustomerResponse(
        id=customer.id,
        name=customer.name,
        description=customer.description,
        email_contact=customer.email_contact,
        comment=customer.comment,
        email_outgoing_price=customer.email_outgoing_price,
        type_prices=customer.type_prices,
        customer_price_lists=[],
        pricelist_configs=[],
    )


@router.get(
    '/customers/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Список покупатель',
    response_model=List[CustomerResponse],
)
async def get_all_customer(session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(Customer).options(
            selectinload(Customer.customer_price_lists)
            .selectinload(CustomerPriceList.autopart_associations)
            .selectinload(CustomerPriceListAutoPartAssociation.autopart),
            selectinload(Customer.pricelist_configs)
            .selectinload(CustomerPriceListConfig.sources)
        )
    )
    customers = result.scalars().all()
    customer_responses = []

    for customer in customers:
        # Convert customer_price_lists
        customer_price_lists = []
        for cpl in customer.customer_price_lists:
            # Convert autoparts_associations
            autoparts = []
            for assoc in cpl.autopart_associations:
                autopart = AutoPartResponse.model_validate(
                    assoc.autopart, from_attributes=True
                )
                autopart_in_pricelist = AutoPartInPricelist(
                    autopart_id=assoc.autopart_id,
                    quantity=assoc.quantity,
                    price=assoc.price,
                    autopart=autopart,
                )
                autoparts.append(autopart_in_pricelist)

            # Create CustomerPriceListResponse
            cpl_response = CustomerPriceListResponse(
                id=cpl.id,
                date=cpl.date,
                customer_id=cpl.customer_id,
                autoparts=autoparts,
            )
            customer_price_lists.append(cpl_response)

        # Create CustomerResponse instance
        customer_data = CustomerResponse(
            id=customer.id,
            name=customer.name,
            description=customer.description,
            email_contact=customer.email_contact,
            comment=customer.comment,
            email_outgoing_price=customer.email_outgoing_price,
            type_prices=customer.type_prices,
            customer_price_lists=customer_price_lists,
            pricelist_configs=[
                CustomerPriceListConfigSummary(
                    id=config.id,
                    name=config.name,
                    sources_count=len(config.sources or []),
                    schedule_days=config.schedule_days or [],
                    schedule_times=config.schedule_times or [],
                    is_active=bool(config.is_active),
                )
                for config in (customer.pricelist_configs or [])
            ],
        )
        customer_responses.append(customer_data)
    return customer_responses


def build_customer_response_short(customer: Customer) -> CustomerResponseShort:
    customer_price_lists_short = [
        CustomerPriceListResponseShort(
            id=cpl.id,
            date=cpl.date,
            autoparts_count=len(cpl.autopart_associations),
        )
        for cpl in customer.customer_price_lists
    ]

    return CustomerResponseShort(
        id=customer.id,
        name=customer.name,
        description=customer.description,
        email_contact=customer.email_contact,
        comment=customer.comment,
        email_outgoing_price=customer.email_outgoing_price,
        type_prices=customer.type_prices,
        customer_price_lists=customer_price_lists_short,
    )


def build_customer_source_response(
    source,
) -> CustomerPriceListSourceResponse:
    provider_config = getattr(source, 'provider_config', None)
    provider = getattr(
        provider_config,
        'provider', None
    ) if provider_config else None

    return CustomerPriceListSourceResponse(
        id=source.id,
        provider_config_id=source.provider_config_id,
        provider_id=getattr(provider, 'id', None),
        provider_name=getattr(provider, 'name', None),
        provider_config_name=getattr(provider_config, 'name_price', None),
        is_own_price=bool(getattr(provider, 'is_own_price', False)),
        enabled=bool(source.enabled),
        markup=source.markup,
        brand_filters=source.brand_filters or {},
        position_filters=source.position_filters or {},
        min_price=source.min_price,
        max_price=source.max_price,
        min_quantity=source.min_quantity,
        max_quantity=source.max_quantity,
        additional_filters=source.additional_filters or {},
    )


@router.get(
    '/customers/{customer_id}/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Покупатель по id',
    response_model=CustomerResponseShort,
)
async def get_customer(
    customer_id: int, session: AsyncSession = Depends(get_session)
):
    result = await session.execute(
        select(Customer)
        .options(
            selectinload(Customer.customer_price_lists)
            .selectinload(CustomerPriceList.autopart_associations)
            .selectinload(CustomerPriceListAutoPartAssociation.autopart)
        )
        .where(Customer.id == customer_id)
    )
    customer = result.scalars().first()
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    return build_customer_response_short(customer)


@router.delete(
    '/customers/{customer_id}/',
    tags=['customers'],
    summary='Удаление покупателя',
    status_code=status.HTTP_200_OK,
    response_model=CustomerResponse,
)
async def delete_customer(
    customer_id: int, session: AsyncSession = Depends(get_session)
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    return await crud_customer.remove(customer, session, commit=True)


@router.patch(
    '/customers/{customer_id}/',
    tags=['customers'],
    summary='Обновление покупателя',
    status_code=status.HTTP_200_OK,
    response_model=CustomerResponseShort,
)
async def update_customer(
    customer_id: int,
    customer_in: CustomerUpdate = Body(...),
    session: AsyncSession = Depends(get_session),
):
    customer_db = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer_db:
        raise HTTPException(status_code=404, detail='Customer not found')

    update_data = customer_in.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(
            status_code=404, detail='No data customer to update.'
        )

    try:
        updated_customer = await crud_customer.update(
            db_obj=customer_db, obj_in=update_data, session=session
        )
    except IntegrityError as e:
        error_message = str(e.orig)
        if (
            'duplicate key value violates '
            'unique constraint "client_name_key"'
        ) in error_message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Customer with this name already exists.',
            )
        if (
            'duplicate key value violates '
            'unique constraint "ix_client_email_contact"'
        ) in error_message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Customer with this contact email already exists.',
            )
        if (
            'duplicate key value violates '
            'unique constraint "ix_customer_email_outgoing_price"'
        ) in error_message:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Customer with this outgoing email already exists.',
            )
        raise
    await session.commit()
    result = await session.execute(
        select(Customer)
        .options(
            selectinload(Customer.customer_price_lists)
            .selectinload(CustomerPriceList.autopart_associations)
            .selectinload(CustomerPriceListAutoPartAssociation.autopart)
        )
        .where(Customer.id == customer_id)
    )
    updated_customer = result.scalars().first()
    if not updated_customer:
        raise HTTPException(
            status_code=404, detail='Customer not found after update.'
        )

    return build_customer_response_short(updated_customer)


@router.post(
    '/providers/{provider_id}/pricelist-config/',
    tags=['providers', 'pricelist-config'],
    status_code=status.HTTP_201_CREATED,
    summary='Create new price list parsing parameters for a provider',
    response_model=ProviderPriceListConfigOut,
)
async def set_provider_pricelist_config(
    provider_id: int,
    config_in: ProviderPriceListConfigCreate,
    session: AsyncSession = Depends(get_session),
):
    # Проверяем, что провайдер существует
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail='Provider not found')

    new_config = await crud_provider_pricelist_config.create(
        provider_id=provider_id, config_in=config_in, session=session
    )
    return ProviderPriceListConfigOut.model_validate(new_config)


@router.patch(
    '/providers/{provider_id}/pricelist-config/{config_id}/',
    tags=['providers', 'pricelist-config'],
    status_code=status.HTTP_200_OK,
    summary='Update price list parsing parameters for a provider by config id',
    response_model=ProviderPriceListConfigOut,
)
async def update_provider_pricelist_config(
    provider_id: int,
    config_id: int,
    config_in: ProviderPriceListConfigUpdate,
    session: AsyncSession = Depends(get_session),
):
    # Проверяем, что провайдер существует
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail='Provider not found')
    provider_config = await crud_provider_pricelist_config.get_by_id(
        config_id=config_id, session=session
    )
    if not provider_config:
        raise HTTPException(
            status_code=404,
            detail=f'Config for provider {provider.name} not found',
        )
    update_config = await crud_provider_pricelist_config.update(
        db_obj=provider_config, obj_in=config_in, session=session
    )
    return ProviderPriceListConfigOut.model_validate(update_config)


@router.post(
    '/providers/pricelist-config/exclude-positions/parse',
    tags=['providers', 'pricelist-config'],
    status_code=status.HTTP_200_OK,
    summary='Parse exclude positions file for provider pricelist config',
)
async def parse_provider_pricelist_excludes(
    file: UploadFile = File(...),
):
    filename = file.filename or ''
    if '.' not in filename:
        raise HTTPException(
            status_code=400, detail='File extension is required'
        )
    extension = filename.rsplit('.', 1)[-1].lower()
    file_content = await file.read()
    try:
        items = await asyncio.to_thread(
            parse_exclude_positions_file, extension, file_content
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid exclude file.')
    return {'items': items}


@router.get(
    '/providers/{provider_id}/pricelist-config/',
    tags=['providers', 'pricelist-config'],
    status_code=status.HTTP_200_OK,
    summary='Get list with price lists parsing parameters for provider',
    response_model=List[ProviderPriceListConfigOut],
)
async def get_provider_pricelist_configs(
    provider_id: int, session: AsyncSession = Depends(get_session)
):
    # Check if the provider exists
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail='Provider not found')

    # Check if a config already exists
    existing_configs = await crud_provider_pricelist_config.get_configs(
        provider_id=provider_id, session=session
    )

    if not existing_configs:
        raise HTTPException(
            status_code=404, detail='Config provider not found'
        )
    return [
        ProviderPriceListConfigOut.model_validate(existing_config)
        for existing_config in existing_configs
    ]


@router.get(
    '/providers/{provider_id}/pricelist-config/{config_id}/',
    tags=['providers', 'pricelist-config'],
    status_code=status.HTTP_200_OK,
    summary='Get price list parsing parameters for provider',
    response_model=ProviderPriceListConfigOut,
)
async def get_provider_pricelist_config(
    provider_id: int,
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(
            status_code=404, detail=f'Provider not found for id {provider_id}'
        )
    provider_config = await crud_provider_pricelist_config.get_by_id(
        config_id=config_id, session=session
    )
    if not provider_config:
        raise HTTPException(
            status_code=404,
            detail=f'Configuration for provider {provider.name} ' f'not found',
        )
    return ProviderPriceListConfigOut.model_validate(provider_config)


# @router.post(
#     '/providers/{provider_id}/pricelists/',
#     tags=['providers', 'pricelists'],
#     status_code=status.HTTP_201_CREATED,
#     summary='Create provider\'s pricelist',
#     response_model=PriceListResponse,
# )
# async def create_provider_pricelist(
#     provider_id: int,
#     pricelist_in_base: PriceListUpdate,
#     session: AsyncSession = Depends(get_session),
# ):
#     pricelist_in = PriceListCreate(
#         **pricelist_in_base.model_dump(exclude_unset=True),
#         provider_id=provider_id,
#     )
#     try:
#         # Get id provider
#         provider = await crud_provider.get_by_id(
#             provider_id=provider_id, session=session
#         )
#         if not provider:
#             raise HTTPException(status_code=404, detail='Provider not found')
#
#         pricelist = await crud_pricelist.create(
#             obj_in=pricelist_in, session=session
#         )
#         return PriceListResponse.model_validate(pricelist)
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         logger.error(
#             f'Unexpected error occurred while creating PriceList: {e}'
#         )
#         raise HTTPException(
#             status_code=500,
#             detail='Unexpected error during PriceList creation',
#         )


@router.post(
    '/providers/{provider_id}/pricelists/{provider_list_conf_id}/upload/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_201_CREATED,
    summary='Upload and create price list from file',
    response_model=PriceListResponse,
)
async def upload_provider_pricelist(
    provider_id: int,
    provider_list_conf_id: int,
    file: UploadFile = File(...),
    use_stored_params: bool = Form(True),
    start_row: Optional[int] = Form(
        None, description='Row number where data starts (0-indexed)'
    ),
    oem_col: Optional[int] = Form(
        None, description='Column number for OEM number (0-indexed)'
    ),
    brand_col: Optional[int] = Form(
        None, description='Column number for brand (0-indexed)'
    ),
    name_col: Optional[int] = Form(
        None, description='Column number for name (0-indexed)'
    ),
    qty_col: Optional[int] = Form(
        None, description='Column number for quantity (0-indexed)'
    ),
    price_col: Optional[int] = Form(
        None, description='Column number for price (0-indexed)'
    ),
    session: AsyncSession = Depends(get_session),
):
    # Read the file content
    content = await file.read()
    # Get the file extension
    file_extension = file.filename.split('.')[-1].lower()
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(
            status_code=404, detail=f'Not found provider_id: {provider_id}'
        )
    logger.debug(f'Filename={file.filename}, size={len(content)} bytes')
    logger.debug(f'Extension={file_extension}')
    provider_conf_obj = await crud_provider_pricelist_config.get_by_id(
        config_id=provider_list_conf_id, session=session
    )
    if not provider_conf_obj:
        raise HTTPException(
            status_code=404, detail='Provider configuration not found'
        )
    pricelist, stats = await process_provider_pricelist(
        provider=provider,
        file_content=content,
        file_extension=file_extension,
        provider_list_conf=provider_conf_obj,
        use_stored_params=use_stored_params,
        start_row=start_row,
        oem_col=oem_col,
        brand_col=brand_col,
        name_col=name_col,
        qty_col=qty_col,
        price_col=price_col,
        session=session,
        return_stats=True,
    )

    return PriceListResponse.model_validate(pricelist).model_copy(
        update={'stats': PriceListProcessStats(**stats)}
    )


@router.get(
    '/providers/{provider_id}/pricelists/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Получить список прайс-листов для поставщика',
    response_model=PriceListPaginationResponse,
)
async def get_provider_pricelists(
    provider_id: int,
    skip: int = Query(0, ge=0, description='Сколько записей пропустить'),
    limit: int = Query(
        10, ge=1, description='Максимальное количество записей для возврата'
    ),
    session: AsyncSession = Depends(get_session),
):
    try:
        # Проверяем существование поставщика
        provider = await crud_provider.get_by_id(
            provider_id=provider_id, session=session
        )
        if not provider:
            raise HTTPException(status_code=404, detail='Поставщик не найден')
        total_count = await crud_pricelist.count_by_provider_id(
            provider_id=provider_id, session=session
        )

        if skip >= total_count:
            return PriceListPaginationResponse(
                total_count=total_count, skip=skip, limit=limit, pricelists=[]
            )

        pricelists = await crud_pricelist.get_by_provider_paginated(
            provider_id=provider_id, skip=skip, limit=limit, session=session
        )
        pricelist_summaries = []
        for pl in pricelists:
            summary = PriceListSummary(
                id=pl.id,
                date=pl.date,
                num_positions=pl.num_positions,
                provider_config_id=pl.provider_config_id,
            )
            pricelist_summaries.append(summary)

        return PriceListPaginationResponse(
            total_count=total_count,
            skip=skip,
            limit=limit,
            pricelists=pricelist_summaries,
        )
    except Exception as e:
        logger.error(f'Ошибка при получении прайс-листов: {e}')
        raise HTTPException(
            status_code=500, detail='Внутренняя ошибка сервера'
        )


@router.delete(
    '/providers/{provider_id}/pricelists/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Delete multiple price lists for a provider',
)
async def delete_provider_pricelists(
    provider_id: int,
    request: PriceListDeleteRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        # Проверяем существование поставщика
        provider = await crud_provider.get_by_id(
            provider_id=provider_id, session=session
        )
        if not provider:
            raise HTTPException(status_code=404, detail='Provider not found')

        pricelist_ids = request.pricelist_ids

        if not pricelist_ids:
            raise HTTPException(
                status_code=400, detail='No PriceList IDs provided'
            )

        # Получаем прайс-листы, которые нужно удалить,
        # и проверяем принадлежность поставщику
        stmt = select(PriceList).where(
            PriceList.id.in_(pricelist_ids),
            PriceList.provider_id == provider_id,
        )
        result = await session.execute(stmt)
        pricelists_to_delete = result.scalars().all()

        if not pricelists_to_delete:
            raise HTTPException(
                status_code=404, detail='No PriceLists found for deletion'
            )

        # Удаляем прайс-листы
        for pricelist in pricelists_to_delete:
            await session.delete(pricelist)

        await session.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except HTTPException as e:
        raise e
    except SQLAlchemyError as e:
        logger.error(f'Database error occurred while deleting PriceLists: {e}')
        await session.rollback()
        raise HTTPException(
            status_code=500, detail='Database error during PriceList deletion'
        )
    except Exception as e:
        logger.error(
            f'Unexpected error occurred while deleting PriceLists: {e}'
        )
        await session.rollback()
        raise HTTPException(
            status_code=500,
            detail='Unexpected error during PriceList deletion',
        )


@router.post(
    '/customers/{customer_id}/pricelist-configs/',
    tags=['customers'],
    status_code=status.HTTP_201_CREATED,
    summary='Create a pricelist configuration for a customer',
    response_model=CustomerPriceListConfigResponse,
)
async def create_customer_pricelist_config(
    customer_id: int,
    config_in: CustomerPriceListConfigCreate,
    session: AsyncSession = Depends(get_session),
):
    # Check if the customer exists
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    try:
        # Вызываем метод из CRUD-класса для создания конфигурации
        new_config = await crud_customer_pricelist_config.create_config(
            customer_id=customer_id, config_in=config_in, session=session
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    base = CustomerPriceListConfigResponse.model_validate(new_config)
    return base.model_copy(update={'sources': []})


@router.patch(
    '/customers/{customer_id}/pricelist-configs/{config_id}',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Update a pricelist configuration for a customer',
    response_model=CustomerPriceListConfigResponse,
)
async def update_customer_pricelist_config(
    customer_id: int,
    config_id: int,
    config_in: CustomerPriceListConfigUpdate,
    session: AsyncSession = Depends(get_session),
):
    # Check if the customer exists
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    # Retrieve existing configuration
    config = await crud_customer_pricelist_config.get_by_id(
        session=session, customer_id=customer.id, config_id=config_id
    )
    if not config or config.customer_id != customer_id:
        raise HTTPException(
            status_code=404, detail='Configuration not found for this customer'
        )

    update_data = config_in.model_dump(exclude_unset=True)

    # Handle nested data if necessary
    for field, value in update_data.items():
        setattr(config, field, value)

    session.add(config)
    await session.commit()
    await session.refresh(config)
    sources = await crud_customer_pricelist_source.get_by_config_id(
        config_id=config.id, session=session
    )
    base = CustomerPriceListConfigResponse.model_validate(config)
    return base.model_copy(
        update={
            'sources': [build_customer_source_response(s) for s in sources]
        }
    )


@router.get(
    '/customers/{customer_id}/pricelist-configs/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Get all pricelist configurations for a customer',
    response_model=List[CustomerPriceListConfigResponse],
)
async def get_customer_pricelist_configs(
    customer_id: int, session: AsyncSession = Depends(get_session)
):
    # Retrieve configurations
    configs = await crud_customer_pricelist_config.get_by_customer_id(
        session=session, customer_id=customer_id
    )
    responses = []
    for config in configs:
        sources = await crud_customer_pricelist_source.get_by_config_id(
            config_id=config.id, session=session
        )
        base = CustomerPriceListConfigResponse.model_validate(config)
        responses.append(
            base.model_copy(
                update={
                    'sources': [
                        build_customer_source_response(s) for s in sources
                    ]
                }
            )
        )
    return responses


@router.get(
    '/customers/{customer_id}/pricelist-configs/{config_id}/sources/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Get all sources for a customer pricelist configuration',
    response_model=List[CustomerPriceListSourceResponse],
)
async def get_customer_pricelist_sources(
    customer_id: int,
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_customer_pricelist_config.get_by_id(
        session=session, customer_id=customer_id, config_id=config_id
    )
    if not config:
        raise HTTPException(
            status_code=404, detail='Configuration not found for this customer'
        )
    sources = await crud_customer_pricelist_source.get_by_config_id(
        config_id=config_id, session=session
    )
    return [build_customer_source_response(s) for s in sources]


@router.post(
    '/customers/{customer_id}/pricelist-configs/{config_id}/sources/',
    tags=['customers'],
    status_code=status.HTTP_201_CREATED,
    summary='Create a source for a customer pricelist configuration',
    response_model=CustomerPriceListSourceResponse,
)
async def create_customer_pricelist_source(
    customer_id: int,
    config_id: int,
    source_in: CustomerPriceListSourceCreate,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_customer_pricelist_config.get_by_id(
        session=session, customer_id=customer_id, config_id=config_id
    )
    if not config:
        raise HTTPException(
            status_code=404, detail='Configuration not found for this customer'
        )
    provider_config = await crud_provider_pricelist_config.get_by_id(
        config_id=source_in.provider_config_id, session=session
    )
    if not provider_config:
        raise HTTPException(
            status_code=404, detail='Provider config not found'
        )

    new_source = await crud_customer_pricelist_source.create_source(
        config_id=config_id, source_in=source_in, session=session
    )
    source_full = await crud_customer_pricelist_source.get_by_id(
        source_id=new_source.id, session=session
    )
    return build_customer_source_response(source_full)


@router.patch(
    '/customers/{'
    'customer_id'
    '}/pricelist-configs/{config_id}/sources/{source_id}',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Update a source for a customer pricelist configuration',
    response_model=CustomerPriceListSourceResponse,
)
async def update_customer_pricelist_source(
    customer_id: int,
    config_id: int,
    source_id: int,
    source_in: CustomerPriceListSourceUpdate,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_customer_pricelist_config.get_by_id(
        session=session, customer_id=customer_id, config_id=config_id
    )
    if not config:
        raise HTTPException(
            status_code=404, detail='Configuration not found for this customer'
        )

    source = await crud_customer_pricelist_source.get_by_id(
        source_id=source_id, session=session
    )
    if not source or source.customer_config_id != config_id:
        raise HTTPException(status_code=404, detail='Source not found')

    if source_in.provider_config_id:
        provider_config = await crud_provider_pricelist_config.get_by_id(
            config_id=source_in.provider_config_id, session=session
        )
        if not provider_config:
            raise HTTPException(
                status_code=404, detail='Provider config not found'
            )

    updated = await crud_customer_pricelist_source.update_source(
        db_obj=source, obj_in=source_in, session=session
    )
    updated_full = await crud_customer_pricelist_source.get_by_id(
        source_id=updated.id, session=session
    )
    return build_customer_source_response(updated_full)


@router.delete(
    '/customers/{'
    'customer_id'
    '}/pricelist-configs/{config_id}/sources/{source_id}',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Delete a source for a customer pricelist configuration',
)
async def delete_customer_pricelist_source(
    customer_id: int,
    config_id: int,
    source_id: int,
    session: AsyncSession = Depends(get_session),
):
    config = await crud_customer_pricelist_config.get_by_id(
        session=session, customer_id=customer_id, config_id=config_id
    )
    if not config:
        raise HTTPException(
            status_code=404, detail='Configuration not found for this customer'
        )

    source = await crud_customer_pricelist_source.get_by_id(
        source_id=source_id, session=session
    )
    if not source or source.customer_config_id != config_id:
        raise HTTPException(status_code=404, detail='Source not found')

    await session.delete(source)
    await session.commit()
    return {'detail': 'Source deleted'}


@router.post(
    '/customers/{customer_id}/pricelist-configs/{config_id}/send-now',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Send customer pricelist immediately',
    response_model=CustomerPriceListResponse,
)
async def send_customer_pricelist_now(
    customer_id: int,
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    config = await crud_customer_pricelist_config.get_by_id(
        session=session, customer_id=customer_id, config_id=config_id
    )
    if not config:
        raise HTTPException(
            status_code=404, detail='Configuration not found for this customer'
        )

    request = CustomerPriceListCreate(
        customer_id=customer.id,
        config_id=config.id,
        items=[],
    )
    response = await process_customer_pricelist(
        customer=customer, request=request, session=session
    )
    return response


@router.post(
    '/customers/{customer_id}/pricelists/',
    tags=['customers', 'pricelists'],
    status_code=status.HTTP_201_CREATED,
    summary='Create a pricelist for a customer',
    response_model=CustomerPriceListResponse,
)
async def create_customer_pricelist(
    customer_id: int,
    request: CustomerPriceListCreate,
    session: AsyncSession = Depends(get_session),
):
    logger.info(
        f'Incoming request: customer_id={customer_id}'
        f', body={request.model_dump()}'
    )

    # customer = await crud_customer.get_by_id(
    #     customer_id=customer_id,
    #     session=session
    # )
    # if not customer:
    #     logger.error(f'Customer with id {customer_id} not found.')
    #     raise HTTPException(
    #         status_code=404,
    #         detail='Customer not found'
    #     )
    #
    # config = await crud_customer_pricelist_config.get_by_id(
    #     config_id=request.config_id,
    #     customer_id=customer_id,
    #     session=session
    # )
    # if not config:
    #     raise HTTPException(
    #         status_code=400,
    #         detail='No pricelist configuration found for the customer'
    #     )
    #
    # combined_data = []
    #
    # for pricelist_id in request.items:
    #     associations = await crud_pricelist.fetch_pricelist_data(
    #         pricelist_id,
    #         session
    #     )
    #     if not associations:
    #         continue
    #
    #     df = await crud_pricelist.transform_to_dataframe(
    #         associations=associations,
    #         session=session
    #     )
    #     logger.debug(f'Transform file to dataframe {df}')
    #
    #     df = crud_customer_pricelist.apply_coefficient(df, config)
    #     combined_data.append(df)
    #
    # if combined_data:
    #     final_df = pd.concat(combined_data, ignore_index=True)
    #
    #     # Deduplicate: keep the lowest price for each autopart
    #     final_df = final_df.sort_values(
    #         by=['oem_number', 'brand', 'price']
    #     ).drop_duplicates(subset=['oem_number', 'brand'], keep='first')
    # else:
    #     final_df = pd.DataFrame()
    #
    # logger.debug(
    # f'Final DataFrame before creating associations:\n{final_df}'
    # )
    # # Apply exclusions
    #
    # if not final_df.empty:
    #     if request.excluded_supplier_positions:
    #         for provider_id,
    #         excluded_autoparts
    #         in request.excluded_supplier_positions.items():
    #             final_df = position_exclude(
    #                 provider_id=provider_id,
    #                 excluded_autoparts=excluded_autoparts,
    #                 df=final_df
    #             )
    #     customer_autoparts_data = final_df.to_dict('records')
    # else:
    #     raise HTTPException(
    #         status_code=400,
    #         detail='No autoparts to include in the pricelist'
    #     )
    #
    # customer_pricelist = CustomerPriceList(
    #     customer=customer,
    #     date=request.date or date.today(),
    #     is_active=True
    # )
    # session.add(customer_pricelist)
    # await session.flush()
    #
    # associations = await crud_customer_pricelist.create_associations(
    #     customer_pricelist_id=customer_pricelist.id,
    #     autoparts_data=customer_autoparts_data,
    #     session=session
    # )
    #
    # # Prepare data for Excel file
    # df_excel = prepare_excel_data(associations=associations)
    #
    # if config.additional_filters.get('ZZAP'):
    #     logger.debug(f'Зашел в get additional_filters')
    #     df_excel =
    #     await dz_fastapi.services.process.add_origin_brand_from_dz(
    #         price_zzap=df_excel,
    #         session=session
    #     )
    # logger.debug(f'Измененный файл для ZZAP: {df}')
    # await session.commit()
    #
    # await send_pricelist(
    #     customer=customer,
    #     df_excel=df_excel,
    #     subject=f'Прайс лист {customer_pricelist.date}',
    #     body='Добрый день, высылаем Вам наш прайс-лист',
    #     attachment_filename=f'zzap_kross.xlsx'
    # )
    #
    # autoparts_response = []
    # for assoc in associations:
    #     autopart = AutoPartResponse.model_validate(
    #     assoc.autopart,
    #     from_attributes=True
    #     )
    #     autopart_in_pricelist = AutoPartInPricelist(
    #         autopart_id=assoc.autopart_id,
    #         quantity=assoc.quantity,
    #         price=float(assoc.price),
    #         autopart=autopart
    #     )
    #     autoparts_response.append(autopart_in_pricelist)
    #
    # response = CustomerPriceListResponse(
    #     id=customer_pricelist.id,
    #     date=customer_pricelist.date,
    #     customer_id=customer_id,
    #     autoparts=autoparts_response
    # )
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        logger.error(f'Customer with id {customer_id} not found.')
        raise HTTPException(status_code=404, detail='Customer not found')

    response = await process_customer_pricelist(
        customer=customer, request=request, session=session
    )
    return response


@router.get(
    '/customers/{customer_id}/pricelists/',
    tags=['customers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Get all pricelists for a customer',
    response_model=List[CustomerAllPriceListResponse],
)
async def get_customer_pricelists(
    customer_id: int, session: AsyncSession = Depends(get_session)
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    pricelists = await crud_customer_pricelist.get_all_pricelist(
        session=session, customer_id=customer_id
    )

    if not pricelists:
        raise HTTPException(
            status_code=404, detail='No pricelists found for the customer'
        )

    response = []
    for pricelist in pricelists:
        items = []
        for assoc in pricelist.autopart_associations:
            autopart = AutoPartResponse.model_validate(assoc.autopart)

            item = CustomerPriceListItem(
                autopart=autopart,
                quantity=assoc.quantity,
                price=float(assoc.price),
            )
            items.append(item)

        pricelist_response = CustomerAllPriceListResponse(
            id=pricelist.id,
            date=pricelist.date,
            customer_id=pricelist.customer_id,
            items=items,
        )
        response.append(pricelist_response)
    return response


@router.delete(
    '/customers/{customer_id}/pricelists/{pricelist_id}',
    tags=['customers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Delete all pricelists for a customer',
)
async def delete_customer_pricelists(
    customer_id: int,
    pricelist_id: int,
    session: AsyncSession = Depends(get_session),
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id, session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')

    pricelist = await crud_customer_pricelist.get_by_id(
        session=session, customer_id=customer_id, pricelist_id=pricelist_id
    )

    if not pricelist:
        raise HTTPException(
            status_code=404, detail='No pricelist found for the customer'
        )
    await session.delete(pricelist)
    await session.commit()
    return {
        'detail': (
            f'Deleted {pricelist_id} pricelist for customer {customer_id}'
        )
    }


@router.post(
    '/providers/{provider_id}/download',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Download pricelist from email',
)
async def download_provider_pricelist(
    provider_id: int,
    provider_price_config_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        provider_list_config = await crud_provider_pricelist_config.get_by_id(
            config_id=provider_price_config_id, session=session
        )
        provider = await crud_provider.get_by_id(
            provider_id=provider_id, session=session
        )
        filepath = await download_price_provider(
            provider=provider,
            provider_conf=provider_list_config,
            session=session,
        )
        if not filepath:
            raise HTTPException(
                status_code=404, detail='No price list file downloaded'
            )
        file_extension = filepath.split('.')[-1].lower()
        with open(filepath, "rb") as f:
            file_content = f.read()

        _, stats = await process_provider_pricelist(
            provider=provider,
            file_content=file_content,
            file_extension=file_extension,
            provider_list_conf=provider_list_config,
            use_stored_params=True,
            start_row=None,
            oem_col=None,
            brand_col=None,
            name_col=None,
            qty_col=None,
            price_col=None,
            session=session,
            return_stats=True,
        )
        return {
            'detail': (
                f'Downloaded and processed '
                f'provider price list for provider_id: {provider_id}'
            ),
            'stats': stats,
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(
            f'Error during download and processing of provider price list {e}'
        )
        raise HTTPException(
            status_code=500, detail='Error during download and processing'
        )


@router.get(
    '/providers/{provider_id}/popularity/',
    tags=['providers', 'analytic'],
    status_code=status.HTTP_200_OK,
    summary='Get analytic autoparts for provider',
)
async def get_autopart_popularity(
    background_tasks: BackgroundTasks,
    provider_id: Optional[int],
    date_start: Optional[str] = Query(
        default=None, description='Start date in format YYYY-MM-DD'
    ),
    date_finish: Optional[str] = Query(
        default=None, description='End date in format YYYY-MM-DD'
    ),
    session: AsyncSession = Depends(get_session),
):
    start_dt, finish_dt = check_start_and_finish_date(date_start, date_finish)

    df = await analyze_autopart_popularity(
        provider_id=provider_id,
        session=session,
        date_start=start_dt,
        date_finish=finish_dt,
    )
    excel_file = create_autopart_analysis_excel(df=df)

    background_tasks.add_task(
        send_email_with_attachment,
        to_email=EMAIL_NAME_ANALYTIC,
        subject=SUBJECT_MAIL_ANALYTIC_PRICE_PROVIDER,
        body=BODY_MAIL_ANALYTIC_PRICE_PROVIDER,
        attachment_filename=FILENAME_EXCEL_MAIL_ANALYTIC_PRICE_PROVIDER,
        attachment_bytes=excel_file.getvalue(),
    )

    return {'message': f'Отчёт отправлен на почту {EMAIL_NAME_ANALYTIC}'}


@router.post(
    '/providers/{provider_id}/abbreviations',
    tags=['providers', 'abbreviations'],
    response_model=ProviderAbbreviationOut,
    status_code=status.HTTP_201_CREATED,
    summary='Добавить аббревиатуру',
)
async def add_abbreviation(
    provider_id: int,
    abbreviation_name: str = Body(..., embed=True),
    session: AsyncSession = Depends(get_session),
):
    abbr = await crud_provider_abbreviation.add_abbreviation(
        session=session,
        provider_id=provider_id,
        abbreviation=abbreviation_name,
    )
    return abbr


@router.patch(
    '/providers/{provider_id}/abbreviations/{abbr_id}',
    tags=['providers', 'abbreviations'],
    response_model=ProviderAbbreviationOut,
    status_code=status.HTTP_200_OK,
    summary='Обновить аббревиатуру',
)
async def update_abbreviation(
    provider_id: int,
    abbr_id: int,
    new_abbreviation: str = Body(..., embed=True),
    session: AsyncSession = Depends(get_session),
):
    provider = crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if provider is None:
        raise HTTPException(status_code=404, detail='Provider not found')
    abbr = await crud_provider_abbreviation.update_abbreviation(
        session=session,
        abbreviation_id=abbr_id,
        new_abbreviation=new_abbreviation,
    )
    if provider_id != abbr.provider_id:
        raise HTTPException(
            status_code=404, detail='Provider have not this abbreviation'
        )
    return abbr


@router.delete(
    '/providers/{provider_id}/abbreviations/{abbr_id}',
    tags=['providers', 'abbreviations'],
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Обновить аббревиатуру',
)
async def delete_abbreviation(
    provider_id: int,
    abbr_id: int,
    session: AsyncSession = Depends(get_session),
):
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if provider is None:
        raise HTTPException(status_code=404, detail='Provider not found')
    await crud_provider_abbreviation.delete_abbreviation(
        session=session, abbreviation_id=abbr_id
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    '/providers/{provider_id}/pricelist-config/{config_id}/',
    tags=['providers', 'pricelist-config'],
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Delete price list configuration',
)
async def delete_provider_pricelist_config(
    provider_id: int,
    config_id: int,
    session: AsyncSession = Depends(get_session),
):
    '''Удалить конфигурацию прайс-листа поставщика'''
    provider = await crud_provider.get_by_id(
        provider_id=provider_id, session=session
    )
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail='Provider not found'
        )
    config = await crud_provider_pricelist_config.get_by_id(
        config_id=config_id, session=session
    )
    if not config or config.provider_id != provider_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Configuration not found for this provider',
        )
    await session.delete(config)
    await session.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
