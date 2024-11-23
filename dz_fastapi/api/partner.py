import logging
from datetime import date
from typing import List, Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Body,
    UploadFile,
    File,
    Query,
    Form
)
import io
import pandas as pd
from httpx import Response
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.schemas.autopart import AutoPartCreatePriceList, AutoPartPricelist, AutoPartResponse
from dz_fastapi.schemas.partner import (
    ProviderCreate,
    ProviderUpdate,
    ProviderResponse,
    PriceListResponse,
    PriceListCreate,
    CustomerResponse,
    CustomerCreate,
    CustomerUpdate,
    PriceListAutoPartAssociationCreate,
    ProviderPriceListConfigResponse,
    PriceListDeleteRequest,
    ProviderPriceListConfigCreate,
    PriceListUpdate,
    PriceListSummary,
    PriceListPaginationResponse,
    CustomerPriceListConfigResponse,
    CustomerPriceListConfigUpdate,
    CustomerPriceListConfigCreate,
    CustomerPriceListResponse,
    CustomerPriceListCreate,
    AutoPartInPricelist,
    CustomerPriceListItem,
    CustomerAllPriceListResponse,
)
from dz_fastapi.models.partner import (
    PriceList,
    CustomerPriceListConfig,
    PriceListAutoPartAssociation,
    CustomerPriceListAutoPartAssociation,
    CustomerPriceList
)
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.crud.partner import (
    crud_pricelist,
    crud_provider,
    crud_customer,
    crud_provider_pricelist_config,
    crud_customer_pricelist_config,
    crud_customer_pricelist
)
from dz_fastapi.core.db import get_session
from dz_fastapi.api.validators import change_brand_name

logger = logging.getLogger('dz_fastapi')

router = APIRouter()


@router.post(
    '/providers/',
    tags=['providers'],
    status_code=status.HTTP_201_CREATED,
    summary='Создание поставщика',
    response_model=ProviderResponse
)
async def create_provider(
        provider_in: ProviderCreate,
        session: AsyncSession = Depends(get_session)
):
    provider_in.name = await change_brand_name(brand_name=provider_in.name)
    existing_provider = await crud_provider.get_provider_or_none(
        provider=provider_in.name,
        session=session
    )
    if existing_provider:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Provider with name {provider_in.name} already exists.'
        )

    provider = await crud_provider.create(
        obj_in=provider_in,
        session=session
    )
    return ProviderResponse.model_validate(provider)


@router.get(
    '/providers/',
    tags=['providers'],
    status_code=status.HTTP_200_OK,
    summary='Список поставщиков',
    response_model=List[ProviderResponse]
)
async def get_all_providers(
        session: AsyncSession = Depends(get_session)
):
    providers = await crud_provider.get_multi(session=session)
    return [
        ProviderResponse.model_validate(provider) for provider in providers
    ]


@router.get(
    '/providers/{provider_id}/',
    tags=['providers'],
    status_code=status.HTTP_200_OK,
    summary='Покупатель по id',
    response_model=ProviderResponse
)
async def get_provider(
        provider_id: int,
        session: AsyncSession = Depends(get_session)
):
    provider = await crud_provider.get_by_id(
        provider_id=provider_id,
        session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail='Provider not found')
    return ProviderResponse.model_validate(provider)


@router.delete(
    '/providers/{provider_id}/',
    tags=['providers'],
    summary='Удаление поставщика',
    status_code=status.HTTP_200_OK,
    response_model=ProviderResponse
)
async def delete_provider(
        provider_id: int,
        session: AsyncSession = Depends(get_session)
):
    provider = await crud_provider.get_by_id(
        provider_id=provider_id,
        session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    return await crud_provider.remove(provider, session, commit=True)


@router.patch(
    '/providers/{provider_id}/',
    tags=['providers'],
    summary='Обновление поставщика',
    status_code=status.HTTP_200_OK,
    response_model=ProviderResponse
)
async def update_provider(
        provider_id: int,
        provider_in: ProviderUpdate = Body(...),
        session: AsyncSession = Depends(get_session)
):
    provider_db = await crud_provider.get_by_id(
        provider_id=provider_id,
        session=session
    )
    if not provider_db:
        raise HTTPException(status_code=404, detail="Provider not found")

    update_data = provider_in.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(
            status_code=404,
            detail='No data provider to update.'
        )

    updated_provider = await crud_provider.update(
        db_obj=provider_db,
        obj_in=update_data,
        session=session
    )
    return ProviderResponse.model_validate(updated_provider)


@router.post(
    '/customers/',
    tags=['customers'],
    status_code=status.HTTP_201_CREATED,
    summary='Создание покупателя',
    response_model=CustomerResponse
)
async def create_customer(
        customer_in: CustomerCreate,
        session: AsyncSession = Depends(get_session)
):
    customer_in.name = await change_brand_name(brand_name=customer_in.name)
    existing_customer = await crud_customer.get_customer_or_none(
        customer=customer_in.name,
        session=session
    )
    if existing_customer:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Customer with name '{customer_in.name}' already exists."
        )

    customer = await crud_customer.create(
        obj_in=customer_in,
        session=session
    )
    return CustomerResponse.model_validate(customer)


@router.get(
    '/customers/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Список покупатель',
    response_model=List[CustomerResponse]
)
async def get_all_customer(
        session: AsyncSession = Depends(get_session)
):
    customers = await crud_customer.get_multi(session=session)
    return [CustomerResponse.model_validate(customer) for customer in customers]


@router.get(
    '/customers/{customer_id}/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Покупатель по id',
    response_model=CustomerResponse
)
async def get_customer(
        customer_id: int,
        session: AsyncSession = Depends(get_session)
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail='Customer not found')
    return CustomerResponse.model_validate(customer)


@router.delete(
    '/customers/{customer_id}/',
    tags=['customers'],
    summary='Удаление покупателя',
    status_code=status.HTTP_200_OK,
    response_model=CustomerResponse
)
async def delete_customer(
        customer_id: int,
        session: AsyncSession = Depends(get_session)
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    return await crud_customer.remove(customer, session, commit=True)


@router.patch(
    '/customers/{customer_id}/',
    tags=['customers'],
    summary='Обновление покупателя',
    status_code=status.HTTP_200_OK,
    response_model=CustomerResponse
)
async def update_customer(
        customer_id: int,
        customer_in: CustomerUpdate = Body(...),
        session: AsyncSession = Depends(get_session)
):
    customer_db = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer_db:
        raise HTTPException(status_code=404, detail="Customer not found")

    update_data = customer_in.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(
            status_code=404,
            detail='No data customer to update.'
        )

    updated_customer = await crud_customer.update(
        db_obj=customer_db,
        obj_in=update_data,
        session=session
    )
    return CustomerResponse.model_validate(updated_customer)


@router.post(
    '/providers/{provider_id}/pricelist-config/',
    tags=['providers'],
    status_code=status.HTTP_201_CREATED,
    summary='Create or update price list parsing parameters for a provider',
    response_model=ProviderPriceListConfigResponse
)
async def set_provider_pricelist_config(
        provider_id: int,
        config_in: ProviderPriceListConfigCreate,
        session: AsyncSession = Depends(get_session)
):
    # Check if the provider exists
    provider = await crud_provider.get_by_id(
        provider_id=provider_id,
        session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    # Check if a config already exists
    existing_config = await crud_provider_pricelist_config.get_config_or_none(
        provider_id=provider_id,
        session=session
    )

    if existing_config:
        # Update existing config using the new update method
        updated_config = await crud_provider_pricelist_config.update(
            db_obj=existing_config,
            obj_in=config_in,
            session=session
        )
        return ProviderPriceListConfigResponse.model_validate(updated_config)
    else:
        # Create new config
        new_config = await crud_provider_pricelist_config.create(
            provider_id=provider_id,
            config_in=config_in,
            session=session
        )
        return ProviderPriceListConfigResponse.model_validate(new_config)


@router.post(
    '/providers/{provider_id}/pricelists/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_201_CREATED,
    summary='Create provider\'s pricelist',
    response_model=PriceListResponse
)
async def create_provider_pricelist(
        provider_id: int,
        pricelist_in_base: PriceListUpdate,
        session: AsyncSession = Depends(get_session)
):
    pricelist_in = PriceListCreate(
        **pricelist_in_base.model_dump(exclude_unset=True),
        provider_id=provider_id
    )
    try:
        #Get id provider
        provider = await crud_provider.get_by_id(
            provider_id=provider_id,
            session=session
        )
        if not provider:
            raise HTTPException(status_code=404, detail="Provider not found")

        pricelist = await crud_pricelist.create(
            obj_in=pricelist_in,
            session=session
        )
        return PriceListResponse.model_validate(pricelist)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(
            f'Unexpected error occurred while creating PriceList: {e}'
        )
        raise HTTPException(
            status_code=500,
            detail='Unexpected error during PriceList creation'
        )

@router.post(
    '/providers/{provider_id}/pricelists/upload/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_201_CREATED,
    summary='Upload and create price list from file',
    response_model=PriceListResponse
)
async def upload_provider_pricelist(
        provider_id: int,
        file: UploadFile = File(...),
        use_stored_params: bool = Form(True),
        start_row: Optional[int] = Form(None, description="Row number where data starts (0-indexed)"),
        oem_col: Optional[int] = Form(None, description="Column number for OEM number (0-indexed)"),
        brand_col: Optional[int] = Form(None, description="Column number for brand (0-indexed)"),
        name_col: Optional[int] = Form(None, description="Column number for brand (0-indexed)"),
        qty_col: Optional[int] = Form(None, description="Column number for quantity (0-indexed)"),
        price_col: Optional[int] = Form(None, description="Column number for price (0-indexed)"),
        session: AsyncSession = Depends(get_session)
):
    # Check if the provider exists
    provider = await crud_provider.get_by_id(
        provider_id=provider_id,
        session=session
    )
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    # Fetch stored parameters if required
    if use_stored_params:
        # Check if a config already exists
        existing_config = await crud_provider_pricelist_config.get_config_or_none(
            provider_id=provider_id,
            session=session
        )
        if not existing_config:
            raise HTTPException(
                status_code=400,
                detail='No stored parameters found for this provider.'
            )

        # Use stored parameters
        start_row = existing_config.start_row
        oem_col = existing_config.oem_col
        brand_col = existing_config.brand_col
        name_col = existing_config.name_col
        qty_col = existing_config.qty_col
        price_col = existing_config.price_col
    else:
        # Validate that all necessary parameters are provided
        if None in (start_row, oem_col, qty_col, price_col):
            raise HTTPException(
                status_code=400,
                detail='Missing required parameters.'
            )


    # Read the file content
    content = await file.read()
    file_extension = file.filename.split('.')[-1].lower()

    # Load the file into a DataFrame
    if file_extension in ['xlsx', 'xls']:
        try:
            df = pd.read_excel(io.BytesIO(content), header=None)
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise HTTPException(status_code=400, detail='Invalid Excel file.')
    elif file_extension == 'csv':
        try:
            df = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise HTTPException(status_code=400, detail='Invalid CSV file.')
    else:
        raise HTTPException(
            status_code=400,
            detail='Unsupported file type'
        )

    # Process the DataFrame
    try:
        data_df = df.iloc[start_row:]
        required_columns = {
            'oem_number': oem_col,
            'brand': brand_col,
            'name': name_col,
            'quantity': qty_col,
            'price': price_col
        }
        required_columns = {k: v for k, v in required_columns.items() if v is not None}

        data_df = data_df.loc[:, list(required_columns.values())]
        data_df.columns = list(required_columns.keys())
    except KeyError as e:
        raise HTTPException(
            status_code=400,
            detail=f'Invalid column indices provided: {e}'
        )

    # Data cleaning and type conversion
    try:
        data_df.dropna(subset=['oem_number', 'quantity', 'price'], inplace=True)
        data_df['oem_number'] = data_df['oem_number'].astype(str).str.strip()
        if 'name' in data_df.columns:
            data_df['name'] = data_df['name'].astype(str).str.strip()
        if 'brand' in data_df.columns:
            data_df['brand'] = data_df['brand'].astype(str).str.strip()
        data_df['quantity'] = pd.to_numeric(data_df['quantity'], errors='coerce')
        data_df['price'] = pd.to_numeric(data_df['price'], errors='coerce')
        data_df.dropna(subset=['quantity', 'price'], inplace=True)
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}")
        raise HTTPException(
            status_code=400,
            detail='Error during data cleaning.'
        )

    # Convert DataFrame to list of dictionaries
    autoparts_data = data_df.to_dict(orient='records')

    # Prepare PriceListCreate object
    pricelist_in = PriceListCreate(
        provider_id=provider_id,
        autoparts=[]
    )

    for item in autoparts_data:
        # Log the item for debugging
        logger.debug(f"Processing item: {item}")

        # Create the AutoPartPricelist instance with correct field names
        try:
            autopart_data = AutoPartCreatePriceList(
                oem_number=item['oem_number'],
                brand=item.get('brand'),
                name=item.get('name')
            )
            logger.debug(f'Created AutoPartCreatePriceList: {autopart_data}')
        except KeyError as ke:
            logger.error(f"Missing key in item: {ke}")
            raise HTTPException(status_code=400, detail=f"Missing key in item: {ke}")

        autopart_assoc = PriceListAutoPartAssociationCreate(
            autopart=autopart_data,
            quantity=int(item['quantity']),
            price=float(item['price'])
        )
        pricelist_in.autoparts.append(autopart_assoc)

    # Create the price list
    try:
        pricelist = await crud_pricelist.create(
            obj_in=pricelist_in,
            session=session
        )
        return PriceListResponse.model_validate(pricelist)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(
            f'Unexpected error occurred while creating PriceList: {e}'
                     )
        raise HTTPException(
            status_code=500,
            detail='Unexpected error during PriceList creation'
                   )



@router.get(
    '/providers/{provider_id}/pricelists/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Получить список прайс-листов для поставщика',
    response_model=PriceListPaginationResponse
)
async def get_provider_pricelists(
        provider_id: int,
        skip: int = Query(0, ge=0, description='Сколько записей пропустить'),
        limit: int = Query(2, ge=1, description='Максимальное количество записей для возврата'),
        session: AsyncSession = Depends(get_session)
):
    try:
        # Проверяем существование поставщика
        provider = await crud_provider.get_by_id(
            provider_id=provider_id,
            session=session
        )
        if not provider:
            raise HTTPException(status_code=404, detail="Поставщик не найден")

        # Получаем общее количество прайс-листов
        total_count_stmt = select(func.count(PriceList.id)).where(
            PriceList.provider_id == provider_id
        )
        total_result = await session.execute(total_count_stmt)
        total_count = total_result.scalar_one()

        if skip >= total_count:
            return PriceListPaginationResponse(
                total_count=total_count,
                skip=skip,
                limit=limit,
                pricelists=[]
            )

        # Создаем подзапрос для пагинации
        pricelist_subquery = select(
            PriceList.id.label('id'),
            PriceList.date.label('date')
        ).where(
            PriceList.provider_id == provider_id
        ).order_by(
            PriceList.date.desc()
        ).offset(skip).limit(limit).subquery()

        # Основной запрос с агрегированием
        stmt = select(
            pricelist_subquery.c.id,
            pricelist_subquery.c.date,
            func.count(
                PriceListAutoPartAssociation.autopart_id
            ).label('num_positions')
        ).outerjoin(
            PriceListAutoPartAssociation,
            PriceListAutoPartAssociation.pricelist_id == pricelist_subquery.c.id
        ).group_by(
            pricelist_subquery.c.id,
            pricelist_subquery.c.date
        ).order_by(
            pricelist_subquery.c.date.desc()
        )

        result = await session.execute(stmt)
        pricelists = result.all()

        # Формируем список прайс-листов для ответа
        pricelist_summaries = [
            PriceListSummary(
                id=row.id,
                date=row.date,
                num_positions=row.num_positions
            ) for row in pricelists
        ]

        return PriceListPaginationResponse(
            total_count=total_count,
            skip=skip,
            limit=limit,
            pricelists=pricelist_summaries
        )
    except Exception as e:
        logger.error(
            f'Ошибка при получении прайс-листов: {e}'
        )
        raise HTTPException(
            status_code=500,
            detail='Внутренняя ошибка сервера'
        )


@router.delete(
    '/providers/{provider_id}/pricelists/',
    tags=['providers', 'pricelists'],
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Delete multiple price lists for a provider'
)
async def delete_provider_pricelists(
        provider_id: int,
        request: PriceListDeleteRequest,
        session: AsyncSession = Depends(get_session)
):
    try:
        # Проверяем существование поставщика
        provider = await crud_provider.get_by_id(
            provider_id=provider_id,
            session=session
        )
        if not provider:
            raise HTTPException(status_code=404, detail='Provider not found')

        pricelist_ids = request.pricelist_ids

        if not pricelist_ids:
            raise HTTPException(status_code=400, detail='No PriceList IDs provided')

        # Получаем прайс-листы, которые нужно удалить, и проверяем принадлежность поставщику
        stmt = select(PriceList).where(
            PriceList.id.in_(pricelist_ids),
            PriceList.provider_id == provider_id
        )
        result = await session.execute(stmt)
        pricelists_to_delete = result.scalars().all()

        if not pricelists_to_delete:
            raise HTTPException(status_code=404, detail="No PriceLists found for deletion")

        # Удаляем прайс-листы
        for pricelist in pricelists_to_delete:
            await session.delete(pricelist)

        await session.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except HTTPException as e:
        raise e
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred while deleting PriceLists: {e}")
        await session.rollback()
        raise HTTPException(
            status_code=500,
            detail="Database error during PriceList deletion"
        )
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting PriceLists: {e}")
        await session.rollback()
        raise HTTPException(
            status_code=500,
            detail="Unexpected error during PriceList deletion"
        )


@router.post(
    '/customers/{customer_id}/pricelist-configs/',
    tags=['customers'],
    status_code=status.HTTP_201_CREATED,
    summary='Create a pricelist configuration for a customer',
    response_model=CustomerPriceListConfigResponse
)
async def create_customer_pricelist_config(
    customer_id: int,
    config_in: CustomerPriceListConfigCreate,
    session: AsyncSession = Depends(get_session)
):
    # Check if the customer exists
    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        raise HTTPException(
            status_code=404,
            detail='Customer not found'
        )
    # Проверяем, есть ли уже конфигурация с таким именем
    existing_config = await session.execute(
        select(
            CustomerPriceListConfig
        ).where(
            CustomerPriceListConfig.name == config_in.name
        )
    )
    if existing_config.scalar():
        raise HTTPException(
            status_code=400,
            detail=f'A configuration with the name {
            config_in.name
            } already exists.'
        )

    # Create new configuration
    new_config = CustomerPriceListConfig(
        customer_id=customer_id,
        **config_in.model_dump()
    )
    session.add(new_config)
    await session.commit()
    await session.refresh(new_config)
    return CustomerPriceListConfigResponse.model_validate(new_config)


@router.patch(
    '/customers/{customer_id}/pricelist-configs/{config_id}',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Update a pricelist configuration for a customer',
    response_model=CustomerPriceListConfigResponse
)
async def update_customer_pricelist_config(
    customer_id: int,
    config_id: int,
    config_in: CustomerPriceListConfigUpdate,
    session: AsyncSession = Depends(get_session)
):
    # Check if the customer exists
    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        raise HTTPException(
            status_code=404,
            detail='Customer not found'
        )

    # Retrieve existing configuration
    config = await crud_customer_pricelist_config.get_by_id(
        session=session,
        customer_id=customer.id,
        config_id=config_id
    )
    if not config or config.customer_id != customer_id:
        raise HTTPException(
            status_code=404,
            detail='Configuration not found for this customer'
        )

    update_data = config_in.model_dump(exclude_unset=True)

    # Handle nested data if necessary
    for field, value in update_data.items():
        setattr(config, field, value)

    session.add(config)
    await session.commit()
    await session.refresh(config)
    return CustomerPriceListConfigResponse.model_validate(config)


@router.get(
    '/customers/{customer_id}/pricelist-configs/',
    tags=['customers'],
    status_code=status.HTTP_200_OK,
    summary='Get all pricelist configurations for a customer',
    response_model=List[CustomerPriceListConfigResponse]
)
async def get_customer_pricelist_configs(
    customer_id: int,
    session: AsyncSession = Depends(get_session)
):
    # Retrieve configurations
    configs = await crud_customer_pricelist_config.get_by_customer_id(
        session=session,
        customer_id=customer_id
    )
    return [CustomerPriceListConfigResponse.model_validate(config) for config in configs]


@router.post(
    '/customers/{customer_id}/pricelists/',
    tags=['customers', 'pricelists'],
    status_code=status.HTTP_201_CREATED,
    summary='Create a pricelist for a customer',
    response_model=CustomerPriceListResponse
)
async def create_customer_pricelist(
        customer_id: int,
        request: CustomerPriceListCreate,
        session: AsyncSession = Depends(get_session)
):
    logger.info(
        f'Incoming request: customer_id={
        customer_id
        }, body={
        request.model_dump()
        }')

    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        logger.error(f'Customer with id {customer_id} not found.')
        raise HTTPException(
            status_code=404,
            detail='Customer not found'
        )

    config = await crud_customer_pricelist_config.get_by_id(
        config_id=request.config_id,
        customer_id=customer_id,
        session=session
    )
    if not config:
        raise HTTPException(
            status_code=400,
            detail='No pricelist configuration found for the customer'
        )
    combined_data = []

    for pricelist_id in request.items:
        associations = await crud_pricelist.fetch_pricelist_data(pricelist_id, session)
        if not associations:
            continue

        df = await crud_pricelist.transform_to_dataframe(
            associations=associations,
            session=session
        )

        df = pd.DataFrame(df)
        df = crud_customer_pricelist.apply_coefficient(df, config)
        combined_data.append(df)

    # Combine all DataFrames into one
    # final_df = pd.concat(combined_data, ignore_index=True) if combined_data else pd.DataFrame()
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)

        # Deduplicate: keep the lowest price for each autopart
        final_df = final_df.sort_values(
            by=['autopart_id', 'price']
        ).drop_duplicates(subset='autopart_id', keep='first')
    else:
        final_df = pd.DataFrame()

    # Apply exclusions
    excluded_positions = set(request.excluded_own_positions or []) | set(request.excluded_supplier_positions or [])

    if not final_df.empty:
        if excluded_positions:
            final_df = final_df[~final_df['autopart_id'].isin(excluded_positions)]
        customer_autoparts_data = final_df.to_dict('records')
    else:
        customer_autoparts_data = []

    if not customer_autoparts_data:
        raise HTTPException(
            status_code=400,
            detail='No autoparts to include in the pricelist'
        )

    customer_pricelist = CustomerPriceList(
        customer=customer,
        date=request.date or date.today(),
        is_active=True
    )
    session.add(customer_pricelist)
    await session.flush()

    associations = [
        CustomerPriceListAutoPartAssociation(
            customerpricelist_id=customer_pricelist.id,
            autopart_id=entry['autopart_id'],
            quantity=entry['quantity'],
            price=entry['price']
        ) for entry in customer_autoparts_data
    ]

    session.add_all(associations)
    await session.commit()

    response = CustomerPriceListResponse(
        id=customer_pricelist.id,
        date=customer_pricelist.date,
        customer_id=customer_id,
        autoparts=[
            AutoPartInPricelist(
                autopart_id=assoc.autopart_id,
                quantity=assoc.quantity,
                price=float(assoc.price)
            ) for assoc in associations
        ]
    )
    return response


@router.get(
    '/customers/{customer_id}/pricelists/',
    tags=['customers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Get all pricelists for a customer',
    response_model=List[CustomerAllPriceListResponse]
)
async def get_customer_pricelists(
    customer_id: int,
    session: AsyncSession = Depends(get_session)
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        raise HTTPException(
            status_code=404,
            detail='Customer not found'
        )

    pricelists = await crud_customer_pricelist.get_all_pricelist(
        session=session,
        customer_id=customer_id
    )

    if not pricelists:
        raise HTTPException(
            status_code=404,
            detail='No pricelists found for the customer'
        )

    response = []
    for pricelist in pricelists:
        items = []
        for assoc in pricelist.autopart_associations:
            autopart = AutoPartResponse.model_validate(assoc.autopart)

            item = CustomerPriceListItem(
                autopart=autopart,
                quantity=assoc.quantity,
                price=float(assoc.price)
            )
            items.append(item)

        pricelist_response = CustomerAllPriceListResponse(
            id=pricelist.id,
            date=pricelist.date,
            customer_id=pricelist.customer_id,
            items=items
        )
        response.append(pricelist_response)
    return response


@router.delete(
    '/customers/{customer_id}/pricelists/{pricelist_id}',
    tags=['customers', 'pricelists'],
    status_code=status.HTTP_200_OK,
    summary='Delete all pricelists for a customer'
)
async def delete_customer_pricelists(
        customer_id: int,
        pricelist_id: int,
        session: AsyncSession = Depends(get_session)
):
    customer = await crud_customer.get_by_id(
        customer_id=customer_id,
        session=session
    )
    if not customer:
        raise HTTPException(
            status_code=404,
            detail='Customer not found'
        )

    pricelist = await crud_customer_pricelist.get_by_id(
        session=session,
        customer_id=customer_id,
        pricelist_id=pricelist_id
    )

    if not pricelist:
        raise HTTPException(
            status_code=404,
            detail='No pricelist found for the customer'
        )

    await session.delete(pricelist)

    await session.commit()

    return {'detail': f'Deleted {pricelist_id} pricelist for customer {customer_id}'}
