import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from dz_fastapi.schemas.partner import (
    ProviderCreate,
    ProviderUpdate,
    ProviderResponse,
    PriceListResponse,
    PriceListCreate,
    CustomerResponse,
    CustomerCreate,
    CustomerUpdate
)
from dz_fastapi.models.partner import Provider, PriceList
from dz_fastapi.crud.partner import (
    crud_pricelist,
    crud_provider,
    crud_customer
)
from dz_fastapi.core.db import get_session
from dz_fastapi.api.validators import change_brand_name

logger = logging.getLogger('dz_fastapi')

router = APIRouter()


@router.post(
    '/providers/',
    tags=['provider'],
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
            detail=f"Provider with name '{provider_in.name}' already exists."
        )

    provider = await crud_provider.create(
        obj_in=provider_in,
        session=session
    )
    return ProviderResponse.model_validate(provider)


@router.get(
    '/providers/',
    tags=['provider'],
    status_code=status.HTTP_200_OK,
    summary='Список поставщиков',
    response_model=List[ProviderResponse]
)
async def get_all_providers(
        session: AsyncSession = Depends(get_session)
):
    providers = await crud_provider.get_multi(session=session)
    return [ProviderResponse.model_validate(provider) for provider in providers]


@router.get(
    '/providers/{provider_id}/',
    tags=['provider'],
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
    tags=['provider'],
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
    tags=['provider'],
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
    tags=['customer'],
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
    tags=['customer'],
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
    tags=['customer'],
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
    tags=['customer'],
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
    tags=['customer'],
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


# @router.get('/pricelists/{pricelist_id}', response_model=PriceListResponse)
# async def get_pricelist(pricelist_id: int, session: AsyncSession = Depends(get_session)):
#     result = await session.execute(
#         select(PriceList).where(PriceList.id == pricelist_id).options(
#             selectinload(PriceList.autoparts).joinedload(PriceListAutoPartAssociation.autopart)
#         )
#     )
#     pricelist = result.scalar_one_or_none()
#     if pricelist is None:
#         raise HTTPException(status_code=404, detail="PriceList not found")
#     return pricelist


@router.post("/pricelists/", response_model=PriceListResponse)
async def create_pricelist(
    pricelist_in: PriceListCreate,
    session: AsyncSession = Depends(get_session),
):
    """
    Create a new PriceList.

    Args:
        pricelist_in (PriceListCreate): The PriceList data to create.
        session (AsyncSession): The database session.

    Returns:
        PriceListResponse: The created PriceList.
    """
    try:
        pricelist = await crud_pricelist.create(obj_in=pricelist_in, session=session)
        return pricelist
    except HTTPException as e:
        raise e
    except Exception as e:
        # Log the error
        logger.error(f"Unexpected error occurred while creating PriceList: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error occurred during PriceList creation")