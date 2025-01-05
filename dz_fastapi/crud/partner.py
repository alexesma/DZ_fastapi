import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import delete, func, insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql import and_

from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import (Customer, CustomerPriceList,
                                       CustomerPriceListAutoPartAssociation,
                                       CustomerPriceListConfig, PriceList,
                                       PriceListAutoPartAssociation, Provider,
                                       ProviderLastEmailUID,
                                       ProviderPriceListConfig)
from dz_fastapi.schemas.autopart import AutoPartPricelist
from dz_fastapi.schemas.partner import (CustomerCreate,
                                        CustomerPriceListConfigCreate,
                                        CustomerPriceListConfigUpdate,
                                        CustomerPriceListCreate,
                                        CustomerPriceListUpdate,
                                        CustomerUpdate,
                                        PriceListAutoPartAssociationResponse,
                                        PriceListCreate, PriceListResponse,
                                        PriceListUpdate, ProviderCreate,
                                        ProviderPriceListConfigCreate,
                                        ProviderPriceListConfigUpdate,
                                        ProviderUpdate)
from dz_fastapi.services.utils import (brand_filters, individual_markups,
                                       position_filters, price_intervals,
                                       supplier_quantity_filters)

logger = logging.getLogger('dz_fastapi')


class CRUDProvider(CRUDBase[Provider, ProviderCreate, ProviderUpdate]):
    async def get_provider_or_none(
        self, provider: str, session: AsyncSession
    ) -> Optional[Provider]:
        try:
            result = await session.execute(
                select(Provider).where(Provider.name == provider)
            )
            provider = result.scalar_one_or_none()
            return provider
        except Exception as e:
            logger.error(f'Ошибка в crud_provider.get_provider_or_none: {e}')
            logger.exception('Полный стек ошибки:')
            raise

    async def get_by_id(
        self, provider_id: int, session: AsyncSession
    ) -> Optional[Provider]:
        try:
            result = await session.execute(
                select(Provider)
                .options(selectinload(Provider.price_lists))
                .where(Provider.id == provider_id)
            )
            provider = result.scalar_one_or_none()
            return provider
        except Exception as e:
            logger.error(f'Ошибка в crud_provider.get_by_id: {e}')
            logger.exception('Полный стек ошибки:')
            raise

    async def get_by_email_contact(
        self, session: AsyncSession, email: str
    ) -> Optional[Provider]:
        result = await session.execute(
            select(self.model).where(self.model.email_contact == email)
        )
        return result.scalars().first()

    async def create(
        self, obj_in: ProviderCreate, session: AsyncSession, **kwargs
    ) -> Provider:
        provider = Provider(**obj_in.model_dump())
        session.add(provider)
        await session.commit()
        result = await session.execute(
            select(Provider)
            .options(selectinload(Provider.price_lists))
            .where(Provider.id == provider.id)
        )
        provider = result.scalar_one()
        return provider

    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[Provider]:
        result = await session.execute(
            select(Provider)
            .options(selectinload(Provider.price_lists))
            .offset(skip)
            .limit(limit)
        )
        providers = result.scalars().all()
        return providers


crud_provider = CRUDProvider(Provider)


class CRUDCustomer(CRUDBase[Customer, CustomerCreate, CustomerUpdate]):
    async def get_by_email_contact(
        self, session: AsyncSession, email: str
    ) -> Optional[Customer]:
        result = await session.execute(
            select(self.model).where(self.model.email_contact == email)
        )
        return result.scalars().first()

    async def get_customer_or_none(
        self, customer: str, session: AsyncSession
    ) -> Optional[Customer]:
        result = await session.execute(
            select(Customer).where(Customer.name == customer)
        )
        return result.scalar_one_or_none()

    async def get_by_id(
        self, customer_id: int, session: AsyncSession
    ) -> Optional[Customer]:
        result = await session.execute(
            select(Customer)
            .options(selectinload(Customer.customer_price_lists))
            .where(Customer.id == customer_id)
        )
        return result.scalar_one_or_none()

    async def create(
        self, obj_in: CustomerCreate, session: AsyncSession, **kwargs
    ) -> Customer:
        customer = Customer(**obj_in.model_dump())
        session.add(customer)
        await session.commit()
        result = await session.execute(
            select(Customer)
            .options(selectinload(Customer.customer_price_lists))
            .where(Customer.id == customer.id)
        )
        return result.scalar_one()

    async def get_multi(
        self, session: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[Customer]:
        result = await session.execute(
            select(Customer)
            .options(selectinload(Customer.customer_price_lists))
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


crud_customer = CRUDCustomer(Customer)


class CRUDPriceList(CRUDBase[PriceList, PriceListCreate, PriceListUpdate]):
    async def create(
        self, obj_in: PriceListCreate, session: AsyncSession, **kwargs
    ) -> PriceListResponse:
        try:
            obj_in_data = obj_in.model_dump()
            autoparts_data = obj_in_data.pop('autoparts', [])
            db_obj = self.model(**obj_in_data)
            session.add(db_obj)
            await session.flush()
            default_brand = None

            bulk_insert_data = []

            for autopart_assoc_data in autoparts_data:
                autopart_data_dict = autopart_assoc_data['autopart']
                quantity = autopart_assoc_data['quantity']
                price = autopart_assoc_data['price']

                logger.debug(f'Processing AutoPart data: {autopart_data_dict}')

                # Instantiate AutoPartPricelist
                autopart_data = AutoPartPricelist(**autopart_data_dict)

                autopart = await crud_autopart.create_autopart_from_price(
                    new_autopart=autopart_data,
                    session=session,
                    default_brand=default_brand,
                )

                if not autopart:
                    logger.warning(
                        f'Failed to create or retrieve '
                        f'AutoPart for data: {autopart_data_dict}'
                    )
                    continue

                bulk_insert_data.append(
                    {
                        'pricelist_id': db_obj.id,
                        'autopart_id': autopart.id,
                        'quantity': quantity,
                        'price': price,
                    }
                )

            # Шаг 4: Выполнение массовой вставки ассоциаций, если есть данные
            if bulk_insert_data:
                logger.debug(
                    f'Bulk inserting {len(bulk_insert_data)} associations.'
                )
                await session.execute(
                    insert(PriceListAutoPartAssociation), bulk_insert_data
                )

            await session.commit()
            await session.refresh(db_obj)

            stmt = (
                select(PriceList)
                .where(PriceList.id == db_obj.id)
                .options(
                    selectinload(PriceList.autopart_associations)
                    .selectinload(PriceListAutoPartAssociation.autopart)
                    .selectinload(AutoPart.categories),
                    selectinload(PriceList.autopart_associations)
                    .selectinload(PriceListAutoPartAssociation.autopart)
                    .selectinload(AutoPart.storage_locations),
                )
            )
            result = await session.execute(stmt)
            db_obj = result.scalar_one()
            logger.debug(f'Retrieved PriceList: {db_obj}')
            if hasattr(db_obj, 'autopart_associations'):
                for assoc in db_obj.autopart_associations:
                    logger.debug(
                        f'AutoPart Association - '
                        f'Pricelist ID: {assoc.pricelist_id}, '
                        f'Autopart ID: {assoc.autopart_id}, '
                        f'Quantity: {assoc.quantity}, '
                        f'Price: {assoc.price}'
                    )
                    if hasattr(assoc, 'autopart'):
                        logger.debug(
                            f'AutoPart - ID: {assoc.autopart.id}, '
                            f'OEM Number: {assoc.autopart.oem_number}, '
                            f'Name: {assoc.autopart.name}'
                        )

            try:
                response = PriceListResponse(
                    id=db_obj.id,
                    date=db_obj.date,
                    provider=db_obj.provider,
                    autoparts=[
                        PriceListAutoPartAssociationResponse(
                            autopart=assoc.autopart,
                            quantity=assoc.quantity,
                            price=float(assoc.price),
                        )
                        for assoc in db_obj.autopart_associations
                    ],
                )
                return response
            except ValidationError as e:
                logger.error(f'Validation error: {e.json()}')
                raise HTTPException(
                    status_code=500, detail=f'Validation error: {str(e)}'
                )
        except IntegrityError as e:
            logger.error(f'Integrity error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=400, detail='Integrity error during creation'
            )
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail='Database error during creation'
            )
        except Exception as e:
            logger.error(f'Unexpected error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail='Unexpected error during creation'
            )

    async def count_by_provider_id(
        self, provider_id: int, session: AsyncSession
    ) -> int:
        total_count_stmt = select(func.count(PriceList.id)).where(
            PriceList.provider_id == provider_id
        )
        total_result = await session.execute(total_count_stmt)
        return total_result.scalar_one()

    async def get_by_provider_paginated(
        self, provider_id: int, skip: int, limit: int, session: AsyncSession
    ):
        # Подзапрос для получения ограниченного списка прайс-листов
        pricelist_subquery = (
            select(PriceList.id.label('id'), PriceList.date.label('date'))
            .where(PriceList.provider_id == provider_id)
            .order_by(PriceList.date.desc())
            .offset(skip)
            .limit(limit)
            .subquery()
        )

        # Основной запрос с агрегацией
        stmt = (
            select(
                pricelist_subquery.c.id,
                pricelist_subquery.c.date,
                func.count(PriceListAutoPartAssociation.autopart_id).label(
                    'num_positions'
                ),
            )
            .outerjoin(
                PriceListAutoPartAssociation,
                PriceListAutoPartAssociation.pricelist_id
                == pricelist_subquery.c.id,
            )
            .group_by(pricelist_subquery.c.id, pricelist_subquery.c.date)
            .order_by(pricelist_subquery.c.date.desc())
        )

        result = await session.execute(stmt)
        return result.all()

    async def get(
        self,
        session: AsyncSession,
        obj_id: int,
    ) -> Optional[PriceList]:
        # First, attempt to fetch the PriceList with eager loading
        stmt = (
            select(PriceList)
            .where(PriceList.id == obj_id)
            .options(
                selectinload(PriceList.autopart_associations)
                .selectinload(PriceListAutoPartAssociation.autopart)
                .selectinload(AutoPart.brand),
                selectinload(PriceList.autopart_associations)
                .selectinload(PriceListAutoPartAssociation.autopart)
                .selectinload(AutoPart.categories),
                selectinload(PriceList.autopart_associations)
                .selectinload(PriceListAutoPartAssociation.autopart)
                .selectinload(AutoPart.storage_locations),
            )
        )
        result = await session.execute(stmt)
        db_obj = result.scalar_one_or_none()

        if db_obj is None:
            raise HTTPException(status_code=404, detail='PriceList not found')

        return db_obj

    async def fetch_pricelist_data(
        self, pricelist_id: int, session: AsyncSession
    ):
        result = await session.execute(
            select(PriceListAutoPartAssociation)
            .options(
                joinedload(PriceListAutoPartAssociation.autopart).joinedload(
                    AutoPart.brand
                ),
                joinedload(PriceListAutoPartAssociation.pricelist),
            )
            .where(PriceListAutoPartAssociation.pricelist_id == pricelist_id)
        )
        return result.scalars().all()

    async def transform_to_dataframe(
        self, associations, session: AsyncSession
    ):
        data = []
        for assoc in associations:
            autopart = assoc.autopart
            brand_name = autopart.brand.name if autopart.brand else None
            data.append(
                {
                    'autopart_id': autopart.id,
                    'name': autopart.name,
                    'oem_number': autopart.oem_number,
                    'brand_id': autopart.brand_id,
                    'brand': brand_name,
                    'provider_id': assoc.pricelist.provider_id,
                    'quantity': assoc.quantity,
                    'price': float(assoc.price),
                }
            )
        return pd.DataFrame(data)

    async def get_pricelist_ids_by_provider(
        self, session: AsyncSession, provider_id: int
    ) -> List[int]:
        stmt = select(PriceList.id).where(PriceList.provider_id == provider_id)
        result = await session.execute(stmt)
        rows = result.all()  # вернёт список кортежей (id,)
        pricelist_ids = [row.id for row in rows]
        return pricelist_ids


crud_pricelist = CRUDPriceList(PriceList)


class CRUDCustomerPriceList(
    CRUDBase[
        CustomerPriceList, CustomerPriceListCreate, CustomerPriceListUpdate
    ]
):
    async def create(
        self, obj_in: CustomerPriceListCreate, session: AsyncSession, **kwargs
    ) -> CustomerPriceList:
        try:
            obj_in_data = obj_in.model_dump()
            autoparts_data = obj_in_data.pop('autoparts', [])
            db_obj = self.model(**obj_in_data)
            session.add(db_obj)
            await session.flush()
            default_brand = None

            for autopart_assoc_data in autoparts_data:
                autopart_data = autopart_assoc_data.autopart
                quantity = autopart_assoc_data.quantity
                price = autopart_assoc_data.price

                autopart = await crud_autopart.create_autopart_from_price(
                    new_autopart=autopart_data,
                    session=session,
                    default_brand=default_brand,
                )

                if not autopart:
                    continue

                await session.execute(
                    insert(CustomerPriceListAutoPartAssociation).values(
                        customerpricelist_id=db_obj.id,
                        autopart_id=autopart.id,
                        quantity=quantity,
                        price=price,
                    )
                )

            await session.commit()
            await session.refresh(db_obj)
            return db_obj
        except IntegrityError as e:
            logger.error(f'Integrity error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=400, detail='Integrity error during creation'
            )
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail='Database error during creation'
            )
        except Exception as e:
            logger.error(f'Unexpected error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail='Unexpected error during creation'
            )

    async def update(
        self,
        db_obj: CustomerPriceList,
        obj_in: Union[CustomerPriceListUpdate, Dict[str, Any]],
        session: AsyncSession,
        **kwargs,
    ) -> CustomerPriceList:
        try:
            # Update basic fields
            await super().update(db_obj, obj_in, session, commit=False)

            # Update autoparts if provided
            if isinstance(obj_in, dict):
                update_data = obj_in
            else:
                update_data = obj_in.model_dump(exclude_unset=True)

            if 'autoparts' in update_data:
                autoparts_data = update_data['autoparts']

                # Delete existing associations
                await session.execute(
                    delete(CustomerPriceListAutoPartAssociation).where(
                        CustomerPriceListAutoPartAssociation.c.customerpricelist_id  # noqa для E501
                        == db_obj.id
                    )
                )

                # Insert new associations
                for autopart_assoc_data in autoparts_data:
                    autopart_id = autopart_assoc_data.autopart_id
                    quantity = autopart_assoc_data.quantity
                    price = autopart_assoc_data.price

                    # Verify that the AutoPart exists
                    result = await session.execute(
                        select(AutoPart).where(AutoPart.id == autopart_id)
                    )
                    autopart = result.scalar_one_or_none()
                    if not autopart:
                        raise HTTPException(
                            status_code=404,
                            detail=f'AutoPart with '
                            f'id {autopart_id} not found',
                        )

                    # Insert into association table
                    await session.execute(
                        insert(CustomerPriceListAutoPartAssociation).values(
                            customerpricelist_id=db_obj.id,
                            autopart_id=autopart_id,
                            quantity=quantity,
                            price=price,
                        )
                    )

            await session.commit()
            await session.refresh(db_obj)
            return db_obj
        except IntegrityError as e:
            logger.error(f'Integrity error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=400, detail='Integrity error during update'
            )
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail='Database error during update'
            )
        except Exception as e:
            logger.error(f'Unexpected error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500, detail='Unexpected error during update'
            )

    async def get(
        self,
        session: AsyncSession,
        obj_id: int,
    ) -> Optional[CustomerPriceList]:
        result = await session.execute(
            select(self.model).where(self.model.id == obj_id)
        )
        db_obj = result.scalar_one_or_none()
        if not db_obj:
            raise HTTPException(
                status_code=404, detail='CustomerPriceList not found'
            )

        # Get associated AutoParts with extra fields
        result = await session.execute(
            select(
                CustomerPriceListAutoPartAssociation.c.autopart_id,
                CustomerPriceListAutoPartAssociation.c.quantity,
                CustomerPriceListAutoPartAssociation.c.price,
                AutoPart,
            )
            .join(
                AutoPart,
                AutoPart.id
                == CustomerPriceListAutoPartAssociation.c.autopart_id,
            )
            .where(
                CustomerPriceListAutoPartAssociation.c.customerpricelist_id
                == obj_id
            )
        )
        autoparts = result.all()
        db_obj.autoparts = autoparts
        return db_obj

    def apply_coefficient(
        self,
        df: pd.DataFrame,
        config: CustomerPriceListConfig,
    ) -> pd.DataFrame:
        logger.debug(
            f'Into apply_coefficient data df:{df}, cofig: {config.__dict__}'
        )
        individualmarkups = config.individual_markups
        priceintervals = config.price_intervals
        brandfilters = config.brand_filters
        positionfilters = config.position_filters
        supplierquantityfilters = config.supplier_quantity_filters

        # Ensure 'price' column is numeric
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Apply individual markups per supplier
        if individual_markups:
            df = individual_markups(
                individual_markups=individualmarkups, df=df
            )

        # Apply price intervals with coefficients
        if priceintervals:
            df = price_intervals(price_intervals=priceintervals, df=df)

        # Apply brand filters
        if brandfilters:
            df = brand_filters(brand_filters=brandfilters, df=df)

        # Apply position filters
        if positionfilters:
            df = position_filters(position_filters=positionfilters, df=df)

        # Apply supplier quantity filters
        if supplierquantityfilters:
            df = supplier_quantity_filters(
                supplier_quantity_filters=supplierquantityfilters, df=df
            )

        # Apply general markup
        df['price'] *= config.general_markup / 100 + 1

        return df

    async def get_all_pricelist(
        self,
        session: AsyncSession,
        customer_id: int,
    ):
        result = await session.execute(
            select(CustomerPriceList)
            .options(
                selectinload(
                    CustomerPriceList.autopart_associations
                ).selectinload(CustomerPriceListAutoPartAssociation.autopart)
            )
            .where(CustomerPriceList.customer_id == customer_id)
        )
        return result.scalars().all()

    async def get_by_id(
        self,
        customer_id: int,
        pricelist_id: int,
        session: AsyncSession,
    ):
        result = await session.execute(
            select(CustomerPriceList).where(
                CustomerPriceList.customer_id == customer_id
                and CustomerPriceList.id == pricelist_id
            )
        )
        return result.scalars().first()

    async def create_associations(
        self,
        customer_pricelist_id: int,
        autoparts_data: list[dict],
        session: AsyncSession,
    ) -> list[CustomerPriceListAutoPartAssociation]:
        associations = []
        for entry in autoparts_data:
            if 'autopart_id' in entry and entry['autopart_id']:
                association = CustomerPriceListAutoPartAssociation(
                    customerpricelist_id=customer_pricelist_id,
                    autopart_id=entry['autopart_id'],
                    quantity=entry['quantity'],
                    price=entry['price'],
                )
                associations.append(association)
                session.add(association)
            else:
                # Handle items without autopart_id (log)
                logger.debug(
                    f'Skipping association '
                    f'for item without autopart_id: {entry}'
                )
        await session.commit()
        result = await session.execute(
            select(CustomerPriceListAutoPartAssociation)
            .options(
                selectinload(
                    CustomerPriceListAutoPartAssociation.autopart
                ).selectinload(AutoPart.brand)
            )
            .where(
                CustomerPriceListAutoPartAssociation.customerpricelist_id
                == customer_pricelist_id
            )
        )
        return result.scalars().all()


crud_customer_pricelist = CRUDCustomerPriceList(CustomerPriceList)


class CRUDProviderPriceList(CRUDPriceList):
    async def create(
        self, obj_in: PriceListCreate, session: AsyncSession, **kwargs
    ) -> PriceList:
        pass


crud_provider_pricelist = CRUDProviderPriceList(PriceList)


class CRUDProviderPriceListConfig(
    CRUDBase[
        ProviderPriceListConfig,
        ProviderPriceListConfigCreate,
        ProviderPriceListConfigUpdate,
    ]
):
    async def get_config_or_none(
        self, provider_id: int, session: AsyncSession, **kwargs
    ) -> Optional[ProviderPriceListConfig]:
        existing_config = await session.execute(
            select(ProviderPriceListConfig).where(
                ProviderPriceListConfig.provider_id == provider_id
            )
        )
        return existing_config.scalar_one_or_none()

    async def create(
        self,
        provider_id: int,
        config_in: ProviderPriceListConfigCreate,
        session: AsyncSession,
        **kwargs,
    ) -> ProviderPriceListConfig:
        new_config = ProviderPriceListConfig(
            provider_id=provider_id, **config_in.model_dump()
        )
        session.add(new_config)
        await session.commit()
        await session.refresh(new_config)
        return new_config

    async def update(
        self,
        db_obj: ProviderPriceListConfig,
        obj_in: Union[ProviderPriceListConfigUpdate, Dict[str, Any]],
        session: AsyncSession,
        **kwargs,
    ) -> ProviderPriceListConfig:
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        for field, value in update_data.items():
            setattr(db_obj, field, value)

        session.add(db_obj)
        await session.commit()
        await session.refresh(db_obj)
        return db_obj


crud_provider_pricelist_config = CRUDProviderPriceListConfig(
    ProviderPriceListConfig
)


class CRUDCustomerPriceListConfig(
    CRUDBase[
        CustomerPriceListConfig,
        CustomerPriceListConfigCreate,
        CustomerPriceListConfigUpdate,
    ]
):
    async def get_by_id(
        self,
        customer_id: int,
        config_id: int,
        session: AsyncSession,
    ) -> Optional[CustomerPriceListConfig]:
        result = await session.execute(
            select(CustomerPriceListConfig).where(
                and_(
                    CustomerPriceListConfig.id == config_id,
                    CustomerPriceListConfig.customer_id == customer_id,
                )
            )
        )
        return result.scalar_one_or_none()

    async def get_by_customer_id(
        self, session: AsyncSession, customer_id: int
    ) -> List[CustomerPriceListConfig]:
        result = await session.execute(
            select(CustomerPriceListConfig).where(
                CustomerPriceListConfig.customer_id == customer_id
            )
        )
        return result.scalars().all()

    async def get_by_name(
        self, customer_id: int, name: str, session: AsyncSession
    ):
        result = await session.execute(
            select(CustomerPriceListConfig).where(
                and_(
                    CustomerPriceListConfig.name == name,
                    CustomerPriceListConfig.customer_id == customer_id,
                )
            )
        )
        return result.scalar_one_or_none()

    async def create_config(
        self,
        customer_id: int,
        config_in: CustomerPriceListConfigCreate,
        session: AsyncSession,
    ) -> CustomerPriceListConfig:
        # Проверяем, есть ли уже конфигурация с таким именем
        existing_config = await self.get_by_name(
            customer_id=customer_id, name=config_in.name, session=session
        )
        if existing_config:
            raise ValueError(
                f'A configuration with '
                f'the name {config_in.name} already exists.'
            )

        # Создаем новую конфигурацию
        new_config = CustomerPriceListConfig(
            customer_id=customer_id, **config_in.model_dump()
        )
        session.add(new_config)
        await session.commit()
        await session.refresh(new_config)
        return new_config


crud_customer_pricelist_config = CRUDCustomerPriceListConfig(
    CustomerPriceListConfig
)


async def get_last_uid(provider_id: int, session: AsyncSession) -> int:
    result = await session.execute(
        select(ProviderLastEmailUID).where(
            ProviderLastEmailUID.provider_id == provider_id
        )
    )
    record = result.scalar_one_or_none()
    if record:
        return record.last_uid
    return 0


async def set_last_uid(provider_id: int, last_uid: int, session: AsyncSession):
    result = await session.execute(
        select(ProviderLastEmailUID).where(
            ProviderLastEmailUID.provider_id == provider_id
        )
    )
    record = result.scalar_one_or_none()

    if record:
        record.last_uid = last_uid
    else:
        record = ProviderLastEmailUID(
            provider_id=provider_id, last_uid=last_uid
        )
        session.add(record)

    await session.commit()
