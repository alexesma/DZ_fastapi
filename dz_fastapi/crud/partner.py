from typing import Any, Dict, Optional, Union, List
from fastapi import HTTPException
from sqlalchemy import insert
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import (
    Provider,
    Customer,
    PriceList,
    CustomerPriceList,
    price_list_autopart_association,
    customer_price_list_autopart_association,
)
from dz_fastapi.schemas.partner import (
    ProviderCreate,
    ProviderUpdate,
    CustomerCreate,
    CustomerUpdate,
    PriceListCreate,
    PriceListUpdate,
    CustomerPriceListCreate,
    CustomerPriceListUpdate,
)
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import logging

logger = logging.getLogger('dz_fastapi')


class CRUDProvider(CRUDBase[Provider, ProviderCreate, ProviderUpdate]):
    async def get_provider_or_none(
            self,
            provider: str,
            session: AsyncSession
    ) -> Optional[Provider]:
        result = await session.execute(
            select(Provider).where(Provider.name == provider)
        )
        provider = result.scalar_one_or_none()
        return provider


    async def get_by_id(
            self,
            provider_id: int,
            session: AsyncSession
    ) -> Optional[Provider]:
        result = await session.execute(
            select(Provider)
            .options(selectinload(Provider.price_lists))
            .where(Provider.id == provider_id)
        )
        provider = result.scalar_one_or_none()
        return provider


    async def get_by_email_contact(
            self,
            session: AsyncSession,
            email: str
    ) -> Optional[Provider]:
        result = await session.execute(
            select(self.model).where(self.model.email_contact == email)
        )
        return result.scalars().first()

    async def create(
            self,
            obj_in: ProviderCreate,
            session: AsyncSession,
            **kwargs
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
            self,
            session: AsyncSession,
            *,
            skip: int = 0,
            limit: int = 100
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
            self,
            session: AsyncSession,
            email: str
    ) -> Optional[Customer]:
        result = await session.execute(
            select(self.model).where(self.model.email_contact == email)
        )
        return result.scalars().first()

    async def get_customer_or_none(
            self,
            customer: str,
            session: AsyncSession
    ) -> Optional[Customer]:
        result = await session.execute(
            select(Customer).where(Customer.name == customer)
        )
        return result.scalar_one_or_none()

    async def get_by_id(
            self,
            customer_id: int,
            session: AsyncSession
    ) -> Optional[Customer]:
        result = await session.execute(
            select(Customer)
            .options(selectinload(Customer.customer_price_lists))
            .where(Customer.id == customer_id)
        )
        return result.scalar_one_or_none()

    async def create(
            self,
            obj_in: CustomerCreate,
            session: AsyncSession,
            **kwargs
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
            self,
            session: AsyncSession,
            *,
            skip: int = 0,
            limit: int = 100
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
            self,
            obj_in: PriceListCreate,
            session: AsyncSession,
            **kwargs
    ) -> PriceList:
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
                    insert(price_list_autopart_association).values(
                        pricelist_id=db_obj.id,
                        autopart_id=autopart.id,
                        quantity=quantity,
                        price=price,
                    )
                )

            await session.commit()
            await session.refresh(db_obj)
            return db_obj
        except IntegrityError as e:
            logger.error(f"Integrity error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=400, detail="Integrity error during creation")
        except SQLAlchemyError as e:
            logger.error(f"Database error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Database error during creation")
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Unexpected error during creation")

    async def update(
            self,
            db_obj: PriceList,
            obj_in: Union[PriceListUpdate, Dict[str, Any]],
            session: AsyncSession,
            **kwargs
    ) -> PriceList:
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
                    delete(price_list_autopart_association).where(
                        price_list_autopart_association.c.pricelist_id == db_obj.id
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
                            detail=f"AutoPart with id {autopart_id} not found",
                        )

                    # Insert into association table
                    await session.execute(
                        insert(price_list_autopart_association).values(
                            pricelist_id=db_obj.id,
                            autopart_id=autopart_id,
                            quantity=quantity,
                            price=price,
                        )
                    )

            await session.commit()
            await session.refresh(db_obj)
            return db_obj
        except IntegrityError as e:
            logger.error(f"Integrity error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=400, detail="Integrity error during update")
        except SQLAlchemyError as e:
            logger.error(f"Database error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Database error during update")
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Unexpected error during update")

    async def get(
        self,
        session: AsyncSession,
        obj_id: int,
    ) -> Optional[PriceList]:
        # Get the PriceList
        result = await session.execute(
            select(self.model).where(self.model.id == obj_id)
        )
        db_obj = result.scalar_one_or_none()
        if not db_obj:
            raise HTTPException(status_code=404, detail="PriceList not found")

        # Get associated AutoParts with extra fields
        result = await session.execute(
            select(
                price_list_autopart_association.c.autopart_id,
                price_list_autopart_association.c.quantity,
                price_list_autopart_association.c.price,
                AutoPart,
            ).join(AutoPart, AutoPart.id == price_list_autopart_association.c.autopart_id
            ).where(price_list_autopart_association.c.pricelist_id == obj_id)
        )
        autoparts = result.all()
        # Attach autoparts data to db_obj for response
        db_obj.autoparts = autoparts
        return db_obj


crud_pricelist = CRUDPriceList(PriceList)


class CRUDCustomerPriceList(
    CRUDBase[CustomerPriceList, CustomerPriceListCreate, CustomerPriceListUpdate]
):
    async def create(
        self,
        obj_in: CustomerPriceListCreate,
        session: AsyncSession,
    ) -> CustomerPriceList:
        try:
            obj_in_data = obj_in.model_dump()
            autoparts_data = obj_in_data.pop('autoparts', [])
            db_obj = self.model(**obj_in_data)
            session.add(db_obj)
            await session.flush()  # To get db_obj.id

            # Insert associations into the association table
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
                        detail=f"AutoPart with id {autopart_id} not found",
                    )

                # Insert into association table
                await session.execute(
                    insert(customer_price_list_autopart_association).values(
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
            logger.error(f"Integrity error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=400, detail="Integrity error during creation")
        except SQLAlchemyError as e:
            logger.error(f"Database error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Database error during creation")
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Unexpected error during creation")

    async def update(
        self,
        db_obj: CustomerPriceList,
        obj_in: Union[CustomerPriceListUpdate, Dict[str, Any]],
        session: AsyncSession,
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
                    delete(customer_price_list_autopart_association).where(
                        customer_price_list_autopart_association.c.customerpricelist_id == db_obj.id
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
                            detail=f"AutoPart with id {autopart_id} not found",
                        )

                    # Insert into association table
                    await session.execute(
                        insert(customer_price_list_autopart_association).values(
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
            logger.error(f"Integrity error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=400, detail="Integrity error during update")
        except SQLAlchemyError as e:
            logger.error(f"Database error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Database error during update")
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await session.rollback()
            raise HTTPException(status_code=500, detail="Unexpected error during update")

    async def get(
        self,
        session: AsyncSession,
        obj_id: int,
    ) -> Optional[CustomerPriceList]:
        # Get the CustomerPriceList
        result = await session.execute(
            select(self.model).where(self.model.id == obj_id)
        )
        db_obj = result.scalar_one_or_none()
        if not db_obj:
            raise HTTPException(status_code=404, detail="CustomerPriceList not found")

        # Get associated AutoParts with extra fields
        result = await session.execute(
            select(
                customer_price_list_autopart_association.c.autopart_id,
                customer_price_list_autopart_association.c.quantity,
                customer_price_list_autopart_association.c.price,
                AutoPart,
            ).join(AutoPart, AutoPart.id == customer_price_list_autopart_association.c.autopart_id
            ).where(customer_price_list_autopart_association.c.customerpricelist_id == obj_id)
        )
        autoparts = result.all()
        # Attach autoparts data to db_obj for response
        db_obj.autoparts = autoparts
        return db_obj


crud_customer_pricelist = CRUDCustomerPriceList(CustomerPriceList)
