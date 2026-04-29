import logging
from decimal import ROUND_HALF_UP, Decimal
from math import ceil
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql import and_

from dz_fastapi.api.validators import change_brand_name
from dz_fastapi.core.constants import DEFAULT_PAGE_SIZE
from dz_fastapi.core.db import AsyncSession
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.autopart import crud_autopart
from dz_fastapi.crud.base import CRUDBase
from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.models.autopart import AutoPart, AutoPartPriceHistory
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import (TYPE_PRICES, Customer,
                                       CustomerOrderItem, CustomerPriceList,
                                       CustomerPriceListAutoPartAssociation,
                                       CustomerPriceListConfig,
                                       CustomerPriceListSource, Order,
                                       PriceList, PriceListAutoPartAssociation,
                                       PriceListMissingBrand, Provider,
                                       ProviderAbbreviation,
                                       ProviderConfigLastEmailUID,
                                       ProviderExternalReference,
                                       ProviderLastEmailUID,
                                       ProviderPriceListConfig, SupplierOrder,
                                       SupplierOrderMessage, SupplierReceipt,
                                       SupplierResponseConfig)
from dz_fastapi.schemas.autopart import AutoPartPricelist
from dz_fastapi.schemas.partner import (
    CustomerCreate, CustomerPriceListConfigCreate,
    CustomerPriceListConfigUpdate, CustomerPriceListCreate,
    CustomerPriceListSourceCreate, CustomerPriceListSourceUpdate,
    CustomerPriceListUpdate, CustomerUpdate,
    PriceListAutoPartAssociationResponse, PriceListCreate, PriceListResponse,
    PriceListShort, PriceListUpdate, ProviderAbbreviationCreate,
    ProviderAbbreviationOut, ProviderAbbreviationUpdate, ProviderCoreOut,
    ProviderCreate, ProviderCustomerPriceListSourceUsageOut,
    ProviderExternalReferenceCreate, ProviderExternalReferenceOut,
    ProviderExternalReferenceUpdate, ProviderPageResponse,
    ProviderPriceListConfigCreate, ProviderPriceListConfigOut,
    ProviderPriceListConfigUpdate, ProviderUpdate,
    SupplierResponseConfigCreate, SupplierResponseConfigOut,
    SupplierResponseConfigUpdate)
from dz_fastapi.services.utils import (brand_filters, individual_markups,
                                       normalize_markup, position_filters,
                                       price_intervals,
                                       supplier_quantity_filters)

logger = logging.getLogger('dz_fastapi')


def money(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _derive_provider_is_vat_payer(
    type_prices: object,
    current_value: bool = False,
) -> bool:
    raw_value = getattr(type_prices, 'value', type_prices)
    if raw_value is None:
        return bool(current_value)
    normalized = str(raw_value).strip().lower()
    if normalized in {
        TYPE_PRICES.WHOLESALE.value.lower(),
        TYPE_PRICES.WHOLESALE.name.lower(),
    }:
        return True
    if normalized in {
        TYPE_PRICES.RETAIL.value.lower(),
        TYPE_PRICES.RETAIL.name.lower(),
        TYPE_PRICES.CASH.value.lower(),
        TYPE_PRICES.CASH.name.lower(),
    }:
        return False
    return bool(current_value)


def _build_provider_last_email_uid(
    provider: Provider,
) -> Optional[Dict[str, Any]]:
    active_configs = [
        config
        for config in (provider.pricelist_configs or [])
        if getattr(config, 'is_active', False)
    ]
    config_records = []
    for config in active_configs:
        record = getattr(config, 'last_email_uid', None)
        if record is not None:
            config_records.append(record)

    dated_records = [
        record
        for record in config_records
        if getattr(record, 'updated_at', None) is not None
    ]
    if dated_records:
        # Show the stalest active config at provider level so the list
        # highlights providers whose active pricelist flow needs attention.
        latest_record = min(
            dated_records, key=lambda record: record.updated_at
        )
        return {
            'uid': latest_record.last_uid,
            'updated_at': latest_record.updated_at,
        }

    if config_records:
        latest_record = min(
            config_records, key=lambda record: getattr(record, 'last_uid', 0)
        )
        return {
            'uid': latest_record.last_uid,
            'updated_at': getattr(latest_record, 'updated_at', None),
        }

    if active_configs:
        return None

    if provider.provider_last_uid is not None:
        return {
            'uid': provider.provider_last_uid.last_uid,
            'updated_at': provider.provider_last_uid.updated_at,
        }

    return None


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
            logger.error(
                f'Ошибка в crud_provider.get_provider_or_none: {e}',
                exc_info=True,
            )
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
            logger.error(
                f'Ошибка в crud_provider.get_by_id: {e}', exc_info=True
            )
            raise

    async def get_external_reference_by_source_supplier(
        self,
        *,
        source_system: str,
        external_supplier_id: int,
        session: AsyncSession,
    ) -> Optional[ProviderExternalReference]:
        result = await session.execute(
            select(ProviderExternalReference).where(
                ProviderExternalReference.source_system == source_system,
                ProviderExternalReference.external_supplier_id
                == external_supplier_id,
            )
        )
        return result.scalar_one_or_none()

    async def list_external_references(
        self,
        *,
        provider_id: int,
        session: AsyncSession,
    ) -> list[ProviderExternalReference]:
        rows = await session.execute(
            select(ProviderExternalReference)
            .where(ProviderExternalReference.provider_id == provider_id)
            .order_by(
                ProviderExternalReference.source_system.asc(),
                ProviderExternalReference.external_supplier_name.asc(),
                ProviderExternalReference.id.asc(),
            )
        )
        return list(rows.scalars().all())

    async def upsert_external_reference(
        self,
        *,
        provider_id: int,
        obj_in: ProviderExternalReferenceCreate,
        session: AsyncSession,
    ) -> ProviderExternalReference:
        source_system = str(obj_in.source_system or '').strip().upper()
        if not source_system:
            raise ValueError('source_system is required')
        if obj_in.external_supplier_id is None and not (
            obj_in.external_supplier_name or ''
        ).strip():
            raise ValueError(
                'external_supplier_id or external_supplier_name is required'
            )

        existing = None
        if obj_in.external_supplier_id is not None:
            existing = await self.get_external_reference_by_source_supplier(
                source_system=source_system,
                external_supplier_id=int(obj_in.external_supplier_id),
                session=session,
            )

        if existing is None:
            existing = (
                await session.execute(
                    select(ProviderExternalReference).where(
                        ProviderExternalReference.provider_id == provider_id,
                        ProviderExternalReference.source_system
                        == source_system,
                        ProviderExternalReference.external_supplier_name
                        == (
                            str(obj_in.external_supplier_name or '').strip()
                            or None
                        ),
                    )
                )
            ).scalar_one_or_none()

        if existing is None:
            existing = ProviderExternalReference(
                provider_id=provider_id,
                source_system=source_system,
            )
            session.add(existing)

        existing.provider_id = provider_id
        existing.source_system = source_system
        existing.external_supplier_id = obj_in.external_supplier_id
        existing.external_supplier_name = (
            str(obj_in.external_supplier_name or '').strip() or None
        )
        existing.is_active = bool(obj_in.is_active)
        await session.commit()
        await session.refresh(existing)
        return existing

    async def update_external_reference(
        self,
        *,
        provider_id: int,
        external_reference_id: int,
        obj_in: ProviderExternalReferenceUpdate,
        session: AsyncSession,
    ) -> Optional[ProviderExternalReference]:
        ref = (
            await session.execute(
                select(ProviderExternalReference).where(
                    ProviderExternalReference.id == external_reference_id,
                    ProviderExternalReference.provider_id == provider_id,
                )
            )
        ).scalar_one_or_none()
        if ref is None:
            return None

        if obj_in.source_system is not None:
            source_system = str(obj_in.source_system or '').strip().upper()
            if not source_system:
                raise ValueError('source_system is required')
            ref.source_system = source_system
        if obj_in.external_supplier_id is not None:
            ref.external_supplier_id = int(obj_in.external_supplier_id)
        if obj_in.external_supplier_name is not None:
            ref.external_supplier_name = (
                str(obj_in.external_supplier_name or '').strip() or None
            )
        if obj_in.is_active is not None:
            ref.is_active = bool(obj_in.is_active)

        if ref.external_supplier_id is None and not (
            str(ref.external_supplier_name or '').strip()
        ):
            raise ValueError(
                'external_supplier_id or external_supplier_name is required'
            )

        await session.commit()
        await session.refresh(ref)
        return ref

    async def delete_external_reference(
        self,
        *,
        provider_id: int,
        external_reference_id: int,
        session: AsyncSession,
    ) -> bool:
        ref = (
            await session.execute(
                select(ProviderExternalReference).where(
                    ProviderExternalReference.id == external_reference_id,
                    ProviderExternalReference.provider_id == provider_id,
                )
            )
        ).scalar_one_or_none()
        if ref is None:
            return False
        await session.delete(ref)
        await session.commit()
        return True

    async def get_full_by_id(
        self, provider_id: int, session: AsyncSession
    ) -> Optional[ProviderPageResponse]:
        '''
        Возвращает агрегированный словарь для страницы поставщика:
        :param provider_id:
        :param session:
        :return:
        - основные поля для редактирования
        - все аббревиатуры
        - все конфигурации
        - по каждой конфигурации — последний PriceList
        '''
        try:
            stmt = (
                select(Provider)
                .where(Provider.id == provider_id)
                .options(
                    selectinload(Provider.provider_last_uid),
                    selectinload(Provider.pricelist_configs).selectinload(
                        ProviderPriceListConfig.last_email_uid
                    ),
                    selectinload(Provider.supplier_response_configs)
                    .selectinload(SupplierResponseConfig.inbox_email_account),
                    selectinload(Provider.price_lists),
                    selectinload(Provider.abbreviations),
                    selectinload(Provider.external_references),
                )
            )
            result = await session.execute(stmt)
            provider: Optional[Provider] = result.scalars().first()
            if not provider:
                return None
            config_ids = [
                config.id for config in (provider.pricelist_configs or [])
            ]
            latest_by_cfg: Dict[int, PriceListShort] = {}
            if config_ids:
                max_date_stmt = (
                    select(
                        PriceList.provider_config_id,
                        func.max(PriceList.date).label('max_date'),
                    )
                    .where(
                        PriceList.provider_id == provider_id,
                        PriceList.provider_config_id.in_(config_ids),
                    )
                    .group_by(PriceList.provider_config_id)
                    .subquery()
                )

                latest_stmt = (
                    select(
                        PriceList.id,
                        PriceList.provider_config_id,
                        PriceList.date,
                        PriceList.is_active,
                    )
                    .join(
                        max_date_stmt,
                        and_(
                            PriceList.provider_config_id
                            == max_date_stmt.c.provider_config_id,
                            PriceList.date == max_date_stmt.c.max_date,
                        ),
                    )
                    .where(
                        PriceList.provider_id == provider_id,
                        PriceList.provider_config_id.in_(config_ids),
                    )
                    .order_by(PriceList.id.desc())
                )

                latest_rows = (await session.execute(latest_stmt)).all()
                seen_configs = set()
                for row in latest_rows:
                    if row.provider_config_id not in seen_configs:
                        latest_by_cfg[row.provider_config_id] = PriceListShort(
                            id=row.id, date=row.date, is_active=row.is_active
                        )
                        seen_configs.add(row.provider_config_id)
            # ---------- собрать ответ ----------
            provider_core = ProviderCoreOut.model_validate(provider)
            provider_core.last_email_uid = _build_provider_last_email_uid(
                provider
            )
            abbreviations = [
                ProviderAbbreviationOut.model_validate(abbreviation)
                for abbreviation in (provider.abbreviations or [])
            ]
            external_references = [
                ProviderExternalReferenceOut.model_validate(reference)
                for reference in (provider.external_references or [])
            ]

            pricelist_configs = []
            for config in provider.pricelist_configs or []:
                config_out = ProviderPriceListConfigOut.model_validate(config)
                config_out.latest_pricelist = latest_by_cfg.get(config.id)
                pricelist_configs.append(config_out)

            pricelist_configs.sort(
                key=lambda c: (c.name_price is None, c.name_price or '')
            )
            supplier_response_configs = [
                SupplierResponseConfigOut.model_validate(config)
                for config in (provider.supplier_response_configs or [])
            ]
            supplier_response_configs.sort(
                key=lambda c: (c.name is None, c.name or '', c.id)
            )

            source_usage_rows = (
                await session.execute(
                    select(
                        CustomerPriceListSource,
                        CustomerPriceListConfig,
                        Customer,
                        ProviderPriceListConfig,
                    )
                    .join(
                        CustomerPriceListConfig,
                        CustomerPriceListConfig.id
                        == CustomerPriceListSource.customer_config_id,
                    )
                    .join(
                        Customer,
                        Customer.id == CustomerPriceListConfig.customer_id,
                    )
                    .join(
                        ProviderPriceListConfig,
                        ProviderPriceListConfig.id
                        == CustomerPriceListSource.provider_config_id,
                    )
                    .where(
                        ProviderPriceListConfig.provider_id == provider_id
                    )
                    .order_by(
                        Customer.name.asc(),
                        CustomerPriceListConfig.name.asc(),
                        ProviderPriceListConfig.name_price.asc(),
                        CustomerPriceListSource.id.asc(),
                    )
                )
            ).all()
            customer_pricelist_sources_usage = [
                ProviderCustomerPriceListSourceUsageOut(
                    source_id=source.id,
                    customer_id=customer.id,
                    customer_name=customer.name,
                    customer_config_id=customer_cfg.id,
                    customer_config_name=customer_cfg.name,
                    provider_config_id=provider_cfg.id,
                    provider_config_name=provider_cfg.name_price,
                    enabled=bool(source.enabled),
                    markup=float(source.markup or 1.0),
                    brand_markups=source.brand_markups or {},
                    brand_filters=source.brand_filters or {},
                    position_filters=source.position_filters or {},
                    min_price=source.min_price,
                    max_price=source.max_price,
                    min_quantity=source.min_quantity,
                    max_quantity=source.max_quantity,
                    additional_filters=source.additional_filters or {},
                )
                for source, customer_cfg, customer, provider_cfg
                in source_usage_rows
            ]
            return ProviderPageResponse(
                provider=provider_core,
                abbreviations=abbreviations,
                external_references=external_references,
                pricelist_configs=pricelist_configs,
                supplier_response_configs=supplier_response_configs,
                customer_pricelist_sources_usage=(
                    customer_pricelist_sources_usage
                ),
            )
        except Exception as e:
            logger.error(
                f'Ошибка в crud_provider.get_by_id: {e}', exc_info=True
            )
            raise

    async def update_provider(
        self,
        session: AsyncSession,
        provider_id: int,
        obj_in: ProviderUpdate,
    ) -> None:
        provider = await self.get_by_id(
            provider_id=provider_id, session=session
        )
        if provider is None:
            raise HTTPException(status_code=404, detail='Provider not found')
        update_data = obj_in.model_dump(exclude_unset=True)
        if 'type_prices' in update_data:
            update_data['is_vat_payer'] = _derive_provider_is_vat_payer(
                update_data.get('type_prices'),
                update_data.get(
                    'is_vat_payer',
                    bool(getattr(provider, 'is_vat_payer', False)),
                ),
            )
            obj_in = ProviderUpdate(**update_data)
        updated_provider = await self.update(
            db_obj=provider, obj_in=obj_in, session=session, commit=True
        )
        return updated_provider

    async def get_by_email_incoming_price(
        self, session: AsyncSession, email: str
    ) -> Optional[Provider]:
        result = await session.execute(
            select(self.model).where(self.model.email_incoming_price == email)
        )
        return result.scalar_one_or_none()

    async def create(
        self, obj_in: ProviderCreate, session: AsyncSession, **kwargs
    ) -> Provider:
        payload = obj_in.model_dump()
        payload['is_vat_payer'] = _derive_provider_is_vat_payer(
            payload.get('type_prices'),
            payload.get('is_vat_payer', False),
        )
        provider = Provider(**payload)
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

    async def get_or_create_provider_by_abbreviation(
        self, abbreviation: str, session: AsyncSession
    ):
        try:
            abbr_norm = (abbreviation or '').strip().upper()
            if not abbr_norm:
                raise HTTPException(
                    status_code=400,
                    detail='Abbreviation must not be empty'
                )

            stmt = (
                select(ProviderAbbreviation)
                .where(ProviderAbbreviation.abbreviation == abbr_norm)
                .options(selectinload(ProviderAbbreviation.provider))
            )
            result = await session.execute(stmt)
            abbreviation_entry = result.scalars().first()
            if abbreviation_entry:
                return abbreviation_entry.provider
            new_provider = Provider(
                name=f'Provider {abbr_norm}',
                email_contact=None,
                email_incoming_price=None,
                is_virtual=True,
                description='Created from site abbreviation',
                comment='Automatically created provider from site',
                type_prices=TYPE_PRICES.WHOLESALE,
            )
            session.add(new_provider)
            await session.flush()
            new_abbreviation = ProviderAbbreviation(
                abbreviation=abbr_norm, provider_id=new_provider.id
            )
            session.add(new_abbreviation)
            await session.commit()
            await session.refresh(new_provider)

            return new_provider
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f'Database error: {e}')
            raise

    async def get_all(
        self,
        session: AsyncSession,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        search: Optional[str] = None,
        has_pricelist_config: Optional[bool] = None,
        has_active_pricelists: Optional[bool] = None,
        is_virtual: Optional[bool] = None,
        sort_by: Optional[str] = None,
        sort_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        '''
        Вернёт постранично список поставщиков
        с полной агрегированной информацией.
        :param session:
        :param page:
        :param page_size:
        :param search:
        :return:
        '''
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = DEFAULT_PAGE_SIZE

        filters = []
        if search:
            filters.append(Provider.name.ilike(f'%{search}%'))
        if is_virtual is not None:
            filters.append(Provider.is_virtual.is_(is_virtual))
        if has_pricelist_config is not None:
            config_exists = (
                select(ProviderPriceListConfig.id)
                .where(ProviderPriceListConfig.provider_id == Provider.id)
                .limit(1)
            )
            filters.append(
                config_exists.exists()
                if has_pricelist_config
                else ~config_exists.exists()
            )
        if has_active_pricelists is not None:
            active_exists = (
                select(PriceList.id)
                .where(
                    PriceList.provider_id == Provider.id,
                    PriceList.is_active.is_(True),
                )
                .limit(1)
            )
            filters.append(
                active_exists.exists()
                if has_active_pricelists
                else ~active_exists.exists()
            )

        base = select(Provider)
        count_base = select(Provider.id)
        if filters:
            base = base.where(*filters)
            count_base = count_base.where(*filters)

        count_query = select(func.count()).select_from(count_base.subquery())
        total = (await session.execute(count_query)).scalar()

        sort_map = {
            'name': Provider.name,
            'id': Provider.id,
        }
        sort_column = sort_map.get(sort_by) or Provider.name
        sort_direction = (sort_dir or 'asc').lower()
        order_clause = (
            sort_column.asc()
            if sort_direction != 'desc'
            else sort_column.desc()
        )

        stmt = (
            base.options(
                selectinload(Provider.pricelist_configs).selectinload(
                    ProviderPriceListConfig.last_email_uid
                ),
                selectinload(Provider.provider_last_uid),
                selectinload(Provider.price_lists).selectinload(
                    PriceList.config
                ),
            )
            .order_by(order_clause)
            .limit(page_size)
            .offset((page - 1) * page_size)
        )
        result = await session.execute(stmt)
        providers: List[Provider] = result.scalars().all()
        if not providers:
            pages = ceil(total / page_size) if page_size else 1
            return {
                'items': [],
                'page': page,
                'page_size': page_size,
                'total': total,
                'pages': pages,
            }
        # Получаем provider_ids для загрузки аббревиатур отдельно
        provider_ids = [p.id for p in providers]

        # Загружаем аббревиатуры отдельным запросом
        abbr_query = select(ProviderAbbreviation).where(
            ProviderAbbreviation.provider_id.in_(provider_ids)
        )
        abbr_result = await session.execute(abbr_query)
        abbreviations = abbr_result.scalars().all()

        # Создаем словарь для быстрого поиска аббревиатур
        abbr_dict = {}
        for abbr in abbreviations:
            if abbr.provider_id not in abbr_dict:
                abbr_dict[abbr.provider_id] = []
            abbr_dict[abbr.provider_id].append(abbr.abbreviation)
        items: List[Dict[str, Any]] = []
        for provider in providers:
            provider_abbrs = abbr_dict.get(provider.id, [])
            abbr = provider_abbrs[0] if provider_abbrs else None

            email_contact = getattr(provider, 'email_contact', None)
            items.append(
                {
                    'id': provider.id,
                    'name': provider.name,
                    'abbr': abbr,
                    'email_incoming_price': provider.email_incoming_price,
                    'email_contact': email_contact,
                    'pricelist_configs': [
                        {'id': config.id, 'name_price': config.name_price}
                        for config in sorted(
                            provider.pricelist_configs,
                            key=lambda c: (
                                c.name_price is None,
                                c.name_price or '',
                            ),
                        )
                    ],
                    'last_email_uid': (
                        _build_provider_last_email_uid(provider)
                    ),
                    'price_lists': [
                        {
                            'id': pl.id,
                            'name_price': (
                                pl.config.name_price if pl.config else None
                            ),
                            'date': pl.date,
                            'is_active': pl.is_active,
                        }
                        for pl in (provider.price_lists or [])
                    ],
                }
            )

        pages = ceil(total / page_size) if page_size else 1

        return {
            'items': items,
            'page': page,
            'page_size': page_size,
            'total': total,
            'pages': pages,
        }

    async def should_delete_empty_provider(
        self,
        provider: Provider,
        session: AsyncSession,
    ):
        '''
        Проверяет, можно ли безопасно удалить поставщика.
        Удаляем только если:
        1. Поставщик виртуальный (автоматически созданный)
        2. У него нет других аббревиатур
        3. У него нет прайс-листов
        4. У него нет конфигураций
        5. У него нет связанных заказов/документов
        :param provider:
        :param session:
        :return:
        '''
        if not provider.is_virtual:
            return False

        abbrev_list = await crud_provider_abbreviation.list_abbreviations(
            provider_id=provider.id, session=session
        )
        if len(abbrev_list) > 1:
            return False

        if provider.price_lists:
            return False

        if provider.pricelist_configs:
            return False

        if provider.supplier_response_configs:
            return False

        if provider.external_references:
            return False

        if provider.orders:
            return False

        return True

    async def merge_providers(
        self,
        source_provider_id: int,
        target_provider_id: int,
        session: AsyncSession,
    ) -> bool:
        '''
        Объединяет двух поставщиков: переносит все аббревиатуры
        и другие данные от source к target, затем удаляет source.
        :param source_provider_id:
        :param target_provider_id:
        :param session:
        :return:
        '''
        if source_provider_id == target_provider_id:
            raise ValueError(
                'source_provider_id и target_provider_id совпадают'
            )

        try:
            source_provider = await session.get(
                Provider, source_provider_id, with_for_update=True
            )
            target_provider = await session.get(
                Provider, target_provider_id, with_for_update=True
            )

            if not source_provider or not target_provider:
                raise ValueError('One or both providers not found')

            # Перенести все аббревиатуры
            stmt = (
                update(ProviderAbbreviation)
                .where(ProviderAbbreviation.provider_id == source_provider_id)
                .values(provider_id=target_provider_id)
            )

            await session.execute(stmt)
            # 2) Перевесить конфиги
            await session.execute(
                update(ProviderPriceListConfig)
                .where(
                    ProviderPriceListConfig.provider_id == source_provider_id
                )
                .values(provider_id=target_provider_id)
            )
            await session.execute(
                update(SupplierResponseConfig)
                .where(
                    SupplierResponseConfig.provider_id == source_provider_id
                )
                .values(provider_id=target_provider_id)
            )

            # 3) Перевесить прайсы
            await session.execute(
                update(PriceList)
                .where(PriceList.provider_id == source_provider_id)
                .values(provider_id=target_provider_id)
            )
            await session.execute(
                update(CustomerOrderItem)
                .where(CustomerOrderItem.supplier_id == source_provider_id)
                .values(supplier_id=target_provider_id)
            )
            await session.execute(
                update(SupplierOrder)
                .where(SupplierOrder.provider_id == source_provider_id)
                .values(provider_id=target_provider_id)
            )
            await session.execute(
                update(SupplierOrderMessage)
                .where(
                    SupplierOrderMessage.provider_id == source_provider_id
                )
                .values(provider_id=target_provider_id)
            )
            await session.execute(
                update(SupplierReceipt)
                .where(SupplierReceipt.provider_id == source_provider_id)
                .values(provider_id=target_provider_id)
            )
            await session.execute(
                update(Order)
                .where(Order.provider_id == source_provider_id)
                .values(provider_id=target_provider_id)
            )

            source_refs = (
                await session.execute(
                    select(ProviderExternalReference).where(
                        ProviderExternalReference.provider_id
                        == source_provider_id
                    )
                )
            ).scalars().all()
            for reference in source_refs:
                duplicate_stmt = select(ProviderExternalReference).where(
                    ProviderExternalReference.provider_id
                    == target_provider_id,
                    ProviderExternalReference.source_system
                    == reference.source_system,
                )
                if reference.external_supplier_id is not None:
                    duplicate_stmt = duplicate_stmt.where(
                        ProviderExternalReference.external_supplier_id
                        == reference.external_supplier_id
                    )
                else:
                    duplicate_stmt = duplicate_stmt.where(
                        ProviderExternalReference.external_supplier_id.is_(
                            None
                        ),
                        ProviderExternalReference.external_supplier_name
                        == reference.external_supplier_name,
                    )
                duplicate = (
                    await session.execute(duplicate_stmt)
                ).scalar_one_or_none()
                if duplicate is not None:
                    await session.delete(reference)
                else:
                    reference.provider_id = target_provider_id
            # Удалить исходного поставщика
            await session.delete(source_provider)
            await session.commit()

            logger.info(
                f'Merged provider {source_provider_id} '
                f'into {target_provider_id}'
            )
            return True

        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f'Error merging providers: {e}')
            raise


crud_provider = CRUDProvider(Provider)


class CRUDProviderAbbreviation(
    CRUDBase[
        ProviderAbbreviation,
        ProviderAbbreviationCreate,
        ProviderAbbreviationUpdate,
    ]
):
    @staticmethod
    def _normalize_abbr(s: str) -> str:
        return (s or '').strip().upper()

    async def _get_by_abbr_norm(
        self,
        session: AsyncSession,
        abbr_norm: str,
        exclude_id: int | None = None,
    ) -> ProviderAbbreviation | None:
        stmt = select(ProviderAbbreviation).where(
            func.upper(ProviderAbbreviation.abbreviation) == abbr_norm
        )
        if exclude_id is not None:
            stmt = stmt.where(ProviderAbbreviation.id != exclude_id)
        return (await session.execute(stmt)).scalar_one_or_none()

    async def list_abbreviations(
        self, session: AsyncSession, provider_id: int
    ) -> List[ProviderAbbreviationOut]:
        stmt = (
            select(ProviderAbbreviation)
            .where(ProviderAbbreviation.provider_id == provider_id)
            .order_by(func.lower(ProviderAbbreviation.abbreviation).asc())
        )
        rows = (await session.execute(stmt)).scalars().all()
        return [ProviderAbbreviationOut.model_validate(r) for r in rows]

    async def add_abbreviation(
        self, session: AsyncSession, provider_id: int, abbreviation: str
    ) -> ProviderAbbreviationOut:
        abbreviation_norm = self._normalize_abbr(abbreviation)
        if not abbreviation:
            raise HTTPException(
                status_code=400, detail='Abbreviation must not be empty'
            )
        if await self._get_by_abbr_norm(session, abbreviation_norm):
            raise HTTPException(
                status_code=409, detail='Abbreviation already exists'
            )
        obj = ProviderAbbreviation(
            provider_id=provider_id, abbreviation=abbreviation_norm
        )
        session.add(obj)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise HTTPException(
                status_code=409,
                detail='Abbreviation already exists for this provider',
            )
        await session.refresh(obj)
        return ProviderAbbreviationOut.model_validate(obj)

    async def update_abbreviation(
        self,
        session: AsyncSession,
        abbreviation_id: int,
        new_abbreviation: str,
    ) -> ProviderAbbreviationOut:
        obj = await session.get(ProviderAbbreviation, abbreviation_id)
        if not obj:
            raise HTTPException(
                status_code=404, detail='Abbreviation not found'
            )

        abbreviation_norm = self._normalize_abbr(new_abbreviation)
        if not abbreviation_norm:
            raise HTTPException(
                status_code=400, detail='Abbreviation must not be empty'
            )

        if await self._get_by_abbr_norm(
            session, abbreviation_norm, exclude_id=abbreviation_id
        ):
            raise HTTPException(
                status_code=409, detail='Abbreviation already exists'
            )

        obj.abbreviation = abbreviation_norm
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise HTTPException(
                status_code=409,
                detail='Abbreviation already exists for this provider',
            )

        await session.refresh(obj)
        return ProviderAbbreviationOut.model_validate(obj)

    async def delete_abbreviation(
        self, session: AsyncSession, abbreviation_id: int
    ) -> None:
        stmt = select(ProviderAbbreviation).where(
            ProviderAbbreviation.id == abbreviation_id
        )
        obj = (await session.execute(stmt)).scalars().first()
        if not obj:
            return
        await session.delete(obj)
        await session.commit()

    async def reassign_abbreviation_to_provider(
        self,
        session: AsyncSession,
        abbreviation: str,
        target_provider_id: int,
    ):
        '''
        Переназначает аббревиатуру другому поставщику.
        Если старый поставщик автоматический и пустой - удаляет его.
        :param session:
        :param abbreviation:
        :param target_provider_id:
        :return:
        '''
        try:
            stmt = (
                select(ProviderAbbreviation)
                .where(ProviderAbbreviation.abbreviation == abbreviation)
                .options(selectinload(ProviderAbbreviation.provider))
            )
            result = await session.execute(stmt)
            existing_abbreviation = result.scalars().first()
            if existing_abbreviation:
                old_provider = existing_abbreviation.provider

                # Проверить, существует ли целевой поставщик
                target_provider = await crud_provider.get_by_id(
                    provider_id=target_provider_id, session=session
                )
                if not target_provider:
                    raise ValueError(
                        f'Provider with id {target_provider_id} not found'
                    )
                # Переназначить аббревиатуру
                existing_abbreviation.provider_id = target_provider_id
                # Проверить, можно ли удалить старого поставщика
                should_delete_old = (
                    await crud_provider.should_delete_empty_provider(
                        provider=old_provider,
                        session=session
                    )
                )
                if should_delete_old:
                    await session.delete(old_provider)
                    logger.info(
                        f'Deleted empty auto-created '
                        f'provider {old_provider.id}'
                    )
                await session.commit()
            else:
                # Создать новую аббревиатуру для существующего поставщика
                await self.add_abbreviation(
                    session=session,
                    provider_id=target_provider_id,
                    abbreviation=abbreviation
                )
            return True
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f'Database error while reassigning abbreviation: {e}')
            raise


crud_provider_abbreviation = CRUDProviderAbbreviation(ProviderAbbreviation)


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

    async def _get_last_history_snapshot(
            self,
            provider_id: int,
            session: AsyncSession,
            provider_config_id: int | None = None,
    ) -> dict[int, dict]:
        """
        Последнее известное состояние по каждой позиции данного провайдера
        из таблицы AutoPartPriceHistory (events).
        Возвращает: {autopart_id: {'price': float, 'quantity': int}}
        """
        stmt = (
            select(
                AutoPartPriceHistory.autopart_id,
                AutoPartPriceHistory.price,
                AutoPartPriceHistory.quantity,
            )
            .where(AutoPartPriceHistory.provider_id == provider_id)
            # Postgres DISTINCT ON (autopart_id)
            .distinct(AutoPartPriceHistory.autopart_id)
            .order_by(
                AutoPartPriceHistory.autopart_id,
                AutoPartPriceHistory.created_at.desc(),
                AutoPartPriceHistory.id.desc(),
            )
        )
        if provider_config_id is not None:
            stmt = stmt.where(
                AutoPartPriceHistory.provider_config_id
                == provider_config_id
            )
        rows = (await session.execute(stmt)).all()

        return {
            r.autopart_id: {
                'price': money(r.price),
                'quantity': int(r.quantity)
            }
            for r in rows
        }

    async def cleanup_old_pricelists_keep_last_n(
            self,
            session: AsyncSession,
            keep_last_n: int = 5,
            batch_size: int = 500,
    ) -> int:
        """
        Удаляет старые PriceList и их PriceListAutoPartAssociation,
        оставляя последние keep_last_n прайсов на каждого
        provider_config_id.
        Историю AutoPartPriceHistory НЕ трогаем.

        Возвращает: сколько прайс-листов удалено.
        """

        ranked = (
            select(
                PriceList.id.label("id"),
                func.row_number()
                .over(
                    partition_by=PriceList.provider_config_id,
                    order_by=(
                        PriceList.date.desc().nullslast(),
                        PriceList.id.desc(),
                    ),
                )
                .label("rn"),
            )
            .where(PriceList.provider_config_id.is_not(None))
            .subquery()
        )

        ids = (
            (await session.execute(
                select(ranked.c.id)
                .where(ranked.c.rn > keep_last_n)
                .limit(batch_size)
            ))
            .scalars()
            .all()
        )
        if not ids:
            return 0

        # 1) associations
        await session.execute(
            delete(PriceListAutoPartAssociation).where(
                PriceListAutoPartAssociation.pricelist_id.in_(ids)
            )
        )

        await session.execute(
            delete(PriceListMissingBrand).where(
                PriceListMissingBrand.pricelist_id.in_(ids)
            )
        )

        # 2) сами прайсы
        await session.execute(
            delete(PriceList).where(PriceList.id.in_(ids))
        )

        await session.commit()
        return len(ids)

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
            brand_cache: dict[str, Optional[Brand]] = {}
            missing_brand_counts: dict[str, int] = {}

            bulk_insert_data: list[dict] = []
            bulk_insert_map: dict[int, dict] = {}

            provider_id = db_obj.provider_id
            provider_config_id = db_obj.provider_config_id
            created_at_ts = now_moscow()

            # Держим только актуальный срез отсутствующих брендов
            # для текущей конфигурации прайса.
            if provider_config_id is not None:
                await session.execute(
                    delete(PriceListMissingBrand).where(
                        PriceListMissingBrand.provider_config_id
                        == provider_config_id
                    )
                )

            last_history_map = await self._get_last_history_snapshot(
                session=session,
                provider_id=provider_id,
                provider_config_id=provider_config_id,
            )
            current_positions: dict[int, dict] = {}

            # ===== ОБРАБОТКА ВХОДЯЩИХ ДАННЫХ =====
            for autopart_assoc_data in autoparts_data:
                autopart_data_dict = dict(autopart_assoc_data['autopart'])
                quantity = autopart_assoc_data['quantity']
                price = autopart_assoc_data['price']

                logger.debug(f'Processing AutoPart data: {autopart_data_dict}')

                item_default_brand = default_brand
                raw_brand_name = autopart_data_dict.get('brand')
                if raw_brand_name:
                    normalized_brand_name = await change_brand_name(
                        brand_name=str(raw_brand_name)
                    )
                    db_brand = brand_cache.get(normalized_brand_name)
                    if db_brand is None and (
                        normalized_brand_name not in brand_cache
                    ):
                        db_brand = await brand_crud.get_brand_by_name_or_none(
                            brand_name=normalized_brand_name,
                            session=session,
                        )
                        brand_cache[normalized_brand_name] = db_brand
                    if db_brand is None:
                        missing_brand_counts[normalized_brand_name] = (
                            missing_brand_counts.get(
                                normalized_brand_name, 0
                            )
                            + 1
                        )
                        logger.debug(
                            'Missing brand in pricelist row: %s',
                            normalized_brand_name,
                        )
                        continue
                    item_default_brand = db_brand
                    autopart_data_dict['brand'] = None

                # Instantiate AutoPartPricelist
                autopart_data = AutoPartPricelist(**autopart_data_dict)

                autopart = await crud_autopart.create_autopart_from_price(
                    new_autopart=autopart_data,
                    session=session,
                    default_brand=item_default_brand,
                )

                if not autopart:
                    logger.warning(
                        f'Failed to create or retrieve '
                        f'AutoPart for data: {autopart_data_dict}'
                    )
                    continue
                qty = int(quantity)
                prc = money(price)
                mult = int(autopart_assoc_data.get('multiplicity') or 1)
                existing_assoc = bulk_insert_map.get(autopart.id)
                if existing_assoc and existing_assoc['price'] < prc:
                    continue

                assoc_row = {
                    'pricelist_id': db_obj.id,
                    'autopart_id': autopart.id,
                    'quantity': qty,
                    'price': prc,
                    'multiplicity': mult,
                }
                bulk_insert_map[autopart.id] = assoc_row

                # Запоминаем актуальную позицию для истории изменений.
                current_positions[autopart.id] = {
                    'price': prc,
                    'quantity': qty,
                }

            if bulk_insert_map:
                bulk_insert_data = list(bulk_insert_map.values())

            # Шаг 4: Выполнение массовой вставки ассоциаций, если есть данные
            if bulk_insert_data:
                logger.debug(
                    f'Bulk inserting {len(bulk_insert_data)} associations.'
                )
                await session.execute(
                    insert(PriceListAutoPartAssociation), bulk_insert_data
                )

            # ===== ЗАПИСЬ ТОЛЬКО ИЗМЕНЕНИЙ В ИСТОРИЮ =====
            bulk_insert_data_history = []

            # 1. Проверяем изменения в текущих позициях
            for autopart_id, current_data in current_positions.items():
                last = last_history_map.get(autopart_id)
                if last is None:
                    # впервые видим эту позицию у этого провайдера
                    bulk_insert_data_history.append({
                        'autopart_id': autopart_id,
                        'provider_id': provider_id,
                        'pricelist_id': db_obj.id,
                        'provider_config_id': provider_config_id,
                        'created_at': created_at_ts,
                        'price': current_data['price'],
                        'quantity': current_data['quantity'],
                    })
                    logger.debug(f'New position: autopart_id={autopart_id}')
                    continue
                price_changed = current_data['price'] != last['price']
                qty_changed = current_data['quantity'] != last['quantity']
                if price_changed or qty_changed:
                    bulk_insert_data_history.append({
                        'autopart_id': autopart_id,
                        'provider_id': provider_id,
                        'provider_config_id': provider_config_id,
                        'pricelist_id': db_obj.id,
                        'created_at': created_at_ts,
                        'price': current_data['price'],
                        'quantity': current_data['quantity'],
                    })

            # 3) События исчезновения (qty=0)
            # исчезли те, что были в last_history_map, но нет в current_ids
            current_ids = set(current_positions.keys())
            missing_ids = set(last_history_map.keys()) - current_ids

            for autopart_id in missing_ids:
                last = last_history_map.get(autopart_id)
                if not last:
                    continue
                # если уже было qty=0 — повторно не пишем
                if last['quantity'] != 0:
                    bulk_insert_data_history.append({
                        'autopart_id': autopart_id,
                        'provider_id': provider_id,
                        'provider_config_id': provider_config_id,
                        'pricelist_id': db_obj.id,
                        'created_at': created_at_ts,
                        'price': last['price'],
                        'quantity': 0,
                    })
                    logger.debug(
                        f'Position disappeared: autopart_id={autopart_id}, '
                        f'recording qty=0'
                    )
            if bulk_insert_data_history:
                logger.debug(
                    f'Bulk inserting '
                    f'{len(bulk_insert_data_history)} '
                    f'records into AutoPartPriceHistory.'
                )
                await session.execute(
                    insert(AutoPartPriceHistory), bulk_insert_data_history
                )

            if provider_config_id is not None and missing_brand_counts:
                missing_rows = [
                    {
                        'pricelist_id': db_obj.id,
                        'provider_config_id': provider_config_id,
                        'brand_name': brand_name,
                        'positions_count': positions_count,
                        'created_at': created_at_ts,
                    }
                    for brand_name, positions_count
                    in missing_brand_counts.items()
                ]
                await session.execute(
                    insert(PriceListMissingBrand), missing_rows
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
                    selectinload(PriceList.config),
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
                provider_obj = await session.get(Provider, db_obj.provider_id)
                response = PriceListResponse(
                    id=db_obj.id,
                    date=db_obj.date,
                    provider=provider_obj,
                    provider_config_id=db_obj.provider_config_id,
                    autoparts=[
                        PriceListAutoPartAssociationResponse(
                            autopart=assoc.autopart,
                            quantity=assoc.quantity,
                            price=float(assoc.price),
                            multiplicity=assoc.multiplicity,
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
            select(
                PriceList.id.label('id'),
                PriceList.date.label('date'),
                PriceList.provider_config_id.label('provider_config_id'),
            )
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
                pricelist_subquery.c.provider_config_id,
                func.count(PriceListAutoPartAssociation.autopart_id).label(
                    'num_positions'
                ),
            )
            .outerjoin(
                PriceListAutoPartAssociation,
                PriceListAutoPartAssociation.pricelist_id
                == pricelist_subquery.c.id,
            )
            .group_by(
                pricelist_subquery.c.id,
                pricelist_subquery.c.date,
                pricelist_subquery.c.provider_config_id,
            )
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
                selectinload(PriceList.provider),
                selectinload(PriceList.config),
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
                joinedload(PriceListAutoPartAssociation.pricelist).joinedload(
                    PriceList.provider
                ),
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
                    'provider_config_id': assoc.pricelist.provider_config_id,
                    'pricelist_id': assoc.pricelist.id,
                    'is_own_price': bool(
                        assoc.pricelist.provider.is_own_price
                    )
                    if assoc.pricelist.provider
                    else False,
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
        rows = result.all()
        pricelist_ids = [row.id for row in rows]
        return sorted(pricelist_ids)

    async def get_latest_pricelist_by_config(
        self, session: AsyncSession, provider_config_id: int
    ) -> Optional[PriceList]:
        stmt = (
            select(PriceList)
            .where(PriceList.provider_config_id == provider_config_id)
            .order_by(PriceList.date.desc().nullslast(), PriceList.id.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalars().first()

    async def get_last_pricelists_by_provider(
        self,
        session: AsyncSession,
        provider_id: int,
        limit_last_n: int = 2,
        provider_config_id: int | None = None,
    ) -> List[PriceList]:
        """
        Возвращает прайс-листы провайдера limit_last_n = 2,
        отсортированные по дате (или id).
        """
        stmt = select(PriceList).where(PriceList.provider_id == provider_id)
        if provider_config_id is not None:
            stmt = stmt.where(
                PriceList.provider_config_id == provider_config_id
            )
        stmt = (
            stmt.order_by(
                PriceList.date.desc().nullslast(),
                PriceList.id.desc(),
            )
            .limit(limit_last_n)
            .options(selectinload(PriceList.autopart_associations))
        )
        result = await session.execute(stmt)
        pricelists = result.scalars().all()
        logger.debug(
            'Fetching pricelists for provider_id=%s provider_config_id=%s',
            provider_id,
            provider_config_id,
        )
        return pricelists


crud_pricelist = CRUDPriceList(PriceList)


class CRUDCustomerPriceList(
    CRUDBase[
        CustomerPriceList, CustomerPriceListCreate, CustomerPriceListUpdate
    ]
):
    async def cleanup_old_pricelists_keep_last_n(
        self,
        session: AsyncSession,
        keep_last_n: int = 10,
        batch_size: int = 500,
    ) -> int:
        """
        Удаляет старые CustomerPriceList и их связи,
        оставляя последние keep_last_n прайсов на каждого customer_id.

        Возвращает: сколько прайс-листов удалено.
        """
        ranked = (
            select(
                CustomerPriceList.id.label("id"),
                func.row_number()
                .over(
                    partition_by=CustomerPriceList.customer_id,
                    order_by=(
                        CustomerPriceList.date.desc().nullslast(),
                        CustomerPriceList.id.desc(),
                    ),
                )
                .label("rn"),
            )
            .where(CustomerPriceList.customer_id.is_not(None))
            .subquery()
        )

        ids = (
            (await session.execute(
                select(ranked.c.id)
                .where(ranked.c.rn > keep_last_n)
                .limit(batch_size)
            ))
            .scalars()
            .all()
        )
        if not ids:
            return 0

        await session.execute(
            delete(CustomerPriceListAutoPartAssociation).where(
                CustomerPriceListAutoPartAssociation.customerpricelist_id.in_(
                    ids
                )
            )
        )
        await session.execute(
            delete(CustomerPriceList).where(CustomerPriceList.id.in_(ids))
        )
        await session.commit()
        return len(ids)

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
        apply_general_markup: bool = True,
        provider_id: int | None = None,
        is_own_price: bool | None = None,
        ignore_price_quantity_filters: bool = False,
    ) -> pd.DataFrame:
        logger.debug(
            f'Into apply_coefficient data df:{df}, cofig: {config.__dict__}'
        )
        data_individual_markups = config.individual_markups

        def _base_filters() -> dict:
            if config.default_filters:
                return dict(config.default_filters)
            return {
                'brand_filters': config.brand_filters,
                'category_filter': config.category_filter,
                'price_intervals': config.price_intervals,
                'position_filters': config.position_filters,
                'supplier_quantity_filters': config.supplier_quantity_filters,
                'additional_filters': config.additional_filters,
            }

        def _merge_filters(base: dict, override: dict | None) -> dict:
            if not override:
                return base
            merged = dict(base)
            for key, value in override.items():
                merged[key] = value
            return merged

        def _resolve_filters(pid: int | None, own_flag: bool | None) -> dict:
            base = _base_filters()
            if own_flag:
                return _merge_filters(base, config.own_filters)
            supplier_filters = config.supplier_filters or {}
            if pid is not None:
                override = supplier_filters.get(pid)
                if override is None:
                    override = supplier_filters.get(str(pid))
                if override:
                    return _merge_filters(base, override)
            return _merge_filters(base, config.other_filters)

        def _normalize_list(values):
            return [int(v) for v in (values or []) if str(v).isdigit()]

        def _apply_filter_block(block_df: pd.DataFrame, filters_cfg: dict):
            block_df = block_df.copy()
            block_df['price'] = pd.to_numeric(
                block_df['price'], errors='coerce'
            )
            block_df['quantity'] = pd.to_numeric(
                block_df['quantity'], errors='coerce'
            )

            brand_cfg = filters_cfg.get('brand_filters')
            if isinstance(brand_cfg, list):
                brands = _normalize_list(brand_cfg)
                brand_cfg = (
                    {'type': 'include', 'brands': brands} if brands else None
                )
            elif isinstance(brand_cfg, dict):
                brands = _normalize_list(brand_cfg.get('brands'))
                if not brands:
                    brand_cfg = None
                else:
                    brand_cfg = {**brand_cfg, 'brands': brands}
            if brand_cfg:
                block_df = brand_filters(brand_filters=brand_cfg, df=block_df)

            position_cfg = filters_cfg.get('position_filters')
            if isinstance(position_cfg, list):
                autoparts = _normalize_list(position_cfg)
                position_cfg = (
                    {'type': 'include', 'autoparts': autoparts}
                    if autoparts
                    else None
                )
            elif isinstance(position_cfg, dict):
                autoparts = _normalize_list(position_cfg.get('autoparts'))
                if not autoparts:
                    position_cfg = None
                else:
                    position_cfg = {**position_cfg, 'autoparts': autoparts}
            if position_cfg:
                block_df = position_filters(
                    position_filters=position_cfg, df=block_df
                )

            if not ignore_price_quantity_filters:
                intervals_cfg = filters_cfg.get('price_intervals')
                if intervals_cfg:
                    block_df = price_intervals(
                        price_intervals=intervals_cfg, df=block_df
                    )

                supplier_qty_cfg = filters_cfg.get(
                    'supplier_quantity_filters'
                )
                if supplier_qty_cfg:
                    block_df = supplier_quantity_filters(
                        supplier_quantity_filters=supplier_qty_cfg,
                        df=block_df,
                    )

                min_price = filters_cfg.get('min_price')
                max_price = filters_cfg.get('max_price')
                min_qty = filters_cfg.get('min_quantity')
                max_qty = filters_cfg.get('max_quantity')
                if min_price is not None:
                    block_df = block_df[
                        block_df['price'] >= float(min_price)
                    ]
                if max_price is not None:
                    block_df = block_df[
                        block_df['price'] <= float(max_price)
                    ]
                if min_qty is not None:
                    block_df = block_df[
                        block_df['quantity'] >= int(min_qty)
                    ]
                if max_qty is not None:
                    block_df = block_df[
                        block_df['quantity'] <= int(max_qty)
                    ]

            return block_df

        # Ensure 'price' column is numeric
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Apply individual markups per supplier
        if data_individual_markups:
            df = individual_markups(
                individual_markups=data_individual_markups, df=df
            )

        if provider_id is None and 'provider_id' in df.columns:
            blocks = []
            for (pid, own_flag), block in df.groupby(
                ['provider_id', 'is_own_price'], dropna=False
            ):
                filters_cfg = _resolve_filters(
                    int(pid) if pd.notna(pid) else None, bool(own_flag)
                )
                filtered = _apply_filter_block(block, filters_cfg)
                if not filtered.empty:
                    blocks.append(filtered)
            df = pd.concat(
                blocks, ignore_index=True
            ) if blocks else df.iloc[0:0]
        else:
            filters_cfg = _resolve_filters(provider_id, is_own_price)
            df = _apply_filter_block(df, filters_cfg)

        # Apply general markup
        if apply_general_markup:
            df['price'] *= normalize_markup(config.general_markup)

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
                and_(
                    CustomerPriceList.customer_id == customer_id,
                    CustomerPriceList.id == pricelist_id,
                )
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

    async def delete_by_id(
        self, session: AsyncSession, customer_id: int, pricelist_id: int
    ) -> None:
        """
        Удаляет один прайс-лист конкретного клиента по его ID.
        """
        try:
            # Проверяем, что прайс-лист действительно
            # принадлежит этому customer_id
            result = await session.execute(
                select(self.model).where(
                    self.model.id == pricelist_id,
                    self.model.customer_id == customer_id,
                )
            )
            db_obj = result.scalar_one_or_none()
            if not db_obj:
                raise HTTPException(
                    status_code=404,
                    detail=f'PriceList {pricelist_id} not '
                    f'found for customer {customer_id}',
                )

            # Удаляем сам прайс-лист
            await session.delete(db_obj)
            await session.commit()
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500,
                detail='Database error during pricelist delete',
            )
        except Exception as e:
            logger.error(f'Unexpected error occurred: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500,
                detail='Unexpected error during pricelist delete',
            )

    async def delete_older_pricelists(
        self, session: AsyncSession, customer_id: int, max_count: int = 10
    ) -> None:
        """
        Удаляет самые старые прайс-листы (по дате, потом по id),
        если общее количество у клиента превышает max_count.
        """
        try:
            # Получаем все прайс-листы (сортируем по дате по возрастанию,
            # чтобы первые в списке были самые старые)
            result = await session.execute(
                select(self.model)
                .where(self.model.customer_id == customer_id)
                .order_by(self.model.date.asc(), self.model.id.asc())
            )
            all_pricelists = result.scalars().all()

            if len(all_pricelists) > max_count:
                # Те, что нужно удалить, это "лишние" в начале списка
                num_to_delete = len(all_pricelists) - max_count
                pricelists_to_delete = all_pricelists[:num_to_delete]

                for pl in pricelists_to_delete:
                    await session.delete(pl)

                await session.commit()
        except SQLAlchemyError as e:
            logger.error(f'Database error occurred during cleanup: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500,
                detail='Database error during pricelist cleanup',
            )
        except Exception as e:
            logger.error(f'Unexpected error occurred during cleanup: {e}')
            await session.rollback()
            raise HTTPException(
                status_code=500,
                detail='Unexpected error during pricelist cleanup',
            )


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
    async def get_configs(
        self,
        provider_id: int,
        session: AsyncSession,
        only_active: bool = False,
        **kwargs,
    ) -> List[ProviderPriceListConfig]:
        stmt = select(ProviderPriceListConfig).where(
            ProviderPriceListConfig.provider_id == provider_id
        )
        if only_active:
            stmt = stmt.where(ProviderPriceListConfig.is_active.is_(True))
        stmt = stmt.order_by(ProviderPriceListConfig.id.asc())
        result = await session.execute(stmt)
        configs = result.scalars().all()
        return configs

    async def get_by_id(
        self, config_id: int, session: AsyncSession
    ) -> Optional[ProviderPriceListConfig]:
        result = await session.execute(
            select(ProviderPriceListConfig).where(
                ProviderPriceListConfig.id == config_id
            )
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        provider_id: int,
        config_in: ProviderPriceListConfigCreate,
        session: AsyncSession,
        **kwargs,
    ) -> ProviderPriceListConfig:
        await crud_provider.get_by_id(provider_id=provider_id, session=session)
        new_config = ProviderPriceListConfig(
            provider_id=provider_id, **config_in.model_dump(exclude_unset=True)
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

        REQUIRED = {'start_row', 'oem_col', 'qty_col', 'price_col'}
        for field, value in update_data.items():
            if field in REQUIRED and value is None:
                continue
            setattr(db_obj, field, value)

        session.add(db_obj)
        await session.commit()
        await session.refresh(db_obj)
        return db_obj


crud_provider_pricelist_config = CRUDProviderPriceListConfig(
    ProviderPriceListConfig
)


class CRUDSupplierResponseConfig(
    CRUDBase[
        SupplierResponseConfig,
        SupplierResponseConfigCreate,
        SupplierResponseConfigUpdate,
    ]
):
    async def get_configs(
        self,
        provider_id: int,
        session: AsyncSession,
        only_active: bool = False,
    ) -> List[SupplierResponseConfig]:
        stmt = select(SupplierResponseConfig).where(
            SupplierResponseConfig.provider_id == provider_id
        ).options(
            selectinload(SupplierResponseConfig.inbox_email_account)
        )
        if only_active:
            stmt = stmt.where(SupplierResponseConfig.is_active.is_(True))
        stmt = stmt.order_by(SupplierResponseConfig.id.asc())
        result = await session.execute(stmt)
        return result.scalars().all()

    async def get_by_id(
        self,
        config_id: int,
        session: AsyncSession,
    ) -> Optional[SupplierResponseConfig]:
        result = await session.execute(
            select(SupplierResponseConfig)
            .options(selectinload(SupplierResponseConfig.inbox_email_account))
            .where(SupplierResponseConfig.id == config_id)
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        provider_id: int,
        config_in: SupplierResponseConfigCreate,
        session: AsyncSession,
        **kwargs,
    ) -> SupplierResponseConfig:
        await crud_provider.get_by_id(provider_id=provider_id, session=session)
        new_config = SupplierResponseConfig(
            provider_id=provider_id,
            **config_in.model_dump(exclude_unset=True),
        )
        session.add(new_config)
        await session.commit()
        await session.refresh(new_config)
        return new_config

    async def update(
        self,
        db_obj: SupplierResponseConfig,
        obj_in: Union[SupplierResponseConfigUpdate, Dict[str, Any]],
        session: AsyncSession,
        **kwargs,
    ) -> SupplierResponseConfig:
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


crud_supplier_response_config = CRUDSupplierResponseConfig(
    SupplierResponseConfig
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


class CRUDCustomerPriceListSource(
    CRUDBase[
        CustomerPriceListSource,
        CustomerPriceListSourceCreate,
        CustomerPriceListSourceUpdate,
    ]
):
    @staticmethod
    def _normalize_threshold_fields(
        update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        normalized = dict(update_data)
        for field in (
            'min_price',
            'max_price',
            'min_quantity',
            'max_quantity',
        ):
            if field not in normalized:
                continue
            value = normalized.get(field)
            if value is None or value == '':
                normalized[field] = None
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if numeric <= 0:
                normalized[field] = None
        return normalized

    async def get_by_id(
        self, source_id: int, session: AsyncSession
    ) -> Optional[CustomerPriceListSource]:
        result = await session.execute(
            select(CustomerPriceListSource)
            .options(
                selectinload(
                    CustomerPriceListSource.provider_config
                ).selectinload(
                    ProviderPriceListConfig.provider
                )
            )
            .where(CustomerPriceListSource.id == source_id)
        )
        return result.scalar_one_or_none()

    async def get_by_config_id(
        self, config_id: int, session: AsyncSession
    ) -> List[CustomerPriceListSource]:
        result = await session.execute(
            select(CustomerPriceListSource)
            .options(
                selectinload(
                    CustomerPriceListSource.provider_config
                ).selectinload(
                    ProviderPriceListConfig.provider
                )
            )
            .where(CustomerPriceListSource.customer_config_id == config_id)
            .order_by(CustomerPriceListSource.id.asc())
        )
        return result.scalars().all()

    async def get_by_config_and_provider_config(
        self,
        config_id: int,
        provider_config_id: int,
        session: AsyncSession,
    ) -> Optional[CustomerPriceListSource]:
        result = await session.execute(
            select(CustomerPriceListSource).where(
                CustomerPriceListSource.customer_config_id == config_id,
                CustomerPriceListSource.provider_config_id
                == provider_config_id,
            )
        )
        return result.scalar_one_or_none()

    async def create_source(
        self,
        config_id: int,
        source_in: CustomerPriceListSourceCreate,
        session: AsyncSession,
    ) -> CustomerPriceListSource:
        create_data = self._normalize_threshold_fields(
            source_in.model_dump(exclude_unset=True)
        )
        new_source = CustomerPriceListSource(
            customer_config_id=config_id,
            **create_data,
        )
        session.add(new_source)
        await session.commit()
        await session.refresh(new_source)
        return new_source

    async def update_source(
        self,
        db_obj: CustomerPriceListSource,
        obj_in: Union[CustomerPriceListSourceUpdate, Dict[str, Any]],
        session: AsyncSession,
    ) -> CustomerPriceListSource:
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)
        update_data = self._normalize_threshold_fields(update_data)

        for field, value in update_data.items():
            setattr(db_obj, field, value)

        session.add(db_obj)
        await session.commit()
        await session.refresh(db_obj)
        return db_obj


crud_customer_pricelist_source = CRUDCustomerPriceListSource(
    CustomerPriceListSource
)


async def get_last_uid(
    provider_id: int,
    session: AsyncSession,
    provider_config_id: int | None = None,
    folder: str | None = None,
) -> int:
    if provider_config_id is not None:
        result = await session.execute(
            select(ProviderConfigLastEmailUID).where(
                ProviderConfigLastEmailUID.provider_config_id
                == provider_config_id
            )
        )
        record = result.scalar_one_or_none()
        if record:
            if folder:
                folder_uids = getattr(record, 'folder_last_uids', None) or {}
                folder_uid = folder_uids.get(str(folder))
                if folder_uid is not None:
                    try:
                        return int(folder_uid)
                    except (TypeError, ValueError):
                        pass
            return record.last_uid

    result = await session.execute(
        select(ProviderLastEmailUID).where(
            ProviderLastEmailUID.provider_id == provider_id
        )
    )
    record = result.scalar_one_or_none()
    if record:
        if folder:
            folder_uids = getattr(record, 'folder_last_uids', None) or {}
            folder_uid = folder_uids.get(str(folder))
            if folder_uid is not None:
                try:
                    return int(folder_uid)
                except (TypeError, ValueError):
                    pass
        return record.last_uid
    return 0


async def set_last_uid(
    provider_id: int,
    last_uid: int,
    session: AsyncSession,
    provider_config_id: int | None = None,
    folder: str | None = None,
):
    if provider_config_id is not None:
        result = await session.execute(
            select(ProviderConfigLastEmailUID).where(
                ProviderConfigLastEmailUID.provider_config_id
                == provider_config_id
            )
        )
        record = result.scalar_one_or_none()

        if record:
            record.last_uid = last_uid
        else:
            record = ProviderConfigLastEmailUID(
                provider_config_id=provider_config_id,
                last_uid=last_uid,
            )
            session.add(record)
        if folder:
            folder_uids = dict(getattr(record, 'folder_last_uids', None) or {})
            folder_uids[str(folder)] = int(last_uid)
            record.folder_last_uids = folder_uids
            record.last_uid = max(int(record.last_uid or 0), int(last_uid))

        await session.commit()
        return

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
    if folder:
        folder_uids = dict(getattr(record, 'folder_last_uids', None) or {})
        folder_uids[str(folder)] = int(last_uid)
        record.folder_last_uids = folder_uids
        record.last_uid = max(int(record.last_uid or 0), int(last_uid))

    await session.commit()
