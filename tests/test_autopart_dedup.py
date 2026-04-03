from datetime import date

import pytest
from sqlalchemy import select

from dz_fastapi.crud.brand import brand_crud
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.models.partner import (CustomerOrder, CustomerOrderItem,
                                       CustomerPriceList,
                                       CustomerPriceListAutoPartAssociation,
                                       PriceList, PriceListAutoPartAssociation)
from dz_fastapi.services.autopart_dedup import \
    canonicalize_autoparts_by_brand_synonyms


@pytest.mark.asyncio
async def test_get_brand_by_name_or_none_resolves_main_brand_across_chain(
    test_session,
):
    mercedes_benz = Brand(name='MERCEDES-BENZ', main_brand=True)
    mercedes = Brand(name='MERCEDES')
    mb = Brand(name='MB')
    test_session.add_all([mercedes_benz, mercedes, mb])
    await test_session.flush()

    await test_session.execute(
        brand_synonyms.insert(),
        [
            {'brand_id': mb.id, 'synonym_id': mercedes.id},
            {'brand_id': mercedes.id, 'synonym_id': mb.id},
            {'brand_id': mercedes.id, 'synonym_id': mercedes_benz.id},
            {'brand_id': mercedes_benz.id, 'synonym_id': mercedes.id},
        ],
    )
    await test_session.commit()

    resolved = await brand_crud.get_brand_by_name_or_none('MB', test_session)

    assert resolved is not None
    assert resolved.id == mercedes_benz.id
    assert resolved.name == 'MERCEDES-BENZ'


@pytest.mark.asyncio
async def test_canonicalize_autoparts_by_brand_synonyms_merges_duplicates(
    test_session,
    created_providers,
    created_pricelist_config,
    created_customers,
):
    mercedes_benz = Brand(name='MERCEDES-BENZ', main_brand=True)
    mercedes = Brand(name='MERCEDES')
    mb = Brand(name='MB')
    test_session.add_all([mercedes_benz, mercedes, mb])
    await test_session.flush()

    await test_session.execute(
        brand_synonyms.insert(),
        [
            {'brand_id': mb.id, 'synonym_id': mercedes.id},
            {'brand_id': mercedes.id, 'synonym_id': mb.id},
            {'brand_id': mercedes.id, 'synonym_id': mercedes_benz.id},
            {'brand_id': mercedes_benz.id, 'synonym_id': mercedes.id},
        ],
    )
    await test_session.flush()

    source_autopart = AutoPart(
        name='BRACKET',
        brand_id=mb.id,
        oem_number='A0049905312',
    )
    target_autopart = AutoPart(
        name='BRACKET',
        brand_id=mercedes_benz.id,
        oem_number='A0049905312',
    )
    test_session.add_all([source_autopart, target_autopart])
    await test_session.flush()

    pricelist = PriceList(
        provider_id=created_providers[0].id,
        provider_config_id=created_pricelist_config.id,
        date=date.today(),
    )
    customer_pricelist = CustomerPriceList(
        customer_id=created_customers[0].id,
        date=date.today(),
        is_active=True,
    )
    customer_order = CustomerOrder(customer_id=created_customers[0].id)
    test_session.add_all([pricelist, customer_pricelist, customer_order])
    await test_session.flush()

    test_session.add_all(
        [
            PriceListAutoPartAssociation(
                pricelist_id=pricelist.id,
                autopart_id=target_autopart.id,
                quantity=10,
                price=297,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=pricelist.id,
                autopart_id=source_autopart.id,
                quantity=6,
                price=250,
                multiplicity=1,
            ),
            CustomerPriceListAutoPartAssociation(
                customerpricelist_id=customer_pricelist.id,
                autopart_id=target_autopart.id,
                quantity=10,
                price=305,
            ),
            CustomerPriceListAutoPartAssociation(
                customerpricelist_id=customer_pricelist.id,
                autopart_id=source_autopart.id,
                quantity=6,
                price=260,
            ),
            CustomerOrderItem(
                order_id=customer_order.id,
                row_index=1,
                oem='A0049905312',
                brand='MB',
                requested_qty=1,
                requested_price=260,
                autopart_id=source_autopart.id,
            ),
        ]
    )
    await test_session.commit()

    summary = await canonicalize_autoparts_by_brand_synonyms(test_session)
    await test_session.commit()

    assert summary['autoparts_merged'] == 1

    refreshed_target = await test_session.get(AutoPart, target_autopart.id)
    deleted_source = await test_session.get(AutoPart, source_autopart.id)
    assert refreshed_target is not None
    assert refreshed_target.brand_id == mercedes_benz.id
    assert deleted_source is None

    pricelist_rows = (
        await test_session.execute(
            select(PriceListAutoPartAssociation).where(
                PriceListAutoPartAssociation.pricelist_id == pricelist.id
            )
        )
    ).scalars().all()
    assert len(pricelist_rows) == 1
    assert pricelist_rows[0].autopart_id == target_autopart.id
    assert float(pricelist_rows[0].price) == 250.0
    assert pricelist_rows[0].quantity == 6

    customer_pricelist_rows = (
        await test_session.execute(
            select(CustomerPriceListAutoPartAssociation).where(
                CustomerPriceListAutoPartAssociation.customerpricelist_id
                == customer_pricelist.id
            )
        )
    ).scalars().all()
    assert len(customer_pricelist_rows) == 1
    assert customer_pricelist_rows[0].autopart_id == target_autopart.id
    assert float(customer_pricelist_rows[0].price) == 260.0
    assert customer_pricelist_rows[0].quantity == 6

    customer_order_items = (
        await test_session.execute(
            select(CustomerOrderItem).where(
                CustomerOrderItem.order_id == customer_order.id
            )
        )
    ).scalars().all()
    assert len(customer_order_items) == 1
    assert customer_order_items[0].autopart_id == target_autopart.id
