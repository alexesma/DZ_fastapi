from datetime import date, timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.order_status_mapping import (
    ExternalStatusMapping,
    ExternalStatusMatchMode,
    ExternalStatusUnmapped,
)
from dz_fastapi.models.partner import (
    ORDER_TRACKING_SOURCE,
    SUPPLIER_ORDER_STATUS,
    TYPE_ORDER_ITEM_STATUS,
    TYPE_STATUS_ORDER,
    Customer,
    Order,
    OrderItem,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
    SupplierOrder,
    SupplierOrderItem,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.order_status_mapping import EXTERNAL_STATUS_SOURCE_DRAGONZAP
from dz_fastapi.services.placed_orders import (
    cleanup_old_tracking_history,
    get_tracking_history_insights,
    list_tracking_history,
    sync_site_tracking_statuses,
    update_tracking_item,
)


async def _create_user(test_session, email="manager@example.com"):
    user = User(
        email=email,
        password_hash=get_password_hash("secret123"),
        role=UserRole.MANAGER,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


async def _create_status_mapping(
    test_session,
    *,
    raw_status: str,
    order_status: str,
    item_status: str,
    provider_id: int | None = None,
):
    mapping = ExternalStatusMapping(
        source_key=EXTERNAL_STATUS_SOURCE_DRAGONZAP,
        provider_id=provider_id,
        raw_status=raw_status,
        normalized_status=raw_status,
        match_mode=ExternalStatusMatchMode.CONTAINS,
        internal_order_status=order_status,
        internal_item_status=item_status,
        priority=100,
        is_active=True,
    )
    test_session.add(mapping)
    await test_session.commit()
    await test_session.refresh(mapping)
    return mapping


@pytest.mark.asyncio
async def test_list_tracking_history_returns_site_and_supplier_rows(
    test_session,
):
    user = await _create_user(test_session)
    provider = Provider(
        name="Provider One",
        email_contact="provider@example.com",
        email_incoming_price="prices@example.com",
        type_prices="Wholesale",
    )
    customer = Customer(
        name="Customer One",
        email_contact="customer@example.com",
        email_outgoing_price="out@example.com",
        type_prices="Wholesale",
    )
    brand = Brand(name="TEST")
    test_session.add_all([provider, customer, brand])
    await test_session.flush()

    autopart = AutoPart(
        name="Widget",
        brand_id=brand.id,
        oem_number="OEM123",
    )
    test_session.add(autopart)
    await test_session.flush()

    site_order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        created_by_user_id=user.id,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        created_by_user_id=user.id,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add_all([site_order, supplier_order])
    await test_session.flush()

    test_session.add_all(
        [
            OrderItem(
                order_id=site_order.id,
                autopart_id=autopart.id,
                oem_number="OEM123",
                brand_name="TEST",
                autopart_name="Widget",
                quantity=2,
                price=100,
                min_delivery_day=2,
                max_delivery_day=3,
                status=TYPE_ORDER_ITEM_STATUS.SENT,
            ),
            SupplierOrderItem(
                supplier_order_id=supplier_order.id,
                autopart_id=autopart.id,
                oem_number="OEM123",
                brand_name="TEST",
                autopart_name="Widget",
                quantity=1,
                price=90,
            ),
        ]
    )
    await test_session.commit()

    rows = await list_tracking_history(
        test_session,
        oem_number="OEM123",
    )

    assert len(rows) == 2
    assert {row["source_type"] for row in rows} == {"site", "supplier"}
    assert all(row["ordered_by_email"] == user.email for row in rows)


@pytest.mark.asyncio
async def test_list_tracking_history_includes_cross_oem_rows(
    test_session,
):
    provider = Provider(
        name="Provider Cross",
        email_contact="provider-cross@example.com",
        email_incoming_price="prices-cross@example.com",
        type_prices="Wholesale",
    )
    brand = Brand(name="CROSS")
    test_session.add_all([provider, brand])
    await test_session.flush()

    source_autopart = AutoPart(
        name="Source part",
        brand_id=brand.id,
        oem_number="OEM123",
    )
    cross_autopart = AutoPart(
        name="Cross part",
        brand_id=brand.id,
        oem_number="OEMCROSS",
    )
    test_session.add_all([source_autopart, cross_autopart])
    await test_session.flush()

    test_session.add(
        AutoPartCross(
            source_autopart_id=source_autopart.id,
            cross_brand_id=brand.id,
            cross_oem_number="OEMCROSS",
            cross_autopart_id=cross_autopart.id,
        )
    )

    supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add(supplier_order)
    await test_session.flush()

    test_session.add(
        SupplierOrderItem(
            supplier_order_id=supplier_order.id,
            autopart_id=cross_autopart.id,
            oem_number="OEMCROSS",
            brand_name="CROSS",
            autopart_name="Cross part",
            quantity=2,
            price=77,
        )
    )
    await test_session.commit()

    exact_rows = await list_tracking_history(
        test_session,
        oem_number="OEM123",
    )
    cross_rows = await list_tracking_history(
        test_session,
        oem_number="OEM123",
        include_crosses=True,
    )

    assert exact_rows == []
    assert len(cross_rows) == 1
    assert cross_rows[0]["oem_number"] == "OEMCROSS"
    assert cross_rows[0]["source_type"] == "supplier"


@pytest.mark.asyncio
async def test_get_tracking_history_insights_builds_price_and_own_stock_summary(
    test_session,
):
    today = now_moscow().date()
    provider_exact = Provider(
        name="Exact Provider",
        email_contact="exact@example.com",
        email_incoming_price="exact-prices@example.com",
        type_prices="Wholesale",
    )
    provider_cross = Provider(
        name="Cross Provider",
        email_contact="cross@example.com",
        email_incoming_price="cross-prices@example.com",
        type_prices="Wholesale",
        is_own_price=True,
    )
    customer = Customer(
        name="Insight Customer",
        email_contact="insight-customer@example.com",
        email_outgoing_price="insight-out@example.com",
        type_prices="Wholesale",
    )
    brand = Brand(name="INSIGHT")
    test_session.add_all([provider_exact, provider_cross, customer, brand])
    await test_session.flush()

    source_autopart = AutoPart(
        name="Insight source",
        brand_id=brand.id,
        oem_number="OEM123",
    )
    cross_autopart = AutoPart(
        name="Insight cross",
        brand_id=brand.id,
        oem_number="OEMCROSS",
    )
    test_session.add_all([source_autopart, cross_autopart])
    await test_session.flush()
    test_session.add(
        AutoPartCross(
            source_autopart_id=source_autopart.id,
            cross_brand_id=brand.id,
            cross_oem_number="OEMCROSS",
            cross_autopart_id=cross_autopart.id,
        )
    )
    await test_session.flush()

    exact_cfg = ProviderPriceListConfig(
        provider_id=provider_exact.id,
        start_row=1,
        oem_col=1,
        qty_col=2,
        price_col=3,
        name_price="Exact main",
        min_delivery_day=3,
        max_delivery_day=5,
    )
    own_cfg = ProviderPriceListConfig(
        provider_id=provider_cross.id,
        start_row=1,
        oem_col=1,
        qty_col=2,
        price_col=3,
        name_price="Own stock",
        min_delivery_day=0,
        max_delivery_day=0,
    )
    test_session.add_all([exact_cfg, own_cfg])
    await test_session.flush()

    exact_pricelist = PriceList(
        provider_id=provider_exact.id,
        provider_config_id=exact_cfg.id,
        date=today,
    )
    own_pl_1 = PriceList(
        provider_id=provider_cross.id,
        provider_config_id=own_cfg.id,
        date=today - timedelta(days=100),
    )
    own_pl_2 = PriceList(
        provider_id=provider_cross.id,
        provider_config_id=own_cfg.id,
        date=today - timedelta(days=40),
    )
    own_pl_3 = PriceList(
        provider_id=provider_cross.id,
        provider_config_id=own_cfg.id,
        date=today,
    )
    test_session.add_all([exact_pricelist, own_pl_1, own_pl_2, own_pl_3])
    await test_session.flush()

    test_session.add_all(
        [
            PriceListAutoPartAssociation(
                pricelist_id=exact_pricelist.id,
                autopart_id=source_autopart.id,
                quantity=5,
                price=100,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=own_pl_1.id,
                autopart_id=cross_autopart.id,
                quantity=20,
                price=85,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=own_pl_2.id,
                autopart_id=cross_autopart.id,
                quantity=12,
                price=82,
                multiplicity=1,
            ),
            PriceListAutoPartAssociation(
                pricelist_id=own_pl_3.id,
                autopart_id=cross_autopart.id,
                quantity=7,
                price=80,
                multiplicity=1,
            ),
        ]
    )

    site_order_exact = Order(
        provider_id=provider_exact.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        created_at=now_moscow() - timedelta(days=15),
        status=TYPE_STATUS_ORDER.SHIPPED,
    )
    site_order_cross = Order(
        provider_id=provider_cross.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        created_at=now_moscow() - timedelta(days=10),
        status=TYPE_STATUS_ORDER.SHIPPED,
    )
    supplier_order = SupplierOrder(
        provider_id=provider_exact.id,
        source_type=ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        created_at=now_moscow() - timedelta(days=5),
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add_all([site_order_exact, site_order_cross, supplier_order])
    await test_session.flush()

    test_session.add_all(
        [
            OrderItem(
                order_id=site_order_exact.id,
                autopart_id=source_autopart.id,
                oem_number="OEM123",
                brand_name="INSIGHT",
                autopart_name="Insight source",
                quantity=1,
                received_quantity=1,
                price=90,
                received_at=site_order_exact.created_at + timedelta(days=1),
                status=TYPE_ORDER_ITEM_STATUS.DELIVERED,
            ),
            OrderItem(
                order_id=site_order_cross.id,
                autopart_id=cross_autopart.id,
                oem_number="OEMCROSS",
                brand_name="INSIGHT",
                autopart_name="Insight cross",
                quantity=3,
                received_quantity=3,
                price=70,
                received_at=site_order_cross.created_at + timedelta(days=4),
                status=TYPE_ORDER_ITEM_STATUS.DELIVERED,
            ),
            SupplierOrderItem(
                supplier_order_id=supplier_order.id,
                autopart_id=source_autopart.id,
                oem_number="OEM123",
                brand_name="INSIGHT",
                autopart_name="Insight source",
                quantity=2,
                received_quantity=1,
                price=110,
            ),
        ]
    )
    await test_session.commit()

    summary = await get_tracking_history_insights(
        test_session,
        oem_number="OEM123",
        own_provider_config_id=own_cfg.id,
    )

    assert summary["cross_oem_numbers"] == ["OEMCROSS"]
    assert summary["exact_min_offer"]["provider_id"] == provider_exact.id
    assert float(summary["exact_min_offer"]["price"]) == 100.0
    assert summary["min_offer_with_crosses"]["provider_id"] == provider_cross.id
    assert summary["min_offer_with_crosses"]["oem_number"] == "OEMCROSS"
    assert float(summary["min_offer_with_crosses"]["price"]) == 80.0
    assert summary["order_count_last_year"] == 3
    assert summary["total_ordered_quantity_last_year"] == 6
    assert summary["total_received_quantity_last_year"] == 5
    assert summary["unique_suppliers_last_year"] == 2
    assert summary["fill_rate_percent"] == pytest.approx(83.3, abs=0.1)
    assert float(summary["historical_min_price_exact"]) == 90.0
    assert float(summary["historical_min_price_with_crosses"]) == 70.0
    assert summary["average_actual_lead_days"] == pytest.approx(2.5, abs=0.1)
    assert len(summary["own_price_configs"]) == 1
    assert summary["own_price_configs"][0]["id"] == own_cfg.id
    assert summary["own_price_analysis"]["provider_config_id"] == own_cfg.id
    assert summary["own_price_analysis"]["current_quantity"] == 7
    assert float(summary["own_price_analysis"]["latest_price"]) == 80.0
    assert summary["own_price_analysis"]["arrivals_last_30_days"] == 4
    assert summary["own_price_analysis"]["arrivals_last_90_days"] == 4
    assert summary["own_price_analysis"]["arrivals_last_365_days"] == 4
    assert summary["own_price_analysis"]["sold_last_30_days"] == 9
    assert summary["own_price_analysis"]["sold_last_90_days"] == 17
    assert summary["own_price_analysis"]["sold_last_365_days"] == 17
    assert (
        summary["own_price_analysis"]["average_daily_decrease_30_days"]
        == pytest.approx(0.30, abs=0.01)
    )
    assert summary["own_price_analysis"]["estimated_days_left_30_days"] == 23


@pytest.mark.asyncio
async def test_update_tracking_item_updates_site_status_and_received_qty(
    test_session,
):
    provider = Provider(
        name="Provider Two",
        email_contact="provider2@example.com",
        email_incoming_price="prices2@example.com",
        type_prices="Wholesale",
    )
    customer = Customer(
        name="Customer Two",
        email_contact="customer2@example.com",
        email_outgoing_price="out2@example.com",
        type_prices="Wholesale",
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    test_session.add(order)
    await test_session.flush()

    item = OrderItem(
        order_id=order.id,
        oem_number="OEM555",
        brand_name="TEST",
        autopart_name="Part",
        quantity=2,
        price=50,
        status=TYPE_ORDER_ITEM_STATUS.SENT,
    )
    test_session.add(item)
    await test_session.commit()
    await test_session.refresh(order)
    await test_session.refresh(item)

    result = await update_tracking_item(
        test_session,
        source_type="site",
        item_id=item.id,
        status="SHIPPED",
        received_quantity=2,
    )

    await test_session.refresh(order)
    await test_session.refresh(item)
    assert result["status"] == "SHIPPED"
    assert order.status == TYPE_STATUS_ORDER.SHIPPED
    assert item.status == TYPE_ORDER_ITEM_STATUS.DELIVERED
    assert item.received_quantity == 2
    assert item.received_at is not None


@pytest.mark.asyncio
async def test_cleanup_old_tracking_history_keeps_recent_and_customer_flow(
    test_session,
):
    provider = Provider(
        name="Provider Three",
        email_contact="provider3@example.com",
        email_incoming_price="prices3@example.com",
        type_prices="Wholesale",
    )
    customer = Customer(
        name="Customer Three",
        email_contact="customer3@example.com",
        email_outgoing_price="out3@example.com",
        type_prices="Wholesale",
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    old_time = now_moscow() - timedelta(days=400)

    old_site_order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        created_at=old_time,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    old_supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.SEARCH_OFFERS.value,
        created_at=old_time,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    keep_supplier_order = SupplierOrder(
        provider_id=provider.id,
        source_type=ORDER_TRACKING_SOURCE.CUSTOMER_ORDER.value,
        created_at=old_time,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add_all([old_site_order, old_supplier_order, keep_supplier_order])
    await test_session.flush()
    test_session.add_all(
        [
            OrderItem(
                order_id=old_site_order.id,
                oem_number="OLD1",
                brand_name="TEST",
                autopart_name="Old site",
                quantity=1,
                price=10,
                status=TYPE_ORDER_ITEM_STATUS.SENT,
            ),
            SupplierOrderItem(
                supplier_order_id=old_supplier_order.id,
                oem_number="OLD2",
                brand_name="TEST",
                autopart_name="Old supplier",
                quantity=1,
                price=10,
            ),
            SupplierOrderItem(
                supplier_order_id=keep_supplier_order.id,
                oem_number="KEEP",
                brand_name="TEST",
                autopart_name="Keep supplier",
                quantity=1,
                price=10,
            ),
        ]
    )
    await test_session.commit()

    summary = await cleanup_old_tracking_history(test_session)

    assert summary["orders_deleted"] == 1
    assert summary["supplier_orders_deleted"] == 1

    remaining_rows = await list_tracking_history(test_session, limit=50)
    assert remaining_rows == []


@pytest.mark.asyncio
async def test_sync_site_tracking_statuses_updates_order_from_site(
    test_session,
    monkeypatch,
):
    provider = Provider(
        name="Provider Four",
        email_contact="provider4@example.com",
        email_incoming_price="prices4@example.com",
        type_prices="Wholesale",
    )
    customer = Customer(
        name="Customer Four",
        email_contact="customer4@example.com",
        email_outgoing_price="out4@example.com",
        type_prices="Wholesale",
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    test_session.add(order)
    await test_session.flush()

    item = OrderItem(
        order_id=order.id,
        oem_number="OEMSYNC",
        brand_name="TEST",
        autopart_name="Part sync",
        quantity=2,
        price=55,
        tracking_uuid="site-sync-uuid",
        status=TYPE_ORDER_ITEM_STATUS.SENT,
    )
    test_session.add(item)
    await test_session.commit()
    await _create_status_mapping(
        test_session,
        raw_status="arrived",
        order_status="ARRIVED",
        item_status="IN_PROGRESS",
    )

    class FakeDZSiteClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get_order_items(self, **kwargs):
            assert kwargs["search_comment_eq"] == "site-sync-uuid"
            return {
                "data": [
                    {
                        "comment": "site-sync-uuid",
                        "status_code": "arrived",
                    }
                ]
            }

    monkeypatch.setattr(
        "dz_fastapi.services.placed_orders.SITE_API_KEY",
        "test-key",
    )
    monkeypatch.setattr(
        "dz_fastapi.services.placed_orders.DZSiteClient",
        FakeDZSiteClient,
    )

    summary = await sync_site_tracking_statuses(
        test_session,
        oem_number="OEMSYNC",
    )

    await test_session.refresh(order)
    await test_session.refresh(item)
    assert summary["checked"] == 1
    assert summary["updated"] == 1
    assert order.status == TYPE_STATUS_ORDER.ARRIVED
    assert item.status == TYPE_ORDER_ITEM_STATUS.IN_PROGRESS
    assert item.received_quantity == 2
    assert item.received_at is not None
    assert item.external_status_raw == "arrived"
    assert item.external_status_mapping_id is not None


@pytest.mark.asyncio
async def test_sync_site_tracking_statuses_collects_unmapped_statuses(
    test_session,
    monkeypatch,
):
    provider = Provider(
        name="Provider Unknown",
        email_contact="provider-unknown@example.com",
        email_incoming_price="prices-unknown@example.com",
        type_prices="Wholesale",
    )
    customer = Customer(
        name="Customer Unknown",
        email_contact="customer-unknown@example.com",
        email_outgoing_price="out-unknown@example.com",
        type_prices="Wholesale",
    )
    test_session.add_all([provider, customer])
    await test_session.flush()

    order = Order(
        provider_id=provider.id,
        customer_id=customer.id,
        source_type=ORDER_TRACKING_SOURCE.DRAGONZAP_SEARCH.value,
        status=TYPE_STATUS_ORDER.ORDERED,
    )
    test_session.add(order)
    await test_session.flush()

    item = OrderItem(
        order_id=order.id,
        oem_number="OEM-UNKNOWN",
        brand_name="TEST",
        autopart_name="Part unknown",
        quantity=1,
        price=10,
        tracking_uuid="unknown-sync-uuid",
        status=TYPE_ORDER_ITEM_STATUS.SENT,
    )
    test_session.add(item)
    await test_session.commit()

    class FakeDZSiteClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get_order_items(self, **kwargs):
            return {
                "data": [
                    {
                        "comment": "unknown-sync-uuid",
                        "status_code": "manual review stage",
                        "status_name": "Manual Review",
                    }
                ]
            }

    monkeypatch.setattr(
        "dz_fastapi.services.placed_orders.SITE_API_KEY",
        "test-key",
    )
    monkeypatch.setattr(
        "dz_fastapi.services.placed_orders.DZSiteClient",
        FakeDZSiteClient,
    )

    summary = await sync_site_tracking_statuses(test_session)

    await test_session.refresh(order)
    await test_session.refresh(item)
    unresolved = await test_session.get(ExternalStatusUnmapped, 1)

    assert summary["checked"] == 1
    assert summary["updated"] == 1
    assert order.status == TYPE_STATUS_ORDER.ORDERED
    assert item.status == TYPE_ORDER_ITEM_STATUS.SENT
    assert item.external_status_raw == "manual review stage | Manual Review"
    assert item.external_status_mapping_id is None
    assert unresolved is not None
    assert unresolved.source_key == EXTERNAL_STATUS_SOURCE_DRAGONZAP
    assert unresolved.is_resolved is False
