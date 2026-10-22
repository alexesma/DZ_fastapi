from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    CUSTOMER_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderConfig,
    CustomerOrderItem,
    CustomerPriceList,
    CustomerPriceListConfig,
    CustomerPriceListExportRow,
    SupplierOrderItem,
)
from dz_fastapi.services import customer_orders as service
from dz_fastapi.services.customer_order_routing import compare_order_pricelists


async def setup_routes(session, customer):
    configs, publications = [], []
    for index in range(2):
        price_config = CustomerPriceListConfig(customer_id=customer.id, name=f"Routing {index}")
        session.add(price_config)
        await session.flush()
        config = CustomerOrderConfig(
            customer_id=customer.id,
            pricelist_config_id=price_config.id,
            order_email="buyer@example.com",
            order_start_row=1,
            oem_col=0,
            brand_col=1,
            qty_col=2,
            price_col=3,
        )
        session.add(config)
        publication = CustomerPriceList(
            customer_id=customer.id,
            customer_config_id=price_config.id,
            published_at=now_moscow() - timedelta(hours=2),
        )
        session.add(publication)
        await session.flush()
        session.add(
            CustomerPriceListExportRow(
                customer_pricelist_id=publication.id,
                advertised_oem="ABC1",
                normalized_oem="ABC1",
                advertised_brand="HAVAL",
                normalized_brand="HAVAL",
                price=100 + index * 10,
                quantity=50,
            )
        )
        configs.append(config)
        publications.append(publication)
    await session.commit()
    return configs, publications


def order_row(price=110, oem="ABC1"):
    return service.ParsedOrderRow(0, oem, "haval", "Деталь", 2, price)


def mock_offers(monkeypatch, configs, quantities=(90, 96), prices=(100, 100)):
    async def build(session, price_config, *args, **kwargs):
        index = next(i for i, c in enumerate(configs) if c.pricelist_config_id == price_config.id)
        return {
            ("ABC1", "HAVAL"): service.OfferRow(
                autopart_id=1,
                provider_id=1,
                provider_config_id=None,
                quantity=quantities[index],
                price=prices[index],
                supplier_price=80,
                is_own_price=False,
            )
        }

    monkeypatch.setattr(service, "_build_current_offers", build)


async def compare(session, configs, rows=None, candidates=None, received_at=None):
    return await compare_order_pricelists(
        session,
        candidates
        or [
            {
                "config": c,
                "rows": rows
                or [
                    service.ParsedOrderRow(
                        0,
                        "ABC1",
                        "haval",
                        "Деталь",
                        100,
                        110,
                    )
                ],
            }
            for c in configs
        ],
        received_at or now_moscow(),
        service._normalize_key,
        service._estimate_customer_order_rejections,
    )


@pytest.mark.asyncio
async def test_smaller_rejection_wins_even_without_exact_price_match(
    test_session,
    created_customers,
    monkeypatch,
):
    configs, snapshots = await setup_routes(test_session, created_customers[0])
    mock_offers(monkeypatch, configs)
    warning = AsyncMock()
    monkeypatch.setattr(service, "_send_price_warning", warning)
    result = await compare(test_session, configs)
    assert result["basis"] == "lowest_rejection"
    assert result["selected_order_config_id"] == configs[1].id
    assert result["pricelist_id"] == snapshots[1].id
    assert [c["rejected_pct"] for c in result["candidates"]] == [10, 4]
    assert result["candidates"][0]["prices"] == 0
    assert result["candidates"][1]["rejection_metric"] == "amount"
    assert not test_session.new
    assert not test_session.dirty
    assert not (await test_session.scalars(select(CustomerOrder))).all()
    warning.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("quantities", [(90, 90), (100, 100), (0, 0)])
async def test_equal_rejections_require_manual_selection(
    test_session,
    created_customers,
    monkeypatch,
    quantities,
):
    configs, _ = await setup_routes(test_session, created_customers[0])
    mock_offers(monkeypatch, configs, quantities)
    result = await compare(test_session, configs)
    assert result["basis"] == "review_required"
    assert result["selected_order_config_id"] is None
    assert result["review_reason"]


@pytest.mark.asyncio
async def test_only_latest_publication_is_used_without_seven_day_limit(
    test_session,
    created_customers,
    monkeypatch,
):
    configs, snapshots = await setup_routes(test_session, created_customers[0])
    mock_offers(monkeypatch, configs)
    for snapshot in snapshots:
        snapshot.published_at = now_moscow() - timedelta(days=12)
    newer = CustomerPriceList(
        customer_id=created_customers[0].id,
        customer_config_id=configs[1].pricelist_config_id,
        published_at=now_moscow() - timedelta(days=10),
    )
    test_session.add(newer)
    await test_session.flush()
    test_session.add(
        CustomerPriceListExportRow(
            customer_pricelist_id=newer.id,
            advertised_oem="ABC1",
            normalized_oem="ABC1",
            advertised_brand="HAVAL",
            normalized_brand="HAVAL",
            price=120,
            quantity=50,
        )
    )
    await test_session.flush()
    # Even an older email compares current latest publications, not older matches.
    result = await compare(test_session, configs, received_at=now_moscow() - timedelta(days=11))
    assert result["pricelist_id"] == newer.id
    assert result["candidates"][1]["prices"] == 0
    loaded = await service._load_latest_customer_pricelist(
        test_session,
        configs[1].customer_id,
        configs[1].pricelist_config_id,
        snapshot_id=result["pricelist_id"],
    )
    assert loaded.id == newer.id


@pytest.mark.asyncio
async def test_current_offer_can_fulfil_position_absent_from_export_snapshot(
    test_session,
    created_customers,
    monkeypatch,
):
    configs, snapshots = await setup_routes(test_session, created_customers[0])
    excluded_row = order_row(oem="EXCLUDED")
    current_offer = service.OfferRow(
        autopart_id=1,
        provider_id=1,
        provider_config_id=None,
        quantity=20,
        price=100,
        supplier_price=80,
        is_own_price=False,
    )
    monkeypatch.setattr(
        service,
        "_build_current_offers",
        AsyncMock(return_value={("EXCLUDED", "HAVAL"): current_offer}),
    )
    monkeypatch.setattr(
        service,
        "_load_confirmed_own_cross_aliases",
        AsyncMock(return_value=[]),
    )

    expected_prices, offers, _, _ = await service._prepare_customer_order_context(
        test_session,
        configs[0],
        [excluded_row],
        snapshot_id=snapshots[0].id,
    )

    assert expected_prices == {}
    assert offers == {("EXCLUDED", "HAVAL"): current_offer}
    plan = service._plan_customer_order_row(
        configs[0],
        excluded_row,
        offers[("EXCLUDED", "HAVAL")],
        expected_prices.get(("EXCLUDED", "HAVAL")),
        {},
    )
    assert plan.reject_reason is None
    assert plan.ship_qty == excluded_row.requested_qty
    overpriced = service._plan_customer_order_row(
        configs[0],
        order_row(price=60, oem="EXCLUDED"),
        current_offer,
        None,
        {},
    )
    assert overpriced.reject_reason == "PRICE_TOO_HIGH"


@pytest.mark.asyncio
async def test_imported_order_requires_a_published_pricelist(
    test_session,
    created_customers,
    monkeypatch,
):
    price_config = CustomerPriceListConfig(
        customer_id=created_customers[0].id,
        name="Never published",
    )
    test_session.add(price_config)
    await test_session.flush()
    config = CustomerOrderConfig(
        customer_id=created_customers[0].id,
        pricelist_config_id=price_config.id,
        order_email="buyer@example.com",
        order_start_row=1,
        oem_col=0,
        brand_col=1,
        qty_col=2,
        price_col=3,
    )
    test_session.add(config)
    await test_session.flush()
    monkeypatch.setattr(
        service,
        "_load_confirmed_own_cross_aliases",
        AsyncMock(return_value=[]),
    )

    with pytest.raises(ValueError, match="нет опубликованного прайса"):
        await service._prepare_customer_order_context(
            test_session,
            config,
            [order_row()],
            require_published_pricelist=True,
        )


@pytest.mark.asyncio
async def test_order_can_use_another_customers_published_pricelist(
    test_session,
    created_customers,
    monkeypatch,
):
    owner = created_customers[0]
    legal_entity = created_customers[1]
    reference_config = CustomerPriceListConfig(
        customer_id=owner.id,
        name="Motor shared price",
    )
    local_config = CustomerPriceListConfig(
        customer_id=legal_entity.id,
        name="Legal entity orders",
    )
    test_session.add_all([reference_config, local_config])
    await test_session.flush()
    publication = CustomerPriceList(
        customer_id=owner.id,
        customer_config_id=reference_config.id,
        published_at=now_moscow(),
    )
    test_session.add(publication)
    await test_session.flush()
    test_session.add(
        CustomerPriceListExportRow(
            customer_pricelist_id=publication.id,
            advertised_oem="ABC1",
            normalized_oem="ABC1",
            advertised_brand="HAVAL",
            normalized_brand="HAVAL",
            price=123,
            quantity=50,
        )
    )
    order_config = CustomerOrderConfig(
        customer_id=legal_entity.id,
        pricelist_config_id=local_config.id,
        reference_pricelist_config_id=reference_config.id,
        order_email="entity@example.com",
        order_start_row=1,
        oem_col=0,
        brand_col=1,
        qty_col=2,
        price_col=3,
    )
    test_session.add(order_config)
    await test_session.flush()
    current_offer = service.OfferRow(
        autopart_id=1,
        provider_id=1,
        provider_config_id=None,
        quantity=20,
        price=100,
        supplier_price=80,
        is_own_price=False,
    )
    monkeypatch.setattr(
        service,
        "_build_current_offers",
        AsyncMock(return_value={("ABC1", "HAVAL"): current_offer}),
    )
    monkeypatch.setattr(
        service,
        "_load_confirmed_own_cross_aliases",
        AsyncMock(return_value=[]),
    )

    expected_prices, offers, _, resolved_config = (
        await service._prepare_customer_order_context(
            test_session,
            order_config,
            [order_row(price=123)],
            require_published_pricelist=True,
        )
    )

    assert resolved_config.id == reference_config.id
    assert expected_prices == {("ABC1", "HAVAL"): 123.0}
    assert offers == {("ABC1", "HAVAL"): current_offer}


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["parser", "missing_snapshot", "different_rows"])
async def test_uncomparable_candidates_never_win_by_omitting_rows(
    test_session,
    created_customers,
    monkeypatch,
    problem,
):
    configs, snapshots = await setup_routes(test_session, created_customers[0])
    mock_offers(monkeypatch, configs)
    candidates = [{"config": c, "rows": [order_row()]} for c in configs]
    if problem == "parser":
        candidates[0]["parse_error"] = "Ошибка формата"
    elif problem == "different_rows":
        candidates[0]["rows"].append(order_row(oem="MISSING"))
    else:
        snapshots[0].published_at = None
        await test_session.flush()
    result = await compare(test_session, configs, candidates=candidates)
    assert result["basis"] == "review_required"


@pytest.mark.asyncio
@pytest.mark.parametrize("quantities,expected", [((90, 96), "PROCESSED"), ((90, 90), "ERROR")])
async def test_mail_import_routes_before_processing_and_blocks_retry(
    test_session,
    created_customers,
    async_client,
    monkeypatch,
    quantities,
    expected,
):
    configs, _ = await setup_routes(test_session, created_customers[0])
    mock_offers(monkeypatch, configs, quantities)
    account = EmailAccount(
        name="Routing inbox",
        email="routing@example.com",
        password="test",
        transport="smtp",
        imap_host="imap.example.com",
        purposes=["orders_in"],
        is_active=True,
    )
    test_session.add(account)
    await test_session.commit()
    attachment = SimpleNamespace(filename="order.csv", payload=b"ABC1,haval,100,110\n")
    msg = SimpleNamespace(
        from_="buyer@example.com",
        subject="Заказ 123",
        uid="10",
        text="",
        html="",
        attachments=[attachment],
        received_at=now_moscow(),
        folder_name="INBOX",
    )
    monkeypatch.setattr(service, "_fetch_order_messages", AsyncMock(return_value=[msg]))
    monkeypatch.setattr(service, "_save_order_source_file", AsyncMock())
    monkeypatch.setattr(
        service, "_load_order_source_file", AsyncMock(return_value=attachment.payload)
    )
    monkeypatch.setattr(service, "_send_order_import_notification", AsyncMock())
    handled = []

    async def complete(session, config, order, *args):
        handled.append(config.id)
        order.status = CUSTOMER_ORDER_STATUS.PROCESSED

    monkeypatch.setattr(service, "_complete_imported_order_processing", complete)
    await service.process_customer_orders(test_session, config_id=configs[0].id)
    orders = (await test_session.scalars(select(CustomerOrder))).all()
    assert len(orders) == 1
    order = orders[0]
    assert order.status.name == expected
    if expected == "PROCESSED":
        assert handled == [configs[1].id]
        assert order.order_config_id == configs[1].id
        assert order.external_payload["pricelist_routing"]["basis"] == "lowest_rejection"
    else:
        assert handled == []
        with pytest.raises(ValueError, match="Сначала выберите"):
            await service.retry_customer_order(test_session, order.id)
        response = await async_client.get(f"/customer-orders/{order.id}")
        assert response.status_code == 200, response.text
        assert response.json()["pricelist_routing"]["candidates"][0]["rejected_pct"] == 10
        response = await async_client.post(
            f"/customer-orders/{order.id}/pricelist-routing",
            json={"order_config_id": configs[1].id},
        )
        assert response.status_code == 200, response.text
        assert response.json()["pricelist_routing"]["basis"] == "manual"
        await test_session.refresh(order)
        assert order.order_config_id == configs[1].id
        with pytest.raises(ValueError, match="не ожидает"):
            await service.select_customer_order_pricelist(test_session, order.id, configs[0].id, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_prices", [True, False])
async def test_estimate_weights_partial_rejections_and_shared_stock(monkeypatch, with_prices):
    config = SimpleNamespace(price_warning_pct=5)
    rows = [
        service.ParsedOrderRow(0, "ABC1", "HAVAL", None, 4, 100 if with_prices else None),
        service.ParsedOrderRow(1, "ABC1", "HAVAL", None, 4, 100 if with_prices else None),
        service.ParsedOrderRow(2, "MISSING", "HAVAL", None, 2, 200 if with_prices else None),
    ]
    offer = service.OfferRow(1, 1, None, 5, 100, 80, False)
    prepare = AsyncMock(return_value=({}, {("ABC1", "HAVAL"): offer}, {}, object()))
    monkeypatch.setattr(service, "_prepare_customer_order_context", prepare)
    result = await service._estimate_customer_order_rejections(None, config, rows, 123)
    assert result["requested_qty"] == 10
    assert result["rejected_qty"] == 5
    assert result["rejected_pct"] == pytest.approx(700 / 1200 * 100 if with_prices else 50)
    assert result["rejection_metric"] == ("amount" if with_prices else "quantity")
    prepare.assert_awaited_once_with(None, config, rows, snapshot_id=123)


@pytest.mark.parametrize("own,offer_price,ship", [(False, 120, 0), (False, 104, 2), (True, 120, 2)])
def test_plan_applies_actual_price_tolerance_and_own_stock_policy(own, offer_price, ship):
    config = SimpleNamespace(price_warning_pct=5)
    row = order_row(price=100)
    offer = service.OfferRow(1, 1, None, 10, offer_price, 80, own)
    allocation = {}
    plan = service._plan_customer_order_row(config, row, offer, 100, allocation)
    assert plan.ship_qty == ship
    assert plan.reject_qty == 2 - ship
    assert allocation.get(1, 0) == ship


def test_mailbox_scope_remains_stronger_than_unscoped_config():
    configs = [
        SimpleNamespace(id=1, email_account_id=10, email_account_ids=[]),
        SimpleNamespace(id=2, email_account_id=20, email_account_ids=[]),
        SimpleNamespace(id=3, email_account_id=None, email_account_ids=[]),
    ]
    assert service._pick_configs_for_account(configs, 20) == [configs[1]]


@pytest.mark.asyncio
@pytest.mark.parametrize("own,price", [(False, 100), (False, 120), (True, 120)])
async def test_dry_estimate_matches_processing_without_creating_orders(
    test_session,
    created_customers,
    created_providers,
    created_autopart,
    monkeypatch,
    own,
    price,
):
    configs, _ = await setup_routes(test_session, created_customers[0])
    config = configs[0]
    rows = [
        service.ParsedOrderRow(0, "ABC1", "HAVAL", None, 4, 100),
        service.ParsedOrderRow(1, "ABC1", "HAVAL", None, 4, 100),
    ]
    offer = service.OfferRow(
        created_autopart.id,
        created_providers[0].id,
        None,
        5,
        price,
        80,
        own,
    )
    monkeypatch.setattr(
        service,
        "_prepare_customer_order_context",
        AsyncMock(
            return_value=({}, {("ABC1", "HAVAL"): offer}, {}, object()),
        ),
    )
    warning = AsyncMock()
    monkeypatch.setattr(service, "_send_price_warning", warning)
    monkeypatch.setattr(service, "assert_customer_credit_available", AsyncMock())
    estimate = await service._estimate_customer_order_rejections(test_session, config, rows, 123)
    assert not (await test_session.scalars(select(CustomerOrder))).all()
    assert not (await test_session.scalars(select(service.SupplierOrder))).all()
    assert not (await test_session.scalars(select(service.StockOrder))).all()
    warning.assert_not_called()
    order = CustomerOrder(customer_id=config.customer_id, order_config_id=config.id)
    test_session.add(order)
    await test_session.flush()
    items, _ = await service._process_manual_rows(test_session, config, order, rows)
    assert sum(item.reject_qty for item in items) == estimate["rejected_qty"]
    assert estimate["rejected_qty"] == (8 if not own and price == 120 else 3)
    assert sum(item.ship_qty for item in items) == 8 - estimate["rejected_qty"]


@pytest.mark.asyncio
async def test_admin_can_force_price_rejection_into_supplier_order(
    test_session,
    created_customers,
    created_providers,
    created_autopart,
    monkeypatch,
):
    config = CustomerOrderConfig(
        customer_id=created_customers[0].id,
        order_email="buyer@example.com",
        order_start_row=1,
        oem_col=0,
        brand_col=1,
        qty_col=2,
        price_col=3,
        price_warning_pct=5,
    )
    test_session.add(config)
    await test_session.flush()
    order = CustomerOrder(
        customer_id=created_customers[0].id,
        order_config_id=config.id,
        external_payload={},
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
    )
    test_session.add(order)
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id,
        row_index=0,
        oem="ABC1",
        brand="HAVAL",
        name="Деталь",
        requested_qty=5,
        requested_price=100,
        ship_qty=0,
        reject_qty=5,
        status=CUSTOMER_ORDER_ITEM_STATUS.REJECTED,
        reject_reason_code="PRICE_TOO_HIGH",
        reject_reason_text="Цена предложения выше допустимой для клиента.",
    )
    test_session.add(item)
    await test_session.commit()

    offer = service.OfferRow(
        autopart_id=created_autopart.id,
        provider_id=created_providers[0].id,
        provider_config_id=None,
        quantity=3,
        price=110,
        supplier_price=90,
        is_own_price=False,
        actual_oem="ABC1",
        actual_brand="HAVAL",
        actual_name="Деталь",
    )
    monkeypatch.setattr(
        service,
        "_prepare_customer_order_context",
        AsyncMock(return_value=({}, {("ABC1", "HAVAL"): offer}, {}, object())),
    )
    monkeypatch.setattr(service, "assert_customer_credit_available", AsyncMock())
    finalize = AsyncMock()
    monkeypatch.setattr(service, "try_finalize_customer_order_response", finalize)

    preview = await service.get_customer_order_price_override_preview(
        test_session,
        item.id,
    )
    assert preview["accepted_qty"] == 3
    assert preview["remaining_reject_qty"] == 2
    assert preview["provider_id"] == created_providers[0].id
    assert float(preview["supplier_price"]) == 90
    assert preview["actual_markup_pct"] == pytest.approx(11.111, rel=1e-3)

    updated, supplier_order_id, result_preview = (
        await service.force_accept_customer_order_price(
            test_session,
            item.id,
            approved_by_user_id=77,
        )
    )
    supplier_item = await test_session.scalar(
        select(SupplierOrderItem).where(
            SupplierOrderItem.customer_order_item_id == item.id
        )
    )
    assert supplier_order_id == supplier_item.supplier_order_id
    assert supplier_item.quantity == 3
    assert float(supplier_item.price) == 90
    assert updated.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER
    assert updated.ship_qty == 3
    assert updated.reject_qty == 2
    assert updated.reject_reason_code == "PARTIAL_STOCK"
    assert updated.source_payload["price_override"]["approved_by_user_id"] == 77
    assert result_preview["accepted_qty"] == 3
    finalize.assert_awaited_once_with(test_session, order_id=order.id)

    with pytest.raises(ValueError, match="только для отказа"):
        await service.force_accept_customer_order_price(
            test_session,
            item.id,
            approved_by_user_id=77,
        )


@pytest.mark.asyncio
async def test_recheck_uses_current_offer_and_does_not_duplicate_supplier_order(
    test_session,
    created_customers,
    created_providers,
    created_autopart,
    monkeypatch,
):
    config = CustomerOrderConfig(
        customer_id=created_customers[0].id,
        order_email="buyer@example.com",
        order_start_row=1,
        oem_col=0,
        brand_col=1,
        qty_col=2,
        price_col=3,
        price_warning_pct=5,
    )
    test_session.add(config)
    await test_session.flush()
    order = CustomerOrder(
        customer_id=config.customer_id,
        order_config_id=config.id,
        external_payload={},
        status=CUSTOMER_ORDER_STATUS.PROCESSED,
    )
    test_session.add(order)
    await test_session.flush()
    item = CustomerOrderItem(
        order_id=order.id,
        row_index=0,
        oem="DZ1016002020",
        brand="DRAGONZAP",
        requested_qty=4,
        requested_price=34,
        ship_qty=0,
        reject_qty=4,
        status=CUSTOMER_ORDER_ITEM_STATUS.REJECTED,
        reject_reason_code="OFFER_MATCH_DIAGNOSTIC",
    )
    test_session.add(item)
    await test_session.commit()
    offer = service.OfferRow(
        autopart_id=created_autopart.id,
        provider_id=created_providers[0].id,
        provider_config_id=None,
        quantity=3,
        price=50,
        supplier_price=40,
        is_own_price=False,
    )
    prepare = AsyncMock(return_value=(
        {}, {("DZ1016002020", "DRAGONZAP"): offer}, {}, object(),
    ))
    monkeypatch.setattr(service, "_prepare_customer_order_context", prepare)
    monkeypatch.setattr(service, "assert_customer_credit_available", AsyncMock())
    finalize = AsyncMock()
    monkeypatch.setattr(service, "try_finalize_customer_order_response", finalize)

    with pytest.raises(ValueError, match="цена выше допустимой"):
        await service.recheck_rejected_customer_order_item(
            test_session, item.id, checked_by_user_id=77,
        )
    assert await test_session.scalar(
        select(SupplierOrderItem.id).where(SupplierOrderItem.customer_order_item_id == item.id)
    ) is None

    offer.price = 34
    offer.supplier_price = 30
    updated = await service.recheck_rejected_customer_order_item(
        test_session, item.id, checked_by_user_id=77,
    )
    supplier_item = await test_session.scalar(
        select(SupplierOrderItem).where(SupplierOrderItem.customer_order_item_id == item.id)
    )
    assert updated.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER
    assert updated.ship_qty == 3
    assert updated.reject_qty == 1
    assert supplier_item.quantity == 3
    assert float(supplier_item.price) == 30
    assert updated.source_payload["current_offer_recheck"]["checked_by_user_id"] == 77
    assert (
        updated.source_payload["current_offer_recheck"]["previous_reject_reason_code"]
        == "OFFER_MATCH_DIAGNOSTIC"
    )
    finalize.assert_awaited_once_with(test_session, order_id=order.id)
    with pytest.raises(ValueError, match="только для автоматического отказа"):
        await service.recheck_rejected_customer_order_item(
            test_session, item.id, checked_by_user_id=77,
        )
