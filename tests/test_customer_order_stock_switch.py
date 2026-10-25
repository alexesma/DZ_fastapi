"""A received customer article can replace an unpicked own-stock cross."""

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.inventory import (
    CustomerOrderPickLabel,
    ReserveStatus,
    StockByLocation,
    StockDocument,
    StockDocumentItem,
    StockDocumentStatus,
    StockDocumentType,
    StockLot,
    StockReserve,
)
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    STOCK_ORDER_STATUS,
    CustomerOrder,
    CustomerOrderItem,
    CustomerPriceList,
    CustomerPriceListPublishedAlias,
    StockOrder,
    StockOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.services.customer_assembly import list_assembly
from dz_fastapi.services.inventory_stock import (
    apply_receipt_to_stock_by_id,
    post_stock_document,
    unpost_stock_document,
)


async def _order_with_cross(session, customer, *, requested_oem,
                            requested_brand, cross_part, quantity=1,
                            match_type="dragonzap_cross"):
    order = CustomerOrder(customer_id=customer.id, status="PROCESSED")
    session.add(order)
    await session.flush()
    item = CustomerOrderItem(
        order_id=order.id, oem=requested_oem, brand=requested_brand,
        requested_qty=quantity, ship_qty=quantity, reject_qty=0,
        status=CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK,
        autopart_id=cross_part.id, match_type=match_type,
        actual_oem=cross_part.oem_number, actual_brand=requested_brand,
    )
    stock_order = StockOrder(customer_id=customer.id)
    session.add_all([item, stock_order])
    await session.flush()
    stock_item = StockOrderItem(
        stock_order_id=stock_order.id, customer_order_item_id=item.id,
        autopart_id=cross_part.id, quantity=quantity,
    )
    session.add(stock_item)
    await session.flush()
    return order, item, stock_item


async def _post_direct_receipt(session, *, part, location, quantity):
    doc = StockDocument(
        doc_type=StockDocumentType.MANUAL_RECEIPT,
        status=StockDocumentStatus.DRAFT,
    )
    session.add(doc)
    await session.flush()
    session.add(StockDocumentItem(
        document_id=doc.id, autopart_id=part.id,
        storage_location_id=location.id, quantity=quantity,
    ))
    await session.flush()
    return doc, await post_stock_document(session, document_id=doc.id)


@pytest.mark.asyncio
async def test_manual_receipt_switches_exact_article_and_unpost_restores_cross(
    test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    direct = AutoPart(
        name="Original gasket", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add(direct)
    await test_session.flush()
    order, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem="DZLF-479Q11003015A",
        requested_brand=created_brand.name.lower(), cross_part=created_autopart,
    )
    old_lot = StockLot(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id,
        initial_quantity=1, remaining_quantity=1,
    )
    test_session.add(old_lot)
    await test_session.flush()
    stock_item.preferred_stock_lot_id = old_lot.id

    doc, result = await _post_direct_receipt(
        test_session, part=direct, location=created_storage, quantity=1,
    )
    assert result["source_switches"] == [{
        "order_id": order.id, "item_id": item.id, "status": "switched",
        "reason": None, "oem": direct.oem_number, "basis": "exact",
    }]
    assert item.autopart_id == direct.id
    assert stock_item.autopart_id == direct.id
    assert stock_item.preferred_stock_lot_id is None
    assert item.match_type == "receipt_exact"
    assembly = await list_assembly(test_session, order_ids=[order.id])
    assert assembly[0]["items"][0]["stock_items"][0]["location"] == created_storage.name
    assert assembly[0]["items"][0]["source_switch"]["status"] == "switched"

    await unpost_stock_document(test_session, document_id=doc.id)
    assert item.autopart_id == created_autopart.id
    assert stock_item.autopart_id == created_autopart.id
    assert stock_item.preferred_stock_lot_id == old_lot.id
    assert item.match_type == "dragonzap_cross"
    assert "receipt_stock_source_switch" not in item.source_payload


@pytest.mark.asyncio
async def test_receipt_keeps_cross_when_label_already_exists(
    async_client, test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    direct = AutoPart(
        name="Original gasket", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add(direct)
    await test_session.flush()
    order, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=direct.oem_number,
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    label = CustomerOrderPickLabel(
        stock_order_item_id=stock_item.id,
        storage_location_id=created_storage.id,
        quantity=1, barcode="TEST-ALREADY-PRINTED",
    )
    test_session.add(label)
    await test_session.flush()

    _, result = await _post_direct_receipt(
        test_session, part=direct, location=created_storage, quantity=1,
    )
    assert result["source_switches"][0]["status"] == "blocked"
    assert "этикетка" in result["source_switches"][0]["reason"]
    assert item.autopart_id == created_autopart.id
    assert stock_item.autopart_id == created_autopart.id
    assembly = await list_assembly(test_session, order_ids=[order.id])
    assert assembly[0]["items"][0]["source_switch"]["status"] == "blocked"
    await test_session.commit()
    approve_url = f"/customer-orders/assembly/source-switch/{item.id}/approve"
    blocked = await async_client.post(approve_url)
    assert blocked.status_code == 409
    cancelled = await async_client.post(
        f"/customer-orders/assembly/own-labels/{label.id}/cancel"
    )
    assert cancelled.status_code == 200, cancelled.text
    approved = await async_client.post(approve_url)
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "switched"
    await test_session.refresh(item)
    assert item.autopart_id == direct.id


@pytest.mark.asyncio
async def test_receipt_requires_full_free_quantity_and_same_brand(
    async_client, test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    direct = AutoPart(
        name="Original gasket", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add(direct)
    await test_session.flush()
    _, short_item, _ = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=direct.oem_number,
        requested_brand=created_brand.name, cross_part=created_autopart,
        quantity=2,
    )
    _, other_brand_item, _ = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=direct.oem_number,
        requested_brand="OTHER BRAND", cross_part=created_autopart,
    )
    _, closed_item, closed_stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=direct.oem_number,
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    closed_stock_order = await test_session.get(
        StockOrder, closed_stock_item.stock_order_id,
    )
    closed_stock_order.status = STOCK_ORDER_STATUS.DISPATCHED

    _, result = await _post_direct_receipt(
        test_session, part=direct, location=created_storage, quantity=1,
    )
    assert len(result["source_switches"]) == 1
    assert result["source_switches"][0]["status"] == "blocked"
    assert "недостаточно" in result["source_switches"][0]["reason"]
    assert short_item.autopart_id == created_autopart.id
    assert other_brand_item.autopart_id == created_autopart.id
    assert closed_item.autopart_id == created_autopart.id
    assert "receipt_stock_source_switch" not in (closed_item.source_payload or {})
    await test_session.commit()
    kept = await async_client.post(
        f"/customer-orders/assembly/source-switch/{short_item.id}/keep-cross"
    )
    assert kept.status_code == 200, kept.text
    await test_session.refresh(short_item)
    _, next_result = await _post_direct_receipt(
        test_session, part=direct, location=created_storage, quantity=1,
    )
    assert next_result["source_switches"] == []
    assert short_item.autopart_id == created_autopart.id


@pytest.mark.asyncio
async def test_unlinked_supplier_receipt_resolves_article_before_switch(
    test_session, created_autopart, created_brand, created_customers,
    created_providers,
):
    direct = AutoPart(
        name="Original gasket", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add(direct)
    await test_session.flush()
    _, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=direct.oem_number,
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    receipt = SupplierReceipt(provider_id=created_providers[0].id)
    test_session.add(receipt)
    await test_session.flush()
    test_session.add(SupplierReceiptItem(
        receipt_id=receipt.id, oem_number=direct.oem_number,
        brand_name=created_brand.name, received_quantity=1,
    ))
    await test_session.flush()

    await apply_receipt_to_stock_by_id(test_session, receipt_id=receipt.id)
    assert item.autopart_id == direct.id
    assert stock_item.autopart_id == direct.id
    assert item.source_payload["receipt_stock_source_switch"]["source_type"] == (
        "supplier_receipt"
    )
    await apply_receipt_to_stock_by_id(
        test_session, receipt_id=receipt.id, reverse=True,
    )
    assert item.autopart_id == created_autopart.id
    assert stock_item.autopart_id == created_autopart.id


@pytest.mark.parametrize("reverse_order", ["exact_first", "cross_first"])
@pytest.mark.asyncio
async def test_confirmed_dragonzap_cross_then_exact_receipt_take_priority(
    test_session, created_autopart, created_brand, created_customers,
    created_storage, reverse_order,
):
    dz_brand = Brand(name="DRAGONZAP")
    test_session.add(dz_brand)
    await test_session.flush()
    incoming_cross = AutoPart(
        name="Dragonzap alternative", brand_id=dz_brand.id,
        oem_number="DZ1086001127",
    )
    exact = AutoPart(
        name="Requested article", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add_all([incoming_cross, exact])
    await test_session.flush()
    test_session.add(AutoPartCross(
        source_autopart_id=incoming_cross.id,
        cross_brand_id=created_brand.id,
        cross_oem_number=exact.oem_number,
    ))
    order, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=exact.oem_number,
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    cross_doc, cross_result = await _post_direct_receipt(
        test_session, part=incoming_cross,
        location=created_storage, quantity=1,
    )
    assert cross_result["source_switches"][0]["basis"] == (
        "confirmed_dragonzap_cross"
    )
    assert item.autopart_id == incoming_cross.id
    assert stock_item.autopart_id == incoming_cross.id
    assert item.source_resolution_status == "receipt_dragonzap_cross"
    assert item.match_type == "dragonzap_cross"

    exact_doc, exact_result = await _post_direct_receipt(
        test_session, part=exact, location=created_storage, quantity=1,
    )
    assert exact_result["source_switches"][0]["basis"] == "exact"
    assert item.autopart_id == exact.id
    assert stock_item.autopart_id == exact.id
    assert item.match_type == "receipt_exact"
    assert (item.source_payload["receipt_stock_source_switch"]["previous"]
            ["source_switch"]["document_id"] == cross_doc.id)
    assembly = await list_assembly(test_session, order_ids=[order.id])
    assert assembly[0]["items"][0]["source_switch"]["basis"] == "exact"

    if reverse_order == "exact_first":
        await unpost_stock_document(test_session, document_id=exact_doc.id)
        assert item.autopart_id == incoming_cross.id
        assert item.source_resolution_status == "receipt_dragonzap_cross"
        await unpost_stock_document(test_session, document_id=cross_doc.id)
    else:
        await unpost_stock_document(test_session, document_id=cross_doc.id)
        assert item.autopart_id == exact.id
        await unpost_stock_document(test_session, document_id=exact_doc.id)
    assert item.autopart_id == created_autopart.id
    assert stock_item.autopart_id == created_autopart.id


@pytest.mark.asyncio
async def test_blocked_exact_receipt_keeps_previous_cross_switch(
    test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    dz_brand = Brand(name="DRAGONZAP")
    test_session.add(dz_brand)
    await test_session.flush()
    incoming_cross = AutoPart(
        name="Dragonzap alternative", brand_id=dz_brand.id,
        oem_number="DZ1086001127",
    )
    exact = AutoPart(
        name="Requested article", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add_all([incoming_cross, exact])
    await test_session.flush()
    test_session.add(AutoPartCross(
        source_autopart_id=incoming_cross.id,
        cross_brand_id=created_brand.id,
        cross_oem_number=exact.oem_number,
    ))
    _, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=exact.oem_number,
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    cross_doc, _ = await _post_direct_receipt(
        test_session, part=incoming_cross,
        location=created_storage, quantity=1,
    )
    test_session.add(CustomerOrderPickLabel(
        stock_order_item_id=stock_item.id,
        storage_location_id=created_storage.id,
        quantity=1, barcode="TEST-CROSS-PRINTED",
    ))
    await test_session.flush()
    exact_doc, result = await _post_direct_receipt(
        test_session, part=exact, location=created_storage, quantity=1,
    )
    assert result["source_switches"][0]["status"] == "blocked"
    assert item.autopart_id == incoming_cross.id
    assert item.source_payload["receipt_stock_source_switch"]["document_id"] == (
        cross_doc.id
    )
    assert item.source_payload["receipt_stock_source_suggestion"]["document_id"] == (
        exact_doc.id
    )
    await unpost_stock_document(test_session, document_id=exact_doc.id)
    assert item.autopart_id == incoming_cross.id
    assert "receipt_stock_source_suggestion" not in item.source_payload
    assert item.source_payload["receipt_stock_source_switch"]["document_id"] == (
        cross_doc.id
    )


@pytest.mark.asyncio
async def test_published_dragonzap_alias_is_scoped_to_customer(
    test_session, created_autopart, created_customers, created_storage,
):
    dz_brand = Brand(name="DRAGONZAP")
    test_session.add(dz_brand)
    await test_session.flush()
    incoming = AutoPart(
        name="Published alternative", brand_id=dz_brand.id,
        oem_number="DZALT100",
    )
    test_session.add(incoming)
    await test_session.flush()
    pricelist = CustomerPriceList(
        customer_id=created_customers[0].id,
        published_at=now_moscow(),
    )
    test_session.add(pricelist)
    await test_session.flush()
    test_session.add(CustomerPriceListPublishedAlias(
        customer_pricelist_id=pricelist.id,
        source_autopart_id=incoming.id,
        advertised_oem="DZ-REQUEST-100",
        advertised_brand="DRAGONZAP",
        normalized_oem=preprocess_oem_number("DZ-REQUEST-100"),
        normalized_brand="DRAGONZAP",
        quantity=1, price=100,
    ))
    await test_session.flush()
    stale_pricelist = CustomerPriceList(
        customer_id=created_customers[1].id,
        published_at=now_moscow(),
    )
    test_session.add(stale_pricelist)
    await test_session.flush()
    test_session.add(CustomerPriceListPublishedAlias(
        customer_pricelist_id=stale_pricelist.id,
        source_autopart_id=incoming.id,
        advertised_oem="DZ-REQUEST-100",
        advertised_brand="DRAGONZAP",
        normalized_oem=preprocess_oem_number("DZ-REQUEST-100"),
        normalized_brand="DRAGONZAP",
        quantity=1, price=100,
    ))
    test_session.add(CustomerPriceList(
        customer_id=created_customers[1].id,
        published_at=now_moscow(),
    ))
    await test_session.flush()
    _, allowed_item, _ = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem="DZ-REQUEST-100",
        requested_brand="DRAGONZAP", cross_part=created_autopart,
    )
    _, unrelated_item, _ = await _order_with_cross(
        test_session, created_customers[1],
        requested_oem="DZ-REQUEST-100",
        requested_brand="DRAGONZAP", cross_part=created_autopart,
    )

    _, result = await _post_direct_receipt(
        test_session, part=incoming, location=created_storage, quantity=1,
    )
    assert len(result["source_switches"]) == 1
    assert result["source_switches"][0]["basis"] == "published_dragonzap_cross"
    assert allowed_item.autopart_id == incoming.id
    assert unrelated_item.autopart_id == created_autopart.id


@pytest.mark.asyncio
async def test_cross_receipt_does_not_replace_available_or_unrelated_source(
    test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    dz_brand = Brand(name="DRAGONZAP")
    test_session.add(dz_brand)
    await test_session.flush()
    confirmed = AutoPart(
        name="Confirmed alternative", brand_id=dz_brand.id,
        oem_number="DZALT101",
    )
    unrelated = AutoPart(
        name="Unrelated part", brand_id=dz_brand.id,
        oem_number="DZALT102",
    )
    test_session.add_all([confirmed, unrelated])
    await test_session.flush()
    test_session.add(AutoPartCross(
        source_autopart_id=confirmed.id,
        cross_brand_id=created_brand.id,
        cross_oem_number="REQUEST101",
    ))
    _, item, _ = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem="REQUEST101",
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    test_session.add(StockByLocation(
        autopart_id=created_autopart.id,
        storage_location_id=created_storage.id, quantity=1,
    ))
    await test_session.flush()

    _, confirmed_result = await _post_direct_receipt(
        test_session, part=confirmed, location=created_storage, quantity=1,
    )
    _, unrelated_result = await _post_direct_receipt(
        test_session, part=unrelated, location=created_storage, quantity=1,
    )
    assert confirmed_result["source_switches"] == []
    assert unrelated_result["source_switches"] == []
    assert item.autopart_id == created_autopart.id


@pytest.mark.asyncio
async def test_new_confirmed_cross_replaces_depleted_receipt_cross(
    test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    dz_brand = Brand(name="DRAGONZAP")
    test_session.add(dz_brand)
    await test_session.flush()
    first = AutoPart(
        name="First alternative", brand_id=dz_brand.id,
        oem_number="DZALT201",
    )
    second = AutoPart(
        name="Second alternative", brand_id=dz_brand.id,
        oem_number="DZALT202",
    )
    test_session.add_all([first, second])
    await test_session.flush()
    for part in (first, second):
        test_session.add(AutoPartCross(
            source_autopart_id=part.id,
            cross_brand_id=created_brand.id,
            cross_oem_number="REQUEST201",
        ))
    _, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem="REQUEST201",
        requested_brand=created_brand.name, cross_part=created_autopart,
    )
    first_doc, first_result = await _post_direct_receipt(
        test_session, part=first, location=created_storage, quantity=1,
    )
    assert first_result["source_switches"][0]["status"] == "switched"
    test_session.add(StockReserve(
        autopart_id=first.id, storage_location_id=created_storage.id,
        quantity=1, status=ReserveStatus.ACTIVE,
    ))
    await test_session.flush()

    second_doc, second_result = await _post_direct_receipt(
        test_session, part=second, location=created_storage, quantity=1,
    )
    assert second_result["source_switches"][0]["basis"] == (
        "confirmed_dragonzap_cross"
    )
    assert item.autopart_id == second.id
    assert stock_item.autopart_id == second.id
    await unpost_stock_document(test_session, document_id=second_doc.id)
    assert item.autopart_id == created_autopart.id
    assert stock_item.autopart_id == created_autopart.id
    assert first_doc.id != second_doc.id


@pytest.mark.asyncio
async def test_confirmed_cross_can_replace_unavailable_direct_own_source(
    test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    dz_brand = Brand(name="DRAGONZAP")
    test_session.add(dz_brand)
    await test_session.flush()
    incoming = AutoPart(
        name="Dragonzap alternative", brand_id=dz_brand.id,
        oem_number="DZALT301",
    )
    test_session.add(incoming)
    await test_session.flush()
    test_session.add(AutoPartCross(
        source_autopart_id=incoming.id,
        cross_brand_id=created_brand.id,
        cross_oem_number=created_autopart.oem_number,
    ))
    _, item, stock_item = await _order_with_cross(
        test_session, created_customers[0],
        requested_oem=created_autopart.oem_number,
        requested_brand=created_brand.name,
        cross_part=created_autopart, match_type=None,
    )

    _, result = await _post_direct_receipt(
        test_session, part=incoming, location=created_storage, quantity=1,
    )
    assert result["source_switches"][0]["basis"] == (
        "confirmed_dragonzap_cross"
    )
    assert item.autopart_id == incoming.id
    assert stock_item.autopart_id == incoming.id


@pytest.mark.asyncio
async def test_receipts_do_not_overallocate_unlabelled_orders(
    test_session, created_autopart, created_brand, created_customers,
    created_storage,
):
    direct = AutoPart(
        name="Requested article", brand_id=created_brand.id,
        oem_number="DZLF479Q11003015A",
    )
    test_session.add(direct)
    await test_session.flush()
    items = []
    for _ in range(3):
        _, item, _ = await _order_with_cross(
            test_session, created_customers[0],
            requested_oem=direct.oem_number,
            requested_brand=created_brand.name,
            cross_part=created_autopart,
        )
        items.append(item)

    _, first_result = await _post_direct_receipt(
        test_session, part=direct, location=created_storage, quantity=1,
    )
    assert len([row for row in first_result["source_switches"]
                if row["status"] == "switched"]) == 1
    _, second_result = await _post_direct_receipt(
        test_session, part=direct, location=created_storage, quantity=1,
    )
    assert len([row for row in second_result["source_switches"]
                if row["status"] == "switched"]) == 1
    assert sum(item.autopart_id == direct.id for item in items) == 2
