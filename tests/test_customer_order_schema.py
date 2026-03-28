from dz_fastapi.schemas.customer_order import (CustomerOrderConfigCreate,
                                               CustomerOrderConfigResponse,
                                               CustomerOrderConfigUpdate)


def test_customer_order_config_create_converts_columns_to_zero_based():
    payload = CustomerOrderConfigCreate(
        customer_id=1,
        order_emails=['info@example.com'],
        oem_col=3,
        brand_col=2,
        qty_col=8,
        price_col=6,
    )

    assert payload.oem_col == 2
    assert payload.brand_col == 1
    assert payload.qty_col == 7
    assert payload.price_col == 5


def test_customer_order_config_update_converts_columns_to_zero_based():
    payload = CustomerOrderConfigUpdate(
        order_number_column=4,
        order_date_column=5,
        ship_qty_col=9,
        ship_price_col=10,
    )

    assert payload.order_number_column == 3
    assert payload.order_date_column == 4
    assert payload.ship_qty_col == 8
    assert payload.ship_price_col == 9


def test_customer_order_config_response_serializes_columns_to_one_based():
    response = CustomerOrderConfigResponse(
        id=7,
        customer_id=1,
        order_emails=['info@example.com'],
        oem_col=2,
        brand_col=1,
        qty_col=7,
        price_col=5,
        ship_price_col=6,
        last_uid=0,
    )

    data = response.model_dump()

    assert data['oem_col'] == 3
    assert data['brand_col'] == 2
    assert data['qty_col'] == 8
    assert data['price_col'] == 6
    assert data['ship_price_col'] == 7
