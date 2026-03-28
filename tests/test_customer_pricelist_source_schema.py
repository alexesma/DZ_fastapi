from dz_fastapi.schemas.partner import (CustomerPriceListSourceCreate,
                                        CustomerPriceListSourceResponse,
                                        CustomerPriceListSourceUpdate)


def test_customer_pricelist_source_create_normalizes_nonpositive_limits():
    payload = CustomerPriceListSourceCreate(
        provider_config_id=20,
        min_price=0,
        max_price=-1,
        min_quantity=0,
        max_quantity=-5,
    )

    assert payload.min_price is None
    assert payload.max_price is None
    assert payload.min_quantity is None
    assert payload.max_quantity is None


def test_customer_pricelist_source_update_normalizes_nonpositive_limits():
    payload = CustomerPriceListSourceUpdate(
        min_price='0',
        max_price='10',
        min_quantity='-2',
        max_quantity='3',
    )

    assert payload.min_price is None
    assert str(payload.max_price) == '10'
    assert payload.min_quantity is None
    assert payload.max_quantity == 3


def test_customer_pricelist_source_response_hides_invalid_zero_limits():
    response = CustomerPriceListSourceResponse(
        id=1,
        provider_config_id=20,
        enabled=True,
        markup=1.0,
        min_price=0,
        max_price=0,
        min_quantity=0,
        max_quantity=0,
    )

    assert response.min_price is None
    assert response.max_price is None
    assert response.min_quantity is None
    assert response.max_quantity is None
