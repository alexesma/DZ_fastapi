import pytest
from fastapi import HTTPException

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import CustomerPriceListConfig
from dz_fastapi.schemas.partner import CustomerPriceListCreate
from dz_fastapi.services.process import process_customer_pricelist


@pytest.mark.asyncio
async def test_customer_pricelist_generation_rejects_concurrent_run(
    test_session,
    created_customers,
):
    customer = created_customers[0]
    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name="Concurrent customer pricelist",
        generation_lock_token="already-running",
        generation_locked_at=now_moscow(),
    )
    test_session.add(config)
    await test_session.flush()
    customer_id = customer.id
    config_id = config.id
    await test_session.commit()

    with pytest.raises(HTTPException) as exc_info:
        await process_customer_pricelist(
            customer=customer,
            request=CustomerPriceListCreate(
                customer_id=customer_id,
                config_id=config_id,
                items=[],
            ),
            session=test_session,
        )

    assert exc_info.value.status_code == 409
    await test_session.refresh(config)
    assert config.generation_lock_token == "already-running"
