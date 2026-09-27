from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dz_fastapi.services.customer_orders import create_manual_supplier_order


@pytest.mark.asyncio
async def test_manual_supplier_order_rejects_invalid_multiplicity():
    session = AsyncMock()
    session.get.return_value = SimpleNamespace(id=10)

    with pytest.raises(ValueError, match="должно быть кратно 2"):
        await create_manual_supplier_order(
            session=session,
            provider_id=10,
            items=[
                {
                    "oem": "TEST-LOT-2",
                    "brand": "TEST",
                    "quantity": 3,
                    "multiplicity": 2,
                    "price": 100,
                }
            ],
        )
