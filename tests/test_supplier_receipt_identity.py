import pytest

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services.inventory_stock import resolve_receipt_item_autopart_id
from dz_fastapi.services.supplier_workflow import serialize_supplier_receipt


def test_receipt_displays_one_identity_without_merging_separate_rows():
    receipt = SupplierReceipt(
        provider_id=1,
        items=[
            SupplierReceiptItem(
                oem_number="91553-81030",
                brand_name="Toyota",
                received_quantity=2,
            ),
            SupplierReceiptItem(
                oem_number="9155381030",
                brand_name="TOYOTA",
                received_quantity=1,
            ),
        ],
    )

    rows = serialize_supplier_receipt(receipt)["items"]

    assert len(rows) == 2
    assert [(row["oem_number"], row["brand_name"]) for row in rows] == [
        ("9155381030", "TOYOTA"),
        ("9155381030", "TOYOTA"),
    ]
    assert [row["received_quantity"] for row in rows] == [2, 1]


@pytest.mark.asyncio
async def test_receipt_stock_lookup_matches_hyphenated_oem_and_brand_case(
    test_session,
):
    toyota = Brand(name="TOYOTA")
    another = Brand(name="ANOTHER")
    test_session.add_all([toyota, another])
    await test_session.flush()
    wanted = AutoPart(
        brand_id=toyota.id,
        oem_number="9017910132",
        name="Гайка крепления",
    )
    other = AutoPart(
        brand_id=another.id,
        oem_number="9017910132",
        name="Другая деталь",
    )
    test_session.add_all([wanted, other])
    await test_session.flush()

    item = SupplierReceiptItem(
        oem_number="90179-10132",
        brand_name="Toyota",
        received_quantity=1,
    )

    assert await resolve_receipt_item_autopart_id(test_session, item) == wanted.id
