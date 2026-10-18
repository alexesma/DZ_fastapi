from dz_fastapi.models.partner import SupplierOrderItem
from dz_fastapi.services.supplier_order_state import (
    set_supplier_item_decision,
    supplier_item_refusal_sources,
    supplier_item_state,
)


def test_refusal_sources_preserve_supplier_and_document_quantities():
    item = SupplierOrderItem(quantity=5, received_quantity=0)
    set_supplier_item_decision(item, confirmed_quantity=3, source="supplier")
    item.received_quantity = 1
    set_supplier_item_decision(item, confirmed_quantity=1, source="document_timeout")

    state = supplier_item_state(item, sent_at=None)
    assert state.rejected_quantity == 4
    assert supplier_item_refusal_sources(item, state) == {
        "supplier": 2,
        "document_timeout": 2,
    }


def test_later_confirmation_removes_most_recent_refusal_first():
    item = SupplierOrderItem(quantity=5, received_quantity=0)
    set_supplier_item_decision(item, confirmed_quantity=3, source="supplier")
    set_supplier_item_decision(item, confirmed_quantity=1, source="manager")
    set_supplier_item_decision(item, confirmed_quantity=4, source="supplier")

    state = supplier_item_state(item, sent_at=None)
    assert state.rejected_quantity == 1
    assert supplier_item_refusal_sources(item, state) == {"supplier": 1}
