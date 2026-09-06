from dz_fastapi.models.partner import CustomerOrder, CustomerOrderItem


def _unique_column_sets(model):
    return {
        tuple(column.name for column in constraint.columns)
        for constraint in model.__table__.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }


def test_customer_order_external_identity_is_unique_per_source():
    assert ("external_source", "external_order_id") in _unique_column_sets(CustomerOrder)


def test_customer_order_item_external_identity_is_unique_per_order():
    assert ("order_id", "external_order_item_id") in _unique_column_sets(CustomerOrderItem)
