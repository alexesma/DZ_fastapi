"""Пометки «цена в документе выше цены в заказе» для строк поступления."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.partner import SupplierOrderItem, SupplierReceiptItem

# Допуск: пометка ставится, только если цена в документе выше заказной
# больше чем на этот порог (в рублях).
PRICE_TOLERANCE = Decimal("1.00")


def price_exceeds_order(
    receipt_price: object, order_price: object
) -> bool:
    if receipt_price is None or order_price is None:
        return False
    return Decimal(str(receipt_price)) - Decimal(str(order_price)) > PRICE_TOLERANCE


async def mark_receipt_price_discrepancies(
    session: AsyncSession, *, receipt_id: int
) -> int:
    """Сверяет цены строк поступления с ценами заказа поставщику.

    Сохраняет цену заказа и признак «дороже заказа» в строке. Возвращает
    число строк с превышением. Пометка информационная, проведение не
    блокирует.
    """
    rows = (
        await session.execute(
            select(SupplierReceiptItem, SupplierOrderItem.price)
            .join(
                SupplierOrderItem,
                SupplierOrderItem.id
                == SupplierReceiptItem.supplier_order_item_id,
                isouter=True,
            )
            .where(SupplierReceiptItem.receipt_id == receipt_id)
        )
    ).all()
    flagged = 0
    for item, order_price in rows:
        item.order_price = order_price
        item.price_exceeds_order = price_exceeds_order(item.price, order_price)
        flagged += int(item.price_exceeds_order)
    await session.flush()
    return flagged
