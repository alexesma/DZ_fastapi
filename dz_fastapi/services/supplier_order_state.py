"""Read-only interpretation of supplier order item outcomes.

``auto_refused_at`` is a legacy name for a missed response deadline. It is
never evidence that the supplier explicitly rejected an item.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SupplierItemState:
    status: str
    rejected_quantity: int
    received_quantity: int
    pending_quantity: int
    response_source: str | None
    response_at: datetime | None


def _resize_refusal_sources(entries: list[dict], target: int) -> list[dict]:
    normalized = [
        {"source": str(entry.get("source") or "other"), "quantity": int(entry["quantity"])}
        for entry in entries
        if isinstance(entry, dict) and int(entry.get("quantity") or 0) > 0
    ]
    excess = max(sum(entry["quantity"] for entry in normalized) - target, 0)
    while excess and normalized:
        removed = min(excess, normalized[-1]["quantity"])
        normalized[-1]["quantity"] -= removed
        excess -= removed
        if normalized[-1]["quantity"] == 0:
            normalized.pop()
    return normalized


def set_supplier_item_decision(item, *, confirmed_quantity: int | None, source: str) -> None:
    """Keep source quantities when successive decisions reject one item in parts."""
    ordered = max(int(item.quantity or 0), 0)
    received = min(max(int(item.received_quantity or 0), 0), ordered)
    old_confirmed = item.confirmed_quantity
    old_rejected = (
        ordered - min(max(int(old_confirmed), received), ordered)
        if old_confirmed is not None and item.response_status_raw != "автоотказ по документу"
        else 0
    )
    entries = _resize_refusal_sources(list(item.refusal_sources or []), old_rejected)
    recorded = sum(entry["quantity"] for entry in entries)
    if recorded < old_rejected:
        entries.append({
            "source": str(item.decision_source or "other"),
            "quantity": old_rejected - recorded,
        })
    new_rejected = (
        ordered - min(max(int(confirmed_quantity), received), ordered)
        if confirmed_quantity is not None
        else 0
    )
    entries = _resize_refusal_sources(entries, new_rejected)
    recorded = sum(entry["quantity"] for entry in entries)
    if recorded < new_rejected:
        entries.append({"source": source, "quantity": new_rejected - recorded})
    item.refusal_sources = entries
    item.confirmed_quantity = confirmed_quantity
    item.decision_source = source


def supplier_item_refusal_sources(item, state: SupplierItemState) -> dict[str, int]:
    """Return refusal quantities, capped at the current effective refusal."""
    entries = _resize_refusal_sources(
        list(getattr(item, "refusal_sources", None) or []), state.rejected_quantity
    )
    result: dict[str, int] = {}
    for entry in entries:
        source = entry["source"]
        result[source] = result.get(source, 0) + entry["quantity"]
    untracked = state.rejected_quantity - sum(result.values())
    if untracked > 0:
        source = state.response_source or "other"
        result[source] = result.get(source, 0) + untracked
    return result


def supplier_item_state(
    item,
    *,
    sent_at: datetime | None,
    deadline_at: datetime | None = None,
    now: datetime | None = None,
) -> SupplierItemState:
    ordered = max(int(item.quantity or 0), 0)
    received = min(max(int(item.received_quantity or 0), 0), ordered)
    confirmed = item.confirmed_quantity
    linked_receipts = [
        link.receipt for link in (item.receipt_items or []) if link.receipt is not None
    ]
    manually_rejected_later = any(
        link.confirmed_quantity == 0
        and link.receipt is not None
        and link.receipt.created_by_user_id is not None
        for link in (item.receipt_items or [])
    )
    legacy_inferred_refusal = (
        item.response_status_raw == "автоотказ по документу"
        and not manually_rejected_later
    )
    explicit = confirmed is not None and not legacy_inferred_refusal
    effective_confirmed = (
        min(max(int(confirmed), received), ordered) if explicit else None
    )
    rejected = ordered - effective_confirmed if explicit else 0
    pending = max(ordered - received, 0) if not explicit else 0
    overdue = item.auto_refused_at is not None or (
        deadline_at is not None and now is not None and now >= deadline_at
    )

    response_source = None
    response_at = item.response_status_synced_at if explicit else None
    if explicit:
        if linked_receipts and response_at is None:
            response_at = max(
                (receipt.created_at for receipt in linked_receipts if receipt.created_at),
                default=None,
            )
        if getattr(item, "decision_source", None):
            response_source = item.decision_source
        elif manually_rejected_later:
            response_source = "manager"
            response_at = max(
                (
                    link.receipt.created_at
                    for link in (item.receipt_items or [])
                    if link.confirmed_quantity == 0
                    and link.receipt is not None
                    and link.receipt.created_by_user_id is not None
                    and link.receipt.created_at is not None
                ),
                default=response_at,
            )
        elif str(item.response_status_raw or "").startswith("автоподтверждено"):
            response_source = "automatic"
        elif any(receipt.source_message_id is not None for receipt in linked_receipts):
            response_source = "supplier"
        elif any(receipt.created_by_user_id is not None for receipt in linked_receipts) and not item.response_status_raw:
            response_source = "manager"
        elif item.response_status_raw:
            response_source = "supplier"
        else:
            response_source = "recorded"

    if rejected:
        status = "rejected" if rejected == ordered else "partially_rejected"
    elif received >= ordered and ordered:
        status = "received"
    elif received:
        status = "partially_received_overdue" if overdue and pending else "partially_received"
    elif explicit:
        status = "confirmed"
    elif sent_at is None:
        status = "not_sent"
    elif overdue:
        status = "overdue"
    else:
        status = "awaiting_response"

    return SupplierItemState(status, rejected, received, pending, response_source, response_at)
