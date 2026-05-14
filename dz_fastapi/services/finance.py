"""Finance service — invoice rendering and email delivery."""

import logging
import os
from datetime import date
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.finance import InvoiceStatus, PaymentInvoice
from dz_fastapi.models.inventory import ShipmentDocument

logger = logging.getLogger(__name__)

# ── Jinja2 environment pointing at our templates directory ─────────────────────
_TEMPLATES_DIR = Path(__file__).parent.parent / "templates" / "finance"
_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=select_autoescape(["html"]),
)

# ── Seller defaults (configure via env vars) ──────────────────────────────────
SELLER_NAME = os.getenv("SELLER_NAME", "ООО «ДрагонЗап»")
SELLER_INN = os.getenv("SELLER_INN", "")
SELLER_KPP = os.getenv("SELLER_KPP", "")
SELLER_ADDRESS = os.getenv("SELLER_ADDRESS", "")
SELLER_PHONE = os.getenv("SELLER_PHONE", "")
SELLER_EMAIL = os.getenv("SELLER_EMAIL", os.getenv("EMAIL_NAME_PRICE", ""))

STATUS_LABELS = {
    InvoiceStatus.DRAFT: "Черновик",
    InvoiceStatus.SENT: "Выставлен",
    InvoiceStatus.PARTIALLY_PAID: "Частично оплачен",
    InvoiceStatus.PAID: "Оплачен",
    InvoiceStatus.CANCELLED: "Аннулирован",
    InvoiceStatus.OVERDUE: "Просрочен",
}


async def render_invoice_html(
    session: AsyncSession,
    invoice: PaymentInvoice,
) -> str:
    """Renders an invoice as a print-ready HTML string."""

    # Pull customer
    await session.refresh(invoice, ["customer", "shipment"])
    customer = invoice.customer

    # Pull shipment items if linked
    items = []
    shipment_number = None
    if invoice.shipment_id:
        result = await session.execute(
            select(ShipmentDocument)
            .where(ShipmentDocument.id == invoice.shipment_id)
            .options(selectinload(ShipmentDocument.items))
        )
        shipment = result.scalar_one_or_none()
        if shipment:
            shipment_number = shipment.doc_number
            for si in shipment.items or []:
                ap = si.autopart
                qty = si.quantity
                unit_price = float(si.price or 0)
                items.append(
                    {
                        "oem": getattr(ap, "oem_number", "") or "",
                        "brand": getattr(ap, "brand_name", "") or "",
                        "name": getattr(ap, "name", "") or "",
                        "quantity": qty,
                        "price": unit_price,
                        "total": qty * unit_price,
                    }
                )

    today = date.today()
    is_overdue = bool(
        invoice.due_date
        and invoice.due_date < today
        and invoice.status not in (InvoiceStatus.PAID, InvoiceStatus.CANCELLED)
    )

    template = _jinja_env.get_template("invoice_print.html")
    html = template.render(
        invoice=invoice,
        customer=customer,
        items=items,
        shipment_number=shipment_number,
        is_overdue=is_overdue,
        status_label=STATUS_LABELS.get(invoice.status, invoice.status),
        seller_name=SELLER_NAME,
        seller_inn=SELLER_INN,
        seller_kpp=SELLER_KPP,
        seller_address=SELLER_ADDRESS,
        seller_phone=SELLER_PHONE,
        seller_email=SELLER_EMAIL,
        generated_at=now_moscow().strftime("%d.%m.%Y %H:%M"),
    )
    return html


async def send_invoice_email(
    session: AsyncSession,
    invoice: PaymentInvoice,
    to_email: str | None = None,
) -> bool:
    """
    Sends invoice HTML by email to the customer (or to_email if provided).
    Uses the existing send_email_with_attachment infrastructure.
    Returns True on success.
    """
    from dz_fastapi.services.email import (
        send_email_with_attachment,  # local import to avoid circular
    )

    await session.refresh(invoice, ["customer"])
    customer = invoice.customer

    recipient = to_email or (customer.email_contact if customer else None)
    if not recipient:
        logger.error(
            "Cannot send invoice %s: no recipient email (customer.email_contact is empty)",
            invoice.invoice_number,
        )
        return False

    html_body = await render_invoice_html(session, invoice)
    subject = (
        f"Счёт на оплату № {invoice.invoice_number} "
        f"от {invoice.invoice_date}"
    )

    try:
        result = send_email_with_attachment(
            to_email=recipient,
            subject=subject,
            body=html_body,
            is_html=True,
        )
        if result:
            logger.info(
                "Invoice %s sent to %s", invoice.invoice_number, recipient
            )
        else:
            logger.warning(
                "Invoice %s send returned False for %s",
                invoice.invoice_number,
                recipient,
            )
        return result
    except Exception as exc:
        logger.exception(
            "Error sending invoice %s to %s: %s",
            invoice.invoice_number,
            recipient,
            exc,
        )
        return False
