"""Human-readable preview of an agreed shipment correction."""

from decimal import Decimal
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.shipment_correction import ShipmentCorrection
from dz_fastapi.services.diadoc_outgoing import _gross_price_vat_parts
from dz_fastapi.services.shipment_print_documents import _buyer, _money, load_seller

_env = Environment(
    loader=FileSystemLoader(
        str(Path(__file__).parent.parent / "templates" / "shipment")
    ),
    autoescape=select_autoescape(["html"]),
)


async def render_ukd(session: AsyncSession, doc: ShipmentCorrection) -> str:
    totals = {
        key: Decimal("0") for key in (
            "before_net", "before_vat", "before_gross",
            "after_net", "after_vat", "after_gross",
        )
    }
    rows = []
    for item in doc.items:
        _, before_net, before_vat, before_gross = _gross_price_vat_parts(
            item.price_before, item.quantity_before, item.vat_rate
        )
        _, after_net, after_vat, after_gross = _gross_price_vat_parts(
            item.price_after, item.quantity_after, item.vat_rate
        )
        for key, value in (
            ("before_net", before_net), ("before_vat", before_vat),
            ("before_gross", before_gross), ("after_net", after_net),
            ("after_vat", after_vat), ("after_gross", after_gross),
        ):
            totals[key] += value
        rows.append({
            "original_number": item.original_number,
            "product_name": item.product_name,
            "vendor_code": item.vendor_code,
            "quantity_before": item.quantity_before,
            "quantity_after": item.quantity_after,
            "price_before": _money(Decimal(str(item.price_before))),
            "price_after": _money(Decimal(str(item.price_after))),
            "before_net": _money(before_net),
            "before_vat": _money(before_vat),
            "before_gross": _money(before_gross),
            "after_net": _money(after_net),
            "after_vat": _money(after_vat),
            "after_gross": _money(after_gross),
        })
    return _env.get_template("ukd_preview.html").render(
        doc=doc,
        shipment=doc.shipment,
        seller=await load_seller(session),
        buyer=_buyer(doc.shipment.customer),
        rows=rows,
        totals={key: _money(value) for key, value in totals.items()},
        difference=_money(
            totals["after_gross"] - totals["before_gross"]
        ),
        is_draft=doc.status.value == "draft",
    )
