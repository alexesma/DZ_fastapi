"""Pure matching helpers for the Parts-Soft customer reconciliation."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

PARTS_SOFT_SOURCE = "PARTS_SOFT"


def clean(value: Any) -> str:
    return str(value or "").strip()


def normalize_digits(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    if "." in text or "e" in text.casefold():
        try:
            decimal = Decimal(text)
            if decimal == decimal.to_integral_value():
                return str(int(decimal))
        except InvalidOperation:
            pass
    return re.sub(r"\D", "", text)


def normalize_email(value: Any) -> str:
    return clean(value).casefold()


def normalize_name(value: Any) -> str:
    words = re.findall(r"[0-9a-zа-яё]+", clean(value).casefold())
    legal_form_words = {
        "ао",
        "зао",
        "ип",
        "общество",
        "ограниченной",
        "ооо",
        "ответственностью",
        "пао",
    }
    return " ".join(word for word in words if word not in legal_form_words)


def format_address(value: Any) -> str:
    if not isinstance(value, dict):
        return clean(value)
    fields = ("zip_code", "country", "region", "city", "street", "house", "korpus", "flat")
    return ", ".join(clean(value.get(field)) for field in fields if clean(value.get(field)))


def flatten_remote_customer(customer: dict[str, Any]) -> dict[str, Any]:
    essential = (
        customer.get("essential")
        or customer.get("essential_attributes")
        or {}
    )
    contact = (
        customer.get("contact")
        or customer.get("contact_attributes")
        or {}
    )
    official_address = (
        customer.get("official_address")
        or customer.get("official_address_attributes")
        or {}
    )
    delivery_address = (
        customer.get("delivery_address")
        or customer.get("delivery_address_attributes")
        or {}
    )
    company_name = clean(essential.get("company_name"))
    full_name = clean(customer.get("compile_name")) or " ".join(
        part
        for part in (
            clean(customer.get("family_name")),
            clean(customer.get("name")),
            clean(customer.get("second_name")),
        )
        if part
    )
    return {
        "external_id": customer.get("id"),
        "name": company_name or full_name or clean(customer.get("login_or_email")),
        "email": clean(customer.get("email_org")) or clean(customer.get("email")),
        "inn": normalize_digits(essential.get("inn")),
        "kpp": normalize_digits(essential.get("kpp")),
        "company_type": clean(essential.get("company_type")),
        "legal_address": format_address(official_address),
        "postal_address": format_address(delivery_address),
        "phone": clean(contact.get("phone") or customer.get("phone")),
        "additional_phone": clean(
            contact.get("cell_phone") or customer.get("cell_phone")
        ),
        "vat_rate": customer.get("nds"),
        "bank_bik": normalize_digits(essential.get("bik")),
        "bank_name": clean(essential.get("bank")),
        "bank_city": clean(essential.get("city")),
        "bank_account": clean(essential.get("loro_account")),
        "correspondent_account": clean(essential.get("korr_schet")),
        "credit_limit": customer.get("credit_limit"),
        "payment_terms_days": customer.get("pay_delay"),
        "partssoft_classification": {
            key: customer.get(key)
            for key in (
                "ur_type",
                "discount_type_id",
                "region_id",
                "user_id",
                "send_sms",
                "send_email",
            )
            if customer.get(key) is not None
        },
    }


@dataclass(frozen=True)
class MatchResult:
    classification: str
    basis: str
    local_id: int | None
    candidate_ids: tuple[int, ...] = ()


class CustomerMatcher:
    def __init__(self, local_customers: Iterable[dict[str, Any]]):
        self.rows = {int(row["id"]): row for row in local_customers}
        self.indexes: dict[str, dict[str, set[int]]] = {
            name: defaultdict(set) for name in ("external_id", "inn_kpp", "inn", "email", "name")
        }
        for local_id, row in self.rows.items():
            external_id = clean(row.get("external_id"))
            inn = normalize_digits(row.get("inn"))
            kpp = normalize_digits(row.get("kpp"))
            email = normalize_email(row.get("email"))
            name = normalize_name(row.get("name"))
            if external_id:
                self.indexes["external_id"][external_id].add(local_id)
            if inn and kpp:
                self.indexes["inn_kpp"][f"{inn}:{kpp}"].add(local_id)
            if inn:
                self.indexes["inn"][inn].add(local_id)
            if email:
                self.indexes["email"][email].add(local_id)
            if name:
                self.indexes["name"][name].add(local_id)

    def match(self, remote: dict[str, Any]) -> MatchResult:
        checks = (
            ("external_id", clean(remote.get("external_id")), "linked"),
            (
                "inn_kpp",
                (
                    f"{normalize_digits(remote.get('inn'))}:"
                    f"{normalize_digits(remote.get('kpp'))}"
                    if normalize_digits(remote.get("inn")) and normalize_digits(remote.get("kpp"))
                    else ""
                ),
                "auto_match",
            ),
            ("inn", normalize_digits(remote.get("inn")), "review_match"),
            ("email", normalize_email(remote.get("email")), "review_match"),
            ("name", normalize_name(remote.get("name")), "review_match"),
        )
        for basis, value, classification in checks:
            if not value:
                continue
            ids = tuple(sorted(self.indexes[basis].get(value, ())))
            if len(ids) == 1:
                return MatchResult(classification, basis, ids[0], ids)
            if len(ids) > 1:
                return MatchResult("ambiguous", basis, None, ids)
        return MatchResult("new_customer", "none", None)


def compare_fields(
    remote: dict[str, Any], local: dict[str, Any] | None
) -> tuple[list[str], list[str]]:
    if not local:
        return [], []
    missing: list[str] = []
    conflicts: list[str] = []
    normalizers = {
        "name": normalize_name,
        "email": normalize_email,
        "inn": normalize_digits,
        "kpp": normalize_digits,
        "legal_address": normalize_name,
        "postal_address": normalize_name,
    }
    for field, normalizer in normalizers.items():
        remote_value = clean(remote.get(field))
        local_value = clean(local.get(field))
        if remote_value and not local_value:
            missing.append(field)
        elif remote_value and local_value and normalizer(remote_value) != normalizer(local_value):
            conflicts.append(field)
    return missing, conflicts
