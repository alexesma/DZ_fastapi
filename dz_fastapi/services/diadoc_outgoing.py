import base64
import hashlib
import os
import re
import xml.etree.ElementTree as ET
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from uuid import uuid4

import aiofiles
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ReturnDocumentStatus,
    ReturnFromCustomer,
    ReturnItem,
    ReturnToSupplier,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
)
from dz_fastapi.models.partner import (
    Customer,
    CustomerExternalReference,
    Provider,
    ProviderExternalReference,
)
from dz_fastapi.models.settings import DiadocIntegrationSettings

DIADOC_OUTGOING_DIR = os.getenv(
    "DIADOC_OUTGOING_DIR",
    "uploads/diadoc_outgoing",
)
_INVALID_FILENAME_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")
_DIADOC_PROVIDER_SOURCE_SYSTEMS = (
    "DIADOC_BOX",
    "DIADOC_COUNTERAGENT_BOX",
)
_DIADOC_CUSTOMER_SOURCE_SYSTEMS = (
    "DIADOC_BOX",
    "DIADOC_COUNTERAGENT_BOX",
)
_UTD_TYPE_NAMED_ID = "UniversalTransferDocument"
_UCD_TYPE_NAMED_ID = "UniversalCorrectionDocument"
_DEFAULT_ITEM_UNIT_CODE = "796"
_DEFAULT_ITEM_UNIT_NAME = "шт"
_DEFAULT_NO_VAT_LABEL = "без НДС"


def _safe_filename(filename: str | None) -> str:
    raw = str(filename or "").strip()
    if not raw:
        return "document.bin"
    sanitized = _INVALID_FILENAME_CHARS_RE.sub("_", raw)
    return sanitized[:240] or "document.bin"


def _decode_base64_content(value: str, *, field_name: str) -> bytes:
    try:
        return base64.b64decode(str(value or "").strip(), validate=True)
    except Exception as exc:
        raise ValueError(f"Invalid base64 in {field_name}") from exc


def _build_diadoc_outgoing_relative_path(
    *,
    environment: str,
    from_box_id_guid: str,
    filename: str,
) -> str:
    date_str = now_moscow().strftime("%Y%m%d")
    safe_box = _safe_filename(from_box_id_guid)
    safe_name = _safe_filename(filename)
    unique_prefix = now_moscow().strftime("%H%M%S%f")
    return os.path.join(
        DIADOC_OUTGOING_DIR,
        environment,
        safe_box,
        date_str,
        f"{unique_prefix}_{safe_name}",
    )


async def _write_content_to_relative_path(
    relative_path: str,
    content: bytes,
) -> str:
    abs_path = os.path.abspath(relative_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    async with aiofiles.open(abs_path, "wb") as fh:
        await fh.write(content)
    return relative_path


async def get_diadoc_outgoing_document(
    session: AsyncSession,
    *,
    outgoing_id: int,
) -> DiadocOutgoingDocument | None:
    return await session.get(DiadocOutgoingDocument, outgoing_id)


async def list_diadoc_outgoing_documents(
    session: AsyncSession,
    *,
    document_id: int | None = None,
    customer_id: int | None = None,
    provider_id: int | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
    limit: int = 100,
) -> list[DiadocOutgoingDocument]:
    stmt = (
        select(DiadocOutgoingDocument)
        .order_by(DiadocOutgoingDocument.id.desc())
        .limit(max(1, min(int(limit or 100), 300)))
    )
    if document_id is not None:
        stmt = stmt.where(DiadocOutgoingDocument.id == document_id)
    if customer_id is not None:
        stmt = stmt.where(DiadocOutgoingDocument.customer_id == customer_id)
    if provider_id is not None:
        stmt = stmt.where(DiadocOutgoingDocument.provider_id == provider_id)
    if source_type:
        stmt = stmt.where(DiadocOutgoingDocument.source_type == source_type)
    if source_id is not None:
        stmt = stmt.where(DiadocOutgoingDocument.source_id == source_id)
    return list((await session.execute(stmt)).scalars().all())


async def resolve_diadoc_box_for_provider(
    session: AsyncSession,
    *,
    provider_id: int,
) -> str | None:
    stmt = (
        select(ProviderExternalReference)
        .where(
            ProviderExternalReference.provider_id == provider_id,
            ProviderExternalReference.is_active.is_(True),
            ProviderExternalReference.source_system.in_(
                _DIADOC_PROVIDER_SOURCE_SYSTEMS
            ),
        )
        .order_by(ProviderExternalReference.id.asc())
    )
    reference = (await session.execute(stmt)).scalars().first()
    value = str(getattr(reference, "external_supplier_name", "") or "").strip()
    return value or None


async def resolve_diadoc_box_for_customer(
    session: AsyncSession,
    *,
    customer_id: int,
) -> str | None:
    stmt = (
        select(CustomerExternalReference)
        .where(
            CustomerExternalReference.customer_id == customer_id,
            CustomerExternalReference.is_active.is_(True),
            CustomerExternalReference.source_system.in_(
                _DIADOC_CUSTOMER_SOURCE_SYSTEMS
            ),
        )
        .order_by(CustomerExternalReference.id.asc())
    )
    reference = (await session.execute(stmt)).scalars().first()
    value = str(getattr(reference, "external_customer_name", "") or "").strip()
    return value or None


async def resolve_customer_by_diadoc_counteragent_box(
    session: AsyncSession,
    counteragent_box_id: str | None,
) -> Customer | None:
    value = str(counteragent_box_id or "").strip()
    if not value:
        return None

    stmt = (
        select(Customer)
        .join(CustomerExternalReference)
        .where(
            CustomerExternalReference.is_active.is_(True),
            CustomerExternalReference.source_system.in_(
                _DIADOC_CUSTOMER_SOURCE_SYSTEMS
            ),
            CustomerExternalReference.external_customer_name == value,
        )
        .order_by(CustomerExternalReference.id.asc())
    )
    return (await session.execute(stmt)).scalars().first()


def _is_blank_text(value: Any) -> bool:
    return not str(value or "").strip()


def _format_decimal(value: Any, places: int = 2) -> str:
    quant = Decimal("1").scaleb(-places)
    normalized = Decimal(str(value or 0)).quantize(
        quant,
        rounding=ROUND_HALF_UP,
    )
    return f"{normalized:.{places}f}"


def _split_full_name(value: str | None) -> tuple[str, str, str]:
    parts = [part for part in str(value or "").strip().split() if part]
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], parts[1], ""
    return parts[0], parts[1], " ".join(parts[2:])


def _build_document_creator_name(
    integration: DiadocIntegrationSettings,
    seller_org_payload: dict[str, Any],
) -> str:
    for value in (
        integration.organization_name,
        seller_org_payload.get("ShortName"),
        seller_org_payload.get("FullName"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return "Организация-отправитель"


def _formalized_document_name_for_function(document_function: str) -> str:
    normalized = str(document_function or "").strip().upper()
    if normalized == "СЧФ":
        return "Счет-фактура"
    return (
        "Документ об отгрузке товаров (выполнении работ), "
        "передаче имущественных прав (документ об оказании услуг)"
    )


def _formalized_correction_document_name_for_function(
    document_function: str,
) -> str:
    normalized = str(document_function or "").strip().upper()
    if normalized == "КСЧФ":
        return "Корректировочный счет-фактура"
    return (
        "Документ, подтверждающий согласие (факт уведомления) "
        "покупателя на изменение стоимости отгруженных товаров "
        "(выполненных работ, оказанных услуг), переданных "
        "имущественных прав"
    )


def _map_transfer_function_to_correction_function(
    base_function: str | None,
) -> str:
    normalized = str(base_function or "").strip().upper() or "ДОП"
    mapping = {
        "ДОП": "ДИС",
        "СЧФ": "КСЧФ",
        "СЧФДОП": "КСЧФДИС",
        "ДИС": "ДИС",
        "КСЧФ": "КСЧФ",
        "КСЧФДИС": "КСЧФДИС",
    }
    return mapping.get(normalized, "ДИС")


def _build_preferred_utd_functions(
    integration: DiadocIntegrationSettings,
) -> list[str]:
    raw_value = str(integration.formalized_default_function or "ДОП").strip()
    preferred: list[str] = []
    for candidate in (raw_value, "ДОП", "СЧФДОП", "СЧФ"):
        normalized = str(candidate or "").strip()
        if normalized and normalized not in preferred:
            preferred.append(normalized)
    return preferred


def _build_preferred_ukd_functions(
    integration: DiadocIntegrationSettings,
) -> list[str]:
    preferred_function = _map_transfer_function_to_correction_function(
        integration.formalized_default_function
    )
    fallbacks_map = {
        "ДИС": ["ДИС", "КСЧФДИС", "КСЧФ"],
        "КСЧФДИС": ["КСЧФДИС", "ДИС", "КСЧФ"],
        "КСЧФ": ["КСЧФ", "КСЧФДИС", "ДИС"],
    }
    return fallbacks_map.get(preferred_function, ["ДИС", "КСЧФДИС", "КСЧФ"])


def _find_formalized_doc_type(
    document_types_payload: dict[str, Any],
    *,
    type_named_id: str,
    preferred_functions: list[str],
) -> dict[str, str]:
    document_types = list(document_types_payload.get("DocumentTypes") or [])

    def _iter_versions(type_row: dict[str, Any], function_row: dict[str, Any]):
        for version_row in function_row.get("Versions") or []:
            titles = list(version_row.get("Titles") or [])
            if any(
                int(title.get("Index", title.get("TitleIndex", -1))) == 0
                and bool(title.get("IsFormal", True))
                for title in titles
            ):
                yield version_row

    def _choose_version(
        versions: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        if not versions:
            return None
        for row in versions:
            if bool(row.get("IsActual")):
                return row
        return versions[0]

    for doc_type in document_types:
        if (
            str(doc_type.get("TypeNamedId") or "").strip()
            != str(type_named_id or "").strip()
        ):
            continue
        functions = list(doc_type.get("Functions") or [])
        for function_name in preferred_functions or []:
            for function_row in functions:
                row_name = str(function_row.get("Name") or "").strip()
                if row_name != function_name:
                    continue
                selected_version = _choose_version(
                    list(_iter_versions(doc_type, function_row))
                )
                if selected_version is None:
                    continue
                return {
                    "type_named_id": str(type_named_id or "").strip(),
                    "document_function": row_name,
                    "document_version": str(
                        selected_version.get("Version") or ""
                    ).strip(),
                }
        for function_row in functions:
            selected_version = _choose_version(
                list(_iter_versions(doc_type, function_row))
            )
            if selected_version is None:
                continue
            return {
                "type_named_id": str(type_named_id or "").strip(),
                "document_function": str(
                    function_row.get("Name") or ""
                ).strip(),
                "document_version": str(
                    selected_version.get("Version") or ""
                ).strip(),
            }
    raise ValueError(
        "Diadoc box does not support requested formalized document type"
    )


def _append_address_xml(
    parent: ET.Element,
    *,
    address_payload: dict[str, Any] | None,
    fallback_text: str | None = None,
) -> None:
    address_payload = address_payload or {}
    address_el = ET.SubElement(parent, "Address")
    russian = address_payload.get("RussianAddress") or {}
    if isinstance(russian, dict) and str(russian.get("Region") or "").strip():
        attrs: dict[str, str] = {}
        for src, dest in (
            ("Region", "Region"),
            ("ZipCode", "ZipCode"),
            ("Territory", "Territory"),
            ("City", "City"),
            ("Locality", "Locality"),
            ("Street", "Street"),
            ("Building", "Building"),
            ("Block", "Block"),
            ("Apartment", "Apartment"),
            ("OtherInformation", "OtherInfo"),
            ("OtherInfo", "OtherInfo"),
        ):
            value = str(russian.get(src) or "").strip()
            if value:
                attrs[dest] = value
        if attrs:
            ET.SubElement(address_el, "RussianAddress", attrs)
            return

    text = str(fallback_text or "").strip()
    if text:
        ET.SubElement(
            address_el,
            "ForeignAddress",
            {"Country": "Россия", "Address": text},
        )
        return

    raise ValueError(
        "Organization address is not available for formalized UTD"
    )


def _append_organization_details_xml(
    parent: ET.Element,
    *,
    org_payload: dict[str, Any],
    fallback_name: str | None,
    fallback_inn: str | None,
    fallback_kpp: str | None,
    fallback_address_text: str | None = None,
) -> None:
    org_name = (
        str(org_payload.get("ShortName") or "").strip()
        or str(org_payload.get("FullName") or "").strip()
        or str(fallback_name or "").strip()
    )
    inn = (
        str(org_payload.get("Inn") or "").strip()
        or str(fallback_inn or "").strip()
    )
    kpp = (
        str(org_payload.get("Kpp") or "").strip()
        or str(fallback_kpp or "").strip()
    )
    fns_participant_id = str(org_payload.get("FnsParticipantId") or "").strip()
    if not org_name or not inn or not fns_participant_id:
        raise ValueError(
            "Diadoc organization does not contain enough data for formalized UTD"
        )

    attrs = {
        "OrgType": "2",
        "OrgName": org_name,
        "Inn": inn,
        "FnsParticipantId": fns_participant_id,
    }
    if kpp:
        attrs["Kpp"] = kpp
    details_el = ET.SubElement(parent, "OrganizationDetails", attrs)
    _append_address_xml(
        details_el,
        address_payload=org_payload.get("Address"),
        fallback_text=fallback_address_text,
    )


def _append_signer_details_xml(
    parent: ET.Element,
    *,
    integration: DiadocIntegrationSettings,
    seller_org_payload: dict[str, Any],
) -> None:
    signers_el = ET.SubElement(parent, "Signers")
    seller_name = _build_document_creator_name(integration, seller_org_payload)
    signer_inn = (
        str(seller_org_payload.get("Inn") or "").strip()
        or str(integration.organization_inn or "").strip()
    )
    last_name, first_name, middle_name = _split_full_name(
        integration.signer_full_name
    )
    attrs = {
        "LastName": last_name,
        "FirstName": first_name,
        "SignerPowers": "0",
        "SignerStatus": "1",
        "SignerType": "1",
        "Position": str(integration.signer_position or "").strip(),
        "SignerOrganizationName": seller_name,
    }
    if middle_name:
        attrs["MiddleName"] = middle_name
    if signer_inn:
        attrs["Inn"] = signer_inn
    signer_basis = str(integration.signer_basis or "").strip()
    if signer_basis:
        attrs["SignerPowersBase"] = signer_basis
    ET.SubElement(signers_el, "SignerDetails", attrs)


async def build_shipment_formalized_readiness(
    session: AsyncSession,
    *,
    shipment_id: int,
    integration: DiadocIntegrationSettings,
) -> dict[str, Any]:
    document = await _load_shipment_document(session, shipment_id=shipment_id)
    if document is None:
        raise ValueError("Shipment document not found")

    customer = getattr(document, "customer", None)
    items = list(getattr(document, "items", []) or [])

    missing_required_fields: list[str] = []
    warnings: list[str] = []
    recommended_actions: list[str] = []

    has_counteragent_binding = False
    if customer is not None and getattr(customer, "id", None) is not None:
        has_counteragent_binding = bool(
            await resolve_diadoc_box_for_customer(
                session,
                customer_id=int(customer.id),
            )
        )

    if document.status != ShipmentDocumentStatus.POSTED:
        missing_required_fields.append(
            "Отгрузка должна быть проведена перед отправкой в Диадок."
        )
    if customer is None:
        missing_required_fields.append("У отгрузки не выбран клиент.")
    if not items:
        missing_required_fields.append("В отгрузке нет строк товаров.")
    if items and any(
        int(getattr(item, "quantity", 0) or 0) <= 0 for item in items
    ):
        missing_required_fields.append(
            "Во всех строках отгрузки должно быть положительное количество."
        )
    if _is_blank_text(getattr(integration, "box_id_guid", None)):
        missing_required_fields.append("Не выбран ящик Диадока организации.")
    if _is_blank_text(getattr(integration, "refresh_token", None)):
        missing_required_fields.append(
            "Интеграция Диадока не подключена или истёк refresh token."
        )
    if not has_counteragent_binding:
        missing_required_fields.append(
            "Для клиента не настроена привязка к ящику контрагента Диадока."
        )

    if customer is not None:
        if _is_blank_text(getattr(customer, "inn", None)):
            warnings.append("У клиента не заполнен ИНН.")
        if _is_blank_text(getattr(customer, "legal_address", None)):
            warnings.append("У клиента не заполнен юридический адрес.")
        if _is_blank_text(getattr(customer, "kpp", None)):
            warnings.append(
                "У клиента не заполнен КПП. Для части контрагентов это "
                "допустимо, но для формализованного УПД обычно лучше "
                "заполнить."
            )
        if _is_blank_text(getattr(customer, "postal_address", None)):
            warnings.append("У клиента не заполнен почтовый адрес.")

    if _is_blank_text(getattr(integration, "organization_name", None)):
        warnings.append(
            "В настройках Диадока не определено название организации-продавца."
        )
    if _is_blank_text(getattr(integration, "organization_inn", None)):
        warnings.append(
            "В настройках Диадока не определён ИНН организации-продавца."
        )
    if _is_blank_text(getattr(integration, "seller_legal_address", None)):
        warnings.append(
            "В профиле Диадока не заполнен юридический адрес продавца."
        )
    if _is_blank_text(getattr(integration, "signer_full_name", None)):
        missing_required_fields.append(
            "В профиле Диадока не заполнено ФИО подписанта."
        )
    if _is_blank_text(getattr(integration, "signer_position", None)):
        missing_required_fields.append(
            "В профиле Диадока не заполнена должность подписанта."
        )
    if _is_blank_text(getattr(integration, "signer_basis", None)):
        warnings.append(
            "В профиле Диадока не заполнено основание полномочий подписанта."
        )
    if _is_blank_text(getattr(integration, "seller_postal_address", None)):
        warnings.append(
            "В профиле Диадока не заполнен почтовый адрес продавца."
        )
    if _is_blank_text(getattr(integration, "organization_kpp", None)):
        warnings.append(
            "В настройках Диадока не определён КПП организации-продавца."
        )

    if _is_blank_text(getattr(document, "doc_number", None)):
        warnings.append(
            "У отгрузки нет номера документа. Для формализованного УПД лучше иметь явный номер."
        )

    if any(
        _is_blank_text(getattr(getattr(item, "lot", None), "gtd_number", None))
        for item in items
    ):
        warnings.append(
            "У части строк нет ГТД. Для товара российского происхождения "
            "это нормально, но импортные позиции лучше проверить отдельно."
        )

    if any(_is_blank_text(getattr(item, "price", None)) for item in items):
        missing_required_fields.append(
            "У части строк не заполнена цена. Для формализованного УПД цена нужна в каждой строке."
        )

    if customer is None:
        recommended_actions.append("Откройте отгрузку и укажите клиента.")
    if not has_counteragent_binding and customer is not None:
        recommended_actions.append(
            "Привяжите клиента к контрагенту Диадока на карточке клиента "
            "или в разделе Документы → Диадок."
        )
    if any(
        "клиента" in item.lower()
        for item in missing_required_fields + warnings
    ):
        recommended_actions.append(
            "Заполните юридические реквизиты на карточке клиента."
        )
    if any(
        "продавца" in item.lower() or "подписанта" in item.lower()
        for item in missing_required_fields + warnings
    ):
        recommended_actions.append(
            "Заполните профиль формализованного УПД в разделе Документы → Диадок."
        )

    ready_nonformalized = not any(
        item
        for item in missing_required_fields
        if any(
            marker in item.lower()
            for marker in (
                "отгрузка должна быть проведена",
                "не выбран клиент",
                "нет строк товаров",
                "положительное количество",
                "ящик диадока",
                "refresh token",
                "привязка к ящику контрагента",
            )
        )
    )
    ready_formalized = ready_nonformalized and not missing_required_fields

    return {
        "shipment_id": int(document.id),
        "ready_nonformalized": ready_nonformalized,
        "ready_formalized": ready_formalized,
        "missing_required_fields": missing_required_fields,
        "warnings": warnings,
        "customer_id": (
            int(customer.id)
            if customer is not None
            and getattr(customer, "id", None) is not None
            else None
        ),
        "customer_name": (
            str(getattr(customer, "name", "") or "").strip()
            if customer is not None
            else None
        ),
        "recommended_actions": recommended_actions,
    }


async def build_shipments_formalized_readiness(
    session: AsyncSession,
    *,
    shipment_ids: list[int],
    integration: DiadocIntegrationSettings,
) -> list[dict[str, Any]]:
    readiness_rows: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for raw_id in shipment_ids:
        shipment_id = int(raw_id)
        if shipment_id in seen_ids:
            continue
        seen_ids.add(shipment_id)
        try:
            readiness_rows.append(
                await build_shipment_formalized_readiness(
                    session,
                    shipment_id=shipment_id,
                    integration=integration,
                )
            )
        except ValueError:
            continue
    return readiness_rows


async def _load_customer_return_document(
    session: AsyncSession,
    *,
    return_id: int,
) -> ReturnFromCustomer | None:
    stmt = (
        select(ReturnFromCustomer)
        .where(ReturnFromCustomer.id == return_id)
        .options(
            selectinload(ReturnFromCustomer.customer),
            selectinload(ReturnFromCustomer.shipment_document),
            selectinload(ReturnFromCustomer.warehouse),
            selectinload(ReturnFromCustomer.items)
            .selectinload(ReturnItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ReturnFromCustomer.items).selectinload(
                ReturnItem.lot
            ),
            selectinload(ReturnFromCustomer.items)
            .selectinload(ReturnItem.shipment_item)
            .selectinload(ShipmentDocumentItem.lot),
        )
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def _load_supplier_return_document(
    session: AsyncSession,
    *,
    return_id: int,
) -> ReturnToSupplier | None:
    stmt = (
        select(ReturnToSupplier)
        .where(ReturnToSupplier.id == return_id)
        .options(
            selectinload(ReturnToSupplier.provider),
            selectinload(ReturnToSupplier.supplier_receipt),
            selectinload(ReturnToSupplier.warehouse),
            selectinload(ReturnToSupplier.items)
            .selectinload(ReturnItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ReturnToSupplier.items).selectinload(ReturnItem.lot),
            selectinload(ReturnToSupplier.items).selectinload(
                ReturnItem.supplier_receipt_item
            ),
        )
    )
    return (await session.execute(stmt)).scalar_one_or_none()


def _append_common_formalized_sender_checks(
    integration: DiadocIntegrationSettings,
    *,
    missing_required_fields: list[str],
    warnings: list[str],
) -> None:
    if _is_blank_text(getattr(integration, "box_id_guid", None)):
        missing_required_fields.append("Не выбран ящик Диадока организации.")
    if _is_blank_text(getattr(integration, "refresh_token", None)):
        missing_required_fields.append(
            "Интеграция Диадока не подключена или истёк refresh token."
        )
    if _is_blank_text(getattr(integration, "organization_name", None)):
        warnings.append(
            "В настройках Диадока не определено название организации-отправителя."
        )
    if _is_blank_text(getattr(integration, "organization_inn", None)):
        warnings.append(
            "В настройках Диадока не определён ИНН организации-отправителя."
        )
    if _is_blank_text(getattr(integration, "organization_kpp", None)):
        warnings.append(
            "В настройках Диадока не определён КПП организации-отправителя."
        )
    if _is_blank_text(getattr(integration, "seller_legal_address", None)):
        warnings.append(
            "В профиле Диадока не заполнен юридический адрес отправителя."
        )
    if _is_blank_text(getattr(integration, "seller_postal_address", None)):
        warnings.append(
            "В профиле Диадока не заполнен почтовый адрес отправителя."
        )
    if _is_blank_text(getattr(integration, "signer_full_name", None)):
        missing_required_fields.append(
            "В профиле Диадока не заполнено ФИО подписанта."
        )
    if _is_blank_text(getattr(integration, "signer_position", None)):
        missing_required_fields.append(
            "В профиле Диадока не заполнена должность подписанта."
        )
    if _is_blank_text(getattr(integration, "signer_basis", None)):
        warnings.append(
            "В профиле Диадока не заполнено основание полномочий подписанта."
        )


async def build_customer_return_formalized_readiness(
    session: AsyncSession,
    *,
    return_id: int,
    integration: DiadocIntegrationSettings,
) -> dict[str, Any]:
    document = await _load_customer_return_document(
        session,
        return_id=return_id,
    )
    if document is None:
        raise ValueError("Customer return document not found")

    customer = getattr(document, "customer", None)
    source_document = getattr(document, "shipment_document", None)
    items = list(getattr(document, "items", []) or [])
    missing_required_fields: list[str] = []
    warnings: list[str] = []
    recommended_actions: list[str] = []

    has_counteragent_binding = False
    if customer is not None and getattr(customer, "id", None) is not None:
        has_counteragent_binding = bool(
            await resolve_diadoc_box_for_customer(
                session,
                customer_id=int(customer.id),
            )
        )

    if document.status != ReturnDocumentStatus.CONFIRMED:
        missing_required_fields.append(
            "Возврат от клиента должен быть подтверждён перед отправкой УКД в Диадок."
        )
    if customer is None:
        missing_required_fields.append("У возврата не выбран клиент.")
    if source_document is None:
        missing_required_fields.append(
            "У возврата не привязана исходная отгрузка."
        )
    if source_document is not None and _is_blank_text(
        getattr(source_document, "doc_number", None)
    ):
        missing_required_fields.append(
            "У исходной отгрузки отсутствует номер документа."
        )
    if (
        source_document is not None
        and getattr(source_document, "doc_date", None) is None
    ):
        missing_required_fields.append(
            "У исходной отгрузки отсутствует дата документа."
        )
    if not items:
        missing_required_fields.append("В возврате нет строк товаров.")
    if any(int(getattr(item, "quantity", 0) or 0) <= 0 for item in items):
        missing_required_fields.append(
            "Во всех строках возврата должно быть положительное количество."
        )
    if any(getattr(item, "price", None) is None for item in items):
        missing_required_fields.append(
            "Во всех строках возврата должна быть указана цена."
        )
    if not has_counteragent_binding:
        missing_required_fields.append(
            "Для клиента не настроена привязка к ящику контрагента Диадока."
        )

    _append_common_formalized_sender_checks(
        integration,
        missing_required_fields=missing_required_fields,
        warnings=warnings,
    )

    if customer is not None:
        if _is_blank_text(getattr(customer, "inn", None)):
            warnings.append("У клиента не заполнен ИНН.")
        if _is_blank_text(getattr(customer, "kpp", None)):
            warnings.append(
                "У клиента не заполнен КПП. Для формализованного УКД "
                "обычно лучше заполнить его заранее."
            )
        if _is_blank_text(getattr(customer, "legal_address", None)):
            warnings.append("У клиента не заполнен юридический адрес.")
        if _is_blank_text(getattr(customer, "postal_address", None)):
            warnings.append("У клиента не заполнен почтовый адрес.")

    if _is_blank_text(getattr(document, "doc_number", None)):
        warnings.append(
            "У возврата нет собственного номера документа. Лучше заполнить его перед отправкой."
        )
    if any(
        _is_blank_text(getattr(item, "gtd_number", None))
        and _is_blank_text(
            getattr(getattr(item, "lot", None), "gtd_number", None)
        )
        for item in items
    ):
        warnings.append(
            "У части строк нет ГТД. Для товара российского происхождения это нормально."
        )

    if customer is None:
        recommended_actions.append("Откройте возврат и укажите клиента.")
    if document.status != ReturnDocumentStatus.CONFIRMED:
        recommended_actions.append(
            "Подтвердите возврат на складе, чтобы зафиксировать фактическое поступление товара."
        )
    if not has_counteragent_binding and customer is not None:
        recommended_actions.append(
            "Привяжите клиента к контрагенту Диадока на карточке клиента "
            "или в разделе Документы → Диадок."
        )
    if any(
        "клиента" in item.lower()
        for item in missing_required_fields + warnings
    ):
        recommended_actions.append(
            "Заполните юридические реквизиты на карточке клиента."
        )
    if any(
        marker in item.lower()
        for item in missing_required_fields + warnings
        for marker in ("отправителя", "подписанта")
    ):
        recommended_actions.append(
            "Заполните профиль формализованных документов в разделе Документы → Диадок."
        )

    return {
        "return_kind": "customer",
        "document_id": int(document.id),
        "status": str(document.status.value),
        "ready_formalized": not missing_required_fields,
        "missing_required_fields": missing_required_fields,
        "warnings": warnings,
        "recommended_actions": recommended_actions,
        "customer_id": (
            int(customer.id)
            if customer is not None
            and getattr(customer, "id", None) is not None
            else None
        ),
        "customer_name": (
            str(getattr(customer, "name", "") or "").strip()
            if customer is not None
            else None
        ),
        "provider_id": None,
        "provider_name": None,
        "source_document_id": (
            int(source_document.id)
            if source_document is not None
            and getattr(source_document, "id", None) is not None
            else None
        ),
        "source_document_number": (
            str(getattr(source_document, "doc_number", "") or "").strip()
            or None
            if source_document is not None
            else None
        ),
        "source_document_date": (
            getattr(source_document, "doc_date", None).date()
            if source_document is not None
            and getattr(source_document, "doc_date", None) is not None
            else None
        ),
    }


async def build_supplier_return_formalized_readiness(
    session: AsyncSession,
    *,
    return_id: int,
    integration: DiadocIntegrationSettings,
) -> dict[str, Any]:
    document = await _load_supplier_return_document(
        session,
        return_id=return_id,
    )
    if document is None:
        raise ValueError("Supplier return document not found")

    provider = getattr(document, "provider", None)
    source_document = getattr(document, "supplier_receipt", None)
    items = list(getattr(document, "items", []) or [])
    missing_required_fields: list[str] = []
    warnings: list[str] = []
    recommended_actions: list[str] = []

    has_counteragent_binding = False
    if provider is not None and getattr(provider, "id", None) is not None:
        has_counteragent_binding = bool(
            await resolve_diadoc_box_for_provider(
                session,
                provider_id=int(provider.id),
            )
        )

    if document.status not in {
        ReturnDocumentStatus.SHIPPED,
        ReturnDocumentStatus.CONFIRMED,
    }:
        missing_required_fields.append(
            "Возврат поставщику должен быть отгружен перед отправкой УКД в Диадок."
        )
    if provider is None:
        missing_required_fields.append("У возврата не выбран поставщик.")
    if source_document is None:
        missing_required_fields.append(
            "У возврата не привязано исходное поступление."
        )
    if source_document is not None and _is_blank_text(
        getattr(source_document, "document_number", None)
    ):
        missing_required_fields.append(
            "У исходного поступления отсутствует номер документа."
        )
    if (
        source_document is not None
        and getattr(source_document, "document_date", None) is None
    ):
        missing_required_fields.append(
            "У исходного поступления отсутствует дата документа."
        )
    if not items:
        missing_required_fields.append("В возврате нет строк товаров.")
    if any(int(getattr(item, "quantity", 0) or 0) <= 0 for item in items):
        missing_required_fields.append(
            "Во всех строках возврата должно быть положительное количество."
        )
    if any(getattr(item, "price", None) is None for item in items):
        missing_required_fields.append(
            "Во всех строках возврата должна быть указана цена."
        )
    if not has_counteragent_binding:
        missing_required_fields.append(
            "Для поставщика не настроена привязка к ящику контрагента Диадока."
        )

    _append_common_formalized_sender_checks(
        integration,
        missing_required_fields=missing_required_fields,
        warnings=warnings,
    )

    if _is_blank_text(getattr(document, "doc_number", None)):
        warnings.append(
            "У возврата нет собственного номера документа. Лучше заполнить его перед отправкой."
        )
    if any(
        _is_blank_text(getattr(item, "gtd_number", None))
        and _is_blank_text(
            getattr(getattr(item, "lot", None), "gtd_number", None)
        )
        for item in items
    ):
        warnings.append(
            "У части строк нет ГТД. Для товара российского происхождения это нормально."
        )

    if provider is None:
        recommended_actions.append("Откройте возврат и укажите поставщика.")
    if document.status not in {
        ReturnDocumentStatus.SHIPPED,
        ReturnDocumentStatus.CONFIRMED,
    }:
        recommended_actions.append(
            "Проведите отгрузку возврата поставщику, чтобы зафиксировать складское списание."
        )
    if not has_counteragent_binding and provider is not None:
        recommended_actions.append(
            "Привяжите поставщика к контрагенту Диадока на карточке "
            "поставщика или в разделе Документы → Диадок."
        )
    if any(
        marker in item.lower()
        for item in missing_required_fields + warnings
        for marker in ("отправителя", "подписанта")
    ):
        recommended_actions.append(
            "Заполните профиль формализованных документов в разделе Документы → Диадок."
        )

    return {
        "return_kind": "supplier",
        "document_id": int(document.id),
        "status": str(document.status.value),
        "ready_formalized": not missing_required_fields,
        "missing_required_fields": missing_required_fields,
        "warnings": warnings,
        "recommended_actions": recommended_actions,
        "customer_id": None,
        "customer_name": None,
        "provider_id": (
            int(provider.id)
            if provider is not None
            and getattr(provider, "id", None) is not None
            else None
        ),
        "provider_name": (
            str(getattr(provider, "name", "") or "").strip()
            if provider is not None
            else None
        ),
        "source_document_id": (
            int(source_document.id)
            if source_document is not None
            and getattr(source_document, "id", None) is not None
            else None
        ),
        "source_document_number": (
            str(getattr(source_document, "document_number", "") or "").strip()
            or None
            if source_document is not None
            else None
        ),
        "source_document_date": (
            getattr(source_document, "document_date", None)
            if source_document is not None
            else None
        ),
    }


def _format_quantity_text(value: Decimal) -> str:
    return _format_decimal(value, places=3).rstrip("0").rstrip(".") or "0"


def _build_return_product_name(item: ReturnItem) -> str:
    autopart = getattr(item, "autopart", None)
    for candidate in (
        getattr(autopart, "name", None),
        getattr(item, "autopart_name", None),
        getattr(autopart, "oem_number", None),
        getattr(item, "oem_number", None),
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return f"Товар #{item.autopart_id or item.id}"


def _build_return_item_oem(item: ReturnItem) -> str | None:
    autopart = getattr(item, "autopart", None)
    for candidate in (
        getattr(autopart, "oem_number", None),
        getattr(item, "oem_number", None),
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return None


def _append_customs_declaration_xml(
    parent: ET.Element,
    *,
    declaration_number: str | None,
    country_code: str | None,
) -> None:
    normalized_number = str(declaration_number or "").strip()
    normalized_country = str(country_code or "").strip()
    if not normalized_number or not normalized_country.isdigit():
        return
    declarations_el = ET.SubElement(parent, "CustomsDeclarations")
    ET.SubElement(
        declarations_el,
        "CustomsDeclaration",
        {
            "Country": normalized_country,
            "DeclarationNumber": normalized_number,
        },
    )


def _build_formalized_ukd_user_data_xml(
    *,
    document: ReturnFromCustomer | ReturnToSupplier,
    items: list[ReturnItem],
    integration: DiadocIntegrationSettings,
    seller_org_payload: dict[str, Any],
    buyer_org_payload: dict[str, Any],
    document_function: str,
    buyer_fallback_name: str | None,
    buyer_fallback_inn: str | None,
    buyer_fallback_kpp: str | None,
    buyer_fallback_address: str | None,
    original_document_name: str,
    original_document_number: str,
    original_document_date: date,
    operation_content: str,
    correction_base_name: str,
) -> bytes:
    document_date = document.doc_date.date().strftime("%d.%m.%Y")
    current_document_number = str(
        document.doc_number or f"RETURN-{document.id}"
    ).strip()
    seller_name = _build_document_creator_name(integration, seller_org_payload)
    total = Decimal("0")
    for item in items:
        total += Decimal(str(item.price)) * Decimal(str(item.quantity or 0))

    root_attrs = {
        "DocumentDate": document_date,
        "DocumentNumber": current_document_number,
        "Currency": "643",
        "CurrencyName": "1",
        "Function": document_function,
        "DocumentName": _formalized_correction_document_name_for_function(
            document_function
        ),
        "DocumentCreator": seller_name,
        "Uid": str(uuid4()),
        "xmlns:xs": "http://www.w3.org/2001/XMLSchema",
    }
    sender_fns = str(seller_org_payload.get("FnsParticipantId") or "").strip()
    if sender_fns:
        root_attrs["SenderFnsParticipantId"] = sender_fns
    recipient_fns = str(
        buyer_org_payload.get("FnsParticipantId") or ""
    ).strip()
    if recipient_fns:
        root_attrs["RecipientFnsParticipantId"] = recipient_fns
    root = ET.Element(_UCD_TYPE_NAMED_ID, root_attrs)

    sellers_el = ET.SubElement(root, "Sellers")
    seller_el = ET.SubElement(sellers_el, "Seller")
    _append_organization_details_xml(
        seller_el,
        org_payload=seller_org_payload,
        fallback_name=integration.organization_name,
        fallback_inn=integration.organization_inn,
        fallback_kpp=integration.organization_kpp,
        fallback_address_text=integration.seller_legal_address,
    )

    buyers_el = ET.SubElement(root, "Buyers")
    buyer_el = ET.SubElement(buyers_el, "Buyer")
    _append_organization_details_xml(
        buyer_el,
        org_payload=buyer_org_payload,
        fallback_name=buyer_fallback_name,
        fallback_inn=buyer_fallback_inn,
        fallback_kpp=buyer_fallback_kpp,
        fallback_address_text=buyer_fallback_address,
    )

    _append_signer_details_xml(
        root,
        integration=integration,
        seller_org_payload=seller_org_payload,
    )

    event_el = ET.SubElement(
        root,
        "EventContent",
        {"OperationContent": operation_content},
    )
    ET.SubElement(
        event_el,
        "CorrectionBase",
        {
            "BaseDocumentName": correction_base_name,
            "BaseDocumentNumber": current_document_number,
            "BaseDocumentDate": document_date,
        },
    )
    ET.SubElement(
        event_el,
        "TransferDocDetails",
        {
            "BaseDocumentName": original_document_name,
            "BaseDocumentNumber": str(original_document_number).strip(),
            "BaseDocumentDate": original_document_date.strftime("%d.%m.%Y"),
        },
    )

    table_el = ET.SubElement(root, "Table")
    for idx, item in enumerate(items, start=1):
        quantity = Decimal(str(item.quantity or 0))
        price = Decimal(str(item.price))
        subtotal = price * quantity
        item_el = ET.SubElement(
            table_el,
            "Item",
            {
                "OriginalNumber": str(idx),
                "Product": _build_return_product_name(item),
            },
        )
        oem_number = _build_return_item_oem(item)
        if oem_number:
            item_el.set("ItemVendorCode", oem_number)

        ET.SubElement(
            item_el,
            "Unit",
            {
                "OriginalValue": _DEFAULT_ITEM_UNIT_CODE,
                "CorrectedValue": _DEFAULT_ITEM_UNIT_CODE,
            },
        )
        ET.SubElement(
            item_el,
            "UnitName",
            {
                "OriginalValue": _DEFAULT_ITEM_UNIT_NAME,
                "CorrectedValue": _DEFAULT_ITEM_UNIT_NAME,
            },
        )
        ET.SubElement(
            item_el,
            "Quantity",
            {
                "OriginalValue": _format_quantity_text(quantity),
                "CorrectedValue": "0",
            },
        )
        price_text = _format_decimal(price)
        subtotal_text = _format_decimal(subtotal)
        ET.SubElement(
            item_el,
            "Price",
            {
                "OriginalValue": price_text,
                "CorrectedValue": price_text,
            },
        )
        subtotal_without_vat_el = ET.SubElement(
            item_el,
            "SubtotalWithVatExcluded",
            {
                "OriginalValue": subtotal_text,
                "CorrectedValue": "0.00",
            },
        )
        ET.SubElement(
            subtotal_without_vat_el,
            "AmountsDec",
        ).text = subtotal_text
        subtotal_el = ET.SubElement(
            item_el,
            "Subtotal",
            {
                "OriginalValue": subtotal_text,
                "CorrectedValue": "0.00",
            },
        )
        ET.SubElement(subtotal_el, "AmountsDec").text = subtotal_text
        ET.SubElement(
            item_el,
            "TaxRate",
            {
                "OriginalValue": _DEFAULT_NO_VAT_LABEL,
                "CorrectedValue": _DEFAULT_NO_VAT_LABEL,
            },
        )
        vat_el = ET.SubElement(item_el, "Vat")
        ET.SubElement(vat_el, "AmountsDec").text = "0.00"
        ET.SubElement(
            item_el,
            "WithoutVat",
            {
                "OriginalValue": "true",
                "CorrectedValue": "true",
            },
        )

        item_lot = getattr(item, "lot", None)
        source_lot = getattr(getattr(item, "shipment_item", None), "lot", None)
        _append_customs_declaration_xml(
            item_el,
            declaration_number=(
                item.gtd_number
                or getattr(item_lot, "gtd_number", None)
                or getattr(source_lot, "gtd_number", None)
            ),
            country_code=(
                item.country_code
                or getattr(item_lot, "country_code", None)
                or getattr(source_lot, "country_code", None)
            ),
        )

    zero_text = "0.00"
    total_text = _format_decimal(total)
    totals_inc_el = ET.SubElement(table_el, "TotalsInc")
    ET.SubElement(totals_inc_el, "TotalWithVatExcluded").text = zero_text
    ET.SubElement(totals_inc_el, "Vat").text = zero_text
    ET.SubElement(totals_inc_el, "Total").text = zero_text

    totals_dec_el = ET.SubElement(table_el, "TotalsDec")
    ET.SubElement(totals_dec_el, "TotalWithVatExcluded").text = total_text
    ET.SubElement(totals_dec_el, "Vat").text = zero_text
    ET.SubElement(totals_dec_el, "Total").text = total_text

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _customer_return_formalized_file_name(doc: ReturnFromCustomer) -> str:
    suffix = _safe_filename(doc.doc_number or f"customer_return_{doc.id}")
    return f"{suffix}_ukd.xml"


def _supplier_return_formalized_file_name(doc: ReturnToSupplier) -> str:
    suffix = _safe_filename(doc.doc_number or f"supplier_return_{doc.id}")
    return f"{suffix}_ukd.xml"


def _build_document_metadata(
    *,
    file_name: str,
    type_named_id: str,
    metadata: dict[str, str] | None,
    document_number: str | None,
    document_date: date | None,
) -> list[dict[str, str]]:
    normalized: dict[str, str] = {}
    for key, value in (metadata or {}).items():
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if key_text and value_text:
            normalized[key_text] = value_text

    if type_named_id.strip().lower() == "nonformalized":
        normalized.setdefault("FileName", file_name)
    if document_number:
        normalized.setdefault("DocumentNumber", str(document_number))
    if document_date:
        normalized.setdefault(
            "DocumentDate", document_date.strftime("%d.%m.%Y")
        )
    return [{"Key": key, "Value": value} for key, value in normalized.items()]


def _extract_first_entity_id(payload: Any) -> str | None:
    if isinstance(payload, dict):
        entity_id = payload.get("EntityId")
        if entity_id:
            return str(entity_id)
        for value in payload.values():
            found = _extract_first_entity_id(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _extract_first_entity_id(item)
            if found:
                return found
    return None


def _extract_message_id(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("MessageId", "Id"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


async def _load_shipment_document(
    session: AsyncSession,
    *,
    shipment_id: int,
) -> ShipmentDocument | None:
    stmt = (
        select(ShipmentDocument)
        .where(ShipmentDocument.id == shipment_id)
        .options(
            selectinload(ShipmentDocument.items)
            .selectinload(ShipmentDocumentItem.autopart)
            .selectinload(AutoPart.brand),
            selectinload(ShipmentDocument.items).selectinload(
                ShipmentDocumentItem.storage_location
            ),
            selectinload(ShipmentDocument.items).selectinload(
                ShipmentDocumentItem.lot
            ),
            selectinload(ShipmentDocument.customer),
            selectinload(ShipmentDocument.warehouse),
        )
    )
    return (await session.execute(stmt)).scalar_one_or_none()


def _shipment_export_file_name(doc: ShipmentDocument) -> str:
    suffix = _safe_filename(doc.doc_number or f"shipment_{doc.id}")
    return f"{suffix}.xml"


def _shipment_document_number(doc: ShipmentDocument) -> str | None:
    value = str(doc.doc_number or "").strip()
    return value or None


def _shipment_formalized_file_name(doc: ShipmentDocument) -> str:
    suffix = _safe_filename(doc.doc_number or f"shipment_{doc.id}")
    return f"{suffix}_utd.xml"


def _build_shipment_document_xml(doc: ShipmentDocument) -> bytes:
    customer = getattr(doc, "customer", None)
    warehouse = getattr(doc, "warehouse", None)

    root = ET.Element("ShipmentDocument")
    ET.SubElement(root, "InternalId").text = str(doc.id)
    if doc.doc_number:
        ET.SubElement(root, "DocumentNumber").text = str(doc.doc_number)
    ET.SubElement(root, "DocumentDate").text = doc.doc_date.date().isoformat()
    ET.SubElement(root, "Status").text = str(doc.status.value)
    if customer is not None:
        customer_el = ET.SubElement(root, "Customer")
        ET.SubElement(customer_el, "Id").text = str(customer.id)
        ET.SubElement(customer_el, "Name").text = str(customer.name or "")
        if getattr(customer, "email_contact", None):
            ET.SubElement(customer_el, "Email").text = str(
                customer.email_contact
            )
    if warehouse is not None:
        warehouse_el = ET.SubElement(root, "Warehouse")
        ET.SubElement(warehouse_el, "Id").text = str(warehouse.id)
        ET.SubElement(warehouse_el, "Name").text = str(warehouse.name or "")
    if doc.reason:
        ET.SubElement(root, "Reason").text = str(doc.reason)
    if doc.notes:
        ET.SubElement(root, "Notes").text = str(doc.notes)

    items_el = ET.SubElement(root, "Items")
    for idx, item in enumerate(doc.items or [], start=1):
        autopart = getattr(item, "autopart", None)
        lot = getattr(item, "lot", None)
        location = getattr(item, "storage_location", None)

        item_el = ET.SubElement(items_el, "Item")
        ET.SubElement(item_el, "LineNo").text = str(idx)
        ET.SubElement(item_el, "AutoPartId").text = str(item.autopart_id)
        if autopart is not None:
            ET.SubElement(item_el, "OemNumber").text = str(
                autopart.oem_number or ""
            )
            ET.SubElement(item_el, "Name").text = str(autopart.name or "")
            if getattr(autopart, "brand", None) is not None:
                ET.SubElement(item_el, "Brand").text = str(
                    autopart.brand.name or ""
                )
        ET.SubElement(item_el, "Quantity").text = str(item.quantity)
        if item.price is not None:
            ET.SubElement(item_el, "Price").text = str(item.price)
        if location is not None:
            ET.SubElement(item_el, "StorageLocation").text = str(
                location.name or ""
            )
        if lot is not None:
            if getattr(lot, "gtd_number", None):
                ET.SubElement(item_el, "GtdNumber").text = str(lot.gtd_number)
            if getattr(lot, "country_code", None):
                ET.SubElement(item_el, "CountryCode").text = str(
                    lot.country_code
                )
            if getattr(lot, "country_name", None):
                ET.SubElement(item_el, "CountryName").text = str(
                    lot.country_name
                )

    return ET.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
    )


async def build_diadoc_payload_from_shipment(
    session: AsyncSession,
    *,
    shipment_id: int,
    customer_id: int | None = None,
) -> dict[str, Any]:
    document = await _load_shipment_document(session, shipment_id=shipment_id)
    if document is None:
        raise ValueError("Shipment document not found")
    if document.status != ShipmentDocumentStatus.POSTED:
        raise ValueError(
            "Only posted shipment documents can be sent to Diadoc"
        )

    resolved_customer_id = (
        int(customer_id)
        if customer_id is not None
        else (
            int(document.customer_id)
            if document.customer_id is not None
            else None
        )
    )
    if resolved_customer_id is None:
        raise ValueError(
            "Shipment document has no customer and customer override is not set"
        )

    return {
        "customer_id": resolved_customer_id,
        "file_name": _shipment_export_file_name(document),
        "content_base64": base64.b64encode(
            _build_shipment_document_xml(document)
        ).decode("ascii"),
        "document_number": _shipment_document_number(document),
        "document_date": document.doc_date.date(),
        "source_type": "shipment_document",
        "source_id": int(document.id),
        "type_named_id": "Nonformalized",
        "metadata": {
            "FileName": _shipment_export_file_name(document),
            "DocumentKind": "ShipmentDocument",
        },
    }


def _build_formalized_utd_user_data_xml(
    *,
    document: ShipmentDocument,
    integration: DiadocIntegrationSettings,
    seller_org_payload: dict[str, Any],
    buyer_org_payload: dict[str, Any],
    document_function: str,
) -> bytes:
    document_date = document.doc_date.date().strftime("%d.%m.%Y")
    document_number = (
        _shipment_document_number(document) or f"SHIP-{document.id}"
    )
    seller_name = _build_document_creator_name(integration, seller_org_payload)
    customer = getattr(document, "customer", None)
    items = list(getattr(document, "items", []) or [])

    total = Decimal("0")
    for item in items:
        price = Decimal(str(getattr(item, "price", None)))
        quantity = Decimal(str(getattr(item, "quantity", 0) or 0))
        total += price * quantity

    root = ET.Element(
        _UTD_TYPE_NAMED_ID,
        {
            "DocumentDate": document_date,
            "DocumentNumber": document_number,
            "Currency": "643",
            "Function": document_function,
            "DocumentName": _formalized_document_name_for_function(
                document_function
            ),
            "SenderFnsParticipantId": str(
                seller_org_payload.get("FnsParticipantId") or ""
            ).strip(),
            "RecipientFnsParticipantId": str(
                buyer_org_payload.get("FnsParticipantId") or ""
            ).strip(),
            "DocumentCreator": seller_name,
            "Uid": str(uuid4()),
            "xmlns:xs": "http://www.w3.org/2001/XMLSchema",
        },
    )

    sellers_el = ET.SubElement(root, "Sellers")
    seller_el = ET.SubElement(sellers_el, "Seller")
    _append_organization_details_xml(
        seller_el,
        org_payload=seller_org_payload,
        fallback_name=integration.organization_name,
        fallback_inn=integration.organization_inn,
        fallback_kpp=integration.organization_kpp,
        fallback_address_text=integration.seller_legal_address,
    )

    buyers_el = ET.SubElement(root, "Buyers")
    buyer_el = ET.SubElement(buyers_el, "Buyer")
    _append_organization_details_xml(
        buyer_el,
        org_payload=buyer_org_payload,
        fallback_name=getattr(customer, "name", None),
        fallback_inn=getattr(customer, "inn", None),
        fallback_kpp=getattr(customer, "kpp", None),
        fallback_address_text=getattr(customer, "legal_address", None),
    )

    table_el = ET.SubElement(
        root,
        "Table",
        {
            "TotalWithVatExcluded": _format_decimal(total),
            "WithoutVat": "true",
            "Total": _format_decimal(total),
        },
    )
    for item in items:
        price = Decimal(str(getattr(item, "price", None)))
        quantity = Decimal(str(getattr(item, "quantity", 0) or 0))
        subtotal = price * quantity
        autopart = getattr(item, "autopart", None)
        product_name = (
            str(getattr(autopart, "name", "") or "").strip()
            or str(getattr(autopart, "oem_number", "") or "").strip()
            or f"Товар #{item.autopart_id}"
        )
        item_el = ET.SubElement(
            table_el,
            "Item",
            {
                "TaxRate": "NoVat",
                "Product": product_name,
                "Unit": _DEFAULT_ITEM_UNIT_CODE,
                "UnitName": _DEFAULT_ITEM_UNIT_NAME,
                "Quantity": _format_decimal(quantity, places=3)
                .rstrip("0")
                .rstrip("."),
                "Price": _format_decimal(price),
                "SubtotalWithVatExcluded": _format_decimal(subtotal),
                "WithoutVat": "true",
                "Subtotal": _format_decimal(subtotal),
            },
        )
        lot = getattr(item, "lot", None)
        declaration_number = str(getattr(lot, "gtd_number", "") or "").strip()
        country_code = str(getattr(lot, "country_code", "") or "").strip()
        if declaration_number and country_code.isdigit():
            declarations_el = ET.SubElement(item_el, "CustomsDeclarations")
            ET.SubElement(
                declarations_el,
                "CustomsDeclaration",
                {
                    "Country": country_code,
                    "DeclarationNumber": declaration_number,
                },
            )

    transfer_attrs = {
        "OperationInfo": str(document.reason or "товары переданы").strip()
        or "товары переданы",
        "TransferDate": document_date,
    }
    ET.SubElement(root, "TransferInfo", transfer_attrs)

    signers_el = ET.SubElement(root, "Signers")
    signer_el = ET.SubElement(
        signers_el,
        "Signer",
        {"SignerPowersConfirmationMethod": "6"},
    )
    last_name, first_name, middle_name = _split_full_name(
        integration.signer_full_name
    )
    fio_attrs = {"LastName": last_name, "FirstName": first_name}
    if middle_name:
        fio_attrs["MiddleName"] = middle_name
    ET.SubElement(signer_el, "Fio", fio_attrs)
    position_el = ET.SubElement(
        signer_el,
        "Position",
        {"PositionSource": "Manual"},
    )
    position_el.text = str(integration.signer_position or "").strip()

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


async def build_formalized_diadoc_payload_from_shipment(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    shipment_id: int,
    customer_id: int | None = None,
    to_box_id_guid: str | None = None,
) -> dict[str, Any]:
    document = await _load_shipment_document(session, shipment_id=shipment_id)
    if document is None:
        raise ValueError("Shipment document not found")
    if document.status != ShipmentDocumentStatus.POSTED:
        raise ValueError(
            "Only posted shipment documents can be sent to Diadoc"
        )

    items = list(getattr(document, "items", []) or [])
    if not items:
        raise ValueError("Shipment document has no items")
    if any(getattr(item, "price", None) is None for item in items):
        raise ValueError(
            "Every shipment item must have a price for formalized UTD"
        )

    resolved_customer_id = (
        int(customer_id)
        if customer_id is not None
        else (
            int(document.customer_id)
            if document.customer_id is not None
            else None
        )
    )
    if resolved_customer_id is None:
        raise ValueError(
            "Shipment document has no customer and customer override is not set"
        )

    buyer_box_id_guid = str(to_box_id_guid or "").strip() or None
    if not buyer_box_id_guid:
        buyer_box_id_guid = await resolve_diadoc_box_for_customer(
            session,
            customer_id=resolved_customer_id,
        )
    if not buyer_box_id_guid:
        raise ValueError(
            "Recipient Diadoc box is not resolved for shipment customer"
        )

    document_types = await client.get_document_types(
        integration.box_id_guid or ""
    )
    descriptor = _find_formalized_doc_type(
        document_types,
        type_named_id=_UTD_TYPE_NAMED_ID,
        preferred_functions=_build_preferred_utd_functions(integration),
    )
    seller_org_payload = await client.get_organization(
        box_id_guid=integration.box_id_guid or ""
    )
    buyer_org_payload = await client.get_organization(
        box_id_guid=buyer_box_id_guid,
    )
    user_data_xml = _build_formalized_utd_user_data_xml(
        document=document,
        integration=integration,
        seller_org_payload=seller_org_payload,
        buyer_org_payload=buyer_org_payload,
        document_function=descriptor["document_function"],
    )
    generated_xml, generated_file_name = await client.generate_title_xml(
        box_id_guid=integration.box_id_guid or "",
        document_type_named_id=descriptor["type_named_id"],
        document_function=descriptor["document_function"],
        document_version=descriptor["document_version"],
        title_index=0,
        user_data_xml=user_data_xml,
    )
    file_name = str(
        generated_file_name or ""
    ).strip() or _shipment_formalized_file_name(document)

    return {
        "customer_id": resolved_customer_id,
        "to_box_id_guid": buyer_box_id_guid,
        "file_name": file_name,
        "content_base64": base64.b64encode(generated_xml).decode("ascii"),
        "document_number": _shipment_document_number(document),
        "document_date": document.doc_date.date(),
        "source_type": "shipment_document",
        "source_id": int(document.id),
        "type_named_id": descriptor["type_named_id"],
        "document_function": descriptor["document_function"],
        "document_version": descriptor["document_version"],
        "metadata": {
            "DocumentKind": "FormalizedUniversalTransferDocument",
            "GeneratedBy": "GenerateTitleXml",
        },
    }


async def build_formalized_diadoc_payload_from_customer_return(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    return_id: int,
    customer_id: int | None = None,
    to_box_id_guid: str | None = None,
) -> dict[str, Any]:
    document = await _load_customer_return_document(
        session,
        return_id=return_id,
    )
    if document is None:
        raise ValueError("Customer return document not found")
    if document.status != ReturnDocumentStatus.CONFIRMED:
        raise ValueError(
            "Only confirmed customer returns can be sent as formalized UKD"
        )

    source_document = getattr(document, "shipment_document", None)
    if source_document is None:
        raise ValueError("Customer return has no source shipment document")
    source_number = str(
        getattr(source_document, "doc_number", "") or ""
    ).strip()
    if not source_number:
        raise ValueError(
            "Source shipment document number is required for formalized UKD"
        )
    source_doc_date = getattr(source_document, "doc_date", None)
    if source_doc_date is None:
        raise ValueError(
            "Source shipment document date is required for formalized UKD"
        )

    items = list(getattr(document, "items", []) or [])
    if not items:
        raise ValueError("Customer return document has no items")
    if any(getattr(item, "price", None) is None for item in items):
        raise ValueError(
            "Every return item must have a price for formalized UKD"
        )

    resolved_customer_id = (
        int(customer_id)
        if customer_id is not None
        else (
            int(document.customer_id)
            if document.customer_id is not None
            else None
        )
    )
    if resolved_customer_id is None:
        raise ValueError(
            "Customer return document has no customer and customer override is not set"
        )

    buyer_box_id_guid = str(to_box_id_guid or "").strip() or None
    if not buyer_box_id_guid:
        buyer_box_id_guid = await resolve_diadoc_box_for_customer(
            session,
            customer_id=resolved_customer_id,
        )
    if not buyer_box_id_guid:
        raise ValueError(
            "Recipient Diadoc box is not resolved for customer return"
        )

    document_types = await client.get_document_types(
        integration.box_id_guid or ""
    )
    descriptor = _find_formalized_doc_type(
        document_types,
        type_named_id=_UCD_TYPE_NAMED_ID,
        preferred_functions=_build_preferred_ukd_functions(integration),
    )
    seller_org_payload = await client.get_organization(
        box_id_guid=integration.box_id_guid or ""
    )
    buyer_org_payload = await client.get_organization(
        box_id_guid=buyer_box_id_guid,
    )
    customer = getattr(document, "customer", None)
    user_data_xml = _build_formalized_ukd_user_data_xml(
        document=document,
        items=items,
        integration=integration,
        seller_org_payload=seller_org_payload,
        buyer_org_payload=buyer_org_payload,
        document_function=descriptor["document_function"],
        buyer_fallback_name=getattr(customer, "name", None),
        buyer_fallback_inn=getattr(customer, "inn", None),
        buyer_fallback_kpp=getattr(customer, "kpp", None),
        buyer_fallback_address=getattr(customer, "legal_address", None),
        original_document_name="Исходная отгрузка",
        original_document_number=source_number,
        original_document_date=source_doc_date.date(),
        operation_content=(
            str(document.reason or "").strip() or "Возврат товара от клиента"
        ),
        correction_base_name="Возврат от клиента",
    )
    generated_xml, generated_file_name = await client.generate_title_xml(
        box_id_guid=integration.box_id_guid or "",
        document_type_named_id=descriptor["type_named_id"],
        document_function=descriptor["document_function"],
        document_version=descriptor["document_version"],
        title_index=0,
        user_data_xml=user_data_xml,
    )
    file_name = str(
        generated_file_name or ""
    ).strip() or _customer_return_formalized_file_name(document)

    return {
        "customer_id": resolved_customer_id,
        "to_box_id_guid": buyer_box_id_guid,
        "file_name": file_name,
        "content_base64": base64.b64encode(generated_xml).decode("ascii"),
        "document_number": str(document.doc_number or "").strip() or None,
        "document_date": document.doc_date.date(),
        "source_type": "return_from_customer",
        "source_id": int(document.id),
        "type_named_id": descriptor["type_named_id"],
        "document_function": descriptor["document_function"],
        "document_version": descriptor["document_version"],
        "metadata": {
            "DocumentKind": "FormalizedUniversalCorrectionDocument",
            "GeneratedBy": "GenerateTitleXml",
        },
    }


async def build_formalized_diadoc_payload_from_supplier_return(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    return_id: int,
    provider_id: int | None = None,
    to_box_id_guid: str | None = None,
) -> dict[str, Any]:
    document = await _load_supplier_return_document(
        session,
        return_id=return_id,
    )
    if document is None:
        raise ValueError("Supplier return document not found")
    if document.status not in {
        ReturnDocumentStatus.SHIPPED,
        ReturnDocumentStatus.CONFIRMED,
    }:
        raise ValueError(
            "Only shipped supplier returns can be sent as formalized UKD"
        )

    source_document = getattr(document, "supplier_receipt", None)
    if source_document is None:
        raise ValueError("Supplier return has no source supplier receipt")
    source_number = str(
        getattr(source_document, "document_number", "") or ""
    ).strip()
    if not source_number:
        raise ValueError(
            "Source supplier receipt number is required for formalized UKD"
        )
    source_doc_date = getattr(source_document, "document_date", None)
    if source_doc_date is None:
        raise ValueError(
            "Source supplier receipt date is required for formalized UKD"
        )

    items = list(getattr(document, "items", []) or [])
    if not items:
        raise ValueError("Supplier return document has no items")
    if any(getattr(item, "price", None) is None for item in items):
        raise ValueError(
            "Every return item must have a price for formalized UKD"
        )

    resolved_provider_id = (
        int(provider_id)
        if provider_id is not None
        else (
            int(document.provider_id)
            if document.provider_id is not None
            else None
        )
    )
    if resolved_provider_id is None:
        raise ValueError(
            "Supplier return document has no provider and provider override is not set"
        )

    buyer_box_id_guid = str(to_box_id_guid or "").strip() or None
    if not buyer_box_id_guid:
        buyer_box_id_guid = await resolve_diadoc_box_for_provider(
            session,
            provider_id=resolved_provider_id,
        )
    if not buyer_box_id_guid:
        raise ValueError(
            "Recipient Diadoc box is not resolved for supplier return"
        )

    document_types = await client.get_document_types(
        integration.box_id_guid or ""
    )
    descriptor = _find_formalized_doc_type(
        document_types,
        type_named_id=_UCD_TYPE_NAMED_ID,
        preferred_functions=_build_preferred_ukd_functions(integration),
    )
    seller_org_payload = await client.get_organization(
        box_id_guid=integration.box_id_guid or ""
    )
    buyer_org_payload = await client.get_organization(
        box_id_guid=buyer_box_id_guid,
    )
    provider = getattr(document, "provider", None)
    user_data_xml = _build_formalized_ukd_user_data_xml(
        document=document,
        items=items,
        integration=integration,
        seller_org_payload=seller_org_payload,
        buyer_org_payload=buyer_org_payload,
        document_function=descriptor["document_function"],
        buyer_fallback_name=getattr(provider, "name", None),
        buyer_fallback_inn=None,
        buyer_fallback_kpp=None,
        buyer_fallback_address=None,
        original_document_name="Исходное поступление поставщика",
        original_document_number=source_number,
        original_document_date=source_doc_date,
        operation_content=(
            str(document.reason or "").strip() or "Возврат товара поставщику"
        ),
        correction_base_name="Возврат поставщику",
    )
    generated_xml, generated_file_name = await client.generate_title_xml(
        box_id_guid=integration.box_id_guid or "",
        document_type_named_id=descriptor["type_named_id"],
        document_function=descriptor["document_function"],
        document_version=descriptor["document_version"],
        title_index=0,
        user_data_xml=user_data_xml,
    )
    file_name = str(
        generated_file_name or ""
    ).strip() or _supplier_return_formalized_file_name(document)

    return {
        "provider_id": resolved_provider_id,
        "to_box_id_guid": buyer_box_id_guid,
        "file_name": file_name,
        "content_base64": base64.b64encode(generated_xml).decode("ascii"),
        "document_number": str(document.doc_number or "").strip() or None,
        "document_date": document.doc_date.date(),
        "source_type": "return_to_supplier",
        "source_id": int(document.id),
        "type_named_id": descriptor["type_named_id"],
        "document_function": descriptor["document_function"],
        "document_version": descriptor["document_version"],
        "metadata": {
            "DocumentKind": "FormalizedUniversalCorrectionDocument",
            "GeneratedBy": "GenerateTitleXml",
        },
    }


async def post_diadoc_outgoing_document(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    from_box_id_guid: str,
    to_box_id_guid: str,
    customer_id: int | None = None,
    file_name: str,
    content_base64: str,
    signature_base64: str | None = None,
    comment: str | None = None,
    need_recipient_signature: bool = False,
    need_receipt: bool = True,
    send_mode: str = "draft",
    type_named_id: str = "Nonformalized",
    document_function: str | None = None,
    document_version: str | None = None,
    document_number: str | None = None,
    document_date: date | None = None,
    metadata: dict[str, str] | None = None,
    provider_id: int | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
) -> DiadocOutgoingDocument:
    resolved_send_mode = str(send_mode or "draft").strip().lower()
    is_draft = resolved_send_mode != "send"
    if not is_draft and not str(signature_base64 or "").strip():
        raise ValueError("signature_base64 is required when send_mode=send")

    customer: Customer | None = None
    if customer_id is not None:
        customer = await session.get(Customer, int(customer_id))
        if customer is None:
            raise ValueError("Customer not found")

    provider: Provider | None = None
    if provider_id is not None:
        provider = await session.get(Provider, int(provider_id))
        if provider is None:
            raise ValueError("Provider not found")

    content = _decode_base64_content(
        content_base64,
        field_name="content_base64",
    )
    signature_bytes = None
    if str(signature_base64 or "").strip():
        signature_bytes = _decode_base64_content(
            signature_base64,
            field_name="signature_base64",
        )

    relative_path = _build_diadoc_outgoing_relative_path(
        environment=environment,
        from_box_id_guid=from_box_id_guid,
        filename=file_name,
    )
    await _write_content_to_relative_path(relative_path, content)

    signed_content: dict[str, str] = {
        "Content": base64.b64encode(content).decode("ascii"),
    }
    if signature_bytes is not None:
        signed_content["Signature"] = base64.b64encode(signature_bytes).decode(
            "ascii"
        )

    message_payload: dict[str, Any] = {
        "FromBoxId": from_box_id_guid,
        "ToBoxId": to_box_id_guid,
        "IsDraft": is_draft,
        "DocumentAttachments": [
            {
                "SignedContent": signed_content,
                "NeedRecipientSignature": bool(need_recipient_signature),
                "NeedReceipt": bool(need_receipt),
                "TypeNamedId": str(type_named_id or "Nonformalized").strip(),
                "Metadata": _build_document_metadata(
                    file_name=file_name,
                    type_named_id=type_named_id,
                    metadata=metadata,
                    document_number=document_number,
                    document_date=document_date,
                ),
                "CustomData": [
                    {"Key": "SourceType", "Value": str(source_type)}
                    for _ in [1]
                    if str(source_type or "").strip()
                ]
                + [
                    {"Key": "SourceId", "Value": str(source_id)}
                    for _ in [1]
                    if source_id is not None
                ],
            }
        ],
    }
    attachment = message_payload["DocumentAttachments"][0]
    if comment:
        attachment["Comment"] = str(comment).strip()
    if document_function:
        attachment["Function"] = str(document_function).strip()
    if document_version:
        attachment["Version"] = str(document_version).strip()

    operation_id = hashlib.md5(
        (
            f"{environment}|{from_box_id_guid}|{to_box_id_guid}|"
            f"{file_name}|{hashlib.sha256(content).hexdigest()}|"
            f"{resolved_send_mode}"
        ).encode("utf-8")
    ).hexdigest()

    response_payload = await client.post_message(
        message=message_payload,
        operation_id=operation_id,
    )

    outgoing = DiadocOutgoingDocument(
        environment=environment,
        from_box_id_guid=from_box_id_guid,
        to_box_id_guid=to_box_id_guid,
        customer_id=int(customer.id) if customer is not None else None,
        provider_id=int(provider.id) if provider is not None else None,
        source_type=str(source_type or "").strip() or None,
        source_id=int(source_id) if source_id is not None else None,
        type_named_id=str(type_named_id or "Nonformalized").strip(),
        document_function=(str(document_function or "").strip() or None),
        document_version=str(document_version or "").strip() or None,
        file_name=file_name,
        document_number=str(document_number or "").strip() or None,
        document_date=document_date,
        local_file_path=relative_path,
        content_sha256=hashlib.sha256(content).hexdigest(),
        comment=str(comment or "").strip() or None,
        need_recipient_signature=bool(need_recipient_signature),
        need_receipt=bool(need_receipt),
        is_draft=is_draft,
        message_id=_extract_message_id(response_payload),
        entity_id=_extract_first_entity_id(response_payload),
        status="draft" if is_draft else "sent",
        error_details=None,
        metadata_json={k: v for k, v in (metadata or {}).items()},
        raw_response=response_payload,
        sent_at=now_moscow(),
    )
    session.add(outgoing)
    await session.commit()
    await session.refresh(outgoing)
    return outgoing
