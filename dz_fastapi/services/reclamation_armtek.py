"""Интеграция рекламаций с закрытым кабинетом Armtek SRM."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import preprocess_oem_number
from dz_fastapi.models.partner import (
    RECLAMATION_ITEM_SOURCE,
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    Reclamation,
    ReclamationItem,
)

ARMTEK_NOTICE_SENDER = "cross@armtek.ru"
ARMTEK_PORTAL_HOST = "srm.armtek.ru"
ARMTEK_OPEN_RETURNS_PATH = "/returns-management/opened"
DEFAULT_ARMTEK_AUTH_SYSTEM = "AUTH_MICROSERVICE_V1_OAUTH"
ARMTEK_APPROVED_STATUS = "Подтверждено поставщиком"
ARMTEK_REJECTED_STATUS = "Отказ поставщика"


class ArmtekPortalError(RuntimeError):
    """Ошибка чтения или изменения заявки Armtek."""


@dataclass(frozen=True)
class ArmtekPortalConfig:
    login: str
    password: str
    supplier_id: str | None = None
    auth_system: str = DEFAULT_ARMTEK_AUTH_SYSTEM
    auth_token: str = ""
    oauth_base_url: str = "https://oauth.armtek.ru/rest/ru"
    srm_base_url: str = "https://srm.armtek.ru/rest/ru"

    @classmethod
    def from_env(cls) -> "ArmtekPortalConfig":
        login = str(os.getenv("ARMTEK_SRM_LOGIN") or "").strip()
        password = str(os.getenv("ARMTEK_SRM_PASSWORD") or "")
        auth_token = str(os.getenv("ARMTEK_AUTH_TOKEN") or "").strip()
        if not login or not password or not auth_token:
            raise ArmtekPortalError(
                "Armtek не настроен: задайте ARMTEK_SRM_LOGIN и "
                "ARMTEK_SRM_PASSWORD, ARMTEK_AUTH_TOKEN"
            )
        return cls(
            login=login,
            password=password,
            auth_system=(
                str(os.getenv("ARMTEK_AUTH_SYSTEM") or "").strip()
                or DEFAULT_ARMTEK_AUTH_SYSTEM
            ),
            auth_token=auth_token,
            supplier_id=(
                str(os.getenv("ARMTEK_SRM_SUPPLIER_ID") or "").strip()
                or None
            ),
            oauth_base_url=str(
                os.getenv("ARMTEK_OAUTH_BASE_URL")
                or "https://oauth.armtek.ru/rest/ru"
            ).rstrip("/"),
            srm_base_url=str(
                os.getenv("ARMTEK_SRM_BASE_URL")
                or "https://srm.armtek.ru/rest/ru"
            ).rstrip("/"),
        )


@dataclass(frozen=True)
class ArmtekReturnRef:
    request_number: str
    request_position: str

    @property
    def external_id(self) -> str:
        return f"{self.request_number}:{self.request_position}"

    @property
    def source_link(self) -> str:
        query = urlencode({"RequestPosition": self.request_position})
        return (
            f"https://{ARMTEK_PORTAL_HOST}{ARMTEK_OPEN_RETURNS_PATH}/"
            f"{self.request_number}?{query}"
        )


def is_armtek_portal_notice(*, sender: str | None, body: str | None) -> bool:
    normalized_sender = str(sender or "").strip().lower()
    text = str(body or "").lower()
    return (
        normalized_sender == ARMTEK_NOTICE_SENDER
        and f"https://{ARMTEK_PORTAL_HOST}{ARMTEK_OPEN_RETURNS_PATH}" in text
    )


def parse_armtek_return_url(source_link: str | None) -> ArmtekReturnRef:
    parsed = urlparse(str(source_link or "").strip())
    path_prefix = f"{ARMTEK_OPEN_RETURNS_PATH}/"
    if (
        parsed.scheme.lower() != "https"
        or (parsed.hostname or "").lower() != ARMTEK_PORTAL_HOST
        or parsed.port not in (None, 443)
        or not parsed.path.startswith(path_prefix)
    ):
        raise ArmtekPortalError("В рекламации нет поддерживаемой ссылки Armtek")
    request_number = parsed.path[len(path_prefix):].strip("/")
    request_position = str(
        (parse_qs(parsed.query).get("RequestPosition") or [""])[0]
    ).strip()
    if not request_number or not request_position:
        raise ArmtekPortalError("Ссылка Armtek не содержит номер и позицию заявки")
    return ArmtekReturnRef(request_number, request_position)


def _pick(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:10], fmt).date()
        except ValueError:
            continue
    return None


def armtek_return_state(payload: dict[str, Any]) -> str:
    raw = str(_pick(payload, "StatusName", "Status") or "").strip().lower()
    normalized = raw.replace("ё", "е").replace("-", "_").replace(" ", "_")
    if "подтвержден" in normalized or normalized in {
        "verified_by_supplier",
        "approved",
    }:
        return "approved"
    if "отказ" in normalized or normalized in {
        "refuse_by_supplier",
        "rejected",
    }:
        return "rejected"
    if "закрыт" in normalized or normalized == "closed":
        return "closed"
    if (
        "ожидается_решение_поставщика" in normalized
        or normalized == "awaiting_supplier_decision"
    ):
        return "pending"
    return "unknown"


def build_armtek_snapshot(
    payload: dict[str, Any],
    *,
    supplier_id: str | None = None,
) -> dict[str, Any]:
    request_number = str(_pick(payload, "RequestNumber", "requestNumber") or "")
    request_position = str(
        _pick(payload, "RequestPosition", "requestPosition") or ""
    )
    return {
        "provider": "armtek",
        "external_id": f"{request_number}:{request_position}",
        "request_number": request_number,
        "request_position": request_position,
        "supplier_id": supplier_id,
        "state": armtek_return_state(payload),
        "status_name": _pick(payload, "StatusName", "Status"),
        "oem_number": _pick(
            payload,
            "SupplierMaterial",
            "Material",
            "Article",
            "OemNumber",
        ),
        "brand_name": _pick(payload, "Brand", "BrandName"),
        "autopart_name": _pick(payload, "MaterialName", "Name"),
        "quantity": _as_int(
            _pick(payload, "Quantity", "RequestQuantity", "Qty")
        ),
        "price": _as_float(
            _pick(payload, "Price", "PriceReturnNDS", "PriceNDS")
        ),
        "invoice_number": _pick(
            payload,
            "ExternalInvoiceNumber",
            "Invoice",
            "InvoiceNumber",
        ),
        "invoice_date": (
            parsed_date.isoformat()
            if (
                parsed_date := _as_date(
                    _pick(payload, "InvoiceDate", "RequestDate")
                )
            )
            else None
        ),
        "reason": _pick(payload, "ReasonReturn", "Reason"),
        "warehouse_name": _pick(payload, "WarehouseName", "Warehouse"),
        "seller_name": _pick(payload, "SellerName"),
        "checked_at": now_moscow().isoformat(),
    }


class ArmtekPortalClient:
    def __init__(
        self,
        *,
        config: ArmtekPortalConfig | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.config = config or ArmtekPortalConfig.from_env()
        self._client = client
        self._timeout = timeout
        self._access_token: str | None = None

    async def _request(
        self,
        method: str,
        url: str,
        *,
        auth: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("Accept", "application/json")
        if auth:
            if not self._access_token:
                await self.authenticate()
            headers["Authorization"] = f"Bearer {self._access_token}"
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=False,
        )
        try:
            response = await client.request(
                method,
                url,
                headers=headers,
                **kwargs,
            )
        except httpx.RequestError as exc:
            raise ArmtekPortalError(
                "Портал Armtek сейчас недоступен. Попробуйте повторить позже"
            ) from exc
        finally:
            if owns_client:
                await client.aclose()
        if response.status_code == 429:
            raise ArmtekPortalError(
                "Armtek запросил CAPTCHA. Войдите в портал вручную и "
                "повторите синхронизацию позже"
            )
        if response.status_code >= 400 or response.is_redirect:
            raise ArmtekPortalError(
                f"Armtek вернул ошибку HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ArmtekPortalError("Armtek вернул некорректный ответ") from exc
        if not isinstance(payload, dict):
            raise ArmtekPortalError("Armtek вернул некорректный ответ")
        return payload

    async def authenticate(self) -> None:
        if not self.config.auth_token:
            raise ArmtekPortalError(
                "Armtek не настроен: отсутствует ARMTEK_AUTH_TOKEN"
            )
        payload = await self._request(
            "POST",
            f"{self.config.oauth_base_url}/auth-microservice/v1/auth/login",
            auth=False,
            headers={
                "X-AUTH-SYSTEM": self.config.auth_system,
                "X-AUTH-TOKEN": self.config.auth_token,
            },
            json={
                "login": self.config.login,
                "password": self.config.password,
            },
        )
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        access_token = str(data.get("accessToken") or "").strip()
        if not access_token:
            raise ArmtekPortalError("Armtek не выдал токен доступа")
        self._access_token = access_token

    async def get_profile(self) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"{self.config.srm_base_url}/supplier-microservice/v2/auth/profile",
        )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ArmtekPortalError("Armtek не вернул профиль поставщика")
        return data

    async def resolve_supplier_id(self) -> str:
        if self.config.supplier_id:
            return self.config.supplier_id
        profile = await self.get_profile()
        supplier_data = profile.get("SupplierData")
        supplier_ids = {
            str(item.get("Supplier") or "").strip()
            for item in supplier_data or []
            if isinstance(item, dict) and item.get("Supplier")
        }
        if len(supplier_ids) != 1:
            raise ArmtekPortalError(
                "В кабинете Armtek найдено несколько поставщиков. "
                "Укажите ARMTEK_SRM_SUPPLIER_ID"
            )
        return supplier_ids.pop()

    async def list_open_returns(self, supplier_id: str) -> list[dict[str, Any]]:
        payload = await self._request(
            "POST",
            f"{self.config.srm_base_url}/supplier-microservice/v2/returns/list",
            json={
                "Supplier": supplier_id,
                "Opened": True,
                "Status": "awaiting_supplier_decision",
            },
        )
        data = payload.get("data")
        items = data.get("items") if isinstance(data, dict) else None
        if items is None:
            return []
        if not isinstance(items, list):
            raise ArmtekPortalError("Armtek вернул некорректный список заявок")
        return [item for item in items if isinstance(item, dict)]

    async def get_return(self, ref: ArmtekReturnRef) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"{self.config.srm_base_url}/supplier-microservice/v2/"
            "task-selection/get",
            params={
                "RequestNumber": ref.request_number,
                "RequestPosition": ref.request_position,
            },
        )
        data = payload.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        if isinstance(data, dict):
            return data
        raise ArmtekPortalError("Armtek не вернул данные заявки")

    async def submit_decision(
        self,
        ref: ArmtekReturnRef,
        *,
        decision: str,
        comment: str | None = None,
    ) -> dict[str, Any]:
        if decision not in {"approved", "rejected"}:
            raise ArmtekPortalError("Неизвестное решение для Armtek")
        item = {
            "RequestNumber": ref.request_number,
            "RequestPosition": ref.request_position,
            "StatusName": (
                ARMTEK_APPROVED_STATUS
                if decision == "approved"
                else ARMTEK_REJECTED_STATUS
            ),
        }
        if decision == "rejected":
            reason = str(comment or "").strip()
            if not reason:
                raise ArmtekPortalError(
                    "Для отказа в Armtek укажите комментарий"
                )
            item["CommentSupplier"] = reason
        return await self._request(
            "PATCH",
            f"{self.config.srm_base_url}/supplier-microservice/v2/"
            "returns/change-request",
            json={"Data": [item]},
        )


def _ref_from_payload(payload: dict[str, Any]) -> ArmtekReturnRef | None:
    request_number = str(_pick(payload, "RequestNumber", "requestNumber") or "").strip()
    request_position = str(
        _pick(payload, "RequestPosition", "requestPosition") or ""
    ).strip()
    if not request_number or not request_position:
        return None
    return ArmtekReturnRef(request_number, request_position)


def _validate_reclamation_match(
    reclamation: Reclamation,
    snapshot: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    portal_oem = preprocess_oem_number(snapshot.get("oem_number") or "")
    local_oems = [
        preprocess_oem_number(item.oem_number or "")
        for item in reclamation.items or []
        if preprocess_oem_number(item.oem_number or "")
    ]
    if not portal_oem:
        reasons.append("Armtek не вернул артикул позиции")
    elif not local_oems:
        reasons.append("В рекламации не распознан артикул позиции")
    elif portal_oem not in local_oems:
        reasons.append("Артикул в рекламации не совпадает с Armtek")

    portal_quantity = _as_int(snapshot.get("quantity"))
    if portal_oem and portal_oem in local_oems and portal_quantity is not None:
        local_quantity = sum(
            int(item.quantity or 0)
            for item in reclamation.items or []
            if preprocess_oem_number(item.oem_number or "") == portal_oem
        )
        if local_quantity != portal_quantity:
            reasons.append(
                f"Количество в рекламации ({local_quantity}) не совпадает "
                f"с Armtek ({portal_quantity})"
            )
    return reasons


async def _get_reclamation(
    session: AsyncSession,
    reclamation_id: int,
    *,
    for_update: bool = False,
) -> Reclamation:
    statement = (
        select(Reclamation)
        .where(Reclamation.id == reclamation_id)
        .options(selectinload(Reclamation.items))
    )
    if for_update:
        statement = statement.with_for_update(of=Reclamation)
    reclamation = (await session.execute(statement)).scalar_one_or_none()
    if reclamation is None:
        raise ArmtekPortalError("Рекламация не найдена")
    return reclamation


async def _store_snapshot(
    session: AsyncSession,
    reclamation: Reclamation,
    snapshot: dict[str, Any],
    *,
    blocking_reasons: list[str],
    sent_by_user_id: int | None = None,
) -> None:
    extracted_data = dict(reclamation.extracted_data or {})
    previous = extracted_data.get("armtek")
    previous = dict(previous) if isinstance(previous, dict) else {}
    previous.update(snapshot)
    previous["blocking_reasons"] = blocking_reasons
    if sent_by_user_id is not None:
        previous["sent_at"] = now_moscow().isoformat()
        previous["sent_by_user_id"] = sent_by_user_id
    extracted_data["armtek"] = previous
    reclamation.extracted_data = extracted_data
    session.add(reclamation)
    await session.commit()


async def refresh_armtek_status(
    session: AsyncSession,
    *,
    reclamation_id: int,
    client: ArmtekPortalClient | None = None,
) -> tuple[Reclamation, dict[str, Any]]:
    reclamation = await _get_reclamation(session, reclamation_id)
    ref = parse_armtek_return_url(reclamation.source_link)
    portal_client = client or ArmtekPortalClient()
    supplier_id = await portal_client.resolve_supplier_id()
    payload = await portal_client.get_return(ref)
    snapshot = build_armtek_snapshot(payload, supplier_id=supplier_id)
    blocking_reasons = _validate_reclamation_match(reclamation, snapshot)
    await _store_snapshot(
        session,
        reclamation,
        snapshot,
        blocking_reasons=blocking_reasons,
    )
    return reclamation, snapshot


async def send_armtek_decision(
    session: AsyncSession,
    *,
    reclamation_id: int,
    user_id: int | None,
    comment: str | None = None,
    client: ArmtekPortalClient | None = None,
) -> tuple[Reclamation, dict[str, Any], bool]:
    reclamation = await _get_reclamation(
        session,
        reclamation_id,
        for_update=True,
    )
    decision = str(reclamation.resolution or "").strip().lower()
    if decision not in {"approved", "rejected"}:
        raise ArmtekPortalError(
            "Сначала сохраните решение менеджера: согласовать или отказать"
        )
    ref = parse_armtek_return_url(reclamation.source_link)
    portal_client = client or ArmtekPortalClient()
    supplier_id = await portal_client.resolve_supplier_id()
    payload = await portal_client.get_return(ref)
    snapshot = build_armtek_snapshot(payload, supplier_id=supplier_id)
    blocking_reasons = _validate_reclamation_match(reclamation, snapshot)
    if blocking_reasons:
        await _store_snapshot(
            session,
            reclamation,
            snapshot,
            blocking_reasons=blocking_reasons,
        )
        raise ArmtekPortalError("; ".join(blocking_reasons))
    if snapshot["state"] == decision:
        await _store_snapshot(
            session,
            reclamation,
            snapshot,
            blocking_reasons=[],
            sent_by_user_id=user_id,
        )
        return reclamation, snapshot, True
    if snapshot["state"] != "pending":
        raise ArmtekPortalError(
            "Заявка Armtek уже закрыта или содержит другое решение"
        )

    await portal_client.submit_decision(
        ref,
        decision=decision,
        comment=comment or reclamation.resolution_comment,
    )
    verified_payload = await portal_client.get_return(ref)
    verified_snapshot = build_armtek_snapshot(
        verified_payload,
        supplier_id=supplier_id,
    )
    if verified_snapshot["state"] != decision:
        raise ArmtekPortalError(
            "Armtek принял запрос, но итоговый статус пока не подтвердился"
        )
    await _store_snapshot(
        session,
        reclamation,
        verified_snapshot,
        blocking_reasons=[],
        sent_by_user_id=user_id,
    )
    return reclamation, verified_snapshot, False


async def sync_armtek_open_returns(
    session: AsyncSession,
    *,
    customer_id: int | None,
    sender_email: str = ARMTEK_NOTICE_SENDER,
    email_received_at: datetime | None = None,
    client: ArmtekPortalClient | None = None,
) -> dict[str, Any]:
    portal_client = client or ArmtekPortalClient()
    supplier_id = await portal_client.resolve_supplier_id()
    rows = await portal_client.list_open_returns(supplier_id)
    created = 0
    updated = 0
    skipped = 0

    for row in rows:
        ref = _ref_from_payload(row)
        if ref is None:
            skipped += 1
            continue
        try:
            detail = await portal_client.get_return(ref)
        except ArmtekPortalError:
            detail = row
        snapshot = build_armtek_snapshot(detail, supplier_id=supplier_id)
        existing = (
            await session.execute(
                select(Reclamation)
                .where(Reclamation.source_link == ref.source_link)
                .options(selectinload(Reclamation.items))
            )
        ).scalar_one_or_none()
        if existing is not None:
            blocking_reasons = _validate_reclamation_match(existing, snapshot)
            extracted_data = dict(existing.extracted_data or {})
            previous = extracted_data.get("armtek")
            previous = dict(previous) if isinstance(previous, dict) else {}
            previous.update(snapshot)
            previous["blocking_reasons"] = blocking_reasons
            extracted_data["armtek"] = previous
            existing.extracted_data = extracted_data
            session.add(existing)
            updated += 1
            continue

        oem = preprocess_oem_number(snapshot.get("oem_number") or "")
        matched: dict[str, Any] = {}
        if oem:
            from dz_fastapi.services.reclamations import _match_oems_in_text

            matches = await _match_oems_in_text(
                session,
                oem,
                limit=1,
                customer_id=customer_id,
            )
            matched = matches[0] if matches else {}
        rec = Reclamation(
            source=RECLAMATION_SOURCE.LINK,
            status=(
                RECLAMATION_STATUS.RECOGNIZED
                if customer_id
                else RECLAMATION_STATUS.NEW
            ),
            customer_id=customer_id,
            sender_email=sender_email,
            source_link=ref.source_link,
            email_subject=f"Armtek: возврат № {ref.request_number}",
            email_received_at=email_received_at or now_moscow(),
            email_body=str(snapshot.get("reason") or "") or None,
            stated_document_number=(
                str(snapshot.get("invoice_number"))
                if snapshot.get("invoice_number")
                else None
            ),
            stated_document_date=_as_date(snapshot.get("invoice_date")),
            stated_reason=(
                str(snapshot.get("reason"))
                if snapshot.get("reason")
                else None
            ),
            extracted_data={},
        )
        rec.items.append(
            ReclamationItem(
                oem_number=oem or None,
                brand_name=(
                    str(snapshot.get("brand_name"))
                    if snapshot.get("brand_name")
                    else matched.get("brand_name")
                ),
                autopart_name=(
                    str(snapshot.get("autopart_name"))
                    if snapshot.get("autopart_name")
                    else matched.get("autopart_name")
                ),
                quantity=max(1, _as_int(snapshot.get("quantity")) or 1),
                reason=(
                    str(snapshot.get("reason"))
                    if snapshot.get("reason")
                    else None
                ),
                autopart_id=matched.get("autopart_id"),
                item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
            )
        )
        rec.extracted_data = {
            "armtek": {
                **snapshot,
                "blocking_reasons": _validate_reclamation_match(
                    rec,
                    snapshot,
                ),
            }
        }
        session.add(rec)
        created += 1

    await session.commit()
    return {
        "found": len(rows),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "supplier_id": supplier_id,
    }
