"""Безопасная интеграция рекламаций с публичной формой Froza."""
from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import preprocess_oem_number
from dz_fastapi.models.partner import Reclamation

FROZA_BASE_URL = "https://froza.ru"
FROZA_ALLOWED_HOSTS = {"froza.ru", "www.froza.ru"}
FROZA_TOKEN_RE = re.compile(r"^[a-fA-F0-9]{32}$")
FROZA_QUESTION_PATH = "/supplier/one-question/"
FROZA_DECISIONS = {"approved", "rejected"}


class FrozaPortalError(RuntimeError):
    """Ошибка проверки или передачи решения в Froza."""


@dataclass(frozen=True)
class FrozaQuestionRef:
    token: str
    question_id: int


def parse_froza_question_url(source_link: str | None) -> FrozaQuestionRef:
    """Разбирает только штатную HTTPS-ссылку формы Froza."""
    parsed = urlparse(html.unescape(str(source_link or "")).strip())
    if (
        parsed.scheme.lower() != "https"
        or (parsed.hostname or "").lower() not in FROZA_ALLOWED_HOSTS
        or parsed.port not in (None, 443)
        or parsed.path.rstrip("/") != FROZA_QUESTION_PATH.rstrip("/")
    ):
        raise FrozaPortalError("В рекламации нет поддерживаемой ссылки Froza")

    query = parse_qs(parsed.query)
    token = str((query.get("token") or [""])[0]).strip()
    raw_question_id = str((query.get("id") or [""])[0]).strip()
    if not FROZA_TOKEN_RE.fullmatch(token) or not raw_question_id.isdigit():
        raise FrozaPortalError("Ссылка Froza имеет неверный формат")

    question_id = int(raw_question_id)
    if question_id <= 0:
        raise FrozaPortalError("Ссылка Froza содержит неверный номер заявки")
    return FrozaQuestionRef(token=token, question_id=question_id)


def is_froza_question_url(source_link: str | None) -> bool:
    """Проверяет, что ссылка ведёт на поддерживаемую форму Froza."""
    try:
        parse_froza_question_url(source_link)
    except FrozaPortalError:
        return False
    return True


def froza_question_state(payload: dict[str, Any]) -> str:
    if payload.get("isSupplierRejectedReturn"):
        return "rejected"
    if payload.get("isSupplierAgreedReturn") or payload.get(
        "isFullReturnAgree"
    ):
        return "approved"
    if payload.get("archived"):
        return "archived"
    if payload.get("waitingResponse"):
        return "pending"
    return "unknown"


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_froza_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    order = payload.get("order") if isinstance(payload.get("order"), dict) else {}
    detail = order.get("detail") if isinstance(order.get("detail"), dict) else {}
    invoice = order.get("invoice") if isinstance(order.get("invoice"), dict) else {}
    messages = payload.get("messages") if isinstance(payload.get("messages"), list) else []
    latest_message = messages[-1] if messages and isinstance(messages[-1], dict) else {}
    return {
        "provider": "froza",
        "question_id": _as_int(payload.get("id")),
        "state": froza_question_state(payload),
        "waiting_response": bool(payload.get("waitingResponse")),
        "archived": bool(payload.get("archived")),
        "deadline": payload.get("deadLine"),
        "oem_number": detail.get("num"),
        "brand_name": detail.get("makeName"),
        "autopart_name": detail.get("description"),
        "quantity": _as_int(payload.get("quantity") or order.get("quantity")),
        "price": _as_float(order.get("price")),
        "invoice_number": invoice.get("number"),
        "invoice_date": invoice.get("date"),
        "reason": latest_message.get("text"),
        "checked_at": now_moscow().isoformat(),
    }


class FrozaPortalClient:
    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._client = client
        self._timeout = timeout

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            base_url=FROZA_BASE_URL,
            timeout=self._timeout,
            follow_redirects=False,
            headers={"Accept": "application/json"},
        )
        try:
            response = await client.request(method, path, **kwargs)
        except httpx.RequestError as exc:
            raise FrozaPortalError(
                "Froza сейчас недоступна. Попробуйте повторить позже"
            ) from exc
        finally:
            if owns_client:
                await client.aclose()

        if response.status_code >= 400 or response.is_redirect:
            raise FrozaPortalError(
                f"Froza вернула ошибку HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise FrozaPortalError("Froza вернула некорректный ответ") from exc
        if not isinstance(payload, dict):
            raise FrozaPortalError("Froza вернула некорректный ответ")
        return payload

    async def get_question(self, ref: FrozaQuestionRef) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/supplier/one-question/get/{ref.token}/{ref.question_id}",
        )
        if _as_int(payload.get("id")) != ref.question_id:
            raise FrozaPortalError("Froza вернула данные другой заявки")
        return payload

    async def submit_decision(
        self,
        ref: FrozaQuestionRef,
        *,
        decision: str,
        comment: str | None = None,
    ) -> dict[str, Any]:
        if decision not in FROZA_DECISIONS:
            raise FrozaPortalError("Неизвестное решение для Froza")
        if decision == "approved":
            payload = await self._request(
                "POST",
                f"/supplier/one-question/accept/{ref.token}/{ref.question_id}",
                json={"maxRedirects": 0},
            )
        else:
            reason = str(comment or "").strip()
            if not reason:
                raise FrozaPortalError(
                    "Для отказа во Froza укажите комментарий к решению"
                )
            payload = await self._request(
                "POST",
                f"/supplier/one-question/deny/{ref.token}/{ref.question_id}",
                files={"text": (None, reason)},
            )
        if payload.get("success") is not True:
            raise FrozaPortalError("Froza не подтвердила сохранение решения")
        return payload


def _validate_reclamation_match(
    reclamation: Reclamation,
    snapshot: dict[str, Any],
) -> list[str]:
    blocking_reasons: list[str] = []
    portal_oem = preprocess_oem_number(snapshot.get("oem_number") or "")
    item_quantities: list[int] = []
    matching_oems: list[str] = []
    for item in reclamation.items or []:
        item_oem = preprocess_oem_number(item.oem_number or "")
        if item_oem:
            matching_oems.append(item_oem)
        if portal_oem and item_oem == portal_oem:
            item_quantities.append(int(item.quantity or 0))

    if not portal_oem:
        blocking_reasons.append("Froza не вернула артикул позиции")
    elif not matching_oems:
        blocking_reasons.append("В рекламации не распознан артикул позиции")
    elif portal_oem not in matching_oems:
        blocking_reasons.append(
            "Артикул в рекламации не совпадает с артикулом во Froza"
        )

    portal_quantity = _as_int(snapshot.get("quantity"))
    if portal_oem in matching_oems and portal_quantity is not None:
        local_quantity = sum(item_quantities)
        if local_quantity != portal_quantity:
            blocking_reasons.append(
                "Количество в рекламации "
                f"({local_quantity}) не совпадает с Froza ({portal_quantity})"
            )
    return blocking_reasons


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
        raise FrozaPortalError("Рекламация не найдена")
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
    previous = extracted_data.get("froza")
    previous = dict(previous) if isinstance(previous, dict) else {}
    previous.update(snapshot)
    previous["blocking_reasons"] = blocking_reasons
    if sent_by_user_id is not None:
        previous["sent_at"] = now_moscow().isoformat()
        previous["sent_by_user_id"] = sent_by_user_id
    extracted_data["froza"] = previous
    reclamation.extracted_data = extracted_data
    session.add(reclamation)
    await session.commit()


async def refresh_froza_status(
    session: AsyncSession,
    *,
    reclamation_id: int,
    client: FrozaPortalClient | None = None,
) -> tuple[Reclamation, dict[str, Any]]:
    reclamation = await _get_reclamation(session, reclamation_id)
    ref = parse_froza_question_url(reclamation.source_link)
    portal_client = client or FrozaPortalClient()
    payload = await portal_client.get_question(ref)
    snapshot = build_froza_snapshot(payload)
    blocking_reasons = _validate_reclamation_match(reclamation, snapshot)
    await _store_snapshot(
        session,
        reclamation,
        snapshot,
        blocking_reasons=blocking_reasons,
    )
    return reclamation, snapshot


async def send_froza_decision(
    session: AsyncSession,
    *,
    reclamation_id: int,
    user_id: int | None,
    comment: str | None = None,
    client: FrozaPortalClient | None = None,
) -> tuple[Reclamation, dict[str, Any], bool]:
    reclamation = await _get_reclamation(
        session,
        reclamation_id,
        for_update=True,
    )
    decision = str(reclamation.resolution or "").strip()
    if decision not in FROZA_DECISIONS:
        raise FrozaPortalError(
            "Сначала сохраните решение: согласовать или отклонить"
        )

    ref = parse_froza_question_url(reclamation.source_link)
    portal_client = client or FrozaPortalClient()
    payload = await portal_client.get_question(ref)
    snapshot = build_froza_snapshot(payload)
    blocking_reasons = _validate_reclamation_match(reclamation, snapshot)
    if blocking_reasons:
        await _store_snapshot(
            session,
            reclamation,
            snapshot,
            blocking_reasons=blocking_reasons,
        )
        raise FrozaPortalError("; ".join(blocking_reasons))

    current_state = str(snapshot.get("state") or "unknown")
    if current_state == decision:
        await _store_snapshot(
            session,
            reclamation,
            snapshot,
            blocking_reasons=[],
            sent_by_user_id=user_id,
        )
        return reclamation, snapshot, True
    if current_state != "pending":
        raise FrozaPortalError(
            "Заявка Froza уже закрыта или содержит другое решение"
        )

    await portal_client.submit_decision(
        ref,
        decision=decision,
        comment=(
            str(comment).strip()
            if comment is not None
            else reclamation.resolution_comment
        ),
    )
    verified_snapshot = snapshot
    for delay in (0.0, 0.5, 1.0):
        if delay:
            await asyncio.sleep(delay)
        verified_snapshot = build_froza_snapshot(
            await portal_client.get_question(ref)
        )
        if verified_snapshot.get("state") == decision:
            break
    if verified_snapshot.get("state") != decision:
        raise FrozaPortalError(
            "Froza приняла запрос, но итоговый статус пока не подтвердился"
        )

    if decision == "rejected" and comment is not None:
        reclamation.resolution_comment = str(comment).strip() or None

    await _store_snapshot(
        session,
        reclamation,
        verified_snapshot,
        blocking_reasons=[],
        sent_by_user_id=user_id,
    )
    return reclamation, verified_snapshot, False
