import base64
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any

import httpx

logger = logging.getLogger("dz_fastapi")

RESEND_API_BASE_URL = os.getenv(
    "RESEND_API_BASE_URL",
    "https://api.resend.com",
)
RESEND_API_TIMEOUT = int(os.getenv("RESEND_API_TIMEOUT", "20"))
MAX_RESEND_EMAIL_BYTES = 40 * 1024 * 1024

# Коды, при которых Resend сам просит повторить: перегрузка, ограничение
# частоты, внутренние сбои. На рассылке прайсов это выглядело так —
# «HTTP 408 Operation timed out. Please try again later», и одинаковые по
# объёму прайсы то уходили, то нет. Повтор делается только при наличии
# ключа идемпотентности: без него письмо можно отправить дважды.
RESEND_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
RESEND_SEND_ATTEMPTS = 3
RESEND_RETRY_PAUSES = (1.0, 4.0)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        logger.warning("Failed to parse Resend datetime: %s", value)
        return None


def _build_headers(
    api_key: str,
    *,
    idempotency_key: str | None = None,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key[:256]
    return headers


def _extract_domain(email: str | None) -> str:
    if not email or "@" not in email:
        return ""
    return email.rsplit("@", 1)[1].strip().lower()


def _find_domain_info(
    domains: list[dict[str, Any]],
    email: str | None,
) -> dict[str, Any] | None:
    email_domain = _extract_domain(email)
    if not email_domain:
        return None

    exact_match = None
    suffix_match = None
    for domain in domains:
        name = str(domain.get("name") or "").strip().lower()
        if not name:
            continue
        if email_domain == name:
            exact_match = domain
            break
        if email_domain.endswith(f".{name}"):
            suffix_match = domain
    return exact_match or suffix_match


async def list_resend_domains(
    api_key: str,
    *,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(
        timeout=timeout or RESEND_API_TIMEOUT
    ) as client:
        response = await client.get(
            f"{RESEND_API_BASE_URL}/domains",
            headers=_build_headers(api_key),
        )
        response.raise_for_status()
        payload = response.json()
        return list(payload.get("data") or [])


async def test_resend_api_access(
    *,
    api_key: str | None,
    email: str | None,
    timeout: int | None = None,
    require_receiving: bool = False,
) -> dict[str, Any]:
    if not api_key:
        raise RuntimeError("Resend API key не указан")

    domains = await list_resend_domains(api_key, timeout=timeout)
    domain = _find_domain_info(domains, email)
    if not domain:
        raise RuntimeError(
            f'Домен для адреса {email or "—"} не найден в Resend'
        )

    status = str(domain.get("status") or "").strip().lower()
    if status and status != "verified":
        raise RuntimeError(
            f'Resend домен {domain.get("name")} не подтвержден: {status}'
        )

    capabilities = domain.get("capabilities") or {}
    sending = str(capabilities.get("sending") or "").strip().lower()
    receiving = str(capabilities.get("receiving") or "").strip().lower()

    if sending and sending != "enabled":
        raise RuntimeError(
            f'Отправка для домена {domain.get("name")} в Resend отключена'
        )
    if require_receiving and receiving != "enabled":
        raise RuntimeError(
            f'Прием писем для домена {domain.get("name")} в Resend не включен'
        )

    return domain


async def list_received_email_attachments(
    api_key: str,
    email_id: str,
    *,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(
        timeout=timeout or RESEND_API_TIMEOUT
    ) as client:
        response = await client.get(
            f"{RESEND_API_BASE_URL}/emails/receiving/{email_id}/attachments",
            headers=_build_headers(api_key),
        )
        response.raise_for_status()
        payload = response.json()
        return list(payload.get("data") or [])


async def download_received_attachment(
    download_url: str,
    *,
    timeout: int | None = None,
) -> bytes:
    async with httpx.AsyncClient(
        timeout=timeout or RESEND_API_TIMEOUT
    ) as client:
        response = await client.get(download_url)
        response.raise_for_status()
        return response.content


async def fetch_received_emails_for_address(
    *,
    api_key: str,
    email: str,
    date_from: date | None = None,
    received_after: datetime | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    after: str | None = None
    normalized_email = email.strip().lower()
    async with httpx.AsyncClient(
        timeout=timeout or RESEND_API_TIMEOUT
    ) as client:
        while True:
            params: dict[str, Any] = {"limit": 100}
            if after:
                params["after"] = after
            response = await client.get(
                f"{RESEND_API_BASE_URL}/emails/receiving",
                headers=_build_headers(api_key),
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            items = list(payload.get("data") or [])
            if not items:
                break
            collected.extend(items)
            if not payload.get("has_more"):
                break
            after = items[-1].get("id")
            if not after:
                break

    results: list[dict[str, Any]] = []
    for item in collected:
        recipients = [
            str(addr).strip().lower()
            for addr in (item.get("to") or [])
            if str(addr).strip()
        ]
        if normalized_email not in recipients:
            continue
        created_at = _parse_iso_datetime(item.get("created_at"))
        if created_at is None:
            continue
        if date_from and created_at.date() < date_from:
            continue
        if received_after and created_at <= received_after:
            continue
        attachments_meta = await list_received_email_attachments(
            api_key,
            str(item.get("id")),
            timeout=timeout,
        )
        attachments = []
        for attachment in attachments_meta:
            download_url = attachment.get("download_url")
            if not download_url:
                continue
            payload_bytes = await download_received_attachment(
                download_url,
                timeout=timeout,
            )
            attachments.append(
                {
                    "id": attachment.get("id"),
                    "filename": attachment.get("filename"),
                    "content_type": attachment.get("content_type"),
                    "payload": payload_bytes,
                }
            )
        results.append(
            {
                "id": item.get("id"),
                "created_at": created_at,
                "to": recipients,
                "from": item.get("from"),
                "subject": item.get("subject") or "",
                "text": item.get("text"),
                "html": item.get("html"),
                "attachments": attachments,
            }
        )

    results.sort(
        key=lambda item: (
            item["created_at"],
            str(item.get("id") or ""),
        )
    )
    return results


def _wait_before_resend_retry(
    attempt: int,
    idempotency_key: str | None,
    reason: str,
) -> bool:
    """Ждёт перед повтором и говорит, стоит ли он вообще.

    Без ключа идемпотентности повтор не делаем: первый запрос мог дойти
    до Resend и создать письмо, а ответ потеряться — тогда клиент получит
    прайс дважды.
    """
    if attempt >= RESEND_SEND_ATTEMPTS - 1 or not idempotency_key:
        return False
    pause = RESEND_RETRY_PAUSES[min(attempt, len(RESEND_RETRY_PAUSES) - 1)]
    logger.warning(
        "Resend: %s; повтор %s из %s через %.0f с с ключом %s",
        reason,
        attempt + 2,
        RESEND_SEND_ATTEMPTS,
        pause,
        idempotency_key,
    )
    time.sleep(pause)
    return True


def send_email_via_resend(
    *,
    api_key: str | None,
    from_email: str | None,
    to_email: list[str],
    subject: str,
    body: str,
    is_html: bool,
    attachment_bytes: bytes | None = None,
    attachment_filename: str | None = None,
    timeout: int | None = None,
    idempotency_key: str | None = None,
) -> bool:
    if not api_key:
        logger.error("Resend API key is not configured")
        return False
    if not from_email:
        logger.error("Resend from_email is not configured")
        return False
    if not to_email:
        logger.error("No valid recipients for Resend email send")
        return False

    payload: dict[str, Any] = {
        "from": from_email,
        "to": to_email,
        "subject": subject,
    }
    if is_html:
        payload["html"] = body
    else:
        payload["text"] = body

    if attachment_bytes is not None and attachment_filename:
        payload["attachments"] = [
            {
                "filename": attachment_filename,
                "content": base64.b64encode(attachment_bytes).decode("ascii"),
            }
        ]

    request_size = len(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )
    if request_size > MAX_RESEND_EMAIL_BYTES:
        logger.error(
            "Failed to send email via Resend: request is %.2f MB; limit is 40 MB",
            request_size / 1024 / 1024,
        )
        return False

    configured_timeout = max(int(timeout or RESEND_API_TIMEOUT), 1)
    request_timeout = httpx.Timeout(
        connect=min(configured_timeout, 10),
        read=max(configured_timeout, 30),
        write=max(configured_timeout, 120),
        pool=min(configured_timeout, 10),
    )
    headers = _build_headers(api_key, idempotency_key=idempotency_key)
    logger.info(
        "Sending email via Resend from=%s to=%s attachment_bytes=%s "
        "request_bytes=%s write_timeout=%s",
        from_email,
        ",".join(to_email),
        len(attachment_bytes) if attachment_bytes is not None else 0,
        request_size,
        request_timeout.write,
    )
    for attempt in range(RESEND_SEND_ATTEMPTS):
        try:
            response = httpx.post(
                f"{RESEND_API_BASE_URL}/emails",
                headers=headers,
                json=payload,
                timeout=request_timeout,
            )
            response.raise_for_status()
            logger.info(
                "Email sent via Resend from=%s to=%s id=%s attempt=%s",
                from_email,
                ",".join(to_email),
                response.json().get("id"),
                attempt + 1,
            )
            return True
        except httpx.TimeoutException as exc:
            if _wait_before_resend_retry(
                attempt, idempotency_key, f"истекло время ожидания ({exc})"
            ):
                continue
            logger.error("Failed to send email via Resend: %s", exc)
            return False
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in RESEND_RETRYABLE_STATUSES and (
                _wait_before_resend_retry(
                    attempt, idempotency_key, f"ответ HTTP {status}"
                )
            ):
                continue
            logger.error(
                "Failed to send email via Resend: HTTP %s %s",
                status,
                exc.response.text[:1000],
            )
            return False
        except Exception as exc:
            logger.error("Failed to send email via Resend: %s", exc)
            return False
    return False
