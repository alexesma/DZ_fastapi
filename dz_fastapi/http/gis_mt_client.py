"""HTTP-клиент ГИС МТ (Честный знак, True API).

Авторизация: GET /auth/key → {uuid, data}; data подписывается КЭП
(у нас — облачной подписью Диадока, откреплённая CMS в base64);
POST /auth/simpleSignIn → токен на ~10 часов.
"""
from __future__ import annotations

import json
import os
from typing import Any

import httpx

DEFAULT_GIS_MT_BASE_URL = "https://markirovka.crpt.ru/api/v3/true-api"


class GisMtApiError(RuntimeError):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class GisMtClient:
    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        timeout: float = 60.0,
    ) -> None:
        self.token = token
        self.base_url = (
            base_url
            or os.getenv("GIS_MT_BASE_URL")
            or DEFAULT_GIS_MT_BASE_URL
        ).rstrip("/")
        self.timeout = timeout

    @property
    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> httpx.Response:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(
                method=method,
                url=f"{self.base_url}{path}",
                params=params,
                headers=self._headers,
                json=json_body,
            )
        if response.status_code >= 400:
            try:
                detail = json.dumps(
                    response.json(), ensure_ascii=False
                )
            except Exception:  # noqa: BLE001
                detail = response.text
            raise GisMtApiError(
                response.status_code,
                detail[:4000] or "Unknown GIS MT error",
            )
        return response

    async def get_auth_key(self) -> dict[str, Any]:
        """{uuid, data} — data нужно подписать КЭП."""
        response = await self._request("GET", "/auth/key")
        return response.json()

    async def simple_sign_in(
        self,
        *,
        uuid: str,
        signature_b64: str,
    ) -> str:
        """Обменивает подпись на токен."""
        response = await self._request(
            "POST",
            "/auth/simpleSignIn",
            json_body={"uuid": uuid, "data": signature_b64},
        )
        payload = response.json()
        token = str(payload.get("token") or "").strip()
        if not token:
            raise GisMtApiError(
                502, "GIS MT did not return token on simpleSignIn"
            )
        return token

    async def get_cises_info(
        self,
        codes: list[str],
    ) -> list[dict[str, Any]]:
        """Статусы кодов в ГИС МТ (до 1000 за запрос)."""
        response = await self._request(
            "POST",
            "/cises/info",
            json_body=list(codes),
        )
        payload = response.json()
        if isinstance(payload, list):
            return payload
        return list(payload.get("results") or [])

    async def create_document(
        self,
        *,
        product_group: str,
        document_type: str,
        product_document_b64: str,
        signature_b64: str | None = None,
        document_format: str = "MANUAL",
    ) -> str:
        """Создаёт документ в ЛК (например, вывод из оборота).

        Возвращает идентификатор документа в ГИС МТ.
        """
        body: dict[str, Any] = {
            "document_format": document_format,
            "product_document": product_document_b64,
            "type": document_type,
        }
        if signature_b64:
            body["signature"] = signature_b64
        response = await self._request(
            "POST",
            "/lk/documents/create",
            params={"pg": product_group},
            json_body=body,
        )
        text = response.text.strip().strip('"')
        return text
