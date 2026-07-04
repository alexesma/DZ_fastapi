import asyncio
import json
import time
from datetime import date
from typing import Any

import httpx

from dz_fastapi.core.config import settings


class DiadocApiError(RuntimeError):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class DiadocClient:
    def __init__(
        self,
        access_token: str,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.access_token = access_token
        self.base_url = (base_url or settings.diadoc_api_base_url).rstrip("/")
        self.timeout = timeout

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json; charset=utf-8",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: Any | None = None,
        content: bytes | str | None = None,
    ) -> httpx.Response:
        request_headers = dict(self._headers)
        if headers:
            request_headers.update(headers)
        async with httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            response = await client.request(
                method=method,
                url=f"{self.base_url}{path}",
                params=params,
                headers=request_headers,
                json=json_body,
                content=content,
            )
        if response.status_code >= 400:
            try:
                payload = response.json()
                detail = json.dumps(payload, ensure_ascii=False)
            except Exception:
                detail = response.text
            raise DiadocApiError(
                response.status_code,
                detail[:4000] or "Unknown Diadoc API error",
            )
        return response

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> dict[str, Any]:
        response = await self._request(
            method,
            path,
            params=params,
            json_body=json_body,
        )
        return response.json()

    async def get_my_user(self) -> dict[str, Any]:
        return await self._request_json("GET", "/V2/GetMyUser")

    async def get_my_organizations(
        self,
        *,
        auto_register: bool = False,
    ) -> dict[str, Any]:
        return await self._request_json(
            "GET",
            "/GetMyOrganizations",
            params={"autoRegister": str(auto_register).lower()},
        )

    async def get_organization(
        self,
        *,
        org_id: str | None = None,
        box_id_guid: str | None = None,
        fns_participant_id: str | None = None,
        inn: str | None = None,
        kpp: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if org_id:
            params["orgId"] = org_id
        elif box_id_guid:
            params["boxId"] = box_id_guid
        elif fns_participant_id:
            params["fnsParticipantId"] = fns_participant_id
        elif inn:
            params["inn"] = inn
            if kpp:
                params["kpp"] = kpp
        else:
            raise ValueError(
                "One of org_id, box_id_guid, fns_participant_id, inn must be set"
            )
        return await self._request_json(
            "GET",
            "/GetOrganization",
            params=params,
        )

    async def get_document_types(self, box_id_guid: str) -> dict[str, Any]:
        return await self._request_json(
            "GET",
            "/V2/GetDocumentTypes",
            params={"boxId": box_id_guid},
        )

    async def get_counteragents(
        self,
        *,
        my_box_id_guid: str,
        counteragent_status: str | None = None,
        after_index_key: str | None = None,
        query: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "myBoxId": my_box_id_guid,
            "pageSize": max(1, min(int(page_size or 100), 100)),
        }
        if counteragent_status:
            params["counteragentStatus"] = counteragent_status
        if after_index_key:
            params["afterIndexKey"] = after_index_key
        if query:
            params["query"] = query
        return await self._request_json(
            "GET",
            "/V3/GetCounteragents",
            params=params,
        )

    async def get_documents(
        self,
        *,
        box_id_guid: str,
        filter_category: str,
        count: int = 50,
        after_index_key: str | None = None,
        counteragent_box_id: str | None = None,
        document_number: str | None = None,
        from_document_date: date | None = None,
        to_document_date: date | None = None,
        sort_direction: str = "Descending",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "boxId": box_id_guid,
            "filterCategory": filter_category,
            "count": count,
            "sortDirection": sort_direction,
        }
        if after_index_key:
            params["afterIndexKey"] = after_index_key
        if counteragent_box_id:
            params["counteragentBoxId"] = counteragent_box_id
        if document_number:
            params["documentNumber"] = document_number
        if from_document_date:
            params["fromDocumentDate"] = from_document_date.strftime(
                "%d.%m.%Y"
            )
        if to_document_date:
            params["toDocumentDate"] = to_document_date.strftime("%d.%m.%Y")
        return await self._request_json(
            "GET",
            "/V3/GetDocuments",
            params=params,
        )

    async def get_document(
        self,
        *,
        box_id_guid: str,
        message_id: str,
        entity_id: str,
        inject_entity_content: bool = False,
    ) -> dict[str, Any]:
        return await self._request_json(
            "GET",
            "/V3/GetDocument",
            params={
                "boxId": box_id_guid,
                "messageId": message_id,
                "entityId": entity_id,
                "injectEntityContent": str(inject_entity_content).lower(),
            },
        )

    async def get_entity_content(
        self,
        *,
        box_id_guid: str,
        message_id: str,
        entity_id: str,
    ) -> bytes:
        response = await self._request(
            "GET",
            "/V4/GetEntityContent",
            params={
                "boxId": box_id_guid,
                "messageId": message_id,
                "entityId": entity_id,
            },
            headers={"Accept": "*/*"},
        )
        return response.content

    async def generate_title_xml(
        self,
        *,
        box_id_guid: str,
        document_type_named_id: str,
        document_function: str,
        document_version: str,
        title_index: int,
        user_data_xml: bytes | str,
        disable_validation: bool | None = None,
        editing_setting_id: str | None = None,
        letter_id: str | None = None,
        document_id: str | None = None,
    ) -> tuple[bytes, str | None]:
        params: dict[str, Any] = {
            "boxId": box_id_guid,
            "documentTypeNamedId": document_type_named_id,
            "documentFunction": document_function,
            "documentVersion": document_version,
            "titleIndex": int(title_index),
        }
        if disable_validation is not None:
            params["disableValidation"] = str(disable_validation).lower()
        if editing_setting_id:
            params["editingSettingId"] = editing_setting_id
        if letter_id:
            params["letterId"] = letter_id
        if document_id:
            params["documentId"] = document_id

        response = await self._request(
            "POST",
            "/GenerateTitleXml",
            params=params,
            headers={
                "Accept": "*/*",
                "Content-Type": "application/xml; charset=utf-8",
            },
            content=user_data_xml,
        )
        content_disposition = response.headers.get("content-disposition") or ""
        file_name = None
        for marker in ("filename=", "filename*="):
            idx = content_disposition.lower().find(marker)
            if idx >= 0:
                file_name = (
                    content_disposition[idx + len(marker):].strip().strip('"')
                )
                break
        return response.content, file_name

    async def post_message(
        self,
        *,
        message: dict[str, Any],
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] | None = None
        if operation_id:
            params = {"operationId": operation_id}
        return await self._request_json(
            "POST",
            "/V3/PostMessage",
            params=params,
            json_body=message,
        )

    async def post_message_patch(
        self,
        *,
        patch: dict[str, Any],
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Дополнение сообщения: ИоП, титул покупателя, аннулирование."""
        params: dict[str, Any] | None = None
        if operation_id:
            params = {"operationId": operation_id}
        return await self._request_json(
            "POST",
            "/V3/PostMessagePatch",
            params=params,
            json_body=patch,
        )

    async def _wait_task_result(
        self,
        path: str,
        task_id: str,
        *,
        timeout_seconds: float = 120.0,
        poll_seconds: float = 2.0,
    ) -> dict[str, Any]:
        """Поллинг асинхронной операции Диадока.

        HTTP 204 — операция ещё выполняется (Retry-After в заголовке),
        HTTP 200 — готово.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            response = await self._request(
                "GET",
                path,
                params={"taskId": task_id},
            )
            if response.status_code != 204:
                return response.json()
            if time.monotonic() > deadline:
                raise DiadocApiError(
                    504,
                    f"Diadoc async task {path} timed out "
                    f"after {timeout_seconds}s",
                )
            retry_after = response.headers.get("Retry-After")
            try:
                delay = min(float(retry_after), 15.0)
            except (TypeError, ValueError):
                delay = poll_seconds
            await asyncio.sleep(max(delay, 1.0))

    async def cloud_sign(
        self,
        *,
        files: list[dict[str, Any]],
        certificate_thumbprint: str | None = None,
    ) -> str:
        """Стартует облачное подписание пакета файлов.

        files: [{"FileName": str, "Content": {"Content": base64}}].
        Возвращает token операции; пользователю уходит SMS с кодом.
        """
        params: dict[str, Any] = {}
        if certificate_thumbprint:
            params["certificateThumbprint"] = certificate_thumbprint
        started = await self._request_json(
            "POST",
            "/CloudSign",
            params=params or None,
            json_body={"Files": files},
        )
        task_id = str(started.get("TaskId") or "").strip()
        if not task_id:
            raise DiadocApiError(502, "CloudSign did not return TaskId")
        result = await self._wait_task_result("/CloudSignResult", task_id)
        token = str(result.get("Token") or "").strip()
        if not token:
            raise DiadocApiError(502, "CloudSignResult did not return Token")
        return token

    async def cloud_sign_confirm(
        self,
        *,
        token: str,
        confirmation_code: str,
    ) -> list[str]:
        """Подтверждает подписание SMS-кодом.

        Возвращает подписи (base64) в порядке файлов из cloud_sign.
        """
        started = await self._request_json(
            "POST",
            "/CloudSignConfirm",
            params={
                "token": token,
                "confirmationCode": confirmation_code,
                "return": "Content",
            },
        )
        task_id = str(started.get("TaskId") or "").strip()
        if not task_id:
            raise DiadocApiError(
                502, "CloudSignConfirm did not return TaskId"
            )
        result = await self._wait_task_result(
            "/CloudSignConfirmResult", task_id
        )
        signatures: list[str] = []
        for item in result.get("Signatures") or []:
            content = str((item or {}).get("Content") or "").strip()
            if content:
                signatures.append(content)
        if not signatures:
            raise DiadocApiError(
                502, "CloudSignConfirmResult returned no signatures"
            )
        return signatures

    async def generate_receipt_xml(
        self,
        *,
        box_id_guid: str,
        message_id: str,
        attachment_id: str,
        signer: dict[str, Any],
    ) -> bytes:
        """Извещение о получении (ИоП) к сущности документа."""
        response = await self._request(
            "POST",
            "/GenerateReceiptXml",
            params={
                "boxId": box_id_guid,
                "messageId": message_id,
                "attachmentId": attachment_id,
            },
            headers={"Accept": "*/*"},
            json_body=signer,
        )
        return response.content

    async def generate_print_form(
        self,
        *,
        box_id_guid: str,
        message_id: str,
        document_id: str,
        timeout_seconds: float = 90.0,
    ) -> bytes:
        """Печатная форма документа (PDF).

        Метод асинхронный на стороне Диадока: пока форма готовится,
        приходит ответ с Retry-After — повторяем тот же запрос.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            response = await self._request(
                "GET",
                "/GeneratePrintForm",
                params={
                    "boxId": box_id_guid,
                    "messageId": message_id,
                    "documentId": document_id,
                },
                headers={"Accept": "*/*"},
            )
            content_type = str(
                response.headers.get("Content-Type") or ""
            ).lower()
            retry_after = response.headers.get("Retry-After")
            if response.content and "json" not in content_type and (
                not retry_after
            ):
                return response.content
            if time.monotonic() > deadline:
                raise DiadocApiError(
                    504,
                    "Diadoc print form generation timed out",
                )
            try:
                delay = min(float(retry_after), 15.0)
            except (TypeError, ValueError):
                delay = 3.0
            await asyncio.sleep(max(delay, 1.0))

    async def generate_signature_rejection_xml(
        self,
        *,
        box_id_guid: str,
        message_id: str,
        attachment_id: str,
        comment: str | None = None,
        signer: dict[str, Any] | None = None,
    ) -> bytes:
        """Отказ в подписи входящего документа (XML для подписания)."""
        body: dict[str, Any] = {}
        if comment:
            body["Comment"] = comment
        if signer:
            body["Signer"] = signer
        response = await self._request(
            "POST",
            "/GenerateSignatureRejectionXml",
            params={
                "boxId": box_id_guid,
                "messageId": message_id,
                "attachmentId": attachment_id,
            },
            headers={"Accept": "*/*"},
            json_body=body,
        )
        return response.content

    async def generate_revocation_request_xml(
        self,
        *,
        box_id_guid: str,
        message_id: str,
        attachment_id: str,
        comment: str | None = None,
        signer: dict[str, Any] | None = None,
    ) -> bytes:
        """Запрос на аннулирование документа."""
        body: dict[str, Any] = {}
        if comment:
            body["Comment"] = comment
        if signer:
            body["Signer"] = signer
        response = await self._request(
            "POST",
            "/V2/GenerateRevocationRequestXml",
            params={
                "boxId": box_id_guid,
                "messageId": message_id,
                "attachmentId": attachment_id,
            },
            headers={"Accept": "*/*"},
            json_body=body,
        )
        return response.content
