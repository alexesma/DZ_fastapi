import logging
from datetime import date
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse, Response
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.config import settings
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import crud_customer, crud_provider
from dz_fastapi.crud.settings import crud_diadoc_integration_settings
from dz_fastapi.http.diadoc_client import DiadocApiError, DiadocClient
from dz_fastapi.models.diadoc import DiadocIncomingDocument, DiadocOutgoingDocument
from dz_fastapi.models.inventory import ReturnFromCustomer, ReturnToSupplier
from dz_fastapi.models.user import User
from dz_fastapi.schemas.diadoc import (
    DiadocCounteragentListOut,
    DiadocCounteragentOut,
    DiadocCustomerBindingIn,
    DiadocDocumentListItem,
    DiadocDocumentListOut,
    DiadocEnvironment,
    DiadocInboundDocumentOut,
    DiadocInboundDocumentProcessIn,
    DiadocInboundDocumentProcessResult,
    DiadocInboundDocumentRegisterIn,
    DiadocInboundDocumentRegisterResult,
    DiadocInboundSyncRequest,
    DiadocInboundSyncResult,
    DiadocOAuthInitRequest,
    DiadocOAuthInitResponse,
    DiadocOrganizationOut,
    DiadocOutgoingDocumentCreateIn,
    DiadocOutgoingDocumentOut,
    DiadocProviderBindingIn,
    DiadocReturnFormalizedReadinessOut,
    DiadocReturnOutboundCreateIn,
    DiadocSettingsUpdate,
    DiadocShipmentFormalizedReadinessOut,
    DiadocShipmentOutboundCreateIn,
    DiadocStatusOut,
)
from dz_fastapi.schemas.partner import (
    CustomerExternalReferenceCreate,
    CustomerExternalReferenceOut,
    ProviderExternalReferenceCreate,
    ProviderExternalReferenceOut,
)
from dz_fastapi.services.diadoc_documents import (
    ensure_diadoc_document_content,
    get_diadoc_incoming_document,
    list_diadoc_incoming_documents,
    process_diadoc_incoming_document,
    register_diadoc_document_as_supplier_message,
    resolve_provider_by_diadoc_counteragent_box,
    sync_diadoc_incoming_documents,
)
from dz_fastapi.services.diadoc_integration import (
    DiadocNotConnectedError,
    get_diadoc_client_for_session,
    save_diadoc_authorization,
)
from dz_fastapi.services.diadoc_oauth import (
    build_diadoc_auth_url,
    exchange_diadoc_code_for_tokens,
    normalize_diadoc_environment,
    parse_diadoc_oauth_state,
)
from dz_fastapi.services.diadoc_outgoing import (
    build_customer_return_formalized_readiness,
    build_diadoc_payload_from_shipment,
    build_formalized_diadoc_payload_from_customer_return,
    build_formalized_diadoc_payload_from_shipment,
    build_formalized_diadoc_payload_from_supplier_return,
    build_shipment_formalized_readiness,
    build_shipments_formalized_readiness,
    build_supplier_return_formalized_readiness,
    list_diadoc_outgoing_documents,
    post_diadoc_outgoing_document,
    resolve_customer_by_diadoc_counteragent_box,
    resolve_diadoc_box_for_customer,
    resolve_diadoc_box_for_provider,
)

logger = logging.getLogger("dz_fastapi")

router = APIRouter(prefix="/diadoc", tags=["diadoc"])


def _integration_to_status(
    integration,
) -> DiadocStatusOut:
    configured = bool(
        settings.diadoc_client_id
        and settings.diadoc_client_secret
        and settings.diadoc_oauth_redirect_uri
    )
    connected = bool(integration.refresh_token and integration.connected_at)
    return DiadocStatusOut(
        id=integration.id,
        configured=configured,
        connected=connected,
        environment=DiadocEnvironment(
            normalize_diadoc_environment(integration.environment)
        ),
        organization_id=integration.organization_id,
        organization_name=integration.organization_name,
        organization_inn=integration.organization_inn,
        organization_kpp=integration.organization_kpp,
        seller_legal_address=integration.seller_legal_address,
        seller_postal_address=integration.seller_postal_address,
        signer_full_name=integration.signer_full_name,
        signer_position=integration.signer_position,
        signer_basis=integration.signer_basis,
        formalized_default_function=(
            integration.formalized_default_function or "ДОП"
        ),
        box_id=integration.box_id,
        box_id_guid=integration.box_id_guid,
        connected_user_id=integration.connected_user_id,
        connected_user_name=integration.connected_user_name,
        connected_at=integration.connected_at,
        inbound_sync_enabled=bool(integration.inbound_sync_enabled),
        inbound_sync_count=int(integration.inbound_sync_count or 50),
        inbound_download_content=bool(integration.inbound_download_content),
        inbound_process_enabled=bool(integration.inbound_process_enabled),
        access_token_expires_at=integration.access_token_expires_at,
        last_sync_at=integration.last_sync_at,
        last_error=integration.last_error,
    )


def _incoming_document_to_out(
    document: DiadocIncomingDocument,
) -> DiadocInboundDocumentOut:
    provider = getattr(document, "provider", None)
    supplier_message = getattr(document, "supplier_order_message", None)
    receipt_ids = sorted(
        int(receipt.id)
        for receipt in getattr(supplier_message, "receipts", []) or []
        if getattr(receipt, "id", None) is not None
    )
    return DiadocInboundDocumentOut(
        id=int(document.id),
        environment=DiadocEnvironment(
            normalize_diadoc_environment(document.environment)
        ),
        box_id_guid=document.box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
        index_key=document.index_key,
        counteragent_box_id=document.counteragent_box_id,
        file_name=document.file_name,
        document_number=document.document_number,
        document_date=document.document_date,
        delivery_at=document.delivery_at,
        sent_at=document.sent_at,
        provider_id=document.provider_id,
        provider_name=(
            str(getattr(provider, "name", "") or "").strip()
            if provider is not None
            else None
        ),
        supplier_order_message_id=document.supplier_order_message_id,
        local_file_path=document.local_file_path,
        status=document.status,
        import_error_details=document.import_error_details,
        synced_at=document.synced_at,
        registered_at=document.registered_at,
        can_register_supplier_message=bool(
            (document.provider_id or 0) > 0 and document.local_file_path
        ),
        can_process_supplier_message=bool(
            (document.provider_id or 0) > 0 and document.local_file_path
        ),
        supplier_receipt_ids=receipt_ids,
    )


def _raise_diadoc_http_error(exc: Exception) -> None:
    if isinstance(exc, DiadocApiError):
        raise HTTPException(
            status_code=exc.status_code,
            detail=f"Diadoc API error: {exc.detail}",
        ) from exc
    if isinstance(exc, DiadocNotConnectedError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=str(exc)) from exc


def _extract_counteragent_box_id(payload: dict[str, Any]) -> str | None:
    for key in ("CounteragentBoxId", "BoxIdGuid", "BoxId"):
        value = payload.get(key)
        if value:
            return str(value)
    info = payload.get("CounteragentInfo")
    if isinstance(info, dict):
        value = info.get("CounteragentBoxId")
        if value:
            return str(value)
    current_status = payload.get("CurrentStatus")
    if isinstance(current_status, dict):
        value = current_status.get("CounteragentBoxId")
        if value:
            return str(value)
    return None


def _normalize_counteragent_status(payload: dict[str, Any]) -> str | None:
    for key in ("CurrentStatusNamedId", "StatusNamedId", "Status"):
        value = payload.get(key)
        if value:
            return str(value)
    info = payload.get("CounteragentInfo")
    if isinstance(info, dict):
        value = info.get("Status")
        if value:
            return str(value)
    return None


def _outgoing_document_to_out(
    document: DiadocOutgoingDocument,
) -> DiadocOutgoingDocumentOut:
    customer = getattr(document, "customer", None)
    provider = getattr(document, "provider", None)
    return DiadocOutgoingDocumentOut(
        id=int(document.id),
        environment=DiadocEnvironment(
            normalize_diadoc_environment(document.environment)
        ),
        from_box_id_guid=document.from_box_id_guid,
        to_box_id_guid=document.to_box_id_guid,
        customer_id=document.customer_id,
        customer_name=(
            str(getattr(customer, "name", "") or "").strip()
            if customer is not None
            else None
        ),
        provider_id=document.provider_id,
        provider_name=(
            str(getattr(provider, "name", "") or "").strip()
            if provider is not None
            else None
        ),
        source_type=document.source_type,
        source_id=document.source_id,
        type_named_id=document.type_named_id,
        document_function=document.document_function,
        document_version=document.document_version,
        file_name=document.file_name,
        document_number=document.document_number,
        document_date=document.document_date,
        local_file_path=document.local_file_path,
        content_sha256=document.content_sha256,
        comment=document.comment,
        need_recipient_signature=bool(document.need_recipient_signature),
        need_receipt=bool(document.need_receipt),
        is_draft=bool(document.is_draft),
        message_id=document.message_id,
        entity_id=document.entity_id,
        status=document.status,
        error_details=document.error_details,
        metadata=document.metadata_json or {},
        raw_response=document.raw_response or {},
        created_at=document.created_at,
        sent_at=document.sent_at,
    )


def _normalize_organizations(
    payload: dict[str, Any]
) -> list[DiadocOrganizationOut]:
    organizations = []
    for item in payload.get("Organizations") or []:
        boxes = []
        for box in item.get("Boxes") or []:
            boxes.append(
                {
                    "box_id": box.get("BoxId"),
                    "box_id_guid": box.get("BoxIdGuid"),
                    "title": box.get("Title"),
                    "invoice_format_version": box.get("InvoiceFormatVersion"),
                    "encrypted_documents_allowed": box.get(
                        "EncryptedDocumentsAllowed"
                    ),
                }
            )
        organizations.append(
            DiadocOrganizationOut(
                org_id=item.get("OrgIdGuid") or item.get("OrgId"),
                inn=item.get("Inn"),
                kpp=item.get("Kpp"),
                full_name=item.get("FullName"),
                short_name=item.get("ShortName"),
                is_active=item.get("IsActive"),
                is_test=item.get("IsTest"),
                boxes=boxes,
            )
        )
    return organizations


def _normalize_documents(payload: dict[str, Any]) -> DiadocDocumentListOut:
    documents = []
    for item in payload.get("Documents") or []:
        documents.append(
            DiadocDocumentListItem(
                message_id=str(item.get("MessageId") or ""),
                entity_id=str(item.get("EntityId") or ""),
                index_key=item.get("IndexKey"),
                file_name=item.get("FileName"),
                document_date=item.get("DocumentDate"),
                document_number=item.get("DocumentNumber"),
                counteragent_box_id=item.get("CounteragentBoxId"),
                delivery_timestamp_ticks=item.get("DeliveryTimestampTicks"),
                send_timestamp_ticks=item.get("SendTimestampTicks"),
                raw=item,
            )
        )
    return DiadocDocumentListOut(
        total_count=int(payload.get("TotalCount") or 0),
        has_more_results=bool(payload.get("HasMoreResults")),
        documents=documents,
    )


def _resolve_box_id_guid(integration, override_box_id_guid: str | None) -> str:
    box_id_guid = override_box_id_guid or integration.box_id_guid
    if not box_id_guid:
        raise HTTPException(
            status_code=400,
            detail=(
                "Diadoc box is not selected. "
                "Select a box via /diadoc/settings first."
            ),
        )
    return box_id_guid


@router.get(
    "/status",
    response_model=DiadocStatusOut,
    status_code=status.HTTP_200_OK,
)
async def get_diadoc_status(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    return _integration_to_status(integration)


@router.post(
    "/oauth/init",
    response_model=DiadocOAuthInitResponse,
    status_code=status.HTTP_200_OK,
)
async def init_diadoc_oauth(
    payload: DiadocOAuthInitRequest | None = Body(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    if payload and payload.environment:
        integration = await crud_diadoc_integration_settings.update(
            session,
            {"environment": payload.environment.value},
        )
    try:
        auth_url = build_diadoc_auth_url(integration.environment)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return DiadocOAuthInitResponse(auth_url=auth_url)


@router.get(
    "/oauth/callback",
    include_in_schema=False,
)
async def diadoc_oauth_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    if error:
        return HTMLResponse(
            f"<h3>Diadoc OAuth error</h3><p>{error}</p>",
            status_code=400,
        )
    if not code or not state:
        return HTMLResponse(
            "<h3>Diadoc OAuth error</h3>" "<p>Missing code or state.</p>",
            status_code=400,
        )
    environment = parse_diadoc_oauth_state(state)
    if not environment:
        return HTMLResponse(
            "<h3>Diadoc OAuth error</h3><p>Invalid state.</p>",
            status_code=400,
        )
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    try:
        tokens = await exchange_diadoc_code_for_tokens(code)
        access_token = str(tokens.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError("Diadoc access token not returned")
        client = DiadocClient(access_token=access_token)
        user_payload = await client.get_my_user()
        organizations_payload = await client.get_my_organizations(
            auto_register=False
        )
        await save_diadoc_authorization(
            session=session,
            integration=integration,
            tokens=tokens,
            environment=environment,
            user_payload=user_payload,
            organizations_payload=organizations_payload,
        )
    except Exception as exc:
        logger.error("Diadoc OAuth exchange failed: %s", exc)
        return HTMLResponse(
            "<h3>Diadoc OAuth error</h3>" "<p>Token exchange failed.</p>",
            status_code=400,
        )
    return HTMLResponse(
        "<h3>Диадок подключен</h3>" "<p>Можно закрыть это окно.</p>"
    )


@router.post(
    "/disconnect",
    response_model=DiadocStatusOut,
    status_code=status.HTTP_200_OK,
)
async def disconnect_diadoc(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.clear_connection(
        session
    )
    return _integration_to_status(integration)


@router.put(
    "/settings",
    response_model=DiadocStatusOut,
    status_code=status.HTTP_200_OK,
)
async def update_diadoc_settings(
    payload: DiadocSettingsUpdate,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    data = payload.model_dump(exclude_unset=True)
    environment = data.get("environment")
    if environment is not None:
        environment_value = environment.value
        if (
            normalize_diadoc_environment(integration.environment)
            != environment_value
        ):
            integration.refresh_token = None
            integration.access_token = None
            integration.token_type = None
            integration.token_scope = None
            integration.access_token_expires_at = None
            integration.connected_user_id = None
            integration.connected_user_name = None
            integration.connected_at = None
            integration.organization_id = None
            integration.organization_name = None
            integration.organization_inn = None
            integration.organization_kpp = None
            integration.box_id = None
            integration.box_id_guid = None
        integration.environment = environment_value
        data.pop("environment", None)
    for key, value in data.items():
        setattr(integration, key, value)
    integration.last_error = None
    session.add(integration)
    await session.commit()
    await session.refresh(integration)
    return _integration_to_status(integration)


@router.get(
    "/organizations",
    response_model=list[DiadocOrganizationOut],
    status_code=status.HTTP_200_OK,
)
async def list_diadoc_organizations(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        payload = await client.get_my_organizations(auto_register=False)
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return _normalize_organizations(payload)
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/counteragents",
    response_model=DiadocCounteragentListOut,
    status_code=status.HTTP_200_OK,
)
async def list_diadoc_counteragents(
    query: str | None = Query(default=None),
    counteragent_status: str | None = Query(default=None),
    after_index_key: str | None = Query(default=None),
    page_size: int = Query(default=100, ge=1, le=100),
    box_id_guid: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        selected_box_id_guid = _resolve_box_id_guid(integration, box_id_guid)
        payload = await client.get_counteragents(
            my_box_id_guid=selected_box_id_guid,
            counteragent_status=counteragent_status,
            after_index_key=after_index_key,
            query=query,
            page_size=page_size,
        )
        items: list[DiadocCounteragentOut] = []
        for row in payload.get("Counteragents") or []:
            box_ref = _extract_counteragent_box_id(row)
            provider_id = None
            provider_name = None
            customer_id = None
            customer_name = None
            if box_ref:
                provider_id = (
                    await resolve_provider_by_diadoc_counteragent_box(
                        session,
                        counteragent_box_id=box_ref,
                    )
                )
                if provider_id is not None:
                    provider = await crud_provider.get_by_id(
                        provider_id=provider_id,
                        session=session,
                    )
                    if provider is not None:
                        provider_name = provider.name
                customer = await resolve_customer_by_diadoc_counteragent_box(
                    session,
                    counteragent_box_id=box_ref,
                )
                if customer is not None:
                    customer_id = int(customer.id)
                    customer_name = customer.name
            items.append(
                DiadocCounteragentOut(
                    box_id_guid=box_ref or "",
                    box_id=str(row.get("BoxId") or "") or None,
                    full_name=(str(row.get("FullName") or "").strip() or None),
                    short_name=(
                        str(row.get("ShortName") or "").strip() or None
                    ),
                    inn=str(row.get("Inn") or "").strip() or None,
                    kpp=str(row.get("Kpp") or "").strip() or None,
                    status=_normalize_counteragent_status(row),
                    event_timestamp_ticks=(
                        int(
                            row.get("EventTimestampTicks")
                            or (
                                (row.get("CounteragentInfo") or {}).get(
                                    "EventTimestampTicks"
                                )
                            )
                            or 0
                        )
                        or None
                    ),
                    last_event_comment=(
                        str(
                            row.get("LastEventComment")
                            or (
                                (row.get("CounteragentInfo") or {}).get(
                                    "LastEventComment"
                                )
                            )
                            or ""
                        ).strip()
                        or None
                    ),
                    message_from_counteragent=(
                        str(
                            row.get("MessageFromCounteragent")
                            or (
                                (row.get("CounteragentInfo") or {}).get(
                                    "MessageFromCounteragent"
                                )
                            )
                            or ""
                        ).strip()
                        or None
                    ),
                    message_to_counteragent=(
                        str(
                            row.get("MessageToCounteragent")
                            or (
                                (row.get("CounteragentInfo") or {}).get(
                                    "MessageToCounteragent"
                                )
                            )
                            or ""
                        ).strip()
                        or None
                    ),
                    mapped_provider_id=provider_id,
                    mapped_provider_name=provider_name,
                    mapped_customer_id=customer_id,
                    mapped_customer_name=customer_name,
                    raw=row,
                )
            )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return DiadocCounteragentListOut(
            total_count=int(payload.get("TotalCount") or len(items)),
            has_more_results=bool(payload.get("HasMoreResults")),
            after_index_key=payload.get("AfterIndexKey"),
            counteragents=items,
        )
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.post(
    "/providers/{provider_id}/bind-counteragent",
    response_model=ProviderExternalReferenceOut,
    status_code=status.HTTP_200_OK,
)
async def bind_provider_to_diadoc_counteragent(
    provider_id: int,
    payload: DiadocProviderBindingIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        reference = await crud_provider.upsert_external_reference(
            provider_id=provider_id,
            obj_in=ProviderExternalReferenceCreate(
                source_system=payload.source_system,
                external_supplier_name=payload.counteragent_box_id,
                is_active=payload.is_active,
            ),
            session=session,
        )
        return ProviderExternalReferenceOut.model_validate(reference)
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.post(
    "/customers/{customer_id}/bind-counteragent",
    response_model=CustomerExternalReferenceOut,
    status_code=status.HTTP_200_OK,
)
async def bind_customer_to_diadoc_counteragent(
    customer_id: int,
    payload: DiadocCustomerBindingIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        reference = await crud_customer.upsert_external_reference(
            customer_id=customer_id,
            obj_in=CustomerExternalReferenceCreate(
                source_system=payload.source_system,
                external_customer_name=payload.counteragent_box_id,
                is_active=payload.is_active,
            ),
            session=session,
        )
        return CustomerExternalReferenceOut.model_validate(reference)
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/documents",
    response_model=DiadocDocumentListOut,
    status_code=status.HTTP_200_OK,
)
async def list_diadoc_documents(
    box_id_guid: str | None = Query(default=None),
    filter_category: str = Query(default="Any.Inbound"),
    count: int = Query(default=50, ge=1, le=100),
    after_index_key: str | None = Query(default=None),
    counteragent_box_id: str | None = Query(default=None),
    document_number: str | None = Query(default=None),
    from_document_date: date | None = Query(default=None),
    to_document_date: date | None = Query(default=None),
    sort_direction: str = Query(
        default="Descending",
        pattern="^(Ascending|Descending)$",
    ),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        selected_box_id_guid = _resolve_box_id_guid(integration, box_id_guid)
        payload = await client.get_documents(
            box_id_guid=selected_box_id_guid,
            filter_category=filter_category,
            count=count,
            after_index_key=after_index_key,
            counteragent_box_id=counteragent_box_id,
            document_number=document_number,
            from_document_date=from_document_date,
            to_document_date=to_document_date,
            sort_direction=sort_direction,
        )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return _normalize_documents(payload)
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.post(
    "/sync/inbound",
    response_model=DiadocInboundSyncResult,
    status_code=status.HTTP_200_OK,
)
async def sync_diadoc_inbound_documents(
    payload: DiadocInboundSyncRequest,
    box_id_guid: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        selected_box_id_guid = _resolve_box_id_guid(integration, box_id_guid)
        result = await sync_diadoc_incoming_documents(
            session=session,
            client=client,
            environment=normalize_diadoc_environment(integration.environment),
            box_id_guid=selected_box_id_guid,
            filter_category=payload.filter_category,
            count=payload.count,
            after_index_key=payload.after_index_key,
            counteragent_box_id=payload.counteragent_box_id,
            document_number=payload.document_number,
            from_document_date=payload.from_document_date,
            to_document_date=payload.to_document_date,
            sort_direction=payload.sort_direction,
            download_content=payload.download_content,
            register_supplier_message=payload.register_supplier_message,
            process_supplier_message=payload.process_supplier_message,
        )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return DiadocInboundSyncResult(**result)
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/inbound-documents",
    response_model=list[DiadocInboundDocumentOut],
    status_code=status.HTTP_200_OK,
)
async def list_diadoc_inbound_documents_endpoint(
    document_id: int | None = Query(default=None),
    provider_id: int | None = Query(default=None),
    registered_only: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    rows = await list_diadoc_incoming_documents(
        session,
        document_id=document_id,
        provider_id=provider_id,
        registered_only=registered_only,
        limit=limit,
    )
    return [_incoming_document_to_out(row) for row in rows]


@router.post(
    "/inbound-documents/{document_id}/register",
    response_model=DiadocInboundDocumentRegisterResult,
    status_code=status.HTTP_200_OK,
)
async def register_diadoc_inbound_document(
    document_id: int,
    payload: DiadocInboundDocumentRegisterIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    document = await get_diadoc_incoming_document(
        session,
        document_id=document_id,
    )
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")
    try:
        if (
            payload.download_content_if_missing
            and not document.local_file_path
        ):
            integration, client = await get_diadoc_client_for_session(session)
            if normalize_diadoc_environment(
                integration.environment
            ) != normalize_diadoc_environment(document.environment):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Document environment does not match current "
                        "Diadoc integration environment"
                    ),
                )
            await ensure_diadoc_document_content(
                session,
                client=client,
                document=document,
            )
        message_row = await register_diadoc_document_as_supplier_message(
            session,
            document=document,
            provider_id=payload.provider_id,
            response_config_id=payload.response_config_id,
        )
        return DiadocInboundDocumentRegisterResult(
            document_id=int(document.id),
            provider_id=int(message_row.provider_id),
            supplier_order_message_id=int(message_row.id),
            response_config_id=message_row.response_config_id,
            detail=(
                "Диадок-документ зарегистрирован во входящих "
                "документах поставщика"
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.post(
    "/inbound-documents/{document_id}/process",
    response_model=DiadocInboundDocumentProcessResult,
    status_code=status.HTTP_200_OK,
)
async def process_diadoc_inbound_document_endpoint(
    document_id: int,
    payload: DiadocInboundDocumentProcessIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    document = await get_diadoc_incoming_document(
        session,
        document_id=document_id,
    )
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")
    try:
        client = None
        if (
            payload.download_content_if_missing
            and not document.local_file_path
        ):
            integration, client = await get_diadoc_client_for_session(session)
            if normalize_diadoc_environment(
                integration.environment
            ) != normalize_diadoc_environment(document.environment):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Document environment does not match current "
                        "Diadoc integration environment"
                    ),
                )
        result = await process_diadoc_incoming_document(
            session,
            document=document,
            provider_id=payload.provider_id,
            response_config_id=payload.response_config_id,
            download_content_if_missing=payload.download_content_if_missing,
            register_if_needed=payload.register_if_needed,
            client=client,
        )
        detail = (
            "Диадок-документ уже был обработан ранее"
            if result.get("already_processed")
            else "Диадок-документ обработан и передан в поступления"
        )
        return DiadocInboundDocumentProcessResult(
            document_id=int(document.id),
            provider_id=int(result.get("provider_id") or 0),
            supplier_order_message_id=int(result.get("message_id") or 0),
            response_config_id=(
                int(result["response_config_id"])
                if result.get("response_config_id") is not None
                else None
            ),
            receipt_ids=[
                int(receipt_id)
                for receipt_id in (result.get("receipt_ids") or [])
            ],
            already_processed=bool(result.get("already_processed")),
            processed_messages=int(result.get("processed_messages") or 0),
            parsed_response_files=int(
                result.get("parsed_response_files") or 0
            ),
            recognized_positions=int(result.get("recognized_positions") or 0),
            unresolved_positions=int(result.get("unresolved_positions") or 0),
            unresolved_examples=[
                str(item) for item in (result.get("unresolved_examples") or [])
            ],
            created_receipts=int(result.get("created_receipts") or 0),
            updated_receipts=int(result.get("updated_receipts") or 0),
            posted_receipts=int(result.get("posted_receipts") or 0),
            receipt_items_added=int(result.get("receipt_items_added") or 0),
            updated_items=int(result.get("updated_items") or 0),
            skipped_messages=int(result.get("skipped_messages") or 0),
            message_type=(
                str(result.get("message_type"))
                if result.get("message_type") is not None
                else None
            ),
            import_error_details=(
                str(result.get("import_error_details"))
                if result.get("import_error_details") is not None
                else None
            ),
            detail=detail,
        )
    except HTTPException:
        raise
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/outbound-documents",
    response_model=list[DiadocOutgoingDocumentOut],
    status_code=status.HTTP_200_OK,
)
async def list_diadoc_outbound_documents_endpoint(
    document_id: int | None = Query(default=None),
    customer_id: int | None = Query(default=None),
    provider_id: int | None = Query(default=None),
    source_type: str | None = Query(default=None),
    source_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    rows = await list_diadoc_outgoing_documents(
        session,
        document_id=document_id,
        customer_id=customer_id,
        provider_id=provider_id,
        source_type=source_type,
        source_id=source_id,
        limit=limit,
    )
    return [_outgoing_document_to_out(row) for row in rows]


@router.post(
    "/outbound-documents",
    response_model=DiadocOutgoingDocumentOut,
    status_code=status.HTTP_200_OK,
)
async def create_diadoc_outbound_document(
    payload: DiadocOutgoingDocumentCreateIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        from_box_id_guid = _resolve_box_id_guid(integration, None)
        to_box_id_guid = str(payload.to_box_id_guid or "").strip() or None
        if not to_box_id_guid and payload.customer_id is not None:
            to_box_id_guid = await resolve_diadoc_box_for_customer(
                session,
                customer_id=int(payload.customer_id),
            )
        if not to_box_id_guid and payload.provider_id is not None:
            to_box_id_guid = await resolve_diadoc_box_for_provider(
                session,
                provider_id=int(payload.provider_id),
            )
        if not to_box_id_guid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Recipient Diadoc box is not resolved. "
                    "Pass to_box_id_guid or bind provider/customer to "
                    "counteragent box."
                ),
            )
        outgoing = await post_diadoc_outgoing_document(
            session,
            client=client,
            environment=normalize_diadoc_environment(integration.environment),
            from_box_id_guid=from_box_id_guid,
            to_box_id_guid=to_box_id_guid,
            customer_id=payload.customer_id,
            provider_id=payload.provider_id,
            file_name=payload.file_name,
            content_base64=payload.content_base64,
            signature_base64=payload.signature_base64,
            comment=payload.comment,
            need_recipient_signature=payload.need_recipient_signature,
            need_receipt=payload.need_receipt,
            send_mode=payload.send_mode.value,
            type_named_id=payload.type_named_id,
            document_function=payload.document_function,
            document_version=payload.document_version,
            document_number=payload.document_number,
            document_date=payload.document_date,
            metadata=payload.metadata,
            source_type=payload.source_type,
            source_id=payload.source_id,
        )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return _outgoing_document_to_out(outgoing)
    except HTTPException:
        raise
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/outbound-readiness/shipment/{shipment_id}",
    response_model=DiadocShipmentFormalizedReadinessOut,
    status_code=status.HTTP_200_OK,
)
async def get_diadoc_outbound_readiness_for_shipment(
    shipment_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    try:
        result = await build_shipment_formalized_readiness(
            session,
            shipment_id=shipment_id,
            integration=integration,
        )
        return DiadocShipmentFormalizedReadinessOut(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/outbound-readiness/shipments",
    response_model=list[DiadocShipmentFormalizedReadinessOut],
    status_code=status.HTTP_200_OK,
)
async def get_diadoc_outbound_readiness_for_shipments(
    shipment_ids: list[int] = Query(default_factory=list),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    if not shipment_ids:
        return []
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    rows = await build_shipments_formalized_readiness(
        session,
        shipment_ids=shipment_ids,
        integration=integration,
    )
    return [DiadocShipmentFormalizedReadinessOut(**row) for row in rows]


@router.post(
    "/outbound-documents/from-shipment/{shipment_id}",
    response_model=DiadocOutgoingDocumentOut,
    status_code=status.HTTP_200_OK,
)
async def create_diadoc_outbound_document_from_shipment(
    shipment_id: int,
    payload: DiadocShipmentOutboundCreateIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        from_box_id_guid = _resolve_box_id_guid(integration, None)
        to_box_id_guid = str(payload.to_box_id_guid or "").strip() or None
        if (
            not to_box_id_guid
            and payload.document_format.value == "nonformalized"
        ):
            preview_payload = await build_diadoc_payload_from_shipment(
                session,
                shipment_id=shipment_id,
                customer_id=payload.customer_id,
            )
            to_box_id_guid = await resolve_diadoc_box_for_customer(
                session,
                customer_id=int(preview_payload["customer_id"]),
            )
        if payload.document_format.value == "formalized_utd":
            readiness = await build_shipment_formalized_readiness(
                session,
                shipment_id=shipment_id,
                integration=integration,
            )
            if not readiness["ready_formalized"]:
                detail = "; ".join(readiness["missing_required_fields"]) or (
                    "Shipment is not ready for formalized UTD"
                )
                raise HTTPException(status_code=400, detail=detail)
            shipment_payload = (
                await build_formalized_diadoc_payload_from_shipment(
                    session,
                    client=client,
                    integration=integration,
                    shipment_id=shipment_id,
                    customer_id=payload.customer_id,
                    to_box_id_guid=to_box_id_guid,
                )
            )
            to_box_id_guid = (
                str(shipment_payload.get("to_box_id_guid") or "").strip()
                or None
            )
        else:
            shipment_payload = await build_diadoc_payload_from_shipment(
                session,
                shipment_id=shipment_id,
                customer_id=payload.customer_id,
            )
            if not to_box_id_guid:
                to_box_id_guid = await resolve_diadoc_box_for_customer(
                    session,
                    customer_id=int(shipment_payload["customer_id"]),
                )
        if not to_box_id_guid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Recipient Diadoc box is not resolved for shipment "
                    "customer. Pass to_box_id_guid or bind customer to "
                    "counteragent box."
                ),
            )
        outgoing = await post_diadoc_outgoing_document(
            session,
            client=client,
            environment=normalize_diadoc_environment(integration.environment),
            from_box_id_guid=from_box_id_guid,
            to_box_id_guid=to_box_id_guid,
            customer_id=int(shipment_payload["customer_id"]),
            file_name=str(shipment_payload["file_name"]),
            content_base64=str(shipment_payload["content_base64"]),
            signature_base64=payload.signature_base64,
            comment=payload.comment,
            need_recipient_signature=payload.need_recipient_signature,
            need_receipt=payload.need_receipt,
            send_mode=payload.send_mode.value,
            type_named_id=(
                str(shipment_payload.get("type_named_id") or "").strip()
                or payload.type_named_id
            ),
            document_function=(shipment_payload.get("document_function")),
            document_version=(shipment_payload.get("document_version")),
            document_number=shipment_payload.get("document_number"),
            document_date=shipment_payload.get("document_date"),
            metadata=shipment_payload.get("metadata"),
            source_type=str(shipment_payload["source_type"]),
            source_id=int(shipment_payload["source_id"]),
        )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return _outgoing_document_to_out(outgoing)
    except HTTPException:
        raise
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/outbound-readiness/customer-return/{return_id}",
    response_model=DiadocReturnFormalizedReadinessOut,
    status_code=status.HTTP_200_OK,
)
async def get_diadoc_outbound_readiness_for_customer_return(
    return_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    try:
        result = await build_customer_return_formalized_readiness(
            session,
            return_id=return_id,
            integration=integration,
        )
        return DiadocReturnFormalizedReadinessOut(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/outbound-readiness/supplier-return/{return_id}",
    response_model=DiadocReturnFormalizedReadinessOut,
    status_code=status.HTTP_200_OK,
)
async def get_diadoc_outbound_readiness_for_supplier_return(
    return_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    try:
        result = await build_supplier_return_formalized_readiness(
            session,
            return_id=return_id,
            integration=integration,
        )
        return DiadocReturnFormalizedReadinessOut(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/outbound-documents/from-customer-return/{return_id}",
    response_model=DiadocOutgoingDocumentOut,
    status_code=status.HTTP_200_OK,
)
async def create_diadoc_outbound_document_from_customer_return(
    return_id: int,
    payload: DiadocReturnOutboundCreateIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        from_box_id_guid = _resolve_box_id_guid(integration, None)
        to_box_id_guid = str(payload.to_box_id_guid or "").strip() or None
        readiness = await build_customer_return_formalized_readiness(
            session,
            return_id=return_id,
            integration=integration,
        )
        if not readiness["ready_formalized"]:
            detail = "; ".join(readiness["missing_required_fields"]) or (
                "Customer return is not ready for formalized UKD"
            )
            raise HTTPException(status_code=400, detail=detail)
        return_payload = (
            await build_formalized_diadoc_payload_from_customer_return(
                session,
                client=client,
                integration=integration,
                return_id=return_id,
                customer_id=payload.customer_id,
                to_box_id_guid=to_box_id_guid,
            )
        )
        to_box_id_guid = (
            str(return_payload.get("to_box_id_guid") or "").strip() or None
        )
        if not to_box_id_guid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Recipient Diadoc box is not resolved for customer "
                    "return. Pass to_box_id_guid or bind customer to "
                    "counteragent box."
                ),
            )
        outgoing = await post_diadoc_outgoing_document(
            session,
            client=client,
            environment=normalize_diadoc_environment(integration.environment),
            from_box_id_guid=from_box_id_guid,
            to_box_id_guid=to_box_id_guid,
            customer_id=int(return_payload["customer_id"]),
            file_name=str(return_payload["file_name"]),
            content_base64=str(return_payload["content_base64"]),
            signature_base64=payload.signature_base64,
            comment=payload.comment,
            need_recipient_signature=payload.need_recipient_signature,
            need_receipt=payload.need_receipt,
            send_mode=payload.send_mode.value,
            type_named_id=str(return_payload["type_named_id"]),
            document_function=return_payload.get("document_function"),
            document_version=return_payload.get("document_version"),
            document_number=return_payload.get("document_number"),
            document_date=return_payload.get("document_date"),
            metadata=return_payload.get("metadata"),
            source_type=str(return_payload["source_type"]),
            source_id=int(return_payload["source_id"]),
        )
        return_document = await session.get(ReturnFromCustomer, return_id)
        if return_document is not None:
            return_document.diadoc_outgoing_document_id = outgoing.id
            session.add(return_document)
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return _outgoing_document_to_out(outgoing)
    except HTTPException:
        raise
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.post(
    "/outbound-documents/from-supplier-return/{return_id}",
    response_model=DiadocOutgoingDocumentOut,
    status_code=status.HTTP_200_OK,
)
async def create_diadoc_outbound_document_from_supplier_return(
    return_id: int,
    payload: DiadocReturnOutboundCreateIn,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        from_box_id_guid = _resolve_box_id_guid(integration, None)
        to_box_id_guid = str(payload.to_box_id_guid or "").strip() or None
        readiness = await build_supplier_return_formalized_readiness(
            session,
            return_id=return_id,
            integration=integration,
        )
        if not readiness["ready_formalized"]:
            detail = "; ".join(readiness["missing_required_fields"]) or (
                "Supplier return is not ready for formalized UKD"
            )
            raise HTTPException(status_code=400, detail=detail)
        return_payload = (
            await build_formalized_diadoc_payload_from_supplier_return(
                session,
                client=client,
                integration=integration,
                return_id=return_id,
                provider_id=payload.provider_id,
                to_box_id_guid=to_box_id_guid,
            )
        )
        to_box_id_guid = (
            str(return_payload.get("to_box_id_guid") or "").strip() or None
        )
        if not to_box_id_guid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Recipient Diadoc box is not resolved for supplier "
                    "return. Pass to_box_id_guid or bind provider to "
                    "counteragent box."
                ),
            )
        outgoing = await post_diadoc_outgoing_document(
            session,
            client=client,
            environment=normalize_diadoc_environment(integration.environment),
            from_box_id_guid=from_box_id_guid,
            to_box_id_guid=to_box_id_guid,
            provider_id=int(return_payload["provider_id"]),
            file_name=str(return_payload["file_name"]),
            content_base64=str(return_payload["content_base64"]),
            signature_base64=payload.signature_base64,
            comment=payload.comment,
            need_recipient_signature=payload.need_recipient_signature,
            need_receipt=payload.need_receipt,
            send_mode=payload.send_mode.value,
            type_named_id=str(return_payload["type_named_id"]),
            document_function=return_payload.get("document_function"),
            document_version=return_payload.get("document_version"),
            document_number=return_payload.get("document_number"),
            document_date=return_payload.get("document_date"),
            metadata=return_payload.get("metadata"),
            source_type=str(return_payload["source_type"]),
            source_id=int(return_payload["source_id"]),
        )
        return_document = await session.get(ReturnToSupplier, return_id)
        if return_document is not None:
            return_document.diadoc_outgoing_document_id = outgoing.id
            session.add(return_document)
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return _outgoing_document_to_out(outgoing)
    except HTTPException:
        raise
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/documents/{message_id}/{entity_id}",
    response_model=dict[str, Any],
    status_code=status.HTTP_200_OK,
)
async def get_diadoc_document(
    message_id: str,
    entity_id: str,
    box_id_guid: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        selected_box_id_guid = _resolve_box_id_guid(integration, box_id_guid)
        payload = await client.get_document(
            box_id_guid=selected_box_id_guid,
            message_id=message_id,
            entity_id=entity_id,
            inject_entity_content=False,
        )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        return payload
    except Exception as exc:
        _raise_diadoc_http_error(exc)


@router.get(
    "/documents/{message_id}/{entity_id}/content",
    status_code=status.HTTP_200_OK,
)
async def download_diadoc_document_content(
    message_id: str,
    entity_id: str,
    box_id_guid: str | None = Query(default=None),
    filename: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        integration, client = await get_diadoc_client_for_session(session)
        selected_box_id_guid = _resolve_box_id_guid(integration, box_id_guid)
        resolved_filename = filename
        if not resolved_filename:
            metadata = await client.get_document(
                box_id_guid=selected_box_id_guid,
                message_id=message_id,
                entity_id=entity_id,
                inject_entity_content=False,
            )
            resolved_filename = (
                metadata.get("FileName")
                or metadata.get("DocumentNumber")
                or f"{entity_id}.bin"
            )
        content = await client.get_entity_content(
            box_id_guid=selected_box_id_guid,
            message_id=message_id,
            entity_id=entity_id,
        )
        integration.last_sync_at = now_moscow()
        integration.last_error = None
        session.add(integration)
        await session.commit()
        quoted_name = quote(str(resolved_filename))
        return Response(
            content=content,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": (
                    f"attachment; filename*=UTF-8''{quoted_name}"
                )
            },
        )
    except Exception as exc:
        _raise_diadoc_http_error(exc)
