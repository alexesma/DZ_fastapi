from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.settings import crud_diadoc_integration_settings
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.diadoc_oauth import (
    normalize_diadoc_environment,
    refresh_diadoc_access_token,
)


class DiadocNotConnectedError(RuntimeError):
    pass


def _expires_soon(
    integration: DiadocIntegrationSettings,
    *,
    safety_seconds: int = 120,
) -> bool:
    expires_at = integration.access_token_expires_at
    if not integration.access_token or expires_at is None:
        return True
    return expires_at <= now_moscow() + timedelta(seconds=safety_seconds)


def _build_connected_user_name(payload: dict | None) -> str | None:
    if not payload:
        return None
    parts = [
        str(payload.get("LastName") or "").strip(),
        str(payload.get("FirstName") or "").strip(),
        str(payload.get("MiddleName") or "").strip(),
    ]
    value = " ".join(part for part in parts if part)
    return value or None


def _apply_single_box_selection(
    integration: DiadocIntegrationSettings,
    organizations_payload: dict | None,
) -> None:
    organizations = list(
        (organizations_payload or {}).get("Organizations") or []
    )
    if len(organizations) != 1:
        return
    organization = organizations[0]
    boxes = list(organization.get("Boxes") or [])
    if len(boxes) != 1:
        return
    box = boxes[0]
    integration.organization_id = organization.get(
        "OrgIdGuid"
    ) or organization.get("OrgId")
    integration.organization_name = organization.get(
        "ShortName"
    ) or organization.get("FullName")
    integration.organization_inn = organization.get("Inn")
    integration.organization_kpp = organization.get("Kpp")
    integration.box_id = box.get("BoxId")
    integration.box_id_guid = box.get("BoxIdGuid")


def apply_diadoc_tokens(
    integration: DiadocIntegrationSettings,
    tokens: dict,
    *,
    connected: bool = False,
) -> None:
    access_token = str(tokens.get("access_token") or "").strip()
    refresh_token = str(tokens.get("refresh_token") or "").strip()
    token_type = str(tokens.get("token_type") or "").strip()
    token_scope = str(tokens.get("scope") or "").strip()
    expires_in = tokens.get("expires_in")
    if access_token:
        integration.access_token = access_token
    if refresh_token:
        integration.refresh_token = refresh_token
    if token_type:
        integration.token_type = token_type
    if token_scope:
        integration.token_scope = token_scope
    if expires_in is not None:
        integration.access_token_expires_at = now_moscow() + timedelta(
            seconds=int(expires_in)
        )
    if connected:
        integration.connected_at = now_moscow()
    integration.last_error = None


async def save_diadoc_authorization(
    session: AsyncSession,
    integration: DiadocIntegrationSettings,
    *,
    tokens: dict,
    environment: str,
    user_payload: dict | None = None,
    organizations_payload: dict | None = None,
) -> DiadocIntegrationSettings:
    if not tokens.get("refresh_token") and not integration.refresh_token:
        raise RuntimeError(
            "Diadoc refresh token not returned and no previous token exists."
        )
    integration.environment = normalize_diadoc_environment(environment)
    apply_diadoc_tokens(integration, tokens, connected=True)
    integration.connected_user_id = (user_payload or {}).get("Id")
    integration.connected_user_name = _build_connected_user_name(user_payload)
    _apply_single_box_selection(integration, organizations_payload)
    session.add(integration)
    await session.commit()
    await session.refresh(integration)
    return integration


async def ensure_diadoc_access_token(
    session: AsyncSession,
    integration: DiadocIntegrationSettings,
) -> str:
    if not integration.refresh_token:
        raise DiadocNotConnectedError("Diadoc integration is not connected")
    if not _expires_soon(integration):
        return str(integration.access_token)
    tokens = await refresh_diadoc_access_token(integration.refresh_token)
    apply_diadoc_tokens(integration, tokens, connected=False)
    session.add(integration)
    await session.commit()
    await session.refresh(integration)
    return str(integration.access_token)


async def get_diadoc_client_for_session(
    session: AsyncSession,
) -> tuple[DiadocIntegrationSettings, DiadocClient]:
    integration = await crud_diadoc_integration_settings.get_or_create(session)
    access_token = await ensure_diadoc_access_token(session, integration)
    return integration, DiadocClient(access_token=access_token)
