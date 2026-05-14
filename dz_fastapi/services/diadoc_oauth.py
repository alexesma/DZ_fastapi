import base64
import hashlib
import hmac
import secrets
import time
from urllib.parse import urlencode

import httpx

from dz_fastapi.core.config import settings

DIADOC_ENVIRONMENTS = {"staging", "prod"}
DIADOC_SCOPE_BY_ENVIRONMENT = {
    "staging": "Diadoc.PublicAPI.Staging",
    "prod": "Diadoc.PublicAPI",
}
DIADOC_STATE_TTL_SECONDS = 60 * 15


def normalize_diadoc_environment(environment: str | None) -> str:
    value = (
        str(environment or settings.diadoc_default_environment or "staging")
        .strip()
        .lower()
    )
    if value not in DIADOC_ENVIRONMENTS:
        return "staging"
    return value


def _get_diadoc_oauth_config() -> tuple[str, str, str]:
    client_id = settings.diadoc_client_id
    client_secret = settings.diadoc_client_secret
    redirect_uri = settings.diadoc_oauth_redirect_uri
    if not client_id or not client_secret or not redirect_uri:
        raise RuntimeError(
            "Diadoc OAuth env not configured. "
            "Set DIADOC_CLIENT_ID, DIADOC_CLIENT_SECRET "
            "and DIADOC_OAUTH_REDIRECT_URI."
        )
    return client_id, client_secret, redirect_uri


def _sign_state(value: str) -> str:
    secret = settings.jwt_secret.encode("utf-8")
    digest = hmac.new(secret, value.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


def build_diadoc_oauth_state(environment: str) -> str:
    env = normalize_diadoc_environment(environment)
    timestamp = int(time.time())
    nonce = secrets.token_urlsafe(12)
    payload = f"{env}:{timestamp}:{nonce}"
    signature = _sign_state(payload)
    return f"{payload}:{signature}"


def parse_diadoc_oauth_state(state: str) -> str | None:
    try:
        environment, timestamp_str, nonce, signature = state.split(":", 3)
        payload = f"{environment}:{timestamp_str}:{nonce}"
    except ValueError:
        return None
    expected = _sign_state(payload)
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        return None
    if time.time() - timestamp > DIADOC_STATE_TTL_SECONDS:
        return None
    return normalize_diadoc_environment(environment)


def build_diadoc_auth_url(environment: str) -> str:
    client_id, _client_secret, redirect_uri = _get_diadoc_oauth_config()
    env = normalize_diadoc_environment(environment)
    state = build_diadoc_oauth_state(env)
    scope = " ".join(
        [
            "openid",
            "profile",
            "email",
            "offline_access",
            DIADOC_SCOPE_BY_ENVIRONMENT[env],
        ]
    )
    params = {
        "response_type": "code",
        "client_id": client_id,
        "scope": scope,
        "redirect_uri": redirect_uri,
        "state": state,
        "nonce": secrets.token_urlsafe(16),
    }
    base_url = settings.diadoc_oidc_base_url.rstrip("/")
    return f"{base_url}/connect/authorize?{urlencode(params)}"


async def _post_token(payload: dict[str, str]) -> dict:
    base_url = settings.diadoc_oidc_base_url.rstrip("/")
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            f"{base_url}/connect/token",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise RuntimeError(
            f"Diadoc token request failed: " f"{response.status_code} {detail}"
        )
    return response.json()


async def exchange_diadoc_code_for_tokens(code: str) -> dict:
    client_id, client_secret, redirect_uri = _get_diadoc_oauth_config()
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }
    return await _post_token(payload)


async def refresh_diadoc_access_token(refresh_token: str) -> dict:
    client_id, client_secret, _redirect_uri = _get_diadoc_oauth_config()
    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }
    return await _post_token(payload)
