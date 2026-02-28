import base64
import hashlib
import hmac
import os
import time
from typing import Optional
from urllib.parse import urlencode

import httpx

from dz_fastapi.core.config import settings

GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth'
GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
GOOGLE_GMAIL_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly'


def _get_google_oauth_config() -> tuple[str, str, str]:
    client_id = os.getenv('GOOGLE_OAUTH_CLIENT_ID')
    client_secret = os.getenv('GOOGLE_OAUTH_CLIENT_SECRET')
    redirect_uri = os.getenv('GOOGLE_OAUTH_REDIRECT_URI')
    if not client_id or not client_secret or not redirect_uri:
        raise RuntimeError(
            'Google OAuth env not configured. '
            'Set GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, '
            'GOOGLE_OAUTH_REDIRECT_URI.'
        )
    return client_id, client_secret, redirect_uri


def _sign_state(value: str) -> str:
    secret = settings.jwt_secret.encode('utf-8')
    digest = hmac.new(secret, value.encode('utf-8'), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode('utf-8').rstrip('=')


def build_oauth_state(account_id: int) -> str:
    timestamp = int(time.time())
    payload = f'{account_id}:{timestamp}'
    signature = _sign_state(payload)
    return f'{payload}:{signature}'


def parse_oauth_state(state: str) -> Optional[int]:
    try:
        account_id_str, timestamp_str, signature = state.split(':', 2)
        payload = f'{account_id_str}:{timestamp_str}'
    except ValueError:
        return None
    expected = _sign_state(payload)
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        return int(account_id_str)
    except ValueError:
        return None


def build_google_auth_url(account_id: int) -> str:
    client_id, _client_secret, redirect_uri = _get_google_oauth_config()
    state = build_oauth_state(account_id)
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': GOOGLE_GMAIL_SCOPE,
        'access_type': 'offline',
        'prompt': 'consent',
        'include_granted_scopes': 'true',
        'state': state,
    }
    return f'{GOOGLE_AUTH_URL}?{urlencode(params)}'


async def exchange_code_for_tokens(code: str) -> dict:
    client_id, client_secret, redirect_uri = _get_google_oauth_config()
    payload = {
        'code': code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
        'grant_type': 'authorization_code',
    }
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(GOOGLE_TOKEN_URL, data=payload)
        response.raise_for_status()
        return response.json()


async def refresh_google_access_token(refresh_token: str) -> dict:
    client_id, client_secret, _redirect_uri = _get_google_oauth_config()
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token',
    }
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(GOOGLE_TOKEN_URL, data=payload)
        response.raise_for_status()
        return response.json()


async def test_google_gmail_access(refresh_token: str) -> None:
    token_data = await refresh_google_access_token(refresh_token)
    access_token = token_data.get('access_token')
    if not access_token:
        raise RuntimeError('Google OAuth access token not returned')
    headers = {'Authorization': f'Bearer {access_token}'}
    url = 'https://gmail.googleapis.com/gmail/v1/users/me/profile'
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
