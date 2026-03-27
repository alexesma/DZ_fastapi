#!/usr/bin/env python3
"""One-time local helper to get a Google Gmail refresh token.

Usage:
    python scripts/google_gmail_refresh_token.py \\
        --client-id YOUR_CLIENT_ID \\
        --client-secret YOUR_CLIENT_SECRET

The script starts a local HTTP listener on 127.0.0.1 and opens the
Google consent page in the browser. After login it prints the
refresh_token. Paste that token into the Email Accounts page.
"""

from __future__ import annotations

import argparse
import json
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth'
GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', required=True)
    parser.add_argument('--client-secret', required=True)
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument(
        '--scope',
        action='append',
        dest='scopes',
        default=None,
        help='Optional Google scope. May be passed multiple times.',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    scopes = args.scopes or DEFAULT_SCOPES
    redirect_uri = f'http://127.0.0.1:{args.port}/callback'
    result = {'code': None, 'error': None}
    done = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            code = params.get('code', [None])[0]
            error = params.get('error', [None])[0]
            result['code'] = code
            result['error'] = error
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            if error:
                body = f'<h3>OAuth error</h3><p>{error}</p>'
            else:
                body = (
                    '<h3>Google refresh token captured</h3>'
                    '<p>You can close this window '
                    'and return to the terminal.</p>'
                )
            self.wfile.write(body.encode('utf-8'))
            done.set()

        def log_message(self, fmt, *args):
            return

    server = HTTPServer(('127.0.0.1', args.port), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    params = {
        'client_id': args.client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': ' '.join(scopes),
        'access_type': 'offline',
        'prompt': 'consent',
        'include_granted_scopes': 'true',
    }
    auth_url = f'{GOOGLE_AUTH_URL}?{urlencode(params)}'
    print('Open this URL if the browser does not open automatically:\n')
    print(auth_url)
    print('\nWaiting for Google callback on', redirect_uri)
    webbrowser.open(auth_url)
    done.wait()
    server.shutdown()
    server.server_close()

    if result['error']:
        raise SystemExit(f'Google OAuth error: {result["error"]}')
    if not result['code']:
        raise SystemExit('Authorization code not received.')

    payload = {
        'code': result['code'],
        'client_id': args.client_id,
        'client_secret': args.client_secret,
        'redirect_uri': redirect_uri,
        'grant_type': 'authorization_code',
    }
    response = httpx.post(GOOGLE_TOKEN_URL, data=payload, timeout=20)
    response.raise_for_status()
    token_data = response.json()
    refresh_token = token_data.get('refresh_token')
    if not refresh_token:
        raise SystemExit(
            'Refresh token not returned. Remove previous access for this '
            'OAuth app in Google Account permissions and try again.'
        )

    print('\nRefresh token:\n')
    print(refresh_token)
    print('\nFull token response:\n')
    print(json.dumps(token_data, ensure_ascii=True, indent=2))


if __name__ == '__main__':
    main()
