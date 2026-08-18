#!/usr/bin/env python3
"""Diagnostic client and safe 1C:UT 11 exchange simulator."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

CONTRACT_VERSION = "1.0"
PROTOCOL = "dz-1c-outbox"


class AdapterError(RuntimeError):
    pass


def _auth_header(login: str, password: str) -> str:
    token = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _request_json(
    url: str,
    *,
    login: str,
    password: str,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    body = None
    headers = {
        "Accept": "application/json",
        "Authorization": _auth_header(login, password),
    }
    method = "GET"
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
        method = "POST"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"HTTP {exc.code}: {details}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise AdapterError(str(exc)) from exc


def _validate_envelope(payload: dict[str, Any]) -> None:
    if payload.get("protocol") != PROTOCOL:
        raise AdapterError(f"Неизвестный протокол: {payload.get('protocol')!r}")
    if payload.get("contract_version") != CONTRACT_VERSION:
        raise AdapterError(
            "Несовместимая версия контракта: "
            f"{payload.get('contract_version')!r}; ожидается {CONTRACT_VERSION}"
        )
    events = payload.get("events")
    if events is not None and not isinstance(events, list):
        raise AdapterError("Поле events должно быть массивом")
    for event in events or []:
        required = {
            "event_uid",
            "entity_type",
            "entity_id",
            "event_type",
            "payload_version",
            "idempotency_key",
            "payload",
        }
        missing = sorted(required - set(event))
        if missing:
            raise AdapterError(
                f"Событие {event.get('event_uid')} не содержит: {', '.join(missing)}"
            )


def _load_state(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdapterError(f"Не удалось прочитать журнал {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AdapterError(f"Журнал {path} должен содержать JSON-объект")
    return {str(key): str(external_id) for key, external_id in value.items()}


def _save_state(path: Path, state: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _simulated_external_id(event: dict[str, Any]) -> str:
    entity = str(event["entity_type"]).upper().replace("_", "-")
    return f"SIM-{entity}-{event['entity_id']}-{event['event_uid'][:8]}"


def _base_url(value: str) -> str:
    return value.rstrip("/")


def _print_package(payload: dict[str, Any]) -> None:
    events = payload.get("events") or []
    print(
        f"Пакет: {payload.get('batch_uid') or 'нет'}; "
        f"событий: {len(events)}; попытка: {payload.get('attempt', 0)}"
    )
    for event in events:
        document = event.get("payload") or {}
        number = document.get("document_number") or document.get("number") or "без номера"
        print(
            f"  {event['event_uid']} | {event['entity_type']}:{event['entity_id']} | "
            f"{event['event_type']} | {number}"
        )


def run(args: argparse.Namespace) -> int:
    if not args.login or not args.password:
        raise AdapterError(
            "Укажите --login/--password или переменные "
            "ONE_C_EXCHANGE_LOGIN/ONE_C_EXCHANGE_PASSWORD"
        )
    base_url = _base_url(args.base_url)
    if args.mode == "ping":
        response = _request_json(
            f"{base_url}/1c/outbox/v1/ping",
            login=args.login,
            password=args.password,
        )
        _validate_envelope(response)
        print(
            f"Подключение успешно: {response['protocol']} v{response['contract_version']}; "
            f"сервер {response['server_time']}"
        )
        return 0

    package = _request_json(
        f"{base_url}/1c/outbox/v1/query?limit={args.limit}",
        login=args.login,
        password=args.password,
    )
    _validate_envelope(package)
    _print_package(package)
    if args.mode == "pull" or not package.get("batch_uid"):
        return 0

    state = _load_state(args.state_file)
    results = []
    staged_state = dict(state)
    for event in package["events"]:
        event_uid = event["event_uid"]
        if args.fail_event_uid == event_uid:
            results.append(
                {
                    "event_uid": event_uid,
                    "success": False,
                    "error": "Тестовая ошибка адаптера 1С",
                }
            )
            continue
        key = event["idempotency_key"]
        external_id = state.get(key) or _simulated_external_id(event)
        staged_state[key] = external_id
        results.append(
            {
                "event_uid": event_uid,
                "success": True,
                "external_id": external_id,
            }
        )

    response = _request_json(
        f"{base_url}/1c/outbox/v1/{package['batch_uid']}/ack",
        login=args.login,
        password=args.password,
        payload={"contract_version": CONTRACT_VERSION, "results": results},
    )
    _save_state(args.state_file, staged_state)
    print(
        f"Подтверждение отправлено: пакет {response['batch_uid']}, " f"статус {response['status']}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.getenv("DZ_API_BASE_URL", "http://localhost:8000"),
        help="Адрес API без завершающего слеша",
    )
    parser.add_argument("--login", default=os.getenv("ONE_C_EXCHANGE_LOGIN"))
    parser.add_argument("--password", default=os.getenv("ONE_C_EXCHANGE_PASSWORD"))
    parser.add_argument("--mode", choices=("ping", "pull", "process"), default="ping")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path(".one_c_ut_adapter_state.json"),
        help="Локальный журнал идемпотентности симулятора",
    )
    parser.add_argument(
        "--fail-event-uid",
        help="В режиме process вернуть тестовую ошибку для выбранного события",
    )
    return parser


def main() -> int:
    try:
        return run(build_parser().parse_args())
    except AdapterError as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
