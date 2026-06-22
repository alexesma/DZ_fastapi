import os

import aiohttp
from aiohttp import ClientTimeout

TELEGRAM_REQUEST_TIMEOUT = ClientTimeout(total=30)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_TO")
# Прокси для обхода блокировки Telegram в РФ. Поддерживает http(s):// и
# socks5:// (для socks нужен aiohttp-socks). Пусто — без прокси.
TELEGRAM_PROXY_URL = (os.getenv("TELEGRAM_PROXY_URL") or "").strip() or None
# Базовый адрес Bot API. Можно указать свой relay (reverse-proxy на VPS вне
# РФ, который проксирует https://api.telegram.org) — самый надёжный путь для
# сервера. Пример: https://tg-relay.example.com
TELEGRAM_API_BASE = (
    (os.getenv("TELEGRAM_API_BASE") or "https://api.telegram.org").strip()
    .rstrip("/")
)
TELEGRAM_DOC_URL = (
    f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
)
TELEGRAM_MSG_URL = (
    f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
)


async def send_file_to_telegram(
    chat_id: str, file_bytes: bytes, file_name: str, caption: str = ""
):
    """
    Отправляет Excel-файл в Telegram в виде документа.

    :param chat_id: ID чата или username (например, '@channel')
    :param file_bytes: байты файла
    :param filename: имя файла (например, 'report.xlsx')
    :param caption: сообщение к файлу
    """
    if not TELEGRAM_BOT_TOKEN:
        raise Exception("TELEGRAM_TOKEN is not configured")
    data = aiohttp.FormData()
    data.add_field("chat_id", chat_id)
    data.add_field("caption", caption)
    data.add_field(
        "document",
        file_bytes,
        filename=file_name,
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    async with aiohttp.ClientSession(
        timeout=TELEGRAM_REQUEST_TIMEOUT
    ) as session:
        async with session.post(
            TELEGRAM_DOC_URL, data=data, proxy=TELEGRAM_PROXY_URL
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(
                    f"Ошибка отправки в Telegram: "
                    f"{response.status} — {error_text}"
                )


async def send_message_to_telegram(
    text: str,
    chat_id: str | None = None,
    parse_mode: str | None = None,
):
    if not TELEGRAM_BOT_TOKEN:
        raise Exception("TELEGRAM_TOKEN is not configured")
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        raise Exception("TELEGRAM_TO is not configured")
    payload = {
        "chat_id": chat_id,
        "text": text,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    async with aiohttp.ClientSession(
        timeout=TELEGRAM_REQUEST_TIMEOUT
    ) as session:
        async with session.post(
            TELEGRAM_MSG_URL, data=payload, proxy=TELEGRAM_PROXY_URL
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(
                    f"Ошибка отправки в Telegram: "
                    f"{response.status} — {error_text}"
                )
