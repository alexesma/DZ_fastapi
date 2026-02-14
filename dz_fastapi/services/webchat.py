import os

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.webchat import ChatMessage

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_TOKEN_MESSAGE')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_WIDGET_SECRET: str | None = os.getenv('TELEGRAM_WIDGET_SECRET')
TELEGRAM_WIDGET_ENABLED: bool = (
    os.getenv('TELEGRAM_WIDGET_ENABLED', '1') == '1'
)


class TelegramSendError(RuntimeError):
    pass


async def send_telegram_message(
    text: str,
    reply_markup: dict = None,
) -> None:
    """Senden Nachricht in Telegram und zurückgeben message_id"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise TelegramSendError('Telegram token/chat_id not')

    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
    }
    if reply_markup:
        payload['reply_markup'] = reply_markup

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise TelegramSendError(
                f'Telegram API error: {r.status_code} {r.text}'
            )
        return r.json()


async def save_client_message(
    session: AsyncSession,
    session_id: str,
    client_name: str | None,
    phone_nummer: str | None,
    page: str | None,
    message: str,
    telegram_message_id: int | None = None,
) -> ChatMessage:
    """Wir speichern die Nachricht von Kunden"""
    msg = ChatMessage(
        session_id=session_id,
        client_name=client_name,
        client_phone=phone_nummer,
        page_url=page,
        message_text=message,
        telegram_message_id=telegram_message_id,
    )
    session.add(msg)
    await session.commit()
    await session.refresh(msg)
    return msg


async def save_operator_message(
    session: AsyncSession,
    session_id: str,
    message: str,
) -> ChatMessage:
    """Wir speichern die Antworten von Operator"""
    msg = ChatMessage(
        session_id=session_id,
        message_text=message,
        is_from_client=False,
    )
    session.add(msg)
    await session.commit()
    await session.refresh(msg)
    return msg


async def get_chat_history(
    session: AsyncSession,
    session_id: str,
    limit: int = 50,
) -> list[ChatMessage]:
    """Chatverlauf abrufen"""
    result = await session.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.desc())
        .limit(limit)
    )
    messages = result.scalars().all()
    return list(reversed(messages))
