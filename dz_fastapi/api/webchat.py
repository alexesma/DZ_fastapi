from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_async_session
from dz_fastapi.schemas.webchat import (ChatHistoryResponse,
                                        ChatMessageResponse, SiteChatMessageIn,
                                        SiteChatMessageOut)
from dz_fastapi.services.webchat import (TELEGRAM_WIDGET_ENABLED,
                                         TELEGRAM_WIDGET_SECRET,
                                         TelegramSendError, get_chat_history,
                                         save_client_message,
                                         send_telegram_message)

router = APIRouter(prefix="/site-chat", tags=['site-chat'])


@router.post('/message', response_model=SiteChatMessageOut)
async def send_site_chat_message(
    payload: SiteChatMessageIn,
    session: AsyncSession = Depends(get_async_session),
    x_widget_secret: str | None = Header(
        default=None, alias='X-Widget-Secret'
    ),
):
    if not TELEGRAM_WIDGET_ENABLED:
        raise HTTPException(status_code=404, detail='Widget disabled')
    if not TELEGRAM_WIDGET_SECRET:
        raise HTTPException(
            status_code=500,
            detail='Widget secret not configured',
        )
    if x_widget_secret != TELEGRAM_WIDGET_SECRET:
        raise HTTPException(status_code=401, detail='Unauthorized')

    name = (payload.name or 'Гость').strip()
    contact = (payload.contact or '').strip()
    page = (payload.page or '').strip()
    message = payload.message.strip()
    session_id = payload.session_id.strip()

    text = (
        f'<b>💬 Новое сообщение с сайта</b>\n'
        f'🆔 Сессия: <code>{session_id}</code>\n'
        f'👤 {name}\n'
        f'📞 {contact if contact else "—"}\n'
        f'🌐 {page if page else "—"}\n\n'
        f'<b>Сообщение:</b>\n{message}\n\n'
        f'<i>Ответьте в формате:</i>\n'
        f'<code>/reply {session_id} ваш текст</code>'
    )

    try:
        tg_response = await send_telegram_message(text)
        telegra_message_id = tg_response.get('result', {}).get('message_id')

        saved_msg = await save_client_message(
            session=session,
            session_id=session_id,
            client_name=name,
            phone_nummer=contact,
            page=page,
            message=message,
            telegram_message_id=telegra_message_id,
        )
    except TelegramSendError as e:
        raise HTTPException(status_code=502, detail=str(e))
    return SiteChatMessageOut(ok=True, message_id=saved_msg.id)


@router.get('/history/{session_id}', response_model=ChatHistoryResponse)
async def get_chat_history_endpoint(
    session_id: str,
    session: AsyncSession = Depends(get_async_session),
    x_widget_secret: str | None = Header(
        default=None, alias='X-Widget-Secret'
    ),
):
    """Nehmen Gesichte"""
    if not TELEGRAM_WIDGET_ENABLED:
        raise HTTPException(status_code=404, detail='Widget disabled')

    if x_widget_secret != TELEGRAM_WIDGET_SECRET:
        raise HTTPException(status_code=401, detail='Unauthorized')

    messages = await get_chat_history(session, session_id)

    return ChatHistoryResponse(
        messages=[ChatMessageResponse.from_orm(msg) for msg in messages]
    )
