import os
import re

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from dz_fastapi.core.db import get_async_session
from dz_fastapi.services.webchat import save_operator_message

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_TOKEN_MESSAGE')


async def reply_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    """Обработчик команды /reply для ответов оператора"""
    if not update.message or not update.message.text:
        return

    # Парсим: /reply SESSION_ID текст ответа
    match = re.match(
        r'/reply\s+(\S+)\s+(.+)',
        update.message.text,
        re.DOTALL,
    )

    if not match:
        await update.message.reply_text(
            '❌ Неверный формат!\n'
            'Используйте: /reply SESSION_ID ваш текст'
        )
        return

    session_id = match.group(1)
    reply_text = match.group(2).strip()

    # Сохраняем ответ в БД
    async for db_session in get_async_session():
        await save_operator_message(
            session=db_session,
            session_id=session_id,
            message=reply_text,
        )
        break

    await update.message.reply_text(
        f'✅ Ответ отправлен клиенту!\n'
        f'Сессия: {session_id}'
    )


def start_telegram_bot():
    """Запуск Telegram бота"""
    if not TELEGRAM_BOT_TOKEN:
        return None

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler('reply', reply_command))

    # Запускаем polling в фоне
    application.run_polling(allowed_updates=Update.ALL_TYPES)

    return application
