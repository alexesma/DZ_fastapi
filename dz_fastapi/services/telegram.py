import os

import aiohttp

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_TO')
TELEGRAM_DOC_URL = (
    f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument'
)
TELEGRAM_MSG_URL = (
    f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
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
        raise Exception('TELEGRAM_TOKEN is not configured')
    data = aiohttp.FormData()
    data.add_field('chat_id', chat_id)
    data.add_field('caption', caption)
    data.add_field(
        'document',
        file_bytes,
        filename=file_name,
        content_type=(
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ),
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(TELEGRAM_DOC_URL, data=data) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(
                    f'Ошибка отправки в Telegram: '
                    f'{response.status} — {error_text}'
                )


async def send_message_to_telegram(text: str, chat_id: str | None = None):
    if not TELEGRAM_BOT_TOKEN:
        raise Exception('TELEGRAM_TOKEN is not configured')
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        raise Exception('TELEGRAM_TO is not configured')
    payload = {
        'chat_id': chat_id,
        'text': text,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(TELEGRAM_MSG_URL, data=payload) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(
                    f'Ошибка отправки в Telegram: '
                    f'{response.status} — {error_text}'
                )
