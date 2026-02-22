import asyncio
import logging
import os
import re
import smtplib
import unicodedata
from datetime import date, timedelta
from email.message import EmailMessage
from io import BytesIO
from typing import Optional

import aiofiles
import httpx
import pandas as pd
from fastapi import HTTPException
from imap_tools import AND, MailBox
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import DEPTH_DAY_EMAIL, IMAP_SERVER
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.crud.partner import (crud_provider,
                                     crud_provider_pricelist_config,
                                     get_last_uid, set_last_uid)
from dz_fastapi.models.partner import Order, Provider, ProviderPriceListConfig
from dz_fastapi.services.utils import normalize_str

logger = logging.getLogger('dz_fastapi')

# Email account credentials
EMAIL_NAME = os.getenv('EMAIL_NAME_PRICE')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD_PRICE')
EMAIL_HOST = os.getenv('EMAIL_HOST_PRICE')

SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 465))
SMTP_USERNAME = os.getenv('EMAIL_NAME')
SMTP_PASSWORD = os.getenv('EMAIL_PASSWORD')

DOWNLOAD_FOLDER = 'uploads/pricelistprovider'
PROCESSED_FOLDER = 'processed'


def _extract_email(value: Optional[str]) -> str:
    if not value:
        return ''
    match = re.search(r'[\\w\\.-]+@[\\w\\.-]+\\.[\\w]+', value)
    return match.group(0).lower() if match else value.lower()


# os.makedirs(PROCESSED_FOLDER, exist_ok=True)


def safe_filename(filename: str) -> str:
    # Normalize Unicode characters
    value = (
        unicodedata.normalize('NFKD', filename)
        .encode('ascii', 'ignore')
        .decode('ascii')
    )
    # Remove any remaining non-alphanumeric
    # characters except dot and underscore
    value = re.sub(r'[^\w\s\.-]', '', value)
    # Replace spaces with underscores
    value = re.sub(r'\s+', '_', value).strip()
    return value


async def download_price_provider(
    provider: Provider,
    provider_conf: ProviderPriceListConfig,
    session: AsyncSession,
    max_emails: int = 50,
    server_mail: str = EMAIL_HOST,
    email_account: str = EMAIL_NAME,
    email_password: str = EMAIL_PASSWORD,
):
    """
    Загружает данные провайдера из почты.

    Args:
        provider (Provider): Поставщик.
        provider_conf (ProviderPriceListConfig): Конфигурация поставщика.
        session (AsyncSession): Сессия для взаимодействия с базой данных.
        max_emails (int): Максимальное количество писем для обработки.
        server_mail (str): Адрес IMAP-сервера.
        email_account (str): Учетная запись электронной почты.
        email_password (str): Пароль от учетной записи.

    Raises:
        HTTPException: Если провайдер не найден.
    """
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
        logger.info(f'Created directory: {DOWNLOAD_FOLDER}')

    try:
        since_date = date.today() - timedelta(days=DEPTH_DAY_EMAIL)
        logger.debug(
            f'Email criteria: from = {provider.email_incoming_price}, '
            f'need name_mail = {provider_conf.name_mail}, '
            f'need name_price = {provider_conf.name_price}'
        )
        last_uid = await get_last_uid(provider.id, session)
        logger.debug(f'Last UID: {last_uid}')

        with MailBox(server_mail, IMAP_SERVER).login(
            email_account, email_password
        ) as mailbox:
            mailbox.folder.set('INBOX')
            all_emails = list(
                mailbox.fetch(AND(date_gte=date.today(), all=True))
            )
            for msg in all_emails:
                logger.debug(
                    f'Uid: {msg.uid}, from: {msg.from_}, '
                    f'date: {msg.date}, subject: {msg.subject}, all: {msg}'
                )
            criteria_kwargs = {'date_gte': since_date}
            if provider.email_incoming_price:
                criteria_kwargs['from_'] = provider.email_incoming_price
            if provider_conf.name_mail:
                criteria_kwargs['subject'] = provider_conf.name_mail
            criteria = AND(**criteria_kwargs)
            logger.debug(f'Using criteria: {criteria}')

            email_list = list(
                mailbox.fetch(
                    criteria,
                    charset='utf-8',
                    # limit=max_emails
                )
            )
            logger.debug(f'Found {len(email_list)} emails matching criteria.')
            emails = [msg for msg in email_list if int(msg.uid) > last_uid]
            logger.debug(
                f'{len(emails)} emails have UID greater than {last_uid}.'
            )

            for msg in emails:
                subject = msg.subject
                logger.debug('All headers:')
                for k, v in msg.headers.items():
                    logger.debug(f'{k}: {v}')
                raw_subject = msg.obj.get('Subject')
                if raw_subject is None:
                    logger.debug('No Subject found for this email.')

                # logger.debug(f'all data {msg.__dict__}')
                # logger.debug(f'Subject: {msg.subject},
                # From: {msg.from_}, To: {msg.to}, Date: {msg.date}')
                # logger.debug(f'Processing email with subject: {subject}')
                if (
                    provider_conf.name_mail
                    and provider_conf.name_mail.lower() not in subject.lower()
                ):
                    logger.debug(
                        f'Subject {subject} does not '
                        f'contain {provider_conf.name_mail.lower()}, skipping.'
                    )
                    continue

                for att in msg.attachments:
                    logger.debug(f'Found attachment: {att.filename}')
                    filename_norm = normalize_str(att.filename).lower()
                    name_price_norm = normalize_str(
                        provider_conf.name_price or ''
                    ).lower()
                    if (
                        name_price_norm
                        and (
                            name_price_norm in filename_norm
                            or filename_norm in name_price_norm
                        )
                    ):
                        filepath = os.path.join(DOWNLOAD_FOLDER, att.filename)
                        with open(filepath, 'wb') as f:
                            f.write(att.payload)
                        logger.debug(f'Downloaded attachment: {filepath}')
                        mailbox.flag(msg.uid, [r'\Seen'], True)
                        current_uid = int(msg.uid)
                        if current_uid > last_uid:
                            await set_last_uid(
                                provider.id, current_uid, session
                            )
                        return filepath
                    if not name_price_norm:
                        logger.debug(
                            'name_price is empty, skipping attachment match.'
                        )
            logger.debug('No matching attachments found.')
            # if emails:
            #     max_uid = max(int(msg.uid) for msg in emails)
            #     if max_uid > last_uid:
            #         await set_last_uid(provider.id, max_uid, session)
            return None
    except ValueError as e:
        logger.error(f'Ошибка обработки писем: {e}')
        raise HTTPException(status_code=400, detail='Invalid email data')
    except Exception as e:
        logger.exception(f'Unexpected error while processing emails: {e}')
        raise HTTPException(
            status_code=500, detail='Error fetching provider emails'
        )


def send_email_with_attachment(
    to_email,
    subject,
    body,
    attachment_bytes,
    attachment_filename,
    is_html: bool = False,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    from_email: str | None = None,
    use_ssl: bool = True,
):
    logger.debug(
        'Inside send_email_with_attachment with len(attachment_bytes)=%d',
        len(attachment_bytes),
    )

    smtp_user = smtp_user or EMAIL_NAME
    smtp_password = smtp_password or EMAIL_PASSWORD
    smtp_host = smtp_host or SMTP_SERVER
    smtp_port = smtp_port or SMTP_PORT
    from_email = from_email or EMAIL_NAME

    if not smtp_user or not smtp_password or not smtp_host:
        logger.error('Email credentials are not set.')
        return

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    if is_html:
        msg.add_alternative(body, subtype='html')
    else:
        msg.set_content(body)

    # Add the attachment
    msg.add_attachment(
        attachment_bytes,
        maintype='application',
        subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        filename=attachment_filename,
    )

    try:
        if use_ssl:
            smtp_ctx = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            smtp_ctx = smtplib.SMTP(smtp_host, smtp_port)
        with smtp_ctx as smtp:
            smtp.set_debuglevel(1)
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        logger.info(f'Email sent to {to_email}')
    except Exception as e:
        logger.error(f'Failed to send email: {e}')


async def download_new_price_provider(
    msg: MailBox.email_message_class,
    provider: Provider,
    provider_conf: ProviderPriceListConfig,
    session: AsyncSession,
) -> Optional[str]:
    subject = msg.subject
    logger.debug(f'Письмо uid={msg.uid}, subject={subject}')
    # Если тема не соответствует критерию, пропускаем письмо
    if provider_conf.name_mail and (
        normalize_str(provider_conf.name_mail) not in normalize_str(subject)
    ):
        logger.debug(
            f'Тема {subject} не содержит '
            f'{provider_conf.name_mail}, пропускаем'
        )
        return None
    # Если в конфигурации указан URL, пытаемся скачать файл по URL
    if provider_conf.file_url:
        logger.debug(f'Найден URL в конфигурации = {provider_conf.file_url}')
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(provider_conf.file_url)
                if resp.status_code != 200:
                    logger.debug(
                        f'Failed to download file from URL '
                        f'{provider_conf.file_url}: {resp.status_code}'
                    )
                    return None
                filename = os.path.basename(provider_conf.file_url)
                filepath = os.path.join(DOWNLOAD_FOLDER, filename)
                async with aiofiles.open(filepath, 'wb') as f:
                    await f.write(resp.content)
                logger.debug(f'Загрузка файла из URL: {filepath}')
                current_uid = int(msg.uid)
                last_uid = await get_last_uid(provider.id, session)
                logger.debug(f'Last UID: {last_uid}')
                if current_uid > last_uid:
                    await set_last_uid(provider.id, current_uid, session)
                return filepath
        except Exception as e:
            logger.exception(
                f'Error downloading file from '
                f'URL {provider_conf.file_url}: {e}'
            )
            return None
    for att in msg.attachments:
        logger.debug(f'Found attachment: {att.filename}')

        if (
            not provider_conf.name_price
            or normalize_str(provider_conf.name_price)
            in normalize_str(att.filename)
        ):
            logger.debug('Имя вложения совпало')
            filepath = os.path.join(DOWNLOAD_FOLDER, att.filename)
            try:
                async with aiofiles.open(filepath, 'wb') as f:
                    await f.write(att.payload)
                logger.debug('Скачано вложение: %s', filepath)
            except Exception as e:
                logger.exception(f'Ошибка записи файла {filepath}: {e}')
                continue
            logger.debug(f'Downloaded attachment: {filepath}')
            current_uid = int(msg.uid)
            last_uid = await get_last_uid(provider.id, session)
            logger.debug(f'Last UID: {last_uid}')
            if current_uid > last_uid:
                await set_last_uid(provider.id, current_uid, session)
            return filepath
    logger.debug(
        f'В письме uid={msg.uid} нет вложений, соответствующих критерию'
    )
    return None


def _fetch_mailbox_messages(
    server_mail: str,
    email_account: str,
    email_password: str,
    main_box: str,
    port: int = IMAP_SERVER,
    ssl: bool = True,
):
    with MailBox(server_mail, port, ssl=ssl).login(
        email_account, email_password
    ) as mailbox:
        mailbox.folder.set(main_box)
        return list(
            mailbox.fetch(
                AND(date_gte=date.today(), all=True),
                charset='utf-8',
            )
        )


async def get_emails(
    session: AsyncSession,
    server_mail: str = EMAIL_HOST,
    email_account: str = EMAIL_NAME,
    email_password: str = EMAIL_PASSWORD,
    main_box: str = 'INBOX',
) -> list[tuple[Provider, str]]:
    downloaded_files = []
    all_emails = []
    accounts = await crud_email_account.get_active_by_purpose(
        session, 'prices_in'
    )
    if accounts:
        for account in accounts:
            host = account.imap_host or server_mail
            if not host:
                continue
            messages = await asyncio.to_thread(
                _fetch_mailbox_messages,
                host,
                account.email,
                account.password,
                main_box,
                account.imap_port or IMAP_SERVER,
                True,
            )
            all_emails.extend(messages)
    else:
        all_emails = await asyncio.to_thread(
            _fetch_mailbox_messages,
            server_mail,
            email_account,
            email_password,
            main_box,
            IMAP_SERVER,
            True,
        )
    logger.debug(f'Получено {len(all_emails)} писем за сегодня')
    for msg in all_emails:
        logger.debug(
            f'Письмо: uid={msg.uid}, from={msg.from_}, '
            f'date={msg.date}, subject={msg.subject}'
        )
        provider = await crud_provider.get_by_email_incoming_price(
            session=session, email=_extract_email(msg.from_)
        )
        if not provider:
            logger.debug(
                f'Провайдер для email {msg.from_} '
                f'не найден, пропускаем письмо uid={msg.uid}'
            )
            continue  # Если провайдера нет, пропускаем письмо

        # Получаем все конфигурации для данного провайдера
        provider_confs = await crud_provider_pricelist_config.get_configs(
            provider_id=provider.id, session=session
        )
        if not provider_confs:
            logger.debug(
                f'Конфигураций для провайдера {provider.id} не найдена, '
                f'пропускаем письмо uid={msg.uid}'
            )
            continue
        last_uid = await get_last_uid(provider_id=provider.id, session=session)
        if last_uid >= int(msg.uid):
            logger.debug(f'Старое UID = {msg.uid}, пропускаем письмо')
            continue  # Если UID записанное равно или больше,
            # пропускаем письмо
        file_downloaded = False

        for provider_conf in provider_confs:
            logger.debug(f'Config: {provider_conf}')
            filepath = await download_new_price_provider(
                msg=msg,
                provider=provider,
                provider_conf=provider_conf,
                session=session,
            )
            if filepath:
                # Если файл успешно скачан, помечаем письмо как прочитанное
                # mailbox.flag(msg.uid, [r'\Seen'], True)
                downloaded_files.append((provider, filepath, provider_conf))
                file_downloaded = True
        if not file_downloaded:
            logger.debug(
                f'Письмо uid={msg.uid} не удовлетворило условиям загрузки'
            )
    return downloaded_files


def send_order_to_provider(order: Order, provider_email: str):
    order_items = [
        {
            'make_name': item.autopart.brand.name,
            'oem': item.autopart.oem_number,
            'detail_name': item.autopart.name,
            'qnt': item.quantity,
            'cost': float(item.price),
            'sum': item.quantity * float(item.price),
        }
        for item in order.order_items
    ]
    total_sum = sum(item['sum'] for item in order_items)
    env = Environment(loader=FileSystemLoader('email_form'))
    template = env.get_template('from_order_email.html')
    html_body = template.render(
        customer_title=order.provider.name,
        order_id=order.id,
        order_items=order_items,
        total_sum=total_sum,
    )
    buffer = BytesIO()
    pd.DataFrame(order_items).to_excel(buffer, index=False, sheet_name='Order')
    buffer.seek(0)
    send_email_with_attachment(
        to_email=provider_email,
        subject=f'Заказ №{order.id}',
        body=html_body,
        attachment_filename=f'order_{order.id}.xlsx',
        attachment_bytes=buffer.read(),
        is_html=True,
    )
