import os
import re
import smtplib
import traceback
import unicodedata
from datetime import date, timedelta
from email.message import EmailMessage
from email.header import decode_header


from fastapi import HTTPException
from imap_tools import MailBox, AND
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.crud.partner import crud_provider_pricelist_config, crud_provider, get_last_uid, set_last_uid
import logging

logger = logging.getLogger('dz_fastapi')

# Email account credentials
IMAP_SERVER = os.getenv('EMAIL_HOST')
EMAIL_ACCOUNT = os.getenv('EMAIL_NAME')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

SMTP_SERVER = 'smtp.yandex.ru'
SMTP_PORT = int(os.getenv('SMTP_PORT', 465))
SMTP_USERNAME = os.getenv('EMAIL_NAME')
SMTP_PASSWORD = os.getenv('EMAIL_PASSWORD')

DOWNLOAD_FOLDER = 'uploads/pricelistprovider'
PROCESSED_FOLDER = 'processed'
# os.makedirs(PROCESSED_FOLDER, exist_ok=True)


def safe_filename(filename: str) -> str:
    # Normalize Unicode characters
    value = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode('ascii')
    # Remove any remaining non-alphanumeric characters except dot and underscore
    value = re.sub(r'[^\w\s\.-]', '', value)
    # Replace spaces with underscores
    value = re.sub(r'\s+', '_', value).strip()
    return value


async def download_price_provider(
        provider_id: int,
        session: AsyncSession,
        max_emails: int = 50,
):
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
        logger.info(f'Created directory: {DOWNLOAD_FOLDER}')

    provider = await crud_provider.get_by_id(
        provider_id=provider_id,
        session=session
    )
    if not provider:
        logger.error(
            f'Не нашли поставщика по provider_id : {provider_id}'
        )
        raise HTTPException(
            status_code=404,
            detail='Provider not found'
        )

    provider_conf = await crud_provider_pricelist_config.get_config_or_none(
        provider_id=provider_id,
        session=session
    )
    if not provider_conf:
        logger.error(
            f'Не нашли настройку прайса по provider_id : {provider_id}'
        )
        raise HTTPException(
            status_code=404,
            detail='Provider config not found'
        )

    try:
        since_date = date.today() - timedelta(days=2)
        logger.debug(
            f'Email criteria: from = {provider.email_incoming_price}, '
            f'need name_mail = {provider_conf.name_mail}, '
            f'need name_price = {provider_conf.name_price}'
        )
        last_uid = await get_last_uid(provider_id, session)

        with MailBox(IMAP_SERVER).login(EMAIL_ACCOUNT, EMAIL_PASSWORD) as mailbox:
            criteria = AND(
                from_=provider.email_incoming_price,
                date_gte=since_date,
                seen=False
            )

            email_list = list(mailbox.fetch(criteria, charset='utf-8', limit=max_emails))
            logger.debug(f'Found {len(email_list)} emails matching criteria.')
            emails = [msg for msg in email_list if int(msg.uid) > last_uid]

            for msg in emails:
                subject = msg.subject
                logger.debug("All headers:")
                for k, v in msg.headers.items():
                    logger.debug(f"{k}: {v}")
                raw_subject = msg.obj.get('Subject')
                if raw_subject is None:
                    logger.debug("No Subject found for this email.")

                # logger.debug(f'all data {msg.__dict__}')
                # logger.debug(f'Subject: {msg.subject}, From: {msg.from_}, To: {msg.to}, Date: {msg.date}')
                # logger.debug(f'Processing email with subject: {subject}')
                if not provider_conf.name_mail.lower() in subject.lower():
                    logger.debug(
                        f'Subject {subject} does not contain {provider_conf.name_mail.lower()}, skipping.'
                    )
                    continue

                for att in msg.attachments:
                    logger.debug(f'Found attachment: {att.filename}')
                    filename = safe_filename(att.filename)

                    if filename.lower() in provider_conf.name_price.lower():
                        filepath = os.path.join(DOWNLOAD_FOLDER, filename)
                        with open(filepath, 'wb') as f:
                            f.write(att.payload)
                        logger.debug(f'Downloaded attachment: {filepath}')
                        mailbox.flag(msg.uid, [r'\Seen'], True)
                        current_uid = int(msg.uid)
                        if current_uid > last_uid:
                            await set_last_uid(provider_id, current_uid, session)
                        return filepath
            mailbox.flag([msg.uid for msg in emails], ['SEEN'], True)
            logger.debug('No matching attachments found.')
            if emails:
                max_uid = max(int(msg.uid) for msg in emails)
                if max_uid > last_uid:
                    await set_last_uid(provider_id, max_uid, session)
            return None
    except ValueError as e:
        logger.error(f'Ошибка обработки писем: {e}')
        raise HTTPException(status_code=400, detail='Invalid email data')
    except Exception as e:
        logger.exception(f'Unexpected error while processing emails: {e}')
        raise HTTPException(
            status_code=500,
            detail='Error fetching provider emails'
        )


def send_email_with_attachment(
        to_email,
        subject,
        body,
        attachment_bytes,
        attachment_filename
):
    if not EMAIL_ACCOUNT or not EMAIL_PASSWORD:
        logger.error('Email credentials are not set.')
        return

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_ACCOUNT
    msg['To'] = to_email
    msg.set_content(body)

    # Add the attachment
    msg.add_attachment(
        attachment_bytes,
        maintype='application',
        subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        filename=attachment_filename
    )

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
            smtp.send_message(msg)
        logger.info(f'Email sent to {to_email}')
    except Exception as e:
        logger.error(f'Failed to send email: {e}')
