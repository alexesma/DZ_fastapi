import asyncio
import base64
import logging
import mimetypes
import os
import re
import smtplib
import socket
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from io import BytesIO
from typing import Optional

import aiofiles
import httpx
import pandas as pd
from fastapi import HTTPException

try:
    from imap_tools import AND, MailBox, MailBoxSsl
except ImportError:  # pragma: no cover - fallback for older imap_tools
    from imap_tools import AND, MailBox
    MailBoxSsl = None
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import DEPTH_DAY_EMAIL, IMAP_SERVER
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.crud.partner import (crud_provider,
                                     crud_provider_pricelist_config,
                                     get_last_uid, set_last_uid)
from dz_fastapi.models.partner import Order, Provider, ProviderPriceListConfig
from dz_fastapi.services.google_oauth import refresh_google_access_token_sync
from dz_fastapi.services.resend_api import (fetch_received_emails_for_address,
                                            send_email_via_resend)
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
EMAIL_TRANSPORT = os.getenv('EMAIL_TRANSPORT', 'smtp').strip().lower()
GOOGLE_OAUTH_REFRESH_TOKEN = os.getenv('GOOGLE_OAUTH_REFRESH_TOKEN')
GMAIL_API_TIMEOUT = int(os.getenv('GMAIL_API_TIMEOUT', 20))
RESEND_API_KEY = os.getenv('RESEND_API_KEY')
RESEND_TIMEOUT = int(os.getenv('RESEND_API_TIMEOUT', '20'))
RESEND_FROM_EMAIL = os.getenv('RESEND_FROM_EMAIL')

DOWNLOAD_FOLDER = 'uploads/pricelistprovider'
PROCESSED_FOLDER = 'processed'
GMAIL_API_SEND_URL = (
    'https://gmail.googleapis.com/gmail/v1/users/me/messages/send'
)


def _clean_mail_setting(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


class _ResolvedHostSMTP(smtplib.SMTP):
    def __init__(self, *args, resolved_host: str | None = None, **kwargs):
        self._resolved_host = resolved_host
        super().__init__(*args, **kwargs)

    def _get_socket(self, host, port, timeout):
        target_host = self._resolved_host or host
        if self.debuglevel > 0:
            self._print_debug('connect:', (target_host, port))
        return socket.create_connection(
            (target_host, port), timeout, self.source_address
        )


class _ResolvedHostSMTP_SSL(smtplib.SMTP_SSL):
    def __init__(self, *args, resolved_host: str | None = None, **kwargs):
        self._resolved_host = resolved_host
        super().__init__(*args, **kwargs)

    def _get_socket(self, host, port, timeout):
        target_host = self._resolved_host or host
        if self.debuglevel > 0:
            self._print_debug('connect:', (target_host, port))
        new_socket = socket.create_connection(
            (target_host, port), timeout, self.source_address
        )
        return self.context.wrap_socket(new_socket, server_hostname=host)


@dataclass
class _FetchedAttachment:
    filename: Optional[str]
    payload: bytes


@dataclass
class _FetchedInboxMessage:
    uid: Optional[str]
    from_: str
    subject: str
    attachments: list[_FetchedAttachment]
    date: Optional[datetime] = None


def _message_sort_key(msg: _FetchedInboxMessage) -> tuple[int, float]:
    uid = _safe_uid_as_int(getattr(msg, 'uid', None))
    if uid is not None:
        return (1, float(uid))
    msg_date = getattr(msg, 'date', None)
    if isinstance(msg_date, datetime):
        try:
            return (0, msg_date.timestamp())
        except Exception:
            return (0, 0.0)
    return (0, 0.0)


def _message_matches_provider_config(
    msg: _FetchedInboxMessage,
    provider_conf: ProviderPriceListConfig,
) -> bool:
    subject = str(getattr(msg, 'subject', '') or '')
    if provider_conf.name_mail and (
        normalize_str(provider_conf.name_mail) not in normalize_str(subject)
    ):
        return False
    if provider_conf.file_url:
        return True
    attachments = getattr(msg, 'attachments', []) or []
    if not attachments:
        return False
    if not provider_conf.name_price:
        return True
    name_price_norm = normalize_str(provider_conf.name_price)
    for att in attachments:
        filename_norm = normalize_str(getattr(att, 'filename', '') or '')
        if (
            name_price_norm in filename_norm
            or filename_norm in name_price_norm
        ):
            return True
    return False


def _resolve_smtp_host(host: str) -> tuple[str, str]:
    try:
        addrinfo = socket.getaddrinfo(
            host,
            None,
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        logger.warning('SMTP host resolve failed for %s: %s', host, exc)
        return host, 'unresolved'

    ipv4_hosts = []
    ipv6_hosts = []
    for family, _, _, _, sockaddr in addrinfo:
        if family == socket.AF_INET:
            ipv4_hosts.append(sockaddr[0])
        elif family == socket.AF_INET6:
            ipv6_hosts.append(sockaddr[0])

    if ipv4_hosts:
        return ipv4_hosts[0], 'ipv4'
    if ipv6_hosts:
        return ipv6_hosts[0], 'ipv6'
    return host, 'unknown'


def _create_mailbox(server_mail: str, port: int, ssl: bool = True):
    if ssl and MailBoxSsl is not None:
        return MailBoxSsl(server_mail, port)
    return MailBox(server_mail, port)


def _extract_email(value: Optional[str]) -> str:
    if not value:
        return ''
    match = re.search(r'[\\w\\.-]+@[\\w\\.-]+\\.[\\w]+', value)
    return match.group(0).lower() if match else value.lower()


def _normalize_recipients(
    to_email: str | list[str] | tuple[str, ...]
) -> list[str]:
    if isinstance(to_email, (list, tuple)):
        raw_values = list(to_email)
    else:
        raw_values = re.split(r'[;,]', str(to_email or ''))
    recipients = []
    for value in raw_values:
        email = _extract_email(str(value).strip())
        if email:
            recipients.append(email)
    return recipients


def build_email_delivery_kwargs(account) -> dict:
    transport = getattr(account, 'transport', 'smtp') or 'smtp'
    transport = transport.strip().lower()
    if transport == 'gmail_api':
        return {
            'transport': 'gmail_api',
            'from_email': account.email,
            'oauth_refresh_token': getattr(
                account, 'oauth_refresh_token', None
            ),
        }
    if transport == 'resend_api':
        return {
            'transport': 'resend_api',
            'from_email': account.email,
            'resend_api_key': getattr(account, 'resend_api_key', None),
            'resend_timeout': getattr(account, 'resend_timeout', None),
        }
    return {
        'transport': 'smtp',
        'smtp_host': account.smtp_host,
        'smtp_port': account.smtp_port,
        'smtp_user': account.email,
        'smtp_password': account.password,
        'from_email': account.email,
        'use_ssl': bool(account.smtp_use_ssl),
    }


def describe_email_delivery(account) -> str:
    transport = getattr(account, 'transport', 'smtp') or 'smtp'
    transport = transport.strip().lower()
    if transport == 'gmail_api':
        return 'transport=gmail_api oauth_provider=%s has_refresh_token=%s' % (
            getattr(account, 'oauth_provider', None),
            bool(getattr(account, 'oauth_refresh_token', None)),
        )
    if transport == 'resend_api':
        return 'transport=resend_api has_api_key=%s timeout=%s' % (
            bool(getattr(account, 'resend_api_key', None)),
            getattr(account, 'resend_timeout', None),
        )
    return (
        'transport=smtp smtp_host=%s smtp_port=%s smtp_ssl=%s'
        % (
            getattr(account, 'smtp_host', None),
            getattr(account, 'smtp_port', None),
            bool(getattr(account, 'smtp_use_ssl', True)),
        )
    )


def _send_email_via_gmail_api(
    *,
    to_email,
    subject,
    body,
    attachment_bytes=None,
    attachment_filename: str | None = None,
    is_html: bool,
    from_email: str,
    oauth_refresh_token: str | None = None,
) -> bool:
    recipients = _normalize_recipients(to_email)
    refresh_token = oauth_refresh_token or GOOGLE_OAUTH_REFRESH_TOKEN

    if not refresh_token or not from_email:
        logger.error(
            'Gmail API settings are incomplete: '
            'from_email=%s has_refresh_token=%s',
            from_email,
            bool(refresh_token),
        )
        return False
    if not recipients:
        logger.error(
            'No valid recipients for Gmail API email send: %s',
            to_email,
        )
        return False

    try:
        token_data = refresh_google_access_token_sync(refresh_token)
        access_token = token_data.get('access_token')
        if not access_token:
            logger.error('Google OAuth access token not returned')
            return False
        msg = _create_email_message(
            to_email=', '.join(recipients),
            subject=subject,
            body=body,
            attachment_bytes=attachment_bytes,
            attachment_filename=attachment_filename,
            from_email=from_email,
            is_html=is_html,
        )
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode(
            'ascii'
        ).rstrip('=')
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }
        payload = {'raw': raw_message}
        logger.info(
            'Sending email via Gmail API timeout=%s '
            'from=%s to=%s',
            GMAIL_API_TIMEOUT,
            from_email,
            ','.join(recipients),
        )
        response = httpx.post(
            GMAIL_API_SEND_URL,
            json=payload,
            headers=headers,
            timeout=GMAIL_API_TIMEOUT,
        )
        response.raise_for_status()
        logger.info(
            'Gmail API email sent successfully status=%s',
            response.status_code,
        )
        return True
    except Exception as exc:
        logger.error(
            'Failed to send email via Gmail API: %s',
            exc,
        )
        return False


def _send_email_via_resend(
    *,
    to_email,
    subject,
    body,
    attachment_bytes=None,
    attachment_filename: str | None = None,
    is_html: bool,
    from_email: str | None = None,
    resend_api_key: str | None = None,
    resend_timeout: int | None = None,
) -> bool:
    recipients = _normalize_recipients(to_email)
    resolved_from_email = (
        _clean_mail_setting(from_email)
        or _clean_mail_setting(RESEND_FROM_EMAIL)
        or _clean_mail_setting(EMAIL_NAME)
    )
    return send_email_via_resend(
        api_key=resend_api_key or RESEND_API_KEY,
        from_email=resolved_from_email,
        to_email=recipients,
        subject=subject,
        body=body,
        is_html=is_html,
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename,
        timeout=resend_timeout or RESEND_TIMEOUT,
    )


def _create_email_message(
    *,
    to_email,
    subject,
    body,
    attachment_bytes=None,
    attachment_filename: str | None = None,
    from_email: str,
    is_html: bool,
) -> EmailMessage:
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    if is_html:
        msg.add_alternative(body, subtype='html')
    else:
        msg.set_content(body)

    if attachment_bytes is not None and attachment_filename:
        content_type, _ = mimetypes.guess_type(attachment_filename)
        if content_type and '/' in content_type:
            maintype, subtype = content_type.split('/', 1)
        else:
            maintype = 'application'
            subtype = (
                'vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        msg.add_attachment(
            attachment_bytes,
            maintype=maintype,
            subtype=subtype,
            filename=attachment_filename,
        )
    return msg


def _send_email_via_smtp(
    *,
    to_email,
    subject,
    body,
    attachment_bytes=None,
    attachment_filename: str | None = None,
    is_html: bool,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    from_email: str | None = None,
    use_ssl: bool = True,
) -> bool:
    smtp_user = _clean_mail_setting(smtp_user) or _clean_mail_setting(
        EMAIL_NAME
    )
    smtp_password = smtp_password or EMAIL_PASSWORD
    smtp_host = _clean_mail_setting(smtp_host) or _clean_mail_setting(
        SMTP_SERVER
    )
    smtp_port = smtp_port or SMTP_PORT
    from_email = _clean_mail_setting(from_email) or _clean_mail_setting(
        EMAIL_NAME
    )

    if not smtp_user or not smtp_password or not smtp_host:
        logger.error('Email credentials are not set.')
        return False

    resolved_host, resolved_family = _resolve_smtp_host(smtp_host)
    msg = _create_email_message(
        to_email=to_email,
        subject=subject,
        body=body,
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename,
        from_email=from_email,
        is_html=is_html,
    )

    try:
        logger.info(
            'Sending email via SMTP host=%s resolved_host=%s family=%s '
            'port=%s user=%s ssl=%s to=%s',
            smtp_host,
            resolved_host,
            resolved_family,
            smtp_port,
            smtp_user,
            use_ssl,
            to_email,
        )
        if use_ssl:
            smtp_ctx = _ResolvedHostSMTP_SSL(
                smtp_host,
                smtp_port,
                timeout=20,
                resolved_host=resolved_host,
            )
        else:
            smtp_ctx = _ResolvedHostSMTP(
                smtp_host,
                smtp_port,
                timeout=20,
                resolved_host=resolved_host,
            )
        with smtp_ctx as smtp:
            smtp.set_debuglevel(1)
            logger.debug(
                'SMTP connection established for host=%s',
                smtp_host,
            )
            if not use_ssl:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
            logger.debug('SMTP login start for user=%s', smtp_user)
            smtp.login(smtp_user, smtp_password)
            logger.debug('SMTP login ok for user=%s', smtp_user)
            logger.debug('SMTP send start to=%s', to_email)
            smtp.send_message(msg)
            logger.debug('SMTP send ok to=%s', to_email)
        logger.info('Email sent to %s', to_email)
        return True
    except Exception as exc:
        logger.error(
            'Failed to send email via host=%s resolved_host=%s port=%s '
            'user=%s ssl=%s: %s',
            smtp_host,
            resolved_host,
            smtp_port,
            smtp_user,
            use_ssl,
            exc,
        )
        return False


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
        mailbox_host = _clean_mail_setting(server_mail)
        mailbox_port = IMAP_SERVER
        mailbox_folder = 'INBOX'
        mailbox_login = _clean_mail_setting(email_account)
        mailbox_password = email_password

        if provider_conf.incoming_email_account_id:
            selected_account = await crud_email_account.get(
                session, provider_conf.incoming_email_account_id
            )
            if selected_account:
                mailbox_host = (
                    _clean_mail_setting(selected_account.imap_host)
                    or mailbox_host
                )
                mailbox_port = selected_account.imap_port or mailbox_port
                mailbox_folder = (
                    _clean_mail_setting(selected_account.imap_folder)
                    or mailbox_folder
                )
                mailbox_login = _clean_mail_setting(selected_account.email)
                mailbox_password = selected_account.password
                logger.debug(
                    'Using configured mailbox for provider config %s: '
                    'email_account_id=%s',
                    provider_conf.id,
                    provider_conf.incoming_email_account_id,
                )
            else:
                logger.warning(
                    'Configured mailbox %s is missing for '
                    'provider config %s. Fallback to ENV mailbox.',
                    provider_conf.incoming_email_account_id,
                    provider_conf.id,
                )

        since_date = date.today() - timedelta(days=DEPTH_DAY_EMAIL)
        logger.debug(
            f'Email criteria: from = {provider.email_incoming_price}, '
            f'need name_mail = {provider_conf.name_mail}, '
            f'need name_price = {provider_conf.name_price}'
        )
        last_uid = await get_last_uid(
            provider.id,
            session,
            provider_config_id=provider_conf.id,
        )
        logger.debug(f'Last UID: {last_uid}')

        logger.debug(
            'Connecting to mailbox host=%s port=%s folder=%s login=%s '
            'for provider_config_id=%s',
            mailbox_host,
            mailbox_port,
            mailbox_folder,
            mailbox_login,
            provider_conf.id if provider_conf else None,
        )
        with _create_mailbox(mailbox_host, mailbox_port, True).login(
            mailbox_login, mailbox_password
        ) as mailbox:
            logger.debug(
                'Mailbox login successful for provider_config_id=%s',
                provider_conf.id if provider_conf else None,
            )
            mailbox.folder.set(mailbox_folder)
            logger.debug(
                'Mailbox folder selected for provider_config_id=%s',
                provider_conf.id if provider_conf else None,
            )
            logger.debug(
                'Fetching recent mailbox headers for diagnostics '
                'provider_config_id=%s',
                provider_conf.id if provider_conf else None,
            )
            debug_emails = list(
                mailbox.fetch(
                    AND(date_gte=date.today(), all=True),
                    headers_only=True,
                    limit=20,
                    reverse=True,
                )
            )
            logger.debug(
                'Fetched %s recent mailbox headers for provider_config_id=%s',
                len(debug_emails),
                provider_conf.id if provider_conf else None,
            )
            for msg in debug_emails:
                logger.debug(
                    'Uid: %s, from: %s, date: %s, subject: %s',
                    msg.uid,
                    msg.from_,
                    msg.date,
                    msg.subject,
                )
            criteria_kwargs = {'date_gte': since_date}
            if provider.email_incoming_price:
                criteria_kwargs['from_'] = provider.email_incoming_price
            use_local_subject_filter = (
                (mailbox_host or '').strip().lower() == 'imap.gmail.com'
                and bool(provider_conf.name_mail)
            )
            if provider_conf.name_mail and not use_local_subject_filter:
                criteria_kwargs['subject'] = provider_conf.name_mail
            criteria = AND(**criteria_kwargs)
            logger.debug(f'Using criteria: {criteria}')
            if use_local_subject_filter:
                logger.debug(
                    'Skipping IMAP SUBJECT search for Gmail '
                    'provider_config_id=%s; local subject filter=%r',
                    provider_conf.id if provider_conf else None,
                    provider_conf.name_mail,
                )

            logger.debug(
                'Fetching candidate emails for provider_config_id=%s',
                provider_conf.id if provider_conf else None,
            )
            fetch_kwargs = {}
            if not use_local_subject_filter:
                fetch_kwargs['charset'] = 'utf-8'
            email_list = list(
                mailbox.fetch(
                    criteria,
                    **fetch_kwargs,
                    # limit=max_emails
                )
            )
            logger.debug(
                'Fetched candidate emails for provider_config_id=%s',
                provider_conf.id if provider_conf else None,
            )
            logger.debug(f'Found {len(email_list)} emails matching criteria.')
            emails = [
                msg
                for msg in email_list
                if (_safe_uid_as_int(getattr(msg, 'uid', None)) or 0)
                > last_uid
            ]
            logger.debug(
                f'{len(emails)} emails have UID greater than {last_uid}.'
            )

            matching_emails = [
                msg
                for msg in emails
                if _message_matches_provider_config(msg, provider_conf)
            ]
            if not matching_emails:
                logger.debug('No matching attachments found.')
                return None

            msg = max(matching_emails, key=_message_sort_key)
            logger.debug(
                'Selected latest matching email uid=%s for provider config %s',
                getattr(msg, 'uid', None),
                provider_conf.id,
            )
            logger.debug('All headers:')
            for k, v in msg.headers.items():
                logger.debug(f'{k}: {v}')
            raw_subject = msg.obj.get('Subject')
            if raw_subject is None:
                logger.debug('No Subject found for this email.')

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
                    current_uid = _safe_uid_as_int(
                        getattr(msg, 'uid', None)
                    )
                    if current_uid is not None and current_uid > last_uid:
                        await set_last_uid(
                            provider.id,
                            current_uid,
                            session,
                            provider_config_id=provider_conf.id,
                        )
                    return filepath
                if not name_price_norm:
                    filepath = os.path.join(DOWNLOAD_FOLDER, att.filename)
                    with open(filepath, 'wb') as f:
                        f.write(att.payload)
                    logger.debug(
                        'name_price is empty, '
                        'downloaded first attachment: '
                        '%s',
                        filepath,
                    )
                    mailbox.flag(msg.uid, [r'\Seen'], True)
                    current_uid = _safe_uid_as_int(
                        getattr(msg, 'uid', None)
                    )
                    if current_uid is not None and current_uid > last_uid:
                        await set_last_uid(
                            provider.id,
                            current_uid,
                            session,
                            provider_config_id=provider_conf.id,
                        )
                    return filepath
            logger.debug('No matching attachments found.')
            return None
    except ValueError as e:
        logger.error(f'Ошибка обработки писем: {e}')
        raise HTTPException(status_code=400, detail='Invalid email data')
    except Exception as e:
        logger.exception(
            'Unexpected error while processing emails for provider_id=%s '
            'provider_config_id=%s mailbox_login=%s mailbox_host=%s '
            'mailbox_port=%s mailbox_folder=%s: %s',
            provider.id if provider else None,
            provider_conf.id if provider_conf else None,
            mailbox_login,
            mailbox_host,
            mailbox_port,
            mailbox_folder,
            e,
        )
        raise HTTPException(
            status_code=500, detail='Error fetching provider emails'
        )


def send_email_with_attachment(
    to_email,
    subject,
    body,
    attachment_bytes=None,
    attachment_filename: str | None = None,
    is_html: bool = False,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    from_email: str | None = None,
    use_ssl: bool = True,
    transport: str | None = None,
    oauth_refresh_token: str | None = None,
    resend_api_key: str | None = None,
    resend_timeout: int | None = None,
) -> bool:
    logger.debug(
        'Inside send_email_with_attachment with attachment=%s size=%s',
        bool(attachment_bytes is not None and attachment_filename),
        len(attachment_bytes) if attachment_bytes is not None else 0,
    )

    transport = (transport or EMAIL_TRANSPORT or 'smtp').strip().lower()
    if transport not in {'smtp', 'gmail_api', 'resend_api'}:
        logger.warning(
            'Unknown email transport %s, fallback to smtp',
            transport,
        )
        transport = 'smtp'
    from_email = _clean_mail_setting(from_email) or _clean_mail_setting(
        EMAIL_NAME
    )

    if transport == 'resend_api':
        return _send_email_via_resend(
            to_email=to_email,
            subject=subject,
            body=body,
            attachment_bytes=attachment_bytes,
            attachment_filename=attachment_filename,
            is_html=is_html,
            from_email=from_email,
            resend_api_key=resend_api_key,
            resend_timeout=resend_timeout,
        )

    if transport == 'gmail_api':
        sent = _send_email_via_gmail_api(
            to_email=to_email,
            subject=subject,
            body=body,
            attachment_bytes=attachment_bytes,
            attachment_filename=attachment_filename,
            is_html=is_html,
            from_email=from_email,
            oauth_refresh_token=oauth_refresh_token,
        )
        if sent:
            return True

        resolved_smtp_user = _clean_mail_setting(
            smtp_user
        ) or _clean_mail_setting(EMAIL_NAME)
        resolved_smtp_password = smtp_password or EMAIL_PASSWORD
        resolved_smtp_host = _clean_mail_setting(
            smtp_host
        ) or _clean_mail_setting(SMTP_SERVER)
        can_fallback_to_smtp = (
            bool(resolved_smtp_user)
            and bool(resolved_smtp_password)
            and bool(resolved_smtp_host)
            and (
                any(
                    value is not None
                    for value in (
                        smtp_host,
                        smtp_port,
                        smtp_user,
                        smtp_password,
                    )
                )
                or from_email == resolved_smtp_user
            )
        )
        if can_fallback_to_smtp:
            logger.warning(
                'Gmail API email send failed, fallback to SMTP '
                'host=%s user=%s',
                resolved_smtp_host,
                resolved_smtp_user,
            )
            return _send_email_via_smtp(
                to_email=to_email,
                subject=subject,
                body=body,
                attachment_bytes=attachment_bytes,
                attachment_filename=attachment_filename,
                is_html=is_html,
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=smtp_password,
                from_email=from_email,
                use_ssl=use_ssl,
            )
        return False

    return _send_email_via_smtp(
        to_email=to_email,
        subject=subject,
        body=body,
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename,
        is_html=is_html,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        from_email=from_email,
        use_ssl=use_ssl,
    )


def send_test_outbound_email(
    to_email: str,
    from_email: str | None = None,
    transport: str | None = None,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    use_ssl: bool = True,
    oauth_refresh_token: str | None = None,
    resend_api_key: str | None = None,
    resend_timeout: int | None = None,
) -> bool:
    return send_email_with_attachment(
        to_email=to_email,
        subject='Тест исходящей почты',
        body=(
            'Это тестовое письмо из DZ_fastapi. '
            'Если вы его получили, исходящая почта настроена.'
        ),
        attachment_bytes=b'Test email from DZ_fastapi',
        attachment_filename='email_test.txt',
        from_email=from_email,
        transport=transport,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        use_ssl=use_ssl,
        oauth_refresh_token=oauth_refresh_token,
        resend_api_key=resend_api_key,
        resend_timeout=resend_timeout,
    )


def send_email_message(
    to_email,
    subject,
    body,
    is_html: bool = False,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    from_email: str | None = None,
    use_ssl: bool = True,
    transport: str | None = None,
    oauth_refresh_token: str | None = None,
    resend_api_key: str | None = None,
    resend_timeout: int | None = None,
) -> bool:
    return send_email_with_attachment(
        to_email=to_email,
        subject=subject,
        body=body,
        attachment_bytes=None,
        attachment_filename=None,
        is_html=is_html,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        from_email=from_email,
        use_ssl=use_ssl,
        transport=transport,
        oauth_refresh_token=oauth_refresh_token,
        resend_api_key=resend_api_key,
        resend_timeout=resend_timeout,
    )


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
                current_uid = _safe_uid_as_int(getattr(msg, 'uid', None))
                if current_uid is not None:
                    last_uid = await get_last_uid(
                        provider.id,
                        session,
                        provider_config_id=provider_conf.id,
                    )
                    logger.debug(f'Last UID: {last_uid}')
                    if current_uid > last_uid:
                        await set_last_uid(
                            provider.id,
                            current_uid,
                            session,
                            provider_config_id=provider_conf.id,
                        )
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
            current_uid = _safe_uid_as_int(getattr(msg, 'uid', None))
            if current_uid is not None:
                last_uid = await get_last_uid(
                    provider.id,
                    session,
                    provider_config_id=provider_conf.id,
                )
                logger.debug(f'Last UID: {last_uid}')
                if current_uid > last_uid:
                    await set_last_uid(
                        provider.id,
                        current_uid,
                        session,
                        provider_config_id=provider_conf.id,
                    )
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
    with _create_mailbox(server_mail, port, ssl).login(
        email_account, email_password
    ) as mailbox:
        mailbox.folder.set(main_box)
        return list(
            mailbox.fetch(
                AND(date_gte=date.today(), all=True),
                charset='utf-8',
            )
        )


def _safe_uid_as_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text.isdigit():
        return None
    try:
        return int(text)
    except ValueError:
        return None


async def _fetch_resend_price_messages(
    account,
) -> tuple[list[_FetchedInboxMessage], datetime | None]:
    emails = await fetch_received_emails_for_address(
        api_key=account.resend_api_key,
        email=account.email,
        date_from=date.today(),
        received_after=getattr(account, 'resend_last_received_at', None),
        timeout=getattr(account, 'resend_timeout', None),
    )
    messages = []
    for item in emails:
        messages.append(
            _FetchedInboxMessage(
                uid=None,
                from_=_extract_email(item.get('from')),
                subject=str(item.get('subject') or ''),
                attachments=[
                    _FetchedAttachment(
                        att.get('filename'),
                        att.get('payload') or b'',
                    )
                    for att in (item.get('attachments') or [])
                ],
                date=item.get('created_at'),
            )
        )
    last_received_at = emails[-1].get('created_at') if emails else None
    return messages, last_received_at


async def get_emails(
    session: AsyncSession,
    server_mail: str = EMAIL_HOST,
    email_account: str = EMAIL_NAME,
    email_password: str = EMAIL_PASSWORD,
    main_box: str = 'INBOX',
) -> list[tuple[Provider, str]]:
    downloaded_files = []
    all_emails = []
    resend_cursors: dict[int, object] = {}
    selected_candidates: dict[
        int,
        tuple[Provider, ProviderPriceListConfig, _FetchedInboxMessage],
    ] = {}
    provider_cache: dict[str, Provider | None] = {}
    provider_configs_cache: dict[int, list[ProviderPriceListConfig]] = {}
    config_last_uid_cache: dict[int, int] = {}
    accounts = await crud_email_account.get_active_by_purpose(
        session, 'prices_in'
    )
    if accounts:
        for account in accounts:
            try:
                if (account.transport or '').strip().lower() == 'resend_api':
                    if not account.resend_api_key:
                        logger.warning(
                            'Resend API key is missing '
                            'for prices_in account id=%s',
                            account.id,
                        )
                        continue
                    (
                        messages,
                        last_received_at,
                    ) = await _fetch_resend_price_messages(account)
                    all_emails.extend(messages)
                    if last_received_at is not None:
                        resend_cursors[account.id] = last_received_at
                    continue
                host = account.imap_host or server_mail
                if not host:
                    continue
                messages = await asyncio.to_thread(
                    _fetch_mailbox_messages,
                    host,
                    account.email,
                    account.password,
                    account.imap_folder or main_box,
                    account.imap_port or IMAP_SERVER,
                    True,
                )
                all_emails.extend(messages)
            except Exception as exc:
                logger.error(
                    'Price inbox fetch failed for account id=%s email=%s '
                    'transport=%s host=%s: %s',
                    getattr(account, 'id', None),
                    getattr(account, 'email', None),
                    getattr(account, 'transport', None),
                    getattr(account, 'imap_host', None) or server_mail,
                    exc,
                    exc_info=True,
                )
                continue
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
        sender_email = _extract_email(msg.from_)
        if sender_email not in provider_cache:
            provider_cache[sender_email] = (
                await crud_provider.get_by_email_incoming_price(
                    session=session, email=sender_email
                )
            )
        provider = provider_cache[sender_email]
        if not provider:
            logger.debug(
                f'Провайдер для email {msg.from_} '
                f'не найден, пропускаем письмо uid={msg.uid}'
            )
            continue  # Если провайдера нет, пропускаем письмо

        # Получаем все конфигурации для данного провайдера
        if provider.id not in provider_configs_cache:
            provider_configs_cache[provider.id] = (
                await crud_provider_pricelist_config.get_configs(
                    provider_id=provider.id,
                    session=session,
                    only_active=True,
                )
            )
        provider_confs = provider_configs_cache[provider.id]
        if not provider_confs:
            logger.debug(
                f'Конфигураций для провайдера {provider.id} не найдена, '
                f'пропускаем письмо uid={msg.uid}'
            )
            continue

        for provider_conf in provider_confs:
            logger.debug('Config: %s', provider_conf)
            if not _message_matches_provider_config(msg, provider_conf):
                continue
            if provider_conf.id not in config_last_uid_cache:
                config_last_uid_cache[provider_conf.id] = await get_last_uid(
                    provider_id=provider.id,
                    provider_config_id=provider_conf.id,
                    session=session,
                )
            last_uid = config_last_uid_cache[provider_conf.id]
            msg_uid_int = _safe_uid_as_int(getattr(msg, 'uid', None))
            if msg_uid_int is not None and last_uid >= msg_uid_int:
                logger.debug(
                    'Старое UID = %s для config_id=%s, пропускаем письмо',
                    msg.uid,
                    provider_conf.id,
                )
                continue
            current = selected_candidates.get(provider_conf.id)
            if (
                current is None
                or _message_sort_key(msg) > _message_sort_key(current[2])
            ):
                selected_candidates[provider_conf.id] = (
                    provider,
                    provider_conf,
                    msg,
                )
        if not any(
            provider_conf.id in selected_candidates
            and selected_candidates[provider_conf.id][2] is msg
            for provider_conf in provider_confs
        ):
            logger.debug(
                f'Письмо uid={msg.uid} не удовлетворило условиям загрузки'
            )
    for provider, provider_conf, msg in selected_candidates.values():
        filepath = await download_new_price_provider(
            msg=msg,
            provider=provider,
            provider_conf=provider_conf,
            session=session,
        )
        if filepath:
            downloaded_files.append((provider, filepath, provider_conf))
    if resend_cursors:
        for account in accounts:
            if account.id in resend_cursors:
                account.resend_last_received_at = resend_cursors[account.id]
                session.add(account)
        await session.commit()
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
