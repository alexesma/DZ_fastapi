import asyncio
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import aiofiles
import pandas as pd
from imap_tools import AND, MailBox
from openpyxl import load_workbook
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from dz_fastapi.core.constants import IMAP_SERVER
from dz_fastapi.crud.email_account import crud_email_account
from dz_fastapi.crud.partner import (crud_customer_pricelist,
                                     crud_customer_pricelist_config,
                                     crud_customer_pricelist_source,
                                     crud_pricelist)
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import (CUSTOMER_ORDER_ITEM_STATUS,
                                       CUSTOMER_ORDER_SHIP_MODE,
                                       CUSTOMER_ORDER_STATUS,
                                       STOCK_ORDER_STATUS,
                                       SUPPLIER_ORDER_STATUS, CustomerOrder,
                                       CustomerOrderConfig, CustomerOrderItem,
                                       CustomerPriceList,
                                       CustomerPriceListAutoPartAssociation,
                                       CustomerPriceListConfig, Provider,
                                       StockOrder, StockOrderItem,
                                       SupplierOrder, SupplierOrderItem)
from dz_fastapi.services.email import send_email_with_attachment
from dz_fastapi.services.process import (_apply_source_filters,
                                         _apply_source_markups)
from dz_fastapi.services.telegram import (send_file_to_telegram,
                                          send_message_to_telegram)

logger = logging.getLogger('dz_fastapi')

EMAIL_NAME_ORDER = os.getenv('EMAIL_NAME_ORDERS')
EMAIL_PASSWORD_ORDER = os.getenv('EMAIL_PASSWORD_ORDERS')
EMAIL_HOST_ORDER = os.getenv('EMAIL_HOST_ORDERS')
EMAIL_FOLDER_ORDER = os.getenv('EMAIL_FOLDER_ORDERS', 'INBOX')

ORDERS_UPLOAD_DIR = os.getenv('CUSTOMER_ORDERS_UPLOAD_DIR', 'uploads/orders')
ORDERS_RESPONSE_DIR = os.getenv(
    'CUSTOMER_ORDERS_RESPONSE_DIR', 'uploads/orders/responses'
)
ORDERS_REPORT_DIR = os.getenv(
    'CUSTOMER_ORDERS_REPORT_DIR', 'uploads/orders/reports'
)
ORDERS_RETENTION_DAYS = int(os.getenv('CUSTOMER_ORDERS_REPORT_DAYS', 7))


@dataclass
class ParsedOrderRow:
    row_index: int
    oem: str
    brand: str
    name: Optional[str]
    requested_qty: int
    requested_price: Optional[float]


@dataclass
class OfferRow:
    autopart_id: int
    provider_id: int
    provider_config_id: Optional[int]
    quantity: int
    price: float
    is_own_price: bool


async def _fetch_order_messages(
    server_mail: str,
    email_account: str,
    email_password: str,
    folder: str,
    port: int = 993,
    ssl: bool = True,
) -> list:
    def _fetch():
        with MailBox(server_mail, port, ssl=ssl).login(
            email_account, email_password
        ) as mailbox:
            mailbox.folder.set(folder)
            return list(
                mailbox.fetch(
                    AND(date_gte=date.today(), all=True),
                    charset='utf-8',
                )
            )

    return await asyncio.to_thread(_fetch)


def _match_pattern(pattern: Optional[str], value: Optional[str]) -> bool:
    if not pattern:
        return True
    if not value:
        return False
    try:
        return re.search(pattern, value, flags=re.IGNORECASE) is not None
    except re.error:
        return pattern.lower() in value.lower()


def _extract_email(value: Optional[str]) -> str:
    if not value:
        return ''
    match = re.search(r'[\\w\\.-]+@[\\w\\.-]+\\.[\\w]+', value)
    return match.group(0).lower() if match else value.lower()


async def _get_out_account(
    session: AsyncSession, purpose: str
):
    accounts = await crud_email_account.get_active_by_purpose(
        session, purpose
    )
    return accounts[0] if accounts else None


def _normalize_email_list(values: Optional[List[str]]) -> List[str]:
    if not values:
        return []
    return [str(v).strip().lower() for v in values if str(v).strip()]


def _strip_html(text: str) -> str:
    return re.sub(r'<[^>]+>', ' ', text)


def _find_order_number_in_text(
    text: Optional[str],
    prefix: Optional[str],
    suffix: Optional[str],
) -> Optional[str]:
    if not text:
        return None
    if prefix or suffix:
        prefix_re = re.escape(prefix) if prefix else ''
        suffix_re = re.escape(suffix) if suffix else ''
        pattern = rf'{prefix_re}\\s*([\\w\\-\\/]+)\\s*{suffix_re}'
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def _normalize_key(oem: str, brand: str) -> Tuple[str, str]:
    return (str(oem).strip().upper(), str(brand).strip().upper())


def _safe_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    try:
        value_str = str(value).strip()
        if value_str == '':
            return None
        return int(float(value_str.replace(',', '.')))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        value_str = str(value).strip()
        if value_str == '':
            return None
        return float(value_str.replace(',', '.'))
    except (TypeError, ValueError):
        return None


def _parse_date(value: Optional[object]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return pd.to_datetime(value).date()
    except (TypeError, ValueError):
        return None


def _extract_order_number(
    config: CustomerOrderConfig,
    subject: Optional[str],
    filename: Optional[str],
    body: Optional[str],
) -> Optional[str]:
    if config.order_number_regex_subject and subject:
        match = re.search(config.order_number_regex_subject, subject)
        if match:
            return match.group(0)
    if config.order_number_regex_body and body:
        match = re.search(config.order_number_regex_body, body)
        if match:
            return match.group(0)
    if config.order_number_regex_filename and filename:
        match = re.search(config.order_number_regex_filename, filename)
        if match:
            return match.group(0)
    source = (config.order_number_source or '').lower()
    sources = []
    if source in ('subject', 'filename', 'body'):
        sources = [source]
    else:
        sources = ['subject', 'body', 'filename']

    for item in sources:
        if item == 'subject':
            found = _find_order_number_in_text(
                subject, config.order_number_prefix, config.order_number_suffix
            )
        elif item == 'body':
            found = _find_order_number_in_text(
                body, config.order_number_prefix, config.order_number_suffix
            )
        else:
            found = _find_order_number_in_text(
                filename,
                config.order_number_prefix,
                config.order_number_suffix,
            )
        if found:
            return found
    return None


def _parse_excel_order(
    file_bytes: bytes,
    config: CustomerOrderConfig,
) -> Tuple[List[ParsedOrderRow], Optional[date], Optional[str], BytesIO]:
    wb = load_workbook(BytesIO(file_bytes))
    ws = wb.active

    parsed_rows: List[ParsedOrderRow] = []
    order_date: Optional[date] = None
    order_number: Optional[str] = None

    oem_col = config.oem_col + 1
    brand_col = config.brand_col + 1
    name_col = config.name_col + 1 if config.name_col is not None else None
    qty_col = config.qty_col + 1
    price_col = config.price_col + 1 if config.price_col is not None else None
    order_date_col = (
        config.order_date_column + 1
        if config.order_date_column is not None
        else None
    )
    order_number_col = (
        config.order_number_column + 1
        if config.order_number_column is not None
        else None
    )

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if order_date is None and order_date_col is not None:
            order_date = _parse_date(row[order_date_col - 1])
        if order_number is None and order_number_col is not None:
            value = row[order_number_col - 1]
            if value is not None and str(value).strip():
                order_number = str(value).strip()
        oem = row[oem_col - 1] if oem_col - 1 < len(row) else None
        brand = row[brand_col - 1] if brand_col - 1 < len(row) else None
        qty = row[qty_col - 1] if qty_col - 1 < len(row) else None
        if not oem or not brand or qty is None:
            continue
        requested_qty = _safe_int(qty)
        if requested_qty is None:
            continue
        name = None
        if name_col is not None and name_col - 1 < len(row):
            name = row[name_col - 1]
            if name is not None:
                name = str(name).strip()
        requested_price = None
        if price_col is not None and price_col - 1 < len(row):
            requested_price = _safe_float(row[price_col - 1])
        parsed_rows.append(
            ParsedOrderRow(
                row_index=row_idx,
                oem=str(oem).strip(),
                brand=str(brand).strip(),
                name=name,
                requested_qty=requested_qty,
                requested_price=requested_price,
            )
        )

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return parsed_rows, order_date, order_number, output


def _parse_csv_order(
    file_bytes: bytes,
    config: CustomerOrderConfig,
) -> Tuple[List[ParsedOrderRow], Optional[date], Optional[str], BytesIO]:
    df = pd.read_csv(BytesIO(file_bytes), header=None)
    parsed_rows: List[ParsedOrderRow] = []
    order_date: Optional[date] = None
    order_number: Optional[str] = None

    for idx, row in df.iterrows():
        if order_date is None and config.order_date_column is not None:
            order_date = _parse_date(row[config.order_date_column])
        if order_number is None and config.order_number_column is not None:
            value = row[config.order_number_column]
            if not pd.isna(value) and str(value).strip():
                order_number = str(value).strip()
        oem = row[config.oem_col]
        brand = row[config.brand_col]
        qty = row[config.qty_col]
        if pd.isna(oem) or pd.isna(brand) or pd.isna(qty):
            continue
        requested_qty = _safe_int(qty)
        if requested_qty is None:
            continue
        name = None
        if config.name_col is not None:
            value = row[config.name_col]
            if not pd.isna(value):
                name = str(value).strip()
        requested_price = None
        if config.price_col is not None:
            value = row[config.price_col]
            if not pd.isna(value):
                requested_price = _safe_float(value)
        parsed_rows.append(
            ParsedOrderRow(
                row_index=idx,
                oem=str(oem).strip(),
                brand=str(brand).strip(),
                name=name,
                requested_qty=requested_qty,
                requested_price=requested_price,
            )
        )

    output = BytesIO()
    df.to_csv(output, index=False, header=False)
    output.seek(0)
    return parsed_rows, order_date, order_number, output


def _parse_xls_order(
    file_bytes: bytes,
    config: CustomerOrderConfig,
) -> Tuple[List[ParsedOrderRow], Optional[date], Optional[str], BytesIO]:
    df = pd.read_excel(BytesIO(file_bytes), header=None, engine='xlrd')
    parsed_rows: List[ParsedOrderRow] = []
    order_date: Optional[date] = None
    order_number: Optional[str] = None

    for idx, row in df.iterrows():
        if order_date is None and config.order_date_column is not None:
            order_date = _parse_date(row[config.order_date_column])
        if order_number is None and config.order_number_column is not None:
            value = row[config.order_number_column]
            if not pd.isna(value) and str(value).strip():
                order_number = str(value).strip()
        oem = row[config.oem_col]
        brand = row[config.brand_col]
        qty = row[config.qty_col]
        if pd.isna(oem) or pd.isna(brand) or pd.isna(qty):
            continue
        requested_qty = _safe_int(qty)
        if requested_qty is None:
            continue
        name = None
        if config.name_col is not None:
            value = row[config.name_col]
            if not pd.isna(value):
                name = str(value).strip()
        requested_price = None
        if config.price_col is not None:
            value = row[config.price_col]
            if not pd.isna(value):
                requested_price = _safe_float(value)
        parsed_rows.append(
            ParsedOrderRow(
                row_index=idx,
                oem=str(oem).strip(),
                brand=str(brand).strip(),
                name=name,
                requested_qty=requested_qty,
                requested_price=requested_price,
            )
        )

    output = BytesIO()
    df.to_excel(output, index=False, header=False)
    output.seek(0)
    return parsed_rows, order_date, order_number, output


def _apply_response_updates_excel(
    file_bytes: BytesIO,
    config: CustomerOrderConfig,
    items: List[CustomerOrderItem],
):
    wb = load_workbook(file_bytes)
    ws = wb.active
    qty_col = config.qty_col + 1
    ship_col = (
        config.ship_qty_col + 1
        if config.ship_qty_col is not None
        else None
    )
    reject_col = (
        config.reject_qty_col + 1
        if config.reject_qty_col is not None
        else None
    )

    for item in items:
        row_index = item.row_index
        if row_index is None:
            continue
        if config.ship_mode == CUSTOMER_ORDER_SHIP_MODE.REPLACE_QTY:
            ws.cell(row=row_index, column=qty_col).value = (
                item.ship_qty or 0
            )
        elif config.ship_mode == CUSTOMER_ORDER_SHIP_MODE.WRITE_SHIP_QTY:
            if ship_col is None:
                raise ValueError('ship_qty_col is required for WRITE_SHIP_QTY')
            ws.cell(row=row_index, column=ship_col).value = (
                item.ship_qty or 0
            )
        elif config.ship_mode == CUSTOMER_ORDER_SHIP_MODE.WRITE_REJECT_QTY:
            if reject_col is None:
                raise ValueError(
                    'reject_qty_col is required for WRITE_REJECT_QTY'
                )
            ws.cell(row=row_index, column=reject_col).value = (
                item.reject_qty or 0
            )

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def _apply_response_updates_csv(
    file_bytes: BytesIO,
    config: CustomerOrderConfig,
    items: List[CustomerOrderItem],
):
    file_bytes.seek(0)
    df = pd.read_csv(file_bytes, header=None)
    for item in items:
        row_index = item.row_index
        if row_index is None:
            continue
        if config.ship_mode == CUSTOMER_ORDER_SHIP_MODE.REPLACE_QTY:
            df.iat[row_index, config.qty_col] = item.ship_qty or 0
        elif config.ship_mode == CUSTOMER_ORDER_SHIP_MODE.WRITE_SHIP_QTY:
            if config.ship_qty_col is None:
                raise ValueError('ship_qty_col is required for WRITE_SHIP_QTY')
            df.iat[row_index, config.ship_qty_col] = item.ship_qty or 0
        elif config.ship_mode == CUSTOMER_ORDER_SHIP_MODE.WRITE_REJECT_QTY:
            if config.reject_qty_col is None:
                raise ValueError(
                    'reject_qty_col is required for WRITE_REJECT_QTY'
                )
            df.iat[row_index, config.reject_qty_col] = item.reject_qty or 0

    output = BytesIO()
    df.to_csv(output, index=False, header=False)
    output.seek(0)
    return output


async def _load_latest_customer_pricelist(
    session: AsyncSession, customer_id: int
) -> Optional[CustomerPriceList]:
    stmt = (
        select(CustomerPriceList)
        .where(CustomerPriceList.customer_id == customer_id)
        .order_by(CustomerPriceList.date.desc(), CustomerPriceList.id.desc())
        .options(
            joinedload(CustomerPriceList.autopart_associations)
            .joinedload(CustomerPriceListAutoPartAssociation.autopart)
            .joinedload(AutoPart.brand)
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalars().first()


def _build_expected_price_map(pricelist: CustomerPriceList) -> Dict:
    expected = {}
    for assoc in pricelist.autopart_associations:
        autopart = assoc.autopart
        if not autopart or not autopart.brand:
            continue
        key = _normalize_key(autopart.oem_number, autopart.brand.name)
        expected[key] = float(assoc.price or 0)
    return expected


async def _build_current_offers(
    session: AsyncSession, config: CustomerPriceListConfig
) -> Dict[Tuple[str, str], OfferRow]:
    sources = await crud_customer_pricelist_source.get_by_config_id(
        config_id=config.id, session=session
    )
    combined_data = []
    for source in sources:
        if not source.enabled:
            continue
        latest_pl = await crud_pricelist.get_latest_pricelist_by_config(
            session=session, provider_config_id=source.provider_config_id
        )
        if not latest_pl:
            continue
        associations = await crud_pricelist.fetch_pricelist_data(
            latest_pl.id, session
        )
        if not associations:
            continue
        df = await crud_pricelist.transform_to_dataframe(
            associations=associations, session=session
        )
        df = _apply_source_filters(df, source)
        if df.empty:
            continue
        df = crud_customer_pricelist.apply_coefficient(
            df, config, apply_general_markup=False
        )
        df = _apply_source_markups(df, config, source)
        combined_data.append(df)

    if not combined_data:
        return {}

    final_df = pd.concat(combined_data, ignore_index=True)

    if 'is_own_price' in final_df.columns:
        final_df['__own_rank'] = final_df['is_own_price'].astype(int)
        final_df = (
            final_df.sort_values(
                by=['oem_number', 'brand', '__own_rank', 'price'],
                ascending=[True, True, False, True],
            )
            .drop_duplicates(subset=['oem_number', 'brand'], keep='first')
            .drop(columns=['__own_rank'])
        )
    else:
        final_df = final_df.sort_values(
            by=['oem_number', 'brand', 'price']
        ).drop_duplicates(subset=['oem_number', 'brand'], keep='first')

    offers = {}
    for _, row in final_df.iterrows():
        key = _normalize_key(row.get('oem_number'), row.get('brand'))
        offers[key] = OfferRow(
            autopart_id=int(row.get('autopart_id')),
            provider_id=int(row.get('provider_id')),
            provider_config_id=row.get('provider_config_id'),
            quantity=int(row.get('quantity') or 0),
            price=float(row.get('price') or 0),
            is_own_price=bool(row.get('is_own_price')),
        )
    return offers


def _compute_price_diff_pct(
        expected_price: float, offered_price: float
) -> float:
    if expected_price <= 0:
        return 0.0
    return ((expected_price - offered_price) / expected_price) * 100


async def _send_price_warning(
    order: CustomerOrder,
    item: CustomerOrderItem,
    expected_price: float,
    offered_price: float,
    diff_pct: float,
    critical: bool,
):
    label = '!!!' if critical else '!'
    text = (
        f'{label} Отклонение цены по заказу клиента {order.customer_id} '
        f'({order.order_number or order.id})\n'
        f'Позиция: {item.oem} / {item.brand}\n'
        f'Цена прайса: {expected_price:.2f}, текущая: {offered_price:.2f}, '
        f'отклонение: {diff_pct:.2f}%'
    )
    try:
        await send_message_to_telegram(text)
    except Exception as exc:
        logger.error('Telegram warning failed: %s', exc, exc_info=True)


async def _send_reject_report(
    session: AsyncSession,
    order: CustomerOrder,
    rejected_items: List[CustomerOrderItem],
):
    if not rejected_items:
        return
    report_email = os.getenv('EMAIL_NAME_ANALYTIC')
    rows = []
    for item in rejected_items:
        rows.append(
            {
                'OEM': item.oem,
                'Brand': item.brand,
                'Name': item.name or '',
                'Requested Qty': item.requested_qty,
                'Rejected Qty': item.reject_qty or 0,
                'Reason': item.status.value,
            }
        )
    buffer = BytesIO()
    pd.DataFrame(rows).to_excel(buffer, index=False)
    buffer.seek(0)
    os.makedirs(ORDERS_REPORT_DIR, exist_ok=True)
    filename = f'reject_report_{order.id}.xlsx'
    path = os.path.join(ORDERS_REPORT_DIR, filename)
    async with aiofiles.open(path, 'wb') as f:
        await f.write(buffer.getvalue())
    chat_id = os.getenv('TELEGRAM_TO')
    if not chat_id:
        chat_id = None
    try:
        if chat_id:
            await send_file_to_telegram(
                chat_id=chat_id,
                file_bytes=buffer.getvalue(),
                file_name=filename,
                caption=(
                    f'Отчет по отказам заказа {order.order_number or order.id}'
                ),
            )
    except Exception as exc:
        logger.error('Reject report telegram failed: %s', exc, exc_info=True)

    if report_email:
        account = await _get_out_account(session, 'reports_out')
        kwargs = {}
        if account:
            kwargs = {
                'smtp_host': account.smtp_host,
                'smtp_port': account.smtp_port,
                'smtp_user': account.email,
                'smtp_password': account.password,
                'from_email': account.email,
                'use_ssl': bool(account.smtp_use_ssl),
            }
        try:
            await asyncio.get_running_loop().run_in_executor(
                None,
                send_email_with_attachment,
                report_email,
                f'Отчет по отказам заказа {order.order_number or order.id}',
                'Во вложении отчет по отказам.',
                buffer.getvalue(),
                filename,
                False,
                **kwargs,
            )
        except Exception as exc:
            logger.error('Reject report email failed: %s', exc, exc_info=True)


async def process_customer_orders(session: AsyncSession) -> None:
    order_accounts = await crud_email_account.get_active_by_purpose(
        session, 'orders_in'
    )
    if not order_accounts and (
        not EMAIL_NAME_ORDER
        or not EMAIL_PASSWORD_ORDER
        or not EMAIL_HOST_ORDER
    ):
        logger.warning('Order email credentials are not configured.')
        return

    configs = await session.execute(
        select(CustomerOrderConfig)
        .where(CustomerOrderConfig.is_active.is_(True))
        .options(joinedload(CustomerOrderConfig.customer))
    )
    configs = configs.scalars().all()
    if not configs:
        logger.info('No active customer order configs found.')
        return

    config_by_email = {}
    for config in configs:
        emails = _normalize_email_list(config.order_emails)
        if config.order_email:
            emails.append(config.order_email.lower())
        for email in emails:
            config_by_email[email] = config

    messages = []
    if order_accounts:
        for account in order_accounts:
            host = account.imap_host or EMAIL_HOST_ORDER
            if not host:
                continue
            account_messages = await _fetch_order_messages(
                host,
                account.email,
                account.password,
                EMAIL_FOLDER_ORDER,
                port=account.imap_port or IMAP_SERVER,
                ssl=True,
            )
            messages.extend(account_messages)
    else:
        messages = await _fetch_order_messages(
            EMAIL_HOST_ORDER,
            EMAIL_NAME_ORDER,
            EMAIL_PASSWORD_ORDER,
            EMAIL_FOLDER_ORDER,
            port=IMAP_SERVER,
            ssl=True,
        )

    if not messages:
        logger.info('No order emails found.')
        return

    logger.debug('Получено %d писем с заказами', len(messages))

    for msg in messages:
        try:
            sender = _extract_email(msg.from_)
            config = config_by_email.get(sender)
            if not config:
                continue
            if msg.uid and int(msg.uid) <= int(config.last_uid or 0):
                continue
            if not _match_pattern(config.order_subject_pattern, msg.subject):
                continue

            attachment = None
            for att in msg.attachments:
                if _match_pattern(config.order_filename_pattern, att.filename):
                    attachment = att
                    break
            if attachment is None and msg.attachments:
                attachment = msg.attachments[0]
            if attachment is None:
                logger.info('No attachment for order email uid=%s', msg.uid)
                continue

            filename = attachment.filename or 'order.xlsx'
            file_bytes = attachment.payload
            file_ext = filename.split('.')[-1].lower()
            body_text = msg.text or ''
            if not body_text and msg.html:
                body_text = _strip_html(msg.html)

            if file_ext not in ('xlsx', 'xls', 'csv'):
                logger.warning('Unsupported order file type: %s', file_ext)
                continue

            if file_ext == 'csv':
                (
                    parsed_rows,
                    order_date,
                    order_number_file,
                    file_buffer,
                ) = _parse_csv_order(file_bytes, config)
            elif file_ext == 'xls':
                (
                    parsed_rows,
                    order_date,
                    order_number_file,
                    file_buffer,
                ) = _parse_xls_order(file_bytes, config)
            else:
                (
                    parsed_rows,
                    order_date,
                    order_number_file,
                    file_buffer,
                ) = _parse_excel_order(file_bytes, config)

            if not parsed_rows:
                logger.info('No order rows found in %s', filename)
                continue

            order_number = _extract_order_number(
                config, msg.subject, filename, body_text
            )
            if not order_number:
                order_number = order_number_file

            last_pricelist = await _load_latest_customer_pricelist(
                session, config.customer_id
            )
            expected_prices = (
                _build_expected_price_map(last_pricelist)
                if last_pricelist
                else {}
            )

            pricelist_config = None
            if config.pricelist_config_id:
                configs = (
                    await crud_customer_pricelist_config.get_by_customer_id(
                        customer_id=config.customer_id,
                        session=session,
                    )
                )
                pricelist_config = next(
                    (
                        c
                        for c in configs
                        if c.id == config.pricelist_config_id
                    ),
                    None,
                )
            if pricelist_config is None:
                configs = (
                    await crud_customer_pricelist_config.get_by_customer_id(
                        customer_id=config.customer_id,
                        session=session,
                    )
                )
                pricelist_config = (
                    sorted(configs, key=lambda c: c.id, reverse=True)[0]
                    if configs
                    else None
                )
            offers = (
                await _build_current_offers(session, pricelist_config)
                if pricelist_config
                else {}
            )

            file_hash = hashlib.sha256(file_bytes).hexdigest()
            existing = await session.execute(
                select(CustomerOrder).where(
                    CustomerOrder.customer_id == config.customer_id,
                    CustomerOrder.file_hash == file_hash,
                )
            )
            if existing.scalars().first():
                config.last_uid = int(msg.uid) if msg.uid else config.last_uid
                await session.commit()
                continue
            order = CustomerOrder(
                customer_id=config.customer_id,
                status=CUSTOMER_ORDER_STATUS.NEW,
                received_at=datetime.now(timezone.utc),
                source_email=sender,
                source_uid=int(msg.uid) if msg.uid else None,
                source_subject=msg.subject,
                source_filename=filename,
                order_number=order_number,
                order_date=order_date,
                file_hash=file_hash,
            )
            session.add(order)
            await session.flush()

            order_items: List[CustomerOrderItem] = []
            supplier_items: Dict[int, List[CustomerOrderItem]] = {}
            stock_items: List[CustomerOrderItem] = []
            rejected_items: List[CustomerOrderItem] = []

            for row in parsed_rows:
                key = _normalize_key(row.oem, row.brand)
                expected_price = expected_prices.get(key)
                offer = offers.get(key)
                requested_price = row.requested_price

                item = CustomerOrderItem(
                    order_id=order.id,
                    row_index=row.row_index,
                    oem=row.oem,
                    brand=row.brand,
                    name=row.name,
                    requested_qty=row.requested_qty,
                    requested_price=requested_price,
                    status=CUSTOMER_ORDER_ITEM_STATUS.NEW,
                )
                if not offer or not expected_price or expected_price <= 0:
                    item.status = CUSTOMER_ORDER_ITEM_STATUS.REJECTED
                    item.ship_qty = 0
                    item.reject_qty = row.requested_qty
                else:
                    offered_price = offer.price
                    diff_pct = _compute_price_diff_pct(
                        expected_price, offered_price
                    )
                    item.price_diff_pct = diff_pct
                    item.matched_price = offered_price

                    warning_pct = config.price_warning_pct or 5.0
                    tolerance_pct = config.price_tolerance_pct or 2.0
                    if diff_pct > warning_pct:
                        if offer.is_own_price:
                            item.status = CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK
                            await _send_price_warning(
                                order,
                                item,
                                expected_price,
                                offered_price,
                                diff_pct,
                                critical=True,
                            )
                        else:
                            item.status = CUSTOMER_ORDER_ITEM_STATUS.REJECTED
                            item.ship_qty = 0
                            item.reject_qty = row.requested_qty
                            await _send_price_warning(
                                order,
                                item,
                                expected_price,
                                offered_price,
                                diff_pct,
                                critical=True,
                            )
                    else:
                        if diff_pct > tolerance_pct:
                            await _send_price_warning(
                                order,
                                item,
                                expected_price,
                                offered_price,
                                diff_pct,
                                critical=False,
                            )
                        item.status = (
                            CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK
                            if offer.is_own_price
                            else CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER
                        )

                    if item.status != CUSTOMER_ORDER_ITEM_STATUS.REJECTED:
                        available_qty = int(offer.quantity or 0)
                        if available_qty < 0:
                            available_qty = 0
                        ship_qty = min(row.requested_qty, available_qty)
                        reject_qty = max(row.requested_qty - ship_qty, 0)
                        item.ship_qty = ship_qty
                        item.reject_qty = reject_qty
                        item.autopart_id = offer.autopart_id
                        if offer.is_own_price:
                            item.supplier_id = None
                        else:
                            item.supplier_id = offer.provider_id
                        if ship_qty == 0:
                            item.status = CUSTOMER_ORDER_ITEM_STATUS.REJECTED

                order_items.append(item)
                session.add(item)
                await session.flush()

                if (
                    item.status == CUSTOMER_ORDER_ITEM_STATUS.OWN_STOCK
                    and item.ship_qty
                ):
                    stock_items.append(item)
                elif (
                    item.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER
                    and item.ship_qty
                ):
                    supplier_items.setdefault(
                        item.supplier_id, []
                    ).append(item)
                elif item.status == CUSTOMER_ORDER_ITEM_STATUS.REJECTED:
                    rejected_items.append(item)

            if stock_items:
                stock_order = StockOrder(
                    customer_id=config.customer_id,
                    status=STOCK_ORDER_STATUS.NEW,
                )
                session.add(stock_order)
                await session.flush()
                for item in stock_items:
                    session.add(
                        StockOrderItem(
                            stock_order_id=stock_order.id,
                            customer_order_item_id=item.id,
                            autopart_id=item.autopart_id,
                            quantity=item.ship_qty or 0,
                        )
                    )

            for provider_id, items in supplier_items.items():
                supplier_order = SupplierOrder(
                    provider_id=provider_id,
                    status=SUPPLIER_ORDER_STATUS.NEW,
                )
                session.add(supplier_order)
                await session.flush()
                for item in items:
                    session.add(
                        SupplierOrderItem(
                            supplier_order_id=supplier_order.id,
                            customer_order_item_id=item.id,
                            autopart_id=item.autopart_id,
                            quantity=item.ship_qty or 0,
                            price=item.matched_price,
                        )
                    )

            if file_ext == 'csv':
                response_buffer = _apply_response_updates_csv(
                    file_buffer, config, order_items
                )
            else:
                response_buffer = _apply_response_updates_excel(
                    file_buffer, config, order_items
                )

            os.makedirs(ORDERS_RESPONSE_DIR, exist_ok=True)
            if file_ext == 'xls':
                base = filename.rsplit('.', 1)[0]
                response_name = f'{base}.xlsx'
            else:
                response_name = filename
            response_path = os.path.join(
                ORDERS_RESPONSE_DIR,
                f'{order.id}_{response_name}',
            )
            async with aiofiles.open(response_path, 'wb') as f:
                await f.write(response_buffer.getvalue())

            order.response_file_path = response_path
            order.response_file_name = response_name
            order.status = CUSTOMER_ORDER_STATUS.PROCESSED
            order.processed_at = datetime.now(timezone.utc)

            await session.commit()

            await _send_reject_report(session, order, rejected_items)

            recipients = set([sender])
            for email in _normalize_email_list(config.order_reply_emails):
                recipients.add(email)
            to_email = ','.join(sorted(recipients))

            try:
                account = await _get_out_account(session, 'orders_out')
                kwargs = {}
                if account:
                    kwargs = {
                        'smtp_host': account.smtp_host,
                        'smtp_port': account.smtp_port,
                        'smtp_user': account.email,
                        'smtp_password': account.password,
                        'from_email': account.email,
                        'use_ssl': bool(account.smtp_use_ssl),
                    }
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    send_email_with_attachment,
                    to_email,
                    f'Ответ по заказу {order.order_number or order.id}',
                    'Во вложении файл с подтвержденными количествами.',
                    response_buffer.getvalue(),
                    response_name,
                    False,
                    **kwargs,
                )
                order.status = CUSTOMER_ORDER_STATUS.SENT
            except Exception as exc:
                logger.error(
                    'Failed to send order response: %s', exc, exc_info=True
                )
                order.status = CUSTOMER_ORDER_STATUS.ERROR

            config.last_uid = int(msg.uid) if msg.uid else config.last_uid
            await session.commit()
        except Exception as exc:
            logger.error(
                'Failed to process order email uid=%s: %s',
                getattr(msg, 'uid', None),
                exc,
                exc_info=True,
            )


async def send_supplier_orders(
    session: AsyncSession,
    supplier_order_ids: List[int],
) -> Dict[str, int]:
    if not supplier_order_ids:
        return {'sent': 0, 'failed': 0}

    stmt = (
        select(SupplierOrder)
        .where(SupplierOrder.id.in_(supplier_order_ids))
        .options(
            joinedload(SupplierOrder.items)
            .joinedload(SupplierOrderItem.autopart)
            .joinedload(AutoPart.brand),
            joinedload(SupplierOrder.provider),
        )
    )
    result = await session.execute(stmt)
    orders = result.scalars().all()
    sent = 0
    failed = 0

    account = await _get_out_account(session, 'orders_out')
    smtp_kwargs = {}
    if account:
        smtp_kwargs = {
            'smtp_host': account.smtp_host,
            'smtp_port': account.smtp_port,
            'smtp_user': account.email,
            'smtp_password': account.password,
            'from_email': account.email,
            'use_ssl': bool(account.smtp_use_ssl),
        }

    for order in orders:
        provider = order.provider
        to_email = provider.email_contact if provider else None
        if not to_email:
            failed += 1
            continue

        rows = []
        for item in order.items:
            autopart = item.autopart
            brand_name = (
                autopart.brand.name if autopart and autopart.brand else ''
            )
            rows.append(
                {
                    'OEM': autopart.oem_number if autopart else '',
                    'Brand': brand_name,
                    'Name': autopart.name if autopart else '',
                    'Qty': item.quantity,
                    'Price': float(item.price) if item.price else None,
                }
            )

        buffer = BytesIO()
        pd.DataFrame(rows).to_excel(buffer, index=False)
        buffer.seek(0)
        try:
            await asyncio.get_running_loop().run_in_executor(
                None,
                send_email_with_attachment,
                to_email,
                f'Заказ поставщику #{order.id}',
                'Во вложении заказ на поставку.',
                buffer.getvalue(),
                f'supplier_order_{order.id}.xlsx',
                False,
                **smtp_kwargs,
            )
            order.status = SUPPLIER_ORDER_STATUS.SENT
            order.sent_at = datetime.now(timezone.utc)
            sent += 1
        except Exception as exc:
            logger.error(
                'Failed to send supplier order %s: %s',
                order.id,
                exc,
                exc_info=True,
            )
            order.status = SUPPLIER_ORDER_STATUS.ERROR
            failed += 1

    await session.commit()
    return {'sent': sent, 'failed': failed}


async def send_scheduled_supplier_orders(
        session: AsyncSession
) -> Dict[str, int]:
    now = datetime.now(timezone.utc)
    day_key = {
        0: 'mon',
        1: 'tue',
        2: 'wed',
        3: 'thu',
        4: 'fri',
        5: 'sat',
        6: 'sun',
    }[now.weekday()]
    time_key = now.strftime('%H:%M')

    providers_stmt = select(Provider).where(
        Provider.order_schedule_enabled.is_(True)
    )
    providers = (await session.execute(providers_stmt)).scalars().all()

    eligible_provider_ids = []
    for provider in providers:
        days = provider.order_schedule_days or []
        times = provider.order_schedule_times or []
        if days and day_key not in days:
            continue
        if times and time_key not in times:
            continue
        eligible_provider_ids.append(provider.id)

    if not eligible_provider_ids:
        return {'sent': 0, 'failed': 0}

    orders_stmt = (
        select(SupplierOrder.id)
        .where(
            SupplierOrder.provider_id.in_(eligible_provider_ids),
            SupplierOrder.status == SUPPLIER_ORDER_STATUS.NEW,
        )
    )
    order_ids = [row[0] for row in (await session.execute(orders_stmt)).all()]
    return await send_supplier_orders(session, order_ids)


def cleanup_order_reports(days: int = ORDERS_RETENTION_DAYS) -> int:
    if days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    if not os.path.isdir(ORDERS_REPORT_DIR):
        return 0
    removed = 0
    for root, _, files in os.walk(ORDERS_REPORT_DIR):
        for name in files:
            path = os.path.join(root, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed += 1
            except Exception as exc:
                logger.error('Failed to remove report %s: %s', path, exc)
    return removed
