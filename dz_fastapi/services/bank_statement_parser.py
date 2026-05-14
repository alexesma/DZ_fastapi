"""
Bank statement parsers.

Supported formats:
  - Tochka Bank CSV  (UTF-8 BOM, semicolon-separated)
  - 1CClientBankExchange (.txt, Windows-1251)
  - Alfa-Bank CSV  (cp1251, semicolon-separated)
"""

import csv
import io
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── VAT extraction ─────────────────────────────────────────────────────────────
# Matches patterns like:
#   "НДС 22%, 977.56 руб."  →  977.56
#   "в т.ч. НДС 540.98"     →  540.98
#   "НДС 20% в т.ч. 1666,67 руб." → 1666.67
#   "в т.ч. НДС (20%) 1000,00 руб." → 1000.00
_VAT_PATTERNS = [
    # НДС XX% в т.ч. AMOUNT руб  (rate + в т.ч. + amount)
    re.compile(
        r"НДС\s*\d+\s*%\s*в\s*т\.?\s*ч\.?[^\d]*(\d[\d\s]*[,.]?\d+)\s*руб",
        re.IGNORECASE,
    ),
    # НДС XX%, AMOUNT руб  (rate, amount)
    re.compile(r"НДС\s*\d+\s*%[,\s]+(\d[\d\s]*[,.]?\d+)\s*руб", re.IGNORECASE),
    # в т.ч. НДС (XX%) AMOUNT руб  — parenthesised rate
    re.compile(
        r"НДС\s*\([^\)]+%[^\)]*\)\s*(\d[\d\s]*[,.]?\d+)\s*руб", re.IGNORECASE
    ),
    # в т.ч. НДС AMOUNT руб|ₓ  — plain amount, no rate
    re.compile(r"в\s*т\.?\s*ч\.?\s*НДС\s*(\d[\d\s]*[,.]?\d+)", re.IGNORECASE),
    # НДС AMOUNT руб  (fallback)
    re.compile(r"НДС[^\d%]*(\d[\d\s]*[,.]?\d{2})\s*руб", re.IGNORECASE),
]


def _extract_vat(purpose: str) -> Optional[Decimal]:
    """Extract VAT amount from payment purpose string."""
    if not purpose:
        return None
    for pattern in _VAT_PATTERNS:
        m = pattern.search(purpose)
        if m:
            raw = (
                m.group(1)
                .replace("\xa0", "")
                .replace(" ", "")
                .replace(" ", "")
                .replace(",", ".")
            )
            try:
                val = Decimal(raw)
                if val > 0:
                    return val
            except InvalidOperation:
                pass
    return None


def _parse_decimal(value: str) -> Optional[Decimal]:
    """Parse Russian-formatted decimal: '5 421,00' → Decimal('5421.00')."""
    if not value or not value.strip():
        return None
    cleaned = (
        value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    )
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _parse_date(value: str) -> Optional[date]:
    """Parse DD.MM.YYYY date string."""
    if not value or not value.strip():
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            pass
    return None


def _clean_quotes(value: str) -> str:
    """Remove CSV double-quoting artifacts."""
    v = value.strip().strip('"')
    v = v.replace('""', '"')
    return v


# ── Data transfer object ───────────────────────────────────────────────────────


@dataclass
class ParsedTransaction:
    doc_number: Optional[str]
    doc_date: Optional[date]
    value_date: date
    direction: str  # 'incoming' | 'outgoing'
    amount: Decimal
    vat_amount: Optional[Decimal]
    currency: str
    purpose: Optional[str]
    balance_after: Optional[Decimal]
    counterparty_name: Optional[str]
    counterparty_inn: Optional[str]
    counterparty_kpp: Optional[str]
    counterparty_account: Optional[str]
    counterparty_bank: Optional[str]
    counterparty_bik: Optional[str]


@dataclass
class ParsedStatement:
    format: str
    period_from: Optional[date]
    period_to: Optional[date]
    account_number: Optional[str]
    bank_name: Optional[str]
    bik: Optional[str]
    opening_balance: Optional[Decimal]
    closing_balance: Optional[Decimal]
    total_incoming: Optional[Decimal]
    total_outgoing: Optional[Decimal]
    transactions: List[ParsedTransaction] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


# ── Tochka Bank CSV parser ─────────────────────────────────────────────────────


def _is_tochka_csv(content: bytes) -> bool:
    """Heuristic: UTF-8 BOM + 'Дата проводки' in first line."""
    try:
        head = content[:200].decode("utf-8-sig", errors="ignore")
        return "Дата проводки" in head and ";" in head
    except Exception:
        return False


def parse_tochka_csv(content: bytes) -> ParsedStatement:
    """Parse Tochka Bank semicolon-CSV (UTF-8 BOM)."""
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text), delimiter=";")
    rows = list(reader)

    result = ParsedStatement(
        format="tochka_csv",
        period_from=None,
        period_to=None,
        account_number=None,
        bank_name=None,
        bik=None,
        opening_balance=None,
        closing_balance=None,
        total_incoming=None,
        total_outgoing=None,
    )

    if not rows:
        result.errors.append("Файл пустой")
        return result

    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header)}

    # Mandatory columns
    required = ["Дата проводки", "Направление", "Сумма операции в рублях"]
    missing = [c for c in required if c not in col]
    if missing:
        result.errors.append(f"Отсутствуют колонки: {missing}")
        return result

    opening: Optional[Decimal] = None

    for row_idx, row in enumerate(rows[1:], start=2):
        if not any(c.strip() for c in row):
            continue
        try:

            def g(name: str) -> str:
                idx = col.get(name)
                return (
                    _clean_quotes(row[idx])
                    if idx is not None and idx < len(row)
                    else ""
                )

            direction_raw = g("Направление").lower()
            if "вход" in direction_raw:
                direction = "incoming"
            elif "исход" in direction_raw:
                direction = "outgoing"
            else:
                result.errors.append(
                    f"Строка {row_idx}: неизвестное направление «{direction_raw}»"
                )
                continue

            amount = _parse_decimal(g("Сумма операции в рублях"))
            if amount is None:
                result.errors.append(
                    f"Строка {row_idx}: не удалось распарсить сумму"
                )
                continue

            # value_date: prefer 'Дата зачисления' for incoming, 'Дата списания' for outgoing
            if direction == "incoming":
                vd = _parse_date(g("Дата зачисления")) or _parse_date(
                    g("Дата проводки")
                )
            else:
                vd = _parse_date(g("Дата списания")) or _parse_date(
                    g("Дата проводки")
                )

            if vd is None:
                result.errors.append(
                    f"Строка {row_idx}: не удалось определить дату"
                )
                continue

            # Track date range
            if result.period_from is None or vd < result.period_from:
                result.period_from = vd
            if result.period_to is None or vd > result.period_to:
                result.period_to = vd

            # Balances
            balance_in = _parse_decimal(g("Входящий остаток"))
            balance_out = _parse_decimal(g("Исходящий остаток"))
            if opening is None and balance_in is not None:
                opening = balance_in

            # Counterparty depends on direction
            if direction == "incoming":
                cp_name = g("Наименование плательщика")
                cp_inn = g("ИНН плательщика")
                cp_kpp = g("КПП плательщика")
                cp_account = g("Счет плательщика")
                cp_bank = g("Наименование банка плательщика")
                cp_bik = g("Бик банка плательщика")
                # our account
                if not result.account_number:
                    result.account_number = g("Счет получателя") or None
                    result.bank_name = (
                        g("Наименование банка получателя") or None
                    )
                    result.bik = g("Бик банка получателя") or None
            else:
                cp_name = g("Наименование получателя")
                cp_inn = g("ИНН получателя")
                cp_kpp = g("КПП получателя")
                cp_account = g("Счет получателя")
                cp_bank = g("Наименование банка получателя")
                cp_bik = g("Бик банка получателя")
                if not result.account_number:
                    result.account_number = g("Счет плательщика") or None

            purpose = g("Назначение платежа") or None

            result.transactions.append(
                ParsedTransaction(
                    doc_number=g("Номер документа") or None,
                    doc_date=_parse_date(g("Дата документа")),
                    value_date=vd,
                    direction=direction,
                    amount=amount,
                    vat_amount=_extract_vat(purpose or ""),
                    currency="RUB",
                    purpose=purpose,
                    balance_after=balance_out,
                    counterparty_name=cp_name or None,
                    counterparty_inn=cp_inn or None,
                    counterparty_kpp=cp_kpp or None,
                    counterparty_account=cp_account or None,
                    counterparty_bank=cp_bank or None,
                    counterparty_bik=cp_bik or None,
                )
            )

        except Exception as exc:
            result.errors.append(f"Строка {row_idx}: {exc}")

    result.opening_balance = opening
    if result.transactions:
        last = result.transactions[-1]
        result.closing_balance = last.balance_after
        result.total_incoming = sum(
            t.amount for t in result.transactions if t.direction == "incoming"
        )
        result.total_outgoing = sum(
            t.amount for t in result.transactions if t.direction == "outgoing"
        )

    return result


# ── 1CClientBankExchange parser ────────────────────────────────────────────────


def _is_1c_exchange(content: bytes) -> bool:
    try:
        head = content[:100].decode("cp1251", errors="ignore")
        return "1CClientBankExchange" in head
    except Exception:
        return False


def parse_1c_exchange(content: bytes) -> ParsedStatement:
    """Parse 1CClientBankExchange text format (cp1251)."""
    text = content.decode("cp1251", errors="replace")

    result = ParsedStatement(
        format="1c_exchange",
        period_from=None,
        period_to=None,
        account_number=None,
        bank_name=None,
        bik=None,
        opening_balance=None,
        closing_balance=None,
        total_incoming=None,
        total_outgoing=None,
    )

    def kv(line: str) -> Tuple[str, str]:
        if "=" in line:
            k, _, v = line.partition("=")
            return k.strip(), v.strip()
        return "", ""

    # Split into sections
    sections = re.split(
        r"\n(?=СекцияДокумент|КонецДокумента|СекцияРасчСчет|КонецРасчСчет)",
        text,
    )

    current_doc: dict = {}

    for section in sections:
        lines = section.strip().splitlines()
        if not lines:
            continue

        first = lines[0].strip()

        # Global header
        if first == "1CClientBankExchange":
            for line in lines:
                k, v = kv(line)
                if k == "ДатаНачала":
                    result.period_from = _parse_date(v)
                elif k == "ДатаКонца":
                    result.period_to = _parse_date(v)
                elif k == "РасчСчет":
                    result.account_number = v or None

        # Account balance section
        elif first == "СекцияРасчСчет":
            for line in lines:
                k, v = kv(line)
                if k == "НачальныйОстаток":
                    result.opening_balance = _parse_decimal(v)
                elif k == "КонечныйОстаток":
                    result.closing_balance = _parse_decimal(v)
                elif k == "ВсегоПоступило":
                    result.total_incoming = _parse_decimal(v)
                elif k == "ВсегоСписано":
                    result.total_outgoing = _parse_decimal(v)
                elif k == "РасчСчет" and not result.account_number:
                    result.account_number = v or None

        # Document section
        elif first.startswith("СекцияДокумент"):
            current_doc = {}
            for line in lines[1:]:
                if line.strip() == "КонецДокумента":
                    break
                k, v = kv(line)
                if k:
                    current_doc[k] = v

            amount = _parse_decimal(current_doc.get("Сумма", ""))
            if amount is None:
                result.errors.append(
                    f'Документ №{current_doc.get("Номер", "?")}: нет суммы'
                )
                continue

            # Determine direction: if our account is payer — outgoing
            our_account = result.account_number or ""
            payer_account = current_doc.get(
                "ПлательщикСчет", ""
            ) or current_doc.get("ПлательщикРасчСчет", "")
            receiver_account = current_doc.get(
                "ПолучательСчет", ""
            ) or current_doc.get("ПолучательРасчСчет", "")

            if our_account and payer_account == our_account:
                direction = "outgoing"
                cp_name = current_doc.get(
                    "Получатель1", ""
                ) or current_doc.get("Получатель", "")
                cp_inn = current_doc.get("ПолучательИНН", "")
                cp_kpp = current_doc.get("ПолучательКПП", "")
                cp_account = receiver_account
                cp_bank = current_doc.get("ПолучательБанк1", "")
                cp_bik = current_doc.get("ПолучательБИК", "")
            elif our_account and receiver_account == our_account:
                direction = "incoming"
                cp_name = current_doc.get(
                    "Плательщик1", ""
                ) or current_doc.get("Плательщик", "")
                cp_inn = current_doc.get("ПлательщикИНН", "")
                cp_kpp = (
                    current_doc.get("ПлательщикКПП", "").replace("0", "") or ""
                )
                cp_account = payer_account
                cp_bank = current_doc.get("ПлательщикБанк1", "")
                cp_bik = current_doc.get("ПлательщикБИК", "")
            else:
                # fallback: if ДатаСписано filled → outgoing, ДатаПоступило → incoming
                if current_doc.get("ДатаСписано"):
                    direction = "outgoing"
                    cp_name = current_doc.get("Получатель1", "")
                    cp_inn = current_doc.get("ПолучательИНН", "")
                    cp_kpp = current_doc.get("ПолучательКПП", "")
                    cp_account = receiver_account
                    cp_bank = current_doc.get("ПолучательБанк1", "")
                    cp_bik = current_doc.get("ПолучательБИК", "")
                else:
                    direction = "incoming"
                    cp_name = current_doc.get("Плательщик1", "")
                    cp_inn = current_doc.get("ПлательщикИНН", "")
                    cp_kpp = current_doc.get("ПлательщикКПП", "")
                    cp_account = payer_account
                    cp_bank = current_doc.get("ПлательщикБанк1", "")
                    cp_bik = current_doc.get("ПлательщикБИК", "")

            value_date = (
                _parse_date(current_doc.get("ДатаПоступило", ""))
                or _parse_date(current_doc.get("ДатаСписано", ""))
                or _parse_date(current_doc.get("Дата", ""))
            )
            if value_date is None:
                result.errors.append(
                    f'Документ №{current_doc.get("Номер", "?")}: нет даты'
                )
                continue

            purpose = current_doc.get("НазначениеПлатежа", "") or None

            result.transactions.append(
                ParsedTransaction(
                    doc_number=current_doc.get("Номер") or None,
                    doc_date=_parse_date(current_doc.get("Дата", "")),
                    value_date=value_date,
                    direction=direction,
                    amount=amount,
                    vat_amount=_extract_vat(purpose or ""),
                    currency="RUB",
                    purpose=purpose,
                    balance_after=None,
                    counterparty_name=cp_name or None,
                    counterparty_inn=cp_inn or None,
                    counterparty_kpp=cp_kpp or None,
                    counterparty_account=cp_account or None,
                    counterparty_bank=cp_bank or None,
                    counterparty_bik=cp_bik or None,
                )
            )

    return result


# ── Alfa-Bank CSV parser ───────────────────────────────────────────────────────


def _is_alfabank_csv(content: bytes) -> bool:
    try:
        head = content[:300].decode("cp1251", errors="ignore")
        return "Альфа" in head or "альфа" in head
    except Exception:
        return False


def parse_alfabank_csv(content: bytes) -> ParsedStatement:
    """Parse Alfa-Bank CSV (cp1251, semicolon).
    Note: Alfa-Bank summary CSVs may only contain aggregate data, not row-level transactions.
    Full transaction data requires requesting 'выписка по операциям'.
    """
    text = content.decode("cp1251", errors="replace")
    result = ParsedStatement(
        format="alfabank_csv",
        period_from=None,
        period_to=None,
        account_number=None,
        bank_name="АО Альфа-Банк",
        bik=None,
        opening_balance=None,
        closing_balance=None,
        total_incoming=None,
        total_outgoing=None,
    )

    lines = text.splitlines()

    # Parse header rows (key;;value format or plain text)
    for line in lines:
        parts = line.split(";")
        if not parts:
            continue
        first = parts[0].strip().strip('"')

        if "период" in first.lower() or "за период" in first.lower():
            # "За период 12.05.2026 по 12.05.2026"
            m = re.search(r"(\d{2}\.\d{2}\.\d{4})", first)
            if m:
                result.period_from = _parse_date(m.group(1))
            m2 = re.search(
                r"(\d{2}\.\d{2}\.\d{4}).*по.*(\d{2}\.\d{2}\.\d{4})", first
            )
            if m2:
                result.period_from = _parse_date(m2.group(1))
                result.period_to = _parse_date(m2.group(2))
        elif "зачислений" in first.lower():
            if len(parts) > 1 and parts[1].strip():
                result.total_incoming = _parse_decimal(parts[1])
        elif "транзакций" in first.lower():
            if len(parts) > 1 and parts[1].strip():
                val = _parse_decimal(parts[1])
                if result.total_outgoing is None:
                    result.total_outgoing = val

    if not result.transactions:
        result.errors.append(
            "Альфа-Банк: данный файл содержит только сводку. "
            "Для загрузки транзакций загрузите выписку в формате 1С (kl_to_1c...) или полный CSV."
        )

    return result


# ── Auto-detect and dispatch ───────────────────────────────────────────────────


def detect_and_parse(content: bytes, filename: str = "") -> ParsedStatement:
    """Auto-detect format and parse bank statement."""
    fname_lower = filename.lower()

    # Explicit by filename
    if "kl_to_1c" in fname_lower or fname_lower.endswith(".txt"):
        if _is_1c_exchange(content):
            logger.info("Detected: 1CClientBankExchange")
            return parse_1c_exchange(content)

    if _is_tochka_csv(content):
        logger.info("Detected: Tochka CSV")
        return parse_tochka_csv(content)

    if _is_1c_exchange(content):
        logger.info("Detected: 1CClientBankExchange")
        return parse_1c_exchange(content)

    if _is_alfabank_csv(content):
        logger.info("Detected: Alfa-Bank CSV")
        return parse_alfabank_csv(content)

    # Fallback: try Tochka CSV
    logger.warning(
        "Unknown format, trying Tochka CSV parser for: %s", filename
    )
    result = parse_tochka_csv(content)
    if result.transactions:
        return result

    result.format = "unknown"
    result.errors.insert(0, f"Не удалось определить формат файла: {filename}")
    return result
