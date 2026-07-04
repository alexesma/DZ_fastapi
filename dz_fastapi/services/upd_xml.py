"""Парсер формализованного УПД из Диадока (формат ФНС ON_NSCHFDOPPR).

Поддерживает версии формата 5.01–5.03: номер/дата документа берутся из
СвСчФакт (НомерДок/ДатаДок в 5.03, НомерСчФ/ДатаСчФ в младших версиях),
позиции — из ТаблСчФакт/СведТов, артикул — из ДопСведТов@АртикулТов
(fallback на КодТов). Все обращения к атрибутам защищены: файлы разных
операторов ЭДО заметно различаются в деталях.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger("dz_fastapi")

UPD_FILENAME_PREFIXES = (
    "on_nschfdoppr",  # УПД (СЧФДОП/ДОП)
    "on_nschfdop",
    "on_korschfdoppr",  # УКД
    "on_korschfdop",
)

_UPD_MARKERS = ("СвСчФакт", "ТаблСчФакт")
_MARKING_CODE_ATTRS = (
    "КИЗ",
    "НомСредИдентТов",
    "НомСредИденТов",
    "ИдентТов",
    "ИдентТрансУпак",
    "НомУпак",
)
_MARKING_CODE_WHITESPACE_RE = re.compile(r"[\r\n\t ]+")


@dataclass(slots=True)
class UpdXmlItem:
    line_number: Optional[int]
    name: Optional[str]
    oem_number: Optional[str]
    quantity: Optional[int]
    price: Optional[float]
    total_with_vat: Optional[float]
    gtd_code: Optional[str] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    marking_codes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class UpdXmlDocument:
    document_number: Optional[str] = None
    document_date: Optional[date] = None
    seller_name: Optional[str] = None
    seller_inn: Optional[str] = None
    items: list[UpdXmlItem] = field(default_factory=list)
    items_without_article: int = 0


def looks_like_upd_xml(payload: bytes, filename: str = "") -> bool:
    """Быстрая проверка: похоже ли содержимое на УПД ФНС."""
    name = str(filename or "").strip().lower()
    if name.endswith(".xml") and name.startswith(UPD_FILENAME_PREFIXES):
        return True
    head = payload[:200_000]
    for marker in _UPD_MARKERS:
        for encoding in ("utf-8", "cp1251"):
            try:
                if marker.encode(encoding) in head:
                    return True
            except UnicodeEncodeError:  # pragma: no cover
                continue
    return False


def _to_int(value: object) -> Optional[int]:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return None
    try:
        return int(round(float(text)))
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> Optional[float]:
    text = str(value or "").strip().replace(",", ".").replace(" ", "")
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_text(value: object) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _parse_upd_date(value: object) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _local_name(tag: object) -> str:
    """Имя тега без namespace. Сравниваем только точно: «ДопСведТов»
    не должен матчиться как «СведТов»."""
    return str(tag or "").rsplit("}", 1)[-1]


def _first_attr(element: ET.Element, *names: str) -> Optional[str]:
    for name in names:
        value = _to_text(element.get(name))
        if value:
            return value
    return None


def _extract_article(item_el: ET.Element) -> Optional[str]:
    # 5.02/5.03: ДопСведТов@АртикулТов; код товара — запасной вариант.
    for dop in item_el.iter():
        if _local_name(dop.tag) == "ДопСведТов":
            article = _first_attr(dop, "АртикулТов", "КодТов")
            if article:
                return article
    return _first_attr(item_el, "АртикулТов", "КодТов")


def _extract_customs_info(
    item_el: ET.Element,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    gtd_code = None
    country_code = None
    country_name = None
    for child in item_el.iter():
        tag = _local_name(child.tag)
        if tag in ("СвДТ", "СвТД"):
            gtd_code = gtd_code or _first_attr(
                child, "НомерДТ", "НомерТД", "НомерГТД"
            )
            country_code = country_code or _first_attr(
                child, "КодПроисх", "КодПроис"
            )
        if tag == "ДопСведТов":
            country_code = country_code or _first_attr(
                child, "КодПроисх", "КодПроис"
            )
            country_name = country_name or _first_attr(
                child, "КрНаимСтрПр", "НаимСтрПр"
            )
    return gtd_code, country_code, country_name


def _normalize_marking_code(value: object) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    # В КИЗ значимыми могут быть спецсимволы GS1, но пробелы/переносы из XML
    # не несут смысла и мешают дедупликации.
    normalized = _MARKING_CODE_WHITESPACE_RE.sub("", text)
    return normalized or None


def _extract_marking_codes(item_el: ET.Element) -> list[str]:
    """Извлекает КИЗ/СИЗ из строки УПД.

    В разных версиях выгрузок операторов ЭДО коды встречаются как текст
    тега НомСредИдентТов или как атрибуты внутри него. Берём оба варианта и
    дедуплицируем без изменения порядка.
    """
    codes: list[str] = []
    seen: set[str] = set()
    for child in item_el.iter():
        if _local_name(child.tag) != "НомСредИдентТов":
            continue
        candidates: list[object] = [child.text]
        candidates.extend(child.get(attr_name) for attr_name in _MARKING_CODE_ATTRS)
        if not any(str(candidate or "").strip() for candidate in candidates):
            candidates.append("".join(child.itertext()))
        for candidate in candidates:
            code = _normalize_marking_code(candidate)
            if code and code not in seen:
                seen.add(code)
                codes.append(code)
    return codes


def _extract_seller(root: ET.Element) -> tuple[Optional[str], Optional[str]]:
    for element in root.iter():
        if _local_name(element.tag) != "СвПрод":
            continue
        for child in element.iter():
            child_tag = _local_name(child.tag)
            if child_tag == "СвЮЛУч":
                return (
                    _first_attr(child, "НаимОрг"),
                    _first_attr(child, "ИННЮЛ"),
                )
            if child_tag == "СвИП":
                return (
                    _first_attr(child, "ФИО"),
                    _first_attr(child, "ИННФЛ"),
                )
        break
    return None, None


def parse_upd_xml(payload: bytes) -> UpdXmlDocument:
    """Разбирает УПД XML в структуру документа с позициями.

    Бросает ValueError, если XML не похож на УПД (нет СвСчФакт).
    """
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise ValueError(f"Некорректный XML УПД: {exc}") from exc

    invoice_header = None
    for element in root.iter():
        if _local_name(element.tag) == "СвСчФакт":
            invoice_header = element
            break
    if invoice_header is None:
        raise ValueError(
            "XML не похож на УПД: не найден блок СвСчФакт"
        )

    document = UpdXmlDocument()
    document.document_number = _first_attr(
        invoice_header, "НомерДок", "НомерСчФ"
    )
    document.document_date = _parse_upd_date(
        _first_attr(invoice_header, "ДатаДок", "ДатаСчФ")
    )
    document.seller_name, document.seller_inn = _extract_seller(root)

    for item_el in root.iter():
        if _local_name(item_el.tag) != "СведТов":
            continue
        article = _extract_article(item_el)
        quantity = _to_int(_first_attr(item_el, "КолТов"))
        price = _to_float(_first_attr(item_el, "ЦенаТов"))
        total_with_vat = _to_float(
            _first_attr(item_el, "СтТовУчНал", "СтоимостьВсего")
        )
        if price is None and total_with_vat is not None and quantity:
            price = round(total_with_vat / quantity, 2)
        gtd_code, country_code, country_name = _extract_customs_info(
            item_el
        )
        item = UpdXmlItem(
            line_number=_to_int(_first_attr(item_el, "НомСтр")),
            name=_first_attr(item_el, "НаимТов"),
            oem_number=article,
            quantity=quantity,
            price=price,
            total_with_vat=total_with_vat,
            gtd_code=gtd_code,
            country_code=country_code,
            country_name=country_name,
            marking_codes=_extract_marking_codes(item_el),
        )
        if not article:
            document.items_without_article += 1
        document.items.append(item)

    logger.debug(
        "Parsed UPD XML: number=%s date=%s items=%s without_article=%s",
        document.document_number,
        document.document_date,
        len(document.items),
        document.items_without_article,
    )
    return document
