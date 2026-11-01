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

TORG12_FILENAME_PREFIXES = ("on_naklpost",)

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
    total_with_vat: Optional[float] = None
    # Дата отгрузки/передачи (ДокПодтвОтгр*, ДатаПер): предпочтительнее даты СФ
    shipment_date: Optional[date] = None
    # КНД формы: 1115131 — УПД, 1115125 — УКД
    knd: Optional[str] = None
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


def _extract_shipment_date(root: ET.Element) -> Optional[date]:
    """Дата отгрузки: документ-подтверждение отгрузки, затем дата передачи."""
    for element in root.iter():
        if _local_name(element.tag).startswith("ДокПодтвОтгр"):
            value = _parse_upd_date(
                _first_attr(element, "ДатаДокПодтвОтгр", "ДатаДокОтгр")
            )
            if value:
                return value
    for element in root.iter():
        if _local_name(element.tag) == "СвПер":
            value = _parse_upd_date(_first_attr(element, "ДатаПер"))
            if value:
                return value
    return None


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
    for element in root.iter():
        if _local_name(element.tag) == "Документ":
            document.knd = _first_attr(element, "КНД")
            break
    document.shipment_date = _extract_shipment_date(root)
    header_total = None
    for element in root.iter():
        if _local_name(element.tag) == "ВсегоОпл":
            header_total = _to_float(_first_attr(element, "СтТовУчНалВсего"))
            break

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

    if header_total is None:
        line_totals = [
            item.total_with_vat
            for item in document.items
            if item.total_with_vat is not None
        ]
        header_total = round(sum(line_totals), 2) if line_totals else None
    document.total_with_vat = header_total

    logger.debug(
        "Parsed UPD XML: number=%s date=%s items=%s without_article=%s",
        document.document_number,
        document.document_date,
        len(document.items),
        document.items_without_article,
    )
    return document


@dataclass(slots=True)
class DocumentParties:
    seller_inn: Optional[str] = None
    seller_kpp: Optional[str] = None
    buyer_inn: Optional[str] = None
    buyer_kpp: Optional[str] = None


_SELLER_TAGS = ("СвПрод", "Продавец", "Поставщик", "Исполнитель")
_BUYER_TAGS = ("СвПокуп", "Покупатель", "Плательщик", "Заказчик")


def _party_requisites(
    root: ET.Element, tags: tuple[str, ...]
) -> tuple[Optional[str], Optional[str]]:
    for tag in tags:
        for element in root.iter():
            if _local_name(element.tag) != tag:
                continue
            for child in element.iter():
                child_tag = _local_name(child.tag)
                if child_tag == "СвЮЛУч":
                    return _first_attr(child, "ИННЮЛ"), _first_attr(child, "КПП")
                if child_tag == "СвИП":
                    return _first_attr(child, "ИННФЛ"), None
            # акты и ТОРГ-12: ИНН может лежать прямо в атрибутах
            for child in element.iter():
                inn = _first_attr(child, "ИННЮЛ", "ИННФЛ", "ИНН")
                if inn:
                    return inn, _first_attr(child, "КПП")
    return None, None


def extract_document_parties(payload: bytes) -> Optional[DocumentParties]:
    """Продавец и покупатель из УПД/УКД/КСЧФ (СвПрод/СвПокуп).

    В отличие от parse_upd_xml не требует блока СвСчФакт, поэтому подходит и
    для корректировочных документов (УКД, КСЧФ), у которых другая шапка.
    None — если это не XML или реквизитов сторон нет.
    """
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None
    seller_inn, seller_kpp = _party_requisites(root, _SELLER_TAGS)
    buyer_inn, buyer_kpp = _party_requisites(root, _BUYER_TAGS)
    if not (seller_inn or buyer_inn):
        return None
    return DocumentParties(seller_inn, seller_kpp, buyer_inn, buyer_kpp)


def parse_correction_header(
    payload: bytes,
) -> Optional[tuple[Optional[str], Optional[date]]]:
    """Номер и дата УКД/КСЧФ из шапки СвКСчФ (5.01–5.03).

    В 5.03 атрибуты называются НомерДок/ДатаДок, в более ранних —
    НомерКСчФ/ДатаКСчФ. None — если шапки нет.
    """
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None
    for element in root.iter():
        if _local_name(element.tag) not in {"СвКСчФ", "СвКСчФакт"}:
            continue
        number = _first_attr(element, "НомерДок", "НомерКСчФ")
        header_date = _parse_upd_date(
            _first_attr(element, "ДатаДок", "ДатаКСчФ")
        )
        if number or header_date:
            return number, header_date
    return None


# --------------------------------------------------------------------------
# Корректировки (УКД/КСЧФ), ТОРГ-12, акты, акты о расхождениях при приёмке
# --------------------------------------------------------------------------


@dataclass(slots=True)
class CorrectionRow:
    line_number: Optional[int]
    original_line_number: Optional[int]
    name: Optional[str]
    article: Optional[str]
    quantity_before: Optional[int]
    quantity_after: Optional[int]
    price_before: Optional[float]
    price_after: Optional[float]
    total_before: Optional[float]
    total_after: Optional[float]
    total_delta: Optional[float]


@dataclass(slots=True)
class CorrectionDocument:
    number: Optional[str] = None
    date: Optional[date] = None
    original_number: Optional[str] = None
    original_date: Optional[date] = None
    total_delta: Optional[float] = None
    parties: Optional[DocumentParties] = None
    rows: list[CorrectionRow] = field(default_factory=list)


def _child(element: ET.Element, name: str) -> Optional[ET.Element]:
    for child in element:
        if _local_name(child.tag) == name:
            return child
    return None


def _num_attrs(element: Optional[ET.Element], *names: str) -> Optional[float]:
    if element is None:
        return None
    return _to_float(_first_attr(element, *names))


def parse_correction_xml(payload: bytes) -> Optional[CorrectionDocument]:
    """УКД/КСЧФ (ON_NKORSCHFDOPPR): строки «до/после» и исходный документ.

    Исходный УПД берётся из СчФ (НомерСчФ/ДатаСчФ), запасной вариант —
    основание СодФХЖ3/ПередатДокум (НомОсн/ДатаОсн). Количество, цена и
    стоимость «до/после» читаются из СведТов/ТаблКСчФ.
    """
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None
    header = None
    for element in root.iter():
        if _local_name(element.tag) in {"СвКСчФ", "СвКСчФакт"}:
            header = element
            break
    if header is None:
        return None
    doc = CorrectionDocument(
        number=_first_attr(header, "НомерКСчФ", "НомерДок"),
        date=_parse_upd_date(_first_attr(header, "ДатаКСчФ", "ДатаДок")),
        parties=extract_document_parties(payload),
    )
    for element in root.iter():
        tag = _local_name(element.tag)
        if tag == "СчФ" and doc.original_number is None:
            doc.original_number = _first_attr(element, "НомерСчФ", "НомерДок")
            doc.original_date = _parse_upd_date(
                _first_attr(element, "ДатаСчФ", "ДатаДок")
            )
        elif tag == "ПередатДокум" and doc.original_number is None:
            doc.original_number = _first_attr(element, "НомОсн")
            doc.original_date = _parse_upd_date(_first_attr(element, "ДатаОсн"))
        elif tag == "ВсегоУм" and doc.total_delta is None:
            doc.total_delta = _num_attrs(
                element, "СтТовУчНалВсего", "СтоимУм", "СтТовУчНал"
            )
    for item_el in root.iter():
        if _local_name(item_el.tag) != "СведТов":
            continue
        total_el = _child(item_el, "СтТовУчНал")
        article = None
        for dop in item_el.iter():
            if _local_name(dop.tag) == "ДопСведТов":
                article = _first_attr(dop, "КодТов", "АртикулТов")
                if article:
                    break
        doc.rows.append(
            CorrectionRow(
                line_number=_to_int(_first_attr(item_el, "НомСтр")),
                original_line_number=_to_int(
                    _first_attr(item_el, "ПорНомТовВСЧФ")
                ),
                name=_first_attr(item_el, "НаимТов"),
                article=article,
                quantity_before=_to_int(_first_attr(item_el, "КолТовДо")),
                quantity_after=_to_int(_first_attr(item_el, "КолТовПосле")),
                price_before=_to_float(_first_attr(item_el, "ЦенаТовДо")),
                price_after=_to_float(_first_attr(item_el, "ЦенаТовПосле")),
                total_before=_num_attrs(total_el, "СтоимДоИзм"),
                total_after=_num_attrs(total_el, "СтоимПослеИзм"),
                total_delta=_num_attrs(total_el, "СтоимУм"),
            )
        )
    return doc


# ТОРГ-12 (ON_NAKLPOST…): без реальных образцов схема читается терпимо — по
# набору возможных имён атрибутов; не распознанное даёт пустой результат.
_TORG12_NUMBER_ATTRS = ("НомерДок", "НомТН", "НомерТН", "НомТТН", "НомНакл")
_TORG12_DATE_ATTRS = ("ДатаДок", "ДатаТН", "ДатаТТН", "ДатаНакл")
_TORG12_ROW_QTY = ("КолТов", "КолТовар", "Количество")
_TORG12_ROW_TOTAL = ("СтТовУчНал", "СтоимУчНал", "СтоимостьВсего", "СуммаСНДС")


def parse_torg12_xml(payload: bytes) -> UpdXmlDocument:
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise ValueError(f"Некорректный XML ТОРГ-12: {exc}") from exc
    doc = UpdXmlDocument()
    for element in root.iter():
        number = _first_attr(element, *_TORG12_NUMBER_ATTRS)
        header_date = _parse_upd_date(_first_attr(element, *_TORG12_DATE_ATTRS))
        if number and header_date:
            doc.document_number, doc.document_date = number, header_date
            break
    parties = extract_document_parties(payload)
    if parties is not None:
        doc.seller_inn = parties.seller_inn
    for item_el in root.iter():
        quantity = _to_int(_first_attr(item_el, *_TORG12_ROW_QTY))
        if quantity is None or not _first_attr(item_el, "НаимТов", "НаимТовар"):
            continue
        article = _extract_article(item_el) or _first_attr(
            item_el, "Артикул", "КодТовара"
        )
        total = _to_float(_first_attr(item_el, *_TORG12_ROW_TOTAL))
        price = _to_float(_first_attr(item_el, "ЦенаТов", "Цена"))
        if price is None and total is not None and quantity:
            price = round(total / quantity, 2)
        if not article:
            doc.items_without_article += 1
        doc.items.append(
            UpdXmlItem(
                line_number=_to_int(_first_attr(item_el, "НомСтр", "НомПП")),
                name=_first_attr(item_el, "НаимТов", "НаимТовар"),
                oem_number=article,
                quantity=quantity,
                price=price,
                total_with_vat=total,
                gtd_code=_extract_customs_info(item_el)[0],
            )
        )
    totals = [i.total_with_vat for i in doc.items if i.total_with_vat is not None]
    doc.total_with_vat = round(sum(totals), 2) if totals else None
    return doc


def parse_discrepancy_act_xml(payload: bytes) -> Optional[dict]:
    """Акт о расхождениях при приёмке (DP_PRIRASXPRIN…): сырой разбор.

    Точные имена атрибутов в блоках РезПрием/СопрДок заранее неизвестны,
    поэтому сохраняем их как есть (по блокам ПоДокум/ПоФакту/Брак), чтобы
    человек видел расхождения, и ничего не проводим автоматически.
    """
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return None
    details: dict = {"result": [], "basis": [], "document": {}}
    for element in root.iter():
        tag = _local_name(element.tag)
        if tag == "ИдентДокПР":
            details["document"] = {k: v for k, v in element.attrib.items()}
        elif tag == "СопрДок":
            details["basis"].append({k: v for k, v in element.attrib.items()})
        elif tag == "РезПрием":
            block: dict = {}
            for child in element.iter():
                child_tag = _local_name(child.tag)
                if child_tag in {"ПоДокум", "ПоФакту", "Брак"} and child.attrib:
                    block.setdefault(child_tag, []).append(dict(child.attrib))
            if block:
                details["result"].append(block)
    parties = extract_document_parties(payload)
    if parties is not None:
        details["seller_inn"] = parties.seller_inn
        details["buyer_inn"] = parties.buyer_inn
    return details
