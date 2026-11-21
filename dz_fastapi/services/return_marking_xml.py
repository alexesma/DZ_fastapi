"""Поэкземплярная сверка строк возврата с титулами УПД и УКД."""

import hashlib
from decimal import Decimal, InvalidOperation
from xml.etree import ElementTree as ET

import aiofiles
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import ReturnDocumentStatus, ReturnItem, ShipmentDocumentItem
from dz_fastapi.models.shipment_correction import ShipmentCorrection, ShipmentCorrectionItem
from dz_fastapi.services.marking_code_parse import return_unit_codes


class ReturnMarkingBlocked(ValueError):
    pass


def _units(line, group_name):
    codes = []
    for group in line.findall(f".//{{*}}{group_name}"):
        units = group.findall("{*}КИЗ")
        if len(group) != len(units) or group.attrib or not units:
            raise ReturnMarkingBlocked("В документе ожидаются индивидуальные КИ экземпляров")
        codes.extend(unit.text for unit in units)
    return codes


def _quantity(line, name):
    try:
        return Decimal(line.get(name) or "")
    except InvalidOperation as exc:
        raise ReturnMarkingBlocked("В XML отсутствует количество маркированной строки") from exc


async def _saved_xml(document):
    try:
        async with aiofiles.open(document.local_file_path, "rb") as file:
            content = await file.read()
        if (
            document.content_sha256
            and hashlib.sha256(content).hexdigest() != document.content_sha256
        ):
            raise ReturnMarkingBlocked("Контрольная сумма сохранённого титула Диадока изменилась")
        return ET.fromstring(content)
    except (OSError, TypeError, ET.ParseError, LookupError) as exc:
        raise ReturnMarkingBlocked(
            "Не удалось прочитать исходный XML Диадока для сверки КИ"
        ) from exc


def _original_line(root, number, attr):
    matches = [line for line in root.findall(".//{*}СведТов") if line.get(attr) == str(number)]
    if len(matches) != 1:
        raise ReturnMarkingBlocked("Не найдена однозначная исходная строка с КИ в XML Диадока")
    return matches[0]


def _remaining(before, removed):
    removed_set = set(removed)
    if not removed_set.issubset(before):
        raise ReturnMarkingBlocked("Возвращаемый КИ отсутствует в остатке исходной строки УПД")
    return [code for code in before if code not in removed_set]


async def customer_return_marking_states(session, document, *, lock=False):
    source_upd = document.source_diadoc_outgoing_document
    marked = []
    for item in document.items:
        source = item.shipment_item
        allocations = getattr(source, "allocations", [])
        allocated = [code for row in allocations for code in row.marking_codes or []]
        if item.marking_codes or allocated:
            marked.append((item, source, allocated))
    source_expected = (getattr(source_upd, "metadata_json", None) or {}).get("gis_mt_expected")
    if not marked and not source_expected:
        return {}
    if not source_upd or not document.shipment_document:
        raise ReturnMarkingBlocked("Для сверки КИ требуется исходная реализация и УПД")
    if document.status != ReturnDocumentStatus.CONFIRMED:
        raise ReturnMarkingBlocked("Для маркированного УКД требуется подтверждённый возврат")
    if lock:
        await session.execute(
            select(ShipmentDocumentItem.id)
            .where(ShipmentDocumentItem.document_id == document.shipment_document_id)
            .order_by(ShipmentDocumentItem.id)
            .with_for_update()
        )
    root = await _saved_xml(source_upd)
    numbers = {row.id: n for n, row in enumerate(document.shipment_document.items, 1)}
    marked_ids = {item.id for item, _, _ in marked}
    for item in document.items:
        if item.id not in marked_ids:
            line = _original_line(root, numbers.get(item.shipment_item_id), "НомСтр")
            if _units(line, "НомСредИдентТов"):
                raise ReturnMarkingBlocked("В возврате отсутствуют сканы маркированной строки УПД")
    states = {}
    seen_lines = set()
    seen_source_codes = set()
    seen_returned_codes = set()
    for item, source, allocated in marked:
        number = numbers.get(item.shipment_item_id)
        if not source or not number or number in seen_lines:
            raise ReturnMarkingBlocked("Маркированной строке возврата нужна отдельная строка УПД")
        seen_lines.add(number)
        line = _original_line(root, number, "НомСтр")
        before = return_unit_codes(_units(line, "НомСредИдентТов"), source.quantity)
        expected = return_unit_codes(allocated, source.quantity)
        if seen_source_codes.intersection(before):
            raise ReturnMarkingBlocked("КИ повторяется в нескольких строках исходной УПД")
        seen_source_codes.update(before)
        if _quantity(line, "КолТов") != len(before) or set(before) != set(expected):
            raise ReturnMarkingBlocked("Состав исходной строки УПД отличается от отгрузки")
        other_correction = await session.scalar(
            select(ShipmentCorrectionItem.id)
            .join(ShipmentCorrection)
            .where(
                ShipmentCorrectionItem.shipment_item_id == source.id,
                ShipmentCorrection.diadoc_outgoing_document_id.is_not(None),
            )
            .limit(1)
        )
        if other_correction:
            raise ReturnMarkingBlocked(
                "По строке есть отдельная корректировка реализации, сверьте УКД"
            )
        prior = await session.execute(
            select(ReturnItem, DiadocOutgoingDocument)
            .join(
                DiadocOutgoingDocument,
                DiadocOutgoingDocument.source_id == ReturnItem.return_from_customer_id,
            )
            .where(
                ReturnItem.shipment_item_id == source.id,
                ReturnItem.return_from_customer_id != document.id,
                DiadocOutgoingDocument.source_type == "return_from_customer",
                DiadocOutgoingDocument.environment == source_upd.environment,
            )
            .order_by(DiadocOutgoingDocument.id)
        )
        for previous, outgoing in prior:
            if outgoing.status in {"rejected", "revoked"}:
                continue
            if outgoing.is_draft or outgoing.status != "completed":
                raise ReturnMarkingBlocked("Ожидается завершение предыдущего УКД по этой строке")
            removed = return_unit_codes(previous.marking_codes or [], previous.quantity)
            after = _remaining(before, removed)
            _verify_line(
                _original_line(await _saved_xml(outgoing), number, "ПорНомТовВСЧФ"), before, after
            )
            before = after
        returned = return_unit_codes(item.marking_codes or [], item.quantity)
        if seen_returned_codes.intersection(returned):
            raise ReturnMarkingBlocked("Возвращаемый КИ повторяется в нескольких строках")
        seen_returned_codes.update(returned)
        after = _remaining(before, returned)
        states[item.id] = dict(
            original_number=number,
            original_codes=before,
            corrected_codes=after,
            quantity_before=len(before),
            quantity_after=len(after),
        )
    return states


def _verify_line(line, before, after):
    for tag, attr, expected in (
        ("НомСредИдентТовДо", "КолТовДо", before),
        ("НомСредИдентТовПосле", "КолТовПосле", after),
    ):
        codes = _units(line, tag)
        if (
            len(expected) != _quantity(line, attr)
            or len(codes) != len(expected)
            or set(codes) != set(expected)
        ):
            raise ReturnMarkingBlocked("Коды или количество в титуле УКД отличаются от возврата")


def verify_return_title(content, states):
    try:
        root = ET.fromstring(content)
    except (ET.ParseError, LookupError, ValueError) as exc:
        raise ReturnMarkingBlocked("Не удалось прочитать итоговый XML УКД") from exc
    marked_lines = {s["original_number"]: s for s in states.values()}
    for number, state in marked_lines.items():
        _verify_line(
            _original_line(root, number, "ПорНомТовВСЧФ"),
            state["original_codes"],
            state["corrected_codes"],
        )
    for row in root.findall(".//{*}СведТов"):
        if row.get("ПорНомТовВСЧФ") not in {str(n) for n in marked_lines} and (
            row.findall(".//{*}НомСредИдентТовДо") or row.findall(".//{*}НомСредИдентТовПосле")
        ):
            raise ReturnMarkingBlocked("Обнаружены лишние маркированные строки в титуле УКД")


def supplier_return_marking_blocker(document):
    for item in document.items:
        receipt = item.supplier_receipt_item
        if item.marking_codes or (
            receipt and (receipt.document_marking_codes or receipt.marking_codes)
        ):
            return (
                "Для возврата по маркированному УПД поставщика требуется УКД от исходного продавца."
            )
    return None
