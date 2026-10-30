"""Разбор документов УНФ, которые она присылает обратно в EnterpriseData.

Нужен только для возврата учётных номеров и статусов наших документов
(реализации и поступления). Разбор не зависит от версии формата: имена
элементов сравниваются без пространства имён, а необязательные поля
(``Проведен``, ``ПометкаУдаления``) при отсутствии остаются неизвестными.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Optional
from xml.etree import ElementTree as ET

TRACKED_DOCUMENTS = frozenset({
    "Документ.РеализацияТоваровУслуг",
    "Документ.ПоступлениеТоваровУслуг",
})
# Объекты, которыми владеет DragonZap: их копии из УНФ нам не нужны, поэтому
# сообщение, где есть только они и отслеживаемые документы, можно подтвердить.
# Любой другой тип (например, оприходование, созданное в УНФ) подтверждать нельзя:
# подтверждение сотрёт данные, которые мы не загрузили.
OWNED_OBJECTS = frozenset({
    "Справочник.Контрагенты",
    "Справочник.Договоры",
    "Справочник.Номенклатура",
    "Документ.ЗаказКлиента",
    "Документ.ЗаказПоставщику",
})
_TRUE = {"true", "1", "да"}
_FALSE = {"false", "0", "нет"}
_NUMBER_RE = re.compile(r"^DZ(\d{9})$")


@dataclass
class IncomingDocument:
    object_type: str
    ref: Optional[str]
    number: Optional[str]
    date: Optional[str]
    posted: Optional[bool]
    deleted: Optional[bool]
    comment: Optional[str]

    def dragonzap_id(self) -> Optional[int]:
        """ID нашего документа из номера ``DZ000000009``, если он сохранился."""
        match = _NUMBER_RE.match((self.number or "").strip())
        return int(match.group(1)) if match else None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _local(tag: Any) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _child(node: ET.Element, name: str) -> Optional[ET.Element]:
    for item in node:
        if _local(item.tag) == name:
            return item
    return None


def _text(node: Optional[ET.Element]) -> Optional[str]:
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def _flag(node: ET.Element, *names: str) -> Optional[bool]:
    for name in names:
        value = (_text(_child(node, name)) or "").lower()
        if value in _TRUE:
            return True
        if value in _FALSE:
            return False
    return None


def _document(node: ET.Element) -> IncomingDocument:
    key = _child(node, "КлючевыеСвойства")
    holder = key if key is not None else node
    common = _child(node, "ОбщиеСвойстваОбъектовФормата")
    return IncomingDocument(
        object_type=_local(node.tag),
        ref=_text(_child(holder, "Ссылка")),
        number=_text(_child(holder, "Номер")),
        date=_text(_child(holder, "Дата")),
        posted=_flag(node, "Проведен", "Проведён"),
        deleted=_flag(node, "ПометкаУдаления"),
        comment=_text(_child(common, "Комментарий")) if common is not None else None,
    )


def body_objects(xml_bytes: bytes) -> list[ET.Element]:
    root = ET.fromstring(xml_bytes)
    objects: list[ET.Element] = []
    for child in root:
        name = _local(child.tag)
        if name == "Header":
            continue
        if name == "Body":
            objects.extend(item for item in child if isinstance(item.tag, str))
        else:  # как в parse_message: объект вне Body тоже считается данными
            objects.append(child)
    return objects


def importable_documents(xml_bytes: bytes) -> Optional[list[IncomingDocument]]:
    """Документы для сопоставления или ``None``, если сообщение подтверждать нельзя."""
    documents: list[IncomingDocument] = []
    for node in body_objects(xml_bytes):
        name = _local(node.tag)
        if name in TRACKED_DOCUMENTS:
            documents.append(_document(node))
        elif name not in OWNED_OBJECTS:
            return None
    return documents
