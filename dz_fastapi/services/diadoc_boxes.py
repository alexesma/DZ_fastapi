"""Сопоставление идентификаторов ящиков Диадока в разных формах.

Один и тот же ящик встречается как ``47509ad9892e4198a94944bfe92b5367@diadoc.ru``
(во входящих документах), как GUID с дефисами (в списке контрагентов) и как
32 hex-символа. Привязка хранит то, что вернул интерфейс, поэтому сравнивать
нужно по всем формам сразу.
"""

from __future__ import annotations

import re
from typing import Any

_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def diadoc_box_variants(value: object) -> set[str]:
    """Все записи одного ящика в нижнем регистре; пусто, если это не ящик."""
    text = str(value or "").strip().lower()
    if not text:
        return set()
    variants = {text}
    compact = text.split("@", 1)[0].replace("-", "")
    if _HEX32.match(compact):
        guid = (
            f"{compact[0:8]}-{compact[8:12]}-{compact[12:16]}-"
            f"{compact[16:20]}-{compact[20:32]}"
        )
        variants.update({compact, guid, f"{compact}@diadoc.ru"})
    return variants


def flatten_counteragent(row: dict[str, Any]) -> dict[str, Any]:
    """Достаёт название, ИНН, КПП и ящик из вложенной ``Organization``."""
    org = row.get("Organization")
    if not isinstance(org, dict):
        return row
    boxes = org.get("Boxes") or []
    box = boxes[0] if boxes and isinstance(boxes[0], dict) else {}
    flat = dict(row)
    for key, value in (
        ("FullName", org.get("FullName")),
        ("ShortName", org.get("ShortName")),
        ("Inn", org.get("Inn")),
        ("Kpp", org.get("Kpp")),
        ("BoxId", box.get("BoxId")),
        ("BoxIdGuid", box.get("BoxIdGuid")),
    ):
        if value and not flat.get(key):
            flat[key] = value
    status = row.get("CurrentStatus")
    if isinstance(status, str) and not flat.get("CurrentStatusNamedId"):
        flat["CurrentStatusNamedId"] = status
    return flat
