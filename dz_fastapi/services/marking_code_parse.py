"""Разбор скана кода маркировки «Честный знак» (DataMatrix, GS1).

Сканер выдаёт полный код: ``01`` GTIN(14) ``21`` серийный номер, затем
``91`` ключ проверки и ``92`` криптохвост, поля разделены символом GS
(0x1D). В личном кабинете, УПД и ГИС МТ код показывается без хвоста:
``01`` + GTIN + ``21`` + серийный номер — это КИ. Разбор нужен, чтобы из
любого скана получить КИ и не зависеть от того, дошёл ли до нас GS и какая
раскладка была включена при сканировании.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_SEPARATORS = re.compile(r"[\x1d\r\n]+")
_SYMBOLOGY_PREFIX = re.compile(r"^\](?:d2|C1|Q3)")
_GTIN_RE = re.compile(r"^01(\d{14})21")
_CYRILLIC = re.compile(r"[А-Яа-яЁё№]")
# Типичная длина серийного номера ЧЗ; нужна, только если GS потерян.
DEFAULT_SERIAL_LENGTH = 13
KEY_LENGTH = 4

_RU_LOWER = "йцукенгшщзхъфывапролджэячсмитьбюё"
_EN_LOWER = "qwertyuiop[]asdfghjkl;'zxcvbnm,.`"
_RU_UPPER_TO_EN = "ЙЦУКЕНГШЩЗФЫВАПРОЛДЯЧСМИТЬ"
_EN_UPPER = "QWERTYUIOPASDFGHJKLZXCVBNM"
# Русская раскладка заменила знаки латинской: на клавише «/» стоит «.»,
# на «shift+7» — «?», на «shift+3» — «№» и т.д.
_LAYOUT_TO_LATIN = {
    **dict(zip(_RU_LOWER, _EN_LOWER)),
    **dict(zip(_RU_UPPER_TO_EN, _EN_UPPER)),
    "Х": "{", "Ъ": "}", "Ж": ":", "Э": '"', "Б": "<", "Ю": ">", "Ё": "~",
    ".": "/", ",": "?", "№": "#", ";": "$", ":": "^", "?": "&", '"': "@",
}
_LAYOUT_TABLE = str.maketrans(_LAYOUT_TO_LATIN)


@dataclass
class ParsedMarkingCode:
    ki: str
    gtin: str
    serial: str
    key: Optional[str] = None
    crypto: Optional[str] = None
    layout_fixed: bool = False

    @property
    def has_crypto_tail(self) -> bool:
        return bool(self.crypto)


def fix_russian_layout(text: str) -> tuple[str, bool]:
    """Возвращает латиницу, если скан набран при включённой русской раскладке."""
    if not _CYRILLIC.search(text):
        return text, False
    return text.translate(_LAYOUT_TABLE), True


def _split_tail(segment: str) -> tuple[str, str]:
    """Серийный номер и остаток, если GS между ними потерян."""
    if len(segment) > DEFAULT_SERIAL_LENGTH and segment[
        DEFAULT_SERIAL_LENGTH:DEFAULT_SERIAL_LENGTH + 2
    ] == "91":
        return segment[:DEFAULT_SERIAL_LENGTH], segment[DEFAULT_SERIAL_LENGTH:]
    return segment, ""


def parse_marking_code(raw: object) -> Optional[ParsedMarkingCode]:
    """Разбирает скан; ``None`` — это не код маркировки с GTIN и серийным номером."""
    text = str(raw or "").strip()
    if not text:
        return None
    text = _SYMBOLOGY_PREFIX.sub("", text)
    text, layout_fixed = fix_russian_layout(text)
    segments = [item for item in _SEPARATORS.split(text) if item]
    if not segments:
        return None

    head = _GTIN_RE.match(segments[0])
    if head is None:
        return None
    serial_part = segments[0][head.end():]
    serial, tail = _split_tail(serial_part)
    if not serial:
        return None
    rest = ([tail] if tail else []) + segments[1:]

    key = crypto = None
    for item in rest:
        if item.startswith("91") and key is None:
            key = item[2:2 + KEY_LENGTH]
            leftover = item[2 + KEY_LENGTH:]
            if leftover.startswith("92"):
                crypto = leftover[2:]
        elif item.startswith(("92", "93")) and crypto is None:
            crypto = item[2:]
    gtin = head.group(1)
    return ParsedMarkingCode(
        ki=f"01{gtin}21{serial}",
        gtin=gtin,
        serial=serial,
        key=key,
        crypto=crypto,
        layout_fixed=layout_fixed,
    )


def marking_code_ki(raw: object) -> Optional[str]:
    parsed = parse_marking_code(raw)
    return parsed.ki if parsed else None
