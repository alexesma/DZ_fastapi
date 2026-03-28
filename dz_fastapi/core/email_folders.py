import re
from typing import Iterable

DEFAULT_IMAP_FOLDER = 'INBOX'


def normalize_imap_folder(
    value: str | None,
    default: str = DEFAULT_IMAP_FOLDER,
) -> str:
    text = str(value or '').strip()
    if not text:
        return default
    if text.upper() == DEFAULT_IMAP_FOLDER:
        return DEFAULT_IMAP_FOLDER
    return text


def parse_imap_additional_folders(value) -> list[str]:
    if value in (None, ''):
        return []
    if isinstance(value, str):
        candidates: Iterable[str] = re.split(r'[\n,;]+', value)
    elif isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = [value]

    folders: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        folder = str(candidate or '').strip()
        if not folder:
            continue
        folder = normalize_imap_folder(folder)
        key = folder.casefold()
        if key in seen:
            continue
        seen.add(key)
        folders.append(folder)
    return folders


def resolve_imap_folders(
    primary_folder: str | None,
    additional_folders=None,
    default: str = DEFAULT_IMAP_FOLDER,
) -> list[str]:
    folders = [normalize_imap_folder(primary_folder, default=default)]
    folders.extend(parse_imap_additional_folders(additional_folders))

    unique: list[str] = []
    seen: set[str] = set()
    for folder in folders:
        key = folder.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(folder)
    return unique
