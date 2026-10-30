"""Обмен с 1С:УНФ через универсальный формат EnterpriseData.

Транспорт — FTP: 1С сама подключается к нашему FTP-серверу, кладёт свои
сообщения и забирает наши. Контейнер `ftp` и контейнеры приложения делят один
том, поэтому здесь мы работаем с файлами напрямую, а не по протоколу FTP.

Имена файлов: ``Message_<Отправитель>_<Получатель>.zip``.
От 1С к нам — ``Message_НФ_ДЗ.zip``, наш ответ — ``Message_ДЗ_НФ.zip``.

Первый шаг протокола — рукопожатие: 1С присылает заголовок без данных, где
перечисляет поддерживаемые версии формата и типы объектов. Мы обязаны ответить
таким же заголовком, иначе 1С считает, что корреспондент не отвечает, и обмен
не двигается.

Ручная пробная отправка реализации и поступления формирует тело исходящего
сообщения. Автоматическая выгрузка очереди и разбор входящих данных пока не
включены. На приём типы не объявлены: неподдержанные сообщения с данными не
подтверждаются.
"""

import base64
import fcntl
import json
import logging
import os
import re
import uuid
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Optional
from xml.etree import ElementTree as ET

from dz_fastapi.core.time import now_moscow
from dz_fastapi.services.one_c_enterprise_data_export import DATA_NS
from dz_fastapi.services.one_c_enterprise_data_import import importable_documents

logger = logging.getLogger("dz_fastapi")

MSG_NS = "http://www.1c.ru/SSL/Exchange/Message"
FORMAT_URI = "http://v8.1c.ru/edi/edi_stnd/EnterpriseData"
EXCHANGE_PLAN = "СинхронизацияДанныхЧерезУниверсальныйФормат"

# Коды узлов заданы при настройке синхронизации в 1С
OUR_NODE_CODE = "ДЗ"
OUR_NODE_NAME = "DragonZap"
PEER_NODE_CODE = "НФ"
# 1С с включённой «Транслитерацией имени» использует латиницу только в
# именах файлов. В XML коды узлов остаются кириллическими.
OUR_NODE_FILE_CODE = "DZ"
PEER_NODE_FILE_CODE = "NF"

# Версия формата по умолчанию. Реально используем ту, которую 1С объявила
# в рукопожатии, — эта только запасная.
DEFAULT_FORMAT_VERSION = "1.22"

# Типы объектов, которые мы объявляем 1С.
#
# SENDING — что МЫ готовы отправлять в 1С. Объявлять безопасно: это лишь
# разрешение, обязанности слать немедленно нет. Без непустого списка 1С не
# может настроить правила обмена и останавливает мастер на шаге «правила
# отправки и получения» с требованием «получить параметры из DragonZap».
#
# RECEIVING — что мы готовы ПРИНИМАТЬ от 1С. Пока пусто НАМЕРЕННО: разбор
# входящих данных не реализован, а объявив приём, мы заставим 1С прислать
# объекты и пометить их доставленными — то есть потеряем их. Заполняется по
# мере готовности разбора.
#
# Объявлять можно только то, что поддерживает встречная сторона: список ниже
# сверен с реальным рукопожатием 1С; окончательное пересечение делаем в ответе.
DEFAULT_SENDING: tuple[str, ...] = (
    "Документ.РеализацияТоваровУслуг",
    "Документ.ПоступлениеТоваровУслуг",
    "Документ.ЗаказКлиента",
    "Документ.ЗаказПоставщику",
    "Справочник.Контрагенты",
    "Справочник.Договоры",
    "Справочник.Номенклатура",
)
DEFAULT_RECEIVING: tuple[str, ...] = ()


def _types_from_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Состав объектов можно переопределить через переменную окружения.

    Нужно при отладке: чтобы попросить у 1С образец документа, приём
    включается на время, без пересборки и передеплоя образа.
    Значение — имена через запятую, пустая строка означает «ничего».
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return tuple(part.strip() for part in raw.split(",") if part.strip())


SUPPORTED_SENDING: tuple[str, ...] = _types_from_env(
    "ONE_C_ED_SENDING", DEFAULT_SENDING
)
SUPPORTED_RECEIVING: tuple[str, ...] = _types_from_env(
    "ONE_C_ED_RECEIVING", DEFAULT_RECEIVING
)

STATE_FILE_NAME = ".dz_enterprise_data_state.json"
LOCK_FILE_NAME = ".dz_enterprise_data.lock"
_MESSAGE_RE = re.compile(
    r"^Message_(?P<from>[^_]+)_(?P<to>[^_.]+)\.(?P<ext>zip|xml)$",
    re.IGNORECASE,
)


def get_exchange_dir() -> str:
    return os.getenv("ONE_C_EXCHANGE_DIR", "/app/onec_exchange")


@dataclass
class IncomingMessage:
    """Разобранное сообщение обмена от 1С."""

    from_code: str
    to_code: str
    message_no: int
    received_no: int
    format_versions: list[str] = field(default_factory=list)
    format_uri: Optional[str] = None
    peer_node_guid: Optional[str] = None
    peer_node_name: Optional[str] = None
    peer_node_prefix: Optional[str] = None
    object_types: dict[str, dict[str, bool]] = field(default_factory=dict)
    has_body: bool = False
    body_types: list[str] = field(default_factory=list)
    file_name: str = ""

    @property
    def is_handshake(self) -> bool:
        """Рукопожатие — сообщение без данных."""
        return not self.has_body


def _text(node: Optional[ET.Element]) -> Optional[str]:
    if node is None or node.text is None:
        return None
    return node.text.strip()


def _b64_name(value: str) -> str:
    return base64.b64encode(str(value).encode("utf-8")).decode("ascii")


def _decode_b64_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        # 1С переносит длинные значения по строкам — склеиваем обратно
        compact = "".join(str(value).split())
        return base64.b64decode(compact).decode("utf-8")
    except Exception:  # noqa: BLE001
        return value


def parse_message(xml_bytes: bytes, *, file_name: str = "") -> IncomingMessage:
    """Разбирает XML сообщения обмена (заголовок и наличие тела)."""
    root = ET.fromstring(xml_bytes)
    header = root.find(f"{{{MSG_NS}}}Header")
    if header is None:
        raise ValueError("В сообщении нет заголовка msg:Header")

    confirmation = header.find(f"{{{MSG_NS}}}Confirmation")
    if confirmation is None:
        raise ValueError("В заголовке нет msg:Confirmation")

    def conf(tag: str) -> Optional[str]:
        return _text(confirmation.find(f"{{{MSG_NS}}}{tag}"))

    versions = [v.text.strip() for v in header.findall(f"{{{MSG_NS}}}AvailableVersion") if v.text]

    object_types: dict[str, dict[str, bool]] = {}
    available = header.find(f"{{{MSG_NS}}}AvailableObjectTypes")
    if available is not None:
        for item in available.findall(f"{{{MSG_NS}}}ObjectType"):
            name = _text(item.find(f"{{{MSG_NS}}}Name"))
            if not name:
                continue
            sending = item.find(f"{{{MSG_NS}}}Sending")
            receiving = item.find(f"{{{MSG_NS}}}Receiving")
            object_types[name] = {
                "sending": _text(sending) == "*",
                "receiving": _text(receiving) == "*",
            }

    peer_guid = _text(header.find(f"{{{MSG_NS}}}NewFrom"))
    peer_name = None
    peer_prefix = None
    contour = header.find(f"{{{MSG_NS}}}SynchronizationContour")
    if contour is not None:
        node = contour.find(f"{{{MSG_NS}}}Node")
        if node is not None:
            peer_name = _decode_b64_name(_text(node.find(f"{{{MSG_NS}}}Name")))
            peer_prefix = _text(node.find(f"{{{MSG_NS}}}Prefix"))
            peer_guid = _text(node.find(f"{{{MSG_NS}}}Code")) or peer_guid

    # Пустой <Body/> — обычное служебное сообщение. Наличие самого контейнера
    # не означает, что 1С прислала объекты для загрузки.
    body_types: list[str] = []
    for child in root:
        if child.tag == f"{{{MSG_NS}}}Header":
            continue
        local_name = child.tag.rsplit("}", 1)[-1]
        if local_name == "Body":
            body_types.extend(
                item.tag.rsplit("}", 1)[-1]
                for item in child
                if isinstance(item.tag, str)
            )
        else:
            body_types.append(local_name)
    has_body = bool(body_types)

    return IncomingMessage(
        from_code=conf("From") or "",
        to_code=conf("To") or "",
        message_no=int(conf("MessageNo") or 0),
        received_no=int(conf("ReceivedNo") or 0),
        format_versions=versions,
        format_uri=_text(header.find(f"{{{MSG_NS}}}Format")),
        peer_node_guid=peer_guid,
        peer_node_name=peer_name,
        peer_node_prefix=peer_prefix,
        object_types=object_types,
        has_body=has_body,
        body_types=body_types,
        file_name=file_name,
    )


# ─── Состояние обмена ──────────────────────────────────────────────────────
# Хранится файлом на том же томе, что и сообщения: счётчики и GUID нашего узла.


def _state_path(directory: str) -> str:
    return os.path.join(directory, STATE_FILE_NAME)


def load_state(directory: str) -> dict[str, Any]:
    path = _state_path(directory)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:  # noqa: BLE001
            logger.exception("Повреждён файл состояния обмена: %s", path)
    return {
        "node_guid": str(uuid.uuid4()),
        "sent_no": 0,
        "received_no": 0,
        "peer_node_guid": None,
        "last_message_at": None,
    }


def save_state(directory: str, state: dict[str, Any]) -> None:
    os.makedirs(directory, exist_ok=True)
    tmp = _state_path(directory) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, _state_path(directory))


@contextmanager
def _exchange_lock(directory: str):
    """Serialize scheduler and admin actions against the shared FTP volume."""
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, LOCK_FILE_NAME), "a+b") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


# ─── Формирование ответа ───────────────────────────────────────────────────


def build_response_xml(
    incoming: IncomingMessage,
    state: dict[str, Any],
    *,
    sending: Optional[tuple[str, ...]] = None,
    receiving: Optional[tuple[str, ...]] = None,
    body_objects: Optional[list[ET.Element]] = None,
    created_at: Optional[datetime] = None,
) -> bytes:
    """Собирает сообщение 1С: заголовок и, если переданы, объекты данных."""
    sending = SUPPORTED_SENDING if sending is None else sending
    receiving = SUPPORTED_RECEIVING if receiving is None else receiving
    created = created_at or now_moscow()
    version = (
        DEFAULT_FORMAT_VERSION
        if DEFAULT_FORMAT_VERSION in incoming.format_versions
        else (incoming.format_versions[0] if incoming.format_versions else DEFAULT_FORMAT_VERSION)
    )
    if body_objects and version != DEFAULT_FORMAT_VERSION:
        raise ValueError("Объекты EnterpriseData поддерживаются только в версии 1.22")
    # В реальном рукопожатии УНФ 3.0.14.178 некоторые типы доступны только
    # в одном направлении (например, корректировка реализации — только от УНФ).
    # Объявляем лишь пересечение наших возможностей и возможностей её узла.
    if incoming.object_types:
        sending = tuple(
            name for name in sending
            if incoming.object_types.get(name, {}).get("receiving", False)
        )
        receiving = tuple(
            name for name in receiving
            if incoming.object_types.get(name, {}).get("sending", False)
        )

    ET.register_namespace("msg", MSG_NS)
    # Нельзя регистрировать DATA_NS как default namespace: ElementTree тогда
    # превращает корневой <Message> в <Message xmlns=".../1.22">. УНФ такой
    # корень не распознаёт и сообщает «обработано 0 объектов».
    ET.register_namespace("ed", DATA_NS)
    root = ET.Element("Message")
    header = ET.SubElement(root, f"{{{MSG_NS}}}Header")
    ET.SubElement(header, f"{{{MSG_NS}}}Format").text = (
        incoming.format_uri or f"{FORMAT_URI}/{version}"
    )
    ET.SubElement(header, f"{{{MSG_NS}}}CreationDate").text = created.strftime("%Y-%m-%dT%H:%M:%S")

    confirmation = ET.SubElement(header, f"{{{MSG_NS}}}Confirmation")
    ET.SubElement(confirmation, f"{{{MSG_NS}}}ExchangePlan").text = EXCHANGE_PLAN
    # После знакомства с узлом УНФ может адресовать входящий XML по его GUID.
    # Коды корреспондентов в нашем исходящем заголовке остаются постоянными.
    ET.SubElement(confirmation, f"{{{MSG_NS}}}To").text = PEER_NODE_CODE
    ET.SubElement(confirmation, f"{{{MSG_NS}}}From").text = OUR_NODE_CODE
    ET.SubElement(confirmation, f"{{{MSG_NS}}}MessageNo").text = str(int(state.get("sent_no", 0)))
    # Подтверждаем номер принятого сообщения
    ET.SubElement(confirmation, f"{{{MSG_NS}}}ReceivedNo").text = str(incoming.message_no)

    ET.SubElement(header, f"{{{MSG_NS}}}AvailableVersion").text = version
    ET.SubElement(header, f"{{{MSG_NS}}}NewFrom").text = str(state.get("node_guid"))

    available = ET.SubElement(header, f"{{{MSG_NS}}}AvailableObjectTypes")
    for name in sorted(set(sending) | set(receiving)):
        item = ET.SubElement(available, f"{{{MSG_NS}}}ObjectType")
        ET.SubElement(item, f"{{{MSG_NS}}}Name").text = name
        send_el = ET.SubElement(item, f"{{{MSG_NS}}}Sending")
        if name in sending:
            send_el.text = "*"
        recv_el = ET.SubElement(item, f"{{{MSG_NS}}}Receiving")
        if name in receiving:
            recv_el.text = "*"

    ET.SubElement(header, f"{{{MSG_NS}}}Prefix").text = OUR_NODE_CODE
    contour = ET.SubElement(header, f"{{{MSG_NS}}}SynchronizationContour")
    node = ET.SubElement(contour, f"{{{MSG_NS}}}Node")
    ET.SubElement(node, f"{{{MSG_NS}}}Name").text = _b64_name(OUR_NODE_NAME)
    ET.SubElement(node, f"{{{MSG_NS}}}Code").text = str(state.get("node_guid"))
    ET.SubElement(node, f"{{{MSG_NS}}}Prefix").text = OUR_NODE_CODE
    ET.SubElement(node, f"{{{MSG_NS}}}LastUpdate").text = created.strftime("%Y-%m-%dT%H:%M:%S")
    corr_nodes = ET.SubElement(node, f"{{{MSG_NS}}}CorrNodes")
    corr = ET.SubElement(corr_nodes, f"{{{MSG_NS}}}CorrNode")
    ET.SubElement(corr, f"{{{MSG_NS}}}Name").text = _b64_name(
        incoming.peer_node_name or incoming.from_code
    )
    ET.SubElement(corr, f"{{{MSG_NS}}}Code").text = incoming.peer_node_guid or incoming.from_code
    ET.SubElement(corr, f"{{{MSG_NS}}}Looping").text = "false"

    body = ET.SubElement(root, f"{{{DATA_NS}}}Body")
    if body_objects:
        for obj in body_objects:
            if obj.tag != f"{{{DATA_NS}}}{obj.tag.rsplit('}', 1)[-1]}":
                raise ValueError("Объект имеет неверное пространство имён EnterpriseData")
            if obj.tag.rsplit("}", 1)[-1] not in sending:
                raise ValueError("УНФ не объявила приём типа отправляемого объекта")
            body.append(obj)

    body = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    # 1С пишет BOM — повторяем, чтобы не отличаться от привычного ей формата
    return b"\xef\xbb\xbf" + body


# ─── Работа с файлами обмена ───────────────────────────────────────────────


def find_incoming_files(directory: str) -> list[str]:
    """Файлы сообщений, адресованных нам (Message_<кто-то>_<наш код>)."""
    if not os.path.isdir(directory):
        return []
    state = load_state(directory)
    senders = {PEER_NODE_CODE.upper(), PEER_NODE_FILE_CODE}
    receivers = {OUR_NODE_CODE.upper(), OUR_NODE_FILE_CODE}
    if _GUID_RE.match(str(state.get("peer_node_guid") or "")):
        senders.add(state["peer_node_guid"].upper())
    if _GUID_RE.match(str(state.get("node_guid") or "")):
        receivers.add(state["node_guid"].upper())
    found = []
    for name in sorted(os.listdir(directory)):
        match = _MESSAGE_RE.match(name)
        if not match:
            continue
        if match.group("from").upper() not in senders:
            continue
        if match.group("to").upper() not in receivers:
            continue
        found.append(name)
    return found


def read_message_file(path: str, *, max_xml_size: int = 32 * 1024 * 1024) -> bytes:
    """Достаёт XML из файла обмена: 1С присылает zip, но бывает и голый xml."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            entries = [
                item for item in archive.infolist()
                if item.filename.lower().endswith(".xml")
            ]
            if not entries:
                raise ValueError(f"В архиве {path} нет XML-файла")
            entry = entries[0]
            if entry.file_size > max_xml_size:
                raise ValueError("XML в архиве 1С больше разрешённого размера")
            with archive.open(entry) as fh:
                data = fh.read(max_xml_size + 1)
            if len(data) > max_xml_size:
                raise ValueError("XML в архиве 1С больше разрешённого размера")
            return data
    with open(path, "rb") as fh:
        data = fh.read(max_xml_size + 1)
    if len(data) > max_xml_size:
        raise ValueError("XML 1С больше разрешённого размера")
    return data


def read_incoming_archive(
    name: str,
    directory: Optional[str] = None,
    *,
    max_size: int = 32 * 1024 * 1024,
    outgoing: bool = False,
) -> bytes:
    """Read one FTP message for an administrator to inspect offline.

    outgoing=True — наш собственный файл (Message_ДЗ_*), чтобы загрузить его
    в УНФ вручную через «Загрузка данных EnterpriseData».
    """
    directory = directory or get_exchange_dir()
    state = load_state(directory)
    match = _MESSAGE_RE.fullmatch(name)
    senders, receivers = (
        ({OUR_NODE_CODE, OUR_NODE_FILE_CODE}, {PEER_NODE_CODE, PEER_NODE_FILE_CODE})
        if outgoing else
        ({PEER_NODE_CODE, PEER_NODE_FILE_CODE}, {OUR_NODE_CODE, OUR_NODE_FILE_CODE})
    )
    guid = state.get("node_guid") if outgoing else state.get("peer_node_guid")
    if _GUID_RE.match(str(guid or "")):
        senders.add(guid.upper())
    guid = state.get("peer_node_guid") if outgoing else state.get("node_guid")
    if _GUID_RE.match(str(guid or "")):
        receivers.add(guid.upper())
    if not match or match.group("from").upper() not in senders:
        raise ValueError("Недопустимое имя сообщения")
    to_part = match.group("to")
    if to_part.upper() not in receivers and not (
        outgoing and _GUID_RE.match(to_part)
    ):
        raise ValueError("Недопустимое имя сообщения")
    path = os.path.join(directory, name)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        raise FileNotFoundError("Входящее сообщение не найдено") from None
    with os.fdopen(fd, "rb") as fh:
        size = os.fstat(fh.fileno()).st_size
        if size > max_size:
            raise ValueError("Входящее сообщение больше разрешённого размера")
        content = fh.read(max_size + 1)
    if len(content) > max_size:
        raise ValueError("Входящее сообщение больше разрешённого размера")
    return content


_GUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _write_zip(directory: str, base: str, xml_bytes: bytes) -> str:
    path = os.path.join(directory, f"{base}.zip")
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{base}.xml", xml_bytes)
    tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    with open(tmp_path, "wb") as fh:
        fh.write(buffer.getvalue())
    os.replace(tmp_path, path)
    return path


def write_outgoing_message(
    directory: str,
    xml_bytes: bytes,
    *,
    from_code: str,
    to_code: str,
    compress: bool = True,
    receiver_guid: Optional[str] = None,
    sender_guid: Optional[str] = None,
) -> str:
    """Кладёт наш ответ рядом — 1С заберёт его по FTP.

    УНФ ищет входящий файл по маске ``Message*_<наш код>_<GUID её узла>.*``
    (так написано в её журнале регистрации), а не по короткому коду ``НФ``.
    Поэтому, когда GUID узла УНФ известен, рядом кладётся копия под этим
    именем. GUID приходит из внешнего файла — в имя он попадает только после
    проверки формата.
    """
    os.makedirs(directory, exist_ok=True)
    base = f"Message_{from_code}_{to_code}"
    if not compress:
        path = os.path.join(directory, f"{base}.xml")
        tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        with open(tmp_path, "wb") as fh:
            fh.write(xml_bytes)
        os.replace(tmp_path, path)
        return path

    path = _write_zip(directory, base, xml_bytes)
    if receiver_guid and _GUID_RE.match(receiver_guid):
        _write_zip(directory, f"Message_{from_code}_{receiver_guid}", xml_bytes)
    if from_code == OUR_NODE_CODE and to_code == PEER_NODE_CODE:
        # FTP БСП в УНФ работает с латинскими именами после включения опции
        # «Транслитерация имени». Для ZIP имя внутреннего XML тоже должно
        # соответствовать имени архива.
        _write_zip(
            directory,
            f"Message_{OUR_NODE_FILE_CODE}_{PEER_NODE_FILE_CODE}",
            xml_bytes,
        )
        if receiver_guid and _GUID_RE.match(receiver_guid):
            _write_zip(
                directory,
                f"Message_{OUR_NODE_FILE_CODE}_{receiver_guid}",
                xml_bytes,
            )
        if sender_guid and _GUID_RE.match(sender_guid):
            _write_zip(directory, f"Message_{sender_guid}_{PEER_NODE_FILE_CODE}", xml_bytes)
            if receiver_guid and _GUID_RE.match(receiver_guid):
                _write_zip(directory, f"Message_{sender_guid}_{receiver_guid}", xml_bytes)
    return path


def regenerate_outgoing_message(
    directory: Optional[str] = None,
    *,
    bump_message_no: bool = False,
) -> dict[str, Any]:
    directory = directory or get_exchange_dir()
    with _exchange_lock(directory):
        return _regenerate_outgoing_message_unlocked(
            directory, bump_message_no=bump_message_no
        )


def _regenerate_outgoing_message_unlocked(
    directory: Optional[str] = None,
    *,
    bump_message_no: bool = False,
) -> dict[str, Any]:
    """Пересобирает наше исходящее сообщение по сохранённому состоянию.

    Нужно, когда 1С требует «получить параметры из приложения», а нового
    входящего сообщения нет: её рукопожатие мы уже обработали, а состав
    объявленных типов с тех пор изменился.

    По умолчанию номер сообщения НЕ увеличивается — мы заменяем ещё не
    прочитанное 1С сообщение. Если она его уже забрала, нужен bump_message_no.
    """
    directory = directory or get_exchange_dir()
    os.makedirs(directory, exist_ok=True)
    state = load_state(directory)

    sent_no = int(state.get("sent_no", 0))
    # 1С не удаляет наш файл с FTP, а отслеживает номера сообщений сама:
    # повторно присланный номер она просто проигнорирует. Поэтому если она
    # уже подтвердила приём нашего последнего сообщения (сообщила его номер
    # в своём поле ReceivedNo), следующее обязано быть на единицу больше.
    already_read = sent_no <= int(state.get("peer_confirmed_our_no", 0))
    if bump_message_no or sent_no == 0 or already_read:
        state["sent_no"] = sent_no + 1

    # Синтетическое «входящее»: коды узлов постоянны, GUID берём из состояния
    stub = IncomingMessage(
        from_code=PEER_NODE_CODE,
        to_code=OUR_NODE_CODE,
        message_no=int(state.get("received_no", 0)),
        received_no=int(state.get("sent_no", 0)),
        format_versions=[DEFAULT_FORMAT_VERSION],
        format_uri=state.get("peer_format_uri"),
        peer_node_guid=state.get("peer_node_guid"),
        peer_node_name=None,
        object_types=state.get("peer_object_types") or {},
    )
    xml_bytes = build_response_xml(stub, state)
    path = write_outgoing_message(
        directory,
        xml_bytes,
        from_code=OUR_NODE_CODE,
        to_code=PEER_NODE_CODE,
        receiver_guid=state.get("peer_node_guid"),
        sender_guid=state.get("node_guid"),
    )
    state["last_message_at"] = now_moscow().isoformat()
    save_state(directory, state)
    logger.info(
        "1C EnterpriseData: пересобран %s (MessageNo=%s, типов на отправку %s)",
        os.path.basename(path),
        state.get("sent_no"),
        len(SUPPORTED_SENDING),
    )
    return {
        "directory": directory,
        "response_file": os.path.basename(path),
        "message_no": state.get("sent_no"),
        "declared_sending": list(SUPPORTED_SENDING),
        "declared_receiving": list(SUPPORTED_RECEIVING),
    }


def send_body_message(
    body_objects: list[ET.Element],
    *,
    description: str = "",
    directory: Optional[str] = None,
    replace_unread: bool = False,
) -> dict[str, Any]:
    """Кладёт на FTP сообщение с данными для УНФ (ручная отправка).

    replace_unread — осознанно заменить предыдущее непрочитанное сообщение
    (например, если оно было затёрто или УНФ не может его найти).

    Отправка допустима только после рукопожатия (мы знаем, какие типы УНФ
    принимает) и пока УНФ прочитала предыдущее сообщение с данными: файл
    обмена один, и замена непрочитанного сообщения потеряла бы документы.
    Принятыми считаем только служебные сообщения УНФ, которые мы разобрали,
    — не те, что с данными: их разбор не реализован.
    """
    directory = directory or get_exchange_dir()
    with _exchange_lock(directory):
        state = load_state(directory)
        if not state.get("peer_object_types"):
            raise ValueError(
                "Рукопожатие с УНФ ещё не получено: неизвестно, какие типы она принимает"
            )
        # Справочники — только если узел УНФ их принимает; документ обязателен.
        accepted = {
            name for name, flags in state["peer_object_types"].items()
            if flags.get("receiving")
        }
        documents = {
            obj.tag.rsplit("}", 1)[-1]
            for obj in body_objects
            if obj.tag.rsplit("}", 1)[-1].startswith("Документ.")
        }
        if not documents or not documents.issubset(accepted):
            unsupported = ", ".join(sorted(documents - accepted)) or "документ отсутствует"
            raise ValueError(f"УНФ не объявила приём: {unsupported}")
        body_objects = [
            obj for obj in body_objects
            if not obj.tag.rsplit("}", 1)[-1].startswith("Справочник.")
            or obj.tag.rsplit("}", 1)[-1] in accepted
        ]
        sent_no = int(state.get("sent_no", 0))
        confirmed_no = int(state.get("peer_confirmed_our_no", 0))
        pending = state.get("pending_body")
        if pending and int(pending["message_no"]) > confirmed_no and not replace_unread:
            raise ValueError(
                f"УНФ ещё не прочитала сообщение № {pending['message_no']} с данными: "
                "запустите в УНФ синхронизацию, затем повторите отправку"
            )
        # Непрочитанный служебный файл можно заменить, сохранив его номер;
        # прочитанный — нельзя, следующий обязан получить новый номер.
        message_no = sent_no if 0 < sent_no > confirmed_no else sent_no + 1
        stub = IncomingMessage(
            from_code=PEER_NODE_CODE,
            to_code=OUR_NODE_CODE,
            message_no=int(state.get("last_peer_message_no", 0)),
            received_no=confirmed_no,
            format_versions=[DEFAULT_FORMAT_VERSION],
            format_uri=state.get("peer_format_uri"),
            peer_node_guid=state.get("peer_node_guid"),
            object_types=state["peer_object_types"],
        )
        outgoing_state = {**state, "sent_no": message_no}
        xml_bytes = build_response_xml(stub, outgoing_state, body_objects=body_objects)
        path = write_outgoing_message(
            directory,
            xml_bytes,
            from_code=OUR_NODE_CODE,
            to_code=PEER_NODE_CODE,
            receiver_guid=state.get("peer_node_guid"),
            sender_guid=state.get("node_guid"),
        )
        state["sent_no"] = message_no
        state["pending_body"] = {
            "message_no": message_no,
            "objects": [obj.tag.rsplit("}", 1)[-1] for obj in body_objects],
            "description": description,
            "sent_at": now_moscow().isoformat(),
        }
        state["last_message_at"] = now_moscow().isoformat()
        save_state(directory, state)
        logger.info(
            "1C EnterpriseData: отправлено сообщение № %s с данными (%s объектов)",
            message_no,
            len(body_objects),
        )
        return {
            "response_file": os.path.basename(path),
            "message_no": message_no,
            "objects": state["pending_body"]["objects"],
        }


def process_exchange_directory(
    directory: Optional[str] = None,
    *,
    archive_incoming: bool = True,
) -> dict[str, Any]:
    directory = directory or get_exchange_dir()
    with _exchange_lock(directory):
        return _process_exchange_directory_unlocked(
            directory, archive_incoming=archive_incoming
        )


def _process_exchange_directory_unlocked(
    directory: Optional[str] = None,
    *,
    archive_incoming: bool = True,
) -> dict[str, Any]:
    """Разбирает входящие сообщения от 1С и формирует ответ.

    Служебные сообщения подтверждаются сразу. Сообщения с реализациями и
    поступлениями складываются в очередь ``inbox`` внутри состояния и
    подтверждаются; остальные данные не разбираются и остаются в каталоге
    без подтверждения — об этом сообщается в результате.
    """
    directory = directory or get_exchange_dir()
    result: dict[str, Any] = {
        "directory": directory,
        "processed": [],
        "response_file": None,
        "note": None,
    }
    if not os.path.isdir(directory):
        result["note"] = f"Каталог обмена не найден: {directory}"
        return result

    incoming_files = find_incoming_files(directory)
    if not incoming_files:
        result["note"] = "Входящих сообщений от 1С нет"
        return result

    state = load_state(directory)
    latest: Optional[IncomingMessage] = None
    parsed_handshake_files: list[str] = []
    files_with_body: list[str] = []

    for name in incoming_files:
        path = os.path.join(directory, name)
        try:
            xml_bytes = read_message_file(path)
            message = parse_message(xml_bytes, file_name=name)
            body_documents = (
                importable_documents(xml_bytes) if message.has_body else None
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось разобрать сообщение %s", name)
            result["processed"].append({"file": name, "error": str(exc)})
            continue

        result["processed"].append(
            {
                "file": name,
                "from": message.from_code,
                "to": message.to_code,
                "message_no": message.message_no,
                "received_no": message.received_no,
                "handshake": message.is_handshake,
                "object_types": len(message.object_types),
                "format_versions": message.format_versions,
            }
        )
        valid_senders = {PEER_NODE_CODE, state.get("peer_node_guid")}
        valid_receivers = {OUR_NODE_CODE, state.get("node_guid")}
        if message.from_code not in valid_senders or message.to_code not in valid_receivers:
            result["processed"][-1]["error"] = (
                "Коды отправителя или получателя не совпадают с узлом"
            )
            continue
        if message.has_body and body_documents is None:
            files_with_body.append(name)
        else:
            if latest is None or message.message_no >= latest.message_no:
                latest = message
            parsed_handshake_files.append(name)
            # ReceivedNo в нашем следующем ответе означает, что мы успешно
            # приняли сообщение УНФ. Тело с неизвестными нам объектами
            # подтверждать нельзя (его номер сюда не попадает): поздний
            # служебный ответ иначе удалил бы неимпортированные данные.
            # Документы, которые мы умеем сопоставить, сначала кладём в
            # очередь внутри состояния, и только потом подтверждаем.
            if message.message_no > int(state.get("received_no", 0)):
                if body_documents:
                    state.setdefault("inbox", []).append({
                        "message_no": message.message_no,
                        "received_at": now_moscow().isoformat(),
                        "documents": [item.as_dict() for item in body_documents],
                    })
                state["received_no"] = message.message_no
        # В поле ReceivedNo 1С сообщает, какое НАШЕ сообщение она приняла.
        # По нему понимаем, нужно ли увеличивать номер следующего исходящего.
        if message.received_no > int(state.get("peer_confirmed_our_no", 0)):
            state["peer_confirmed_our_no"] = message.received_no
        pending = state.get("pending_body")
        if pending and int(pending["message_no"]) <= int(
            state.get("peer_confirmed_our_no", 0)
        ):
            state["last_confirmed_body"] = {
                **pending,
                "transport_confirmed_at": now_moscow().isoformat(),
            }
            state["pending_body"] = None
        if message.peer_node_guid:
            state["peer_node_guid"] = message.peer_node_guid
        if message.format_uri:
            state["peer_format_uri"] = message.format_uri
        if message.object_types:
            state["peer_object_types"] = message.object_types

    if files_with_body:
        # Номер, который УНФ подтвердила в ReceivedNo, берём из заголовка в любом
        # случае: без него мы бы не узнали, что она прочитала наше сообщение с
        # данными. Само сообщение с данными по-прежнему не подтверждаем.
        save_state(directory, state)
        result["note"] = (
            "Получено сообщение С ДАННЫМИ — разбор данных ещё не реализован; "
            "файл оставлен во входящем каталоге без подтверждения"
        )
        return result

    if latest is None:
        result["note"] = "Ни одно сообщение не удалось разобрать"
        return result

    # 1С может повторно положить тот же файл, пока не увидит подтверждение.
    # Повтор не должен увеличивать наш номер сообщения.
    outgoing_file = f"Message_{OUR_NODE_CODE}_{PEER_NODE_CODE}.zip"
    if state.get("last_peer_message_no") == latest.message_no:
        if not os.path.exists(os.path.join(directory, outgoing_file)):
            write_outgoing_message(
                directory,
                build_response_xml(latest, state),
                from_code=OUR_NODE_CODE,
                to_code=PEER_NODE_CODE,
                receiver_guid=state.get("peer_node_guid"),
                sender_guid=state.get("node_guid"),
            )
        result["note"] = "Повтор уже обработанного служебного сообщения"
        result["response_file"] = outgoing_file
        save_state(directory, state)
        if archive_incoming:
            _archive_handshakes(directory, parsed_handshake_files)
        return result

    pending = state.get("pending_body")
    if pending and int(pending["message_no"]) > int(state.get("peer_confirmed_our_no", 0)):
        # УНФ ещё не прочитала наше сообщение с данными. Служебный ответ
        # занял бы тот же файл и стёр бы документы, поэтому не отвечаем.
        state["last_peer_message_no"] = latest.message_no
        save_state(directory, state)
        if archive_incoming:
            _archive_handshakes(directory, parsed_handshake_files)
        result["note"] = (
            "Ответ не создан: УНФ ещё не прочитала сообщение "
            f"№ {pending['message_no']} с данными"
        )
        return result

    state["sent_no"] = int(state.get("sent_no", 0)) + 1
    state["last_peer_message_no"] = latest.message_no
    state["last_message_at"] = now_moscow().isoformat()

    xml_bytes = build_response_xml(latest, state)
    response_path = write_outgoing_message(
        directory,
        xml_bytes,
        from_code=OUR_NODE_CODE,
        to_code=PEER_NODE_CODE,
        receiver_guid=state.get("peer_node_guid"),
        sender_guid=state.get("node_guid"),
    )
    save_state(directory, state)
    result["response_file"] = os.path.basename(response_path)
    result["state"] = {
        "node_guid": state.get("node_guid"),
        "sent_no": state.get("sent_no"),
        "received_no": state.get("received_no"),
    }

    if archive_incoming:
        _archive_handshakes(directory, parsed_handshake_files)

    logger.info(
        "1C EnterpriseData: обработано %s, ответ %s",
        len(incoming_files),
        result["response_file"],
    )
    return result


def read_inbox(directory: Optional[str] = None) -> list[dict[str, Any]]:
    """Документы УНФ, принятые из FTP, но ещё не сопоставленные с базой."""
    directory = directory or get_exchange_dir()
    if not os.path.isdir(directory):
        return []
    with _exchange_lock(directory):
        return list(load_state(directory).get("inbox") or [])


def drop_inbox(count: int, directory: Optional[str] = None) -> None:
    """Убирает первые ``count`` записей очереди после успешного сохранения в БД."""
    directory = directory or get_exchange_dir()
    with _exchange_lock(directory):
        state = load_state(directory)
        state["inbox"] = list(state.get("inbox") or [])[count:]
        save_state(directory, state)


def _archive_handshakes(directory: str, names: list[str]) -> None:
    processed_dir = os.path.join(directory, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    for name in names:
        try:
            os.replace(
                os.path.join(directory, name),
                os.path.join(processed_dir, name),
            )
        except OSError:
            logger.exception("Не удалось убрать обработанный файл %s", name)


def get_exchange_status(directory: Optional[str] = None) -> dict[str, Any]:
    directory = directory or get_exchange_dir()
    state = load_state(directory) if os.path.isdir(directory) else {}
    peer_types = state.get("peer_object_types") or {}
    incoming = find_incoming_files(directory)
    incoming_messages = []
    for name in incoming[:10]:
        path = os.path.join(directory, name)
        info: dict[str, Any] = {"file": name}
        try:
            info["modified_at"] = datetime.fromtimestamp(
                os.path.getmtime(path)
            ).isoformat()
            if zipfile.is_zipfile(path):
                with zipfile.ZipFile(path) as archive:
                    xml_files = [
                        entry for entry in archive.infolist()
                        if entry.filename.lower().endswith(".xml")
                    ]
                    if not xml_files:
                        raise ValueError("В архиве нет XML")
                    size = xml_files[0].file_size
            else:
                size = os.path.getsize(path)
            info["xml_size_bytes"] = size
            if size > 4 * 1024 * 1024:
                info["note"] = "XML больше 4 МБ; заголовок не прочитан"
            else:
                message = parse_message(read_message_file(path), file_name=name)
                if message.object_types:
                    peer_types = message.object_types
                info.update({
                    "message_no": message.message_no,
                    "received_no": message.received_no,
                    "has_body": message.has_body,
                    "body_types": message.body_types,
                    "format_versions": message.format_versions,
                    "object_types": sorted(message.object_types),
                })
        except (OSError, ValueError, zipfile.BadZipFile, ET.ParseError) as exc:
            info["error"] = str(exc)
        incoming_messages.append(info)
    return {
        "directory": directory,
        "directory_exists": os.path.isdir(directory),
        "incoming_files": incoming,
        "incoming_messages": incoming_messages,
        "our_node_code": OUR_NODE_CODE,
        "peer_node_code": PEER_NODE_CODE,
        "declared_sending": list(SUPPORTED_SENDING),
        "declared_receiving": list(SUPPORTED_RECEIVING),
        "negotiated_sending": [
            name for name in SUPPORTED_SENDING
            if peer_types.get(name, {}).get("receiving", False)
        ],
        "negotiated_receiving": [
            name for name in SUPPORTED_RECEIVING
            if peer_types.get(name, {}).get("sending", False)
        ],
        "peer_receives_corrections": peer_types.get(
            "Документ.КорректировкаРеализацииТоваров", {}
        ).get("receiving"),
        "node_guid": state.get("node_guid"),
        "sent_no": state.get("sent_no", 0),
        "received_no": state.get("received_no", 0),
        "peer_confirmed_our_no": state.get("peer_confirmed_our_no", 0),
        "pending_body": state.get("pending_body"),
        "last_confirmed_body": state.get("last_confirmed_body"),
        "last_message_at": state.get("last_message_at"),
    }
