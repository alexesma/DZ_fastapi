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

Разбор и формирование самих данных (документы, справочники) — следующий этап.
Пока мы намеренно НЕ объявляем ни одного типа объектов: иначе 1С начнёт
присылать данные, обработать которые мы ещё не умеем, и будет считать их
доставленными.
"""

import base64
import json
import logging
import os
import re
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Optional
from xml.etree import ElementTree as ET

from dz_fastapi.core.time import now_moscow

logger = logging.getLogger("dz_fastapi")

MSG_NS = "http://www.1c.ru/SSL/Exchange/Message"
FORMAT_URI = "http://v8.1c.ru/edi/edi_stnd/EnterpriseData"
EXCHANGE_PLAN = "СинхронизацияДанныхЧерезУниверсальныйФормат"

# Коды узлов заданы при настройке синхронизации в 1С
OUR_NODE_CODE = "ДЗ"
OUR_NODE_NAME = "DragonZap"
PEER_NODE_CODE = "НФ"

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
# сверен с рукопожатием 1С (все четыре типа она принимает).
DEFAULT_SENDING: tuple[str, ...] = (
    "Документ.РеализацияТоваровУслуг",
    "Документ.ПоступлениеТоваровУслуг",
    "Справочник.Контрагенты",
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
    peer_node_guid: Optional[str] = None
    peer_node_name: Optional[str] = None
    peer_node_prefix: Optional[str] = None
    object_types: dict[str, dict[str, bool]] = field(default_factory=dict)
    has_body: bool = False
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

    # Тело — всё, что идёт после заголовка внутри Message
    has_body = any(child.tag != f"{{{MSG_NS}}}Header" for child in list(root))

    return IncomingMessage(
        from_code=conf("From") or "",
        to_code=conf("To") or "",
        message_no=int(conf("MessageNo") or 0),
        received_no=int(conf("ReceivedNo") or 0),
        format_versions=versions,
        peer_node_guid=peer_guid,
        peer_node_name=peer_name,
        peer_node_prefix=peer_prefix,
        object_types=object_types,
        has_body=has_body,
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


# ─── Формирование ответа ───────────────────────────────────────────────────


def build_response_xml(
    incoming: IncomingMessage,
    state: dict[str, Any],
    *,
    sending: tuple[str, ...] = SUPPORTED_SENDING,
    receiving: tuple[str, ...] = SUPPORTED_RECEIVING,
    created_at: Optional[datetime] = None,
) -> bytes:
    """Собирает ответное сообщение (заголовок без данных)."""
    created = created_at or now_moscow()
    version = (
        DEFAULT_FORMAT_VERSION
        if DEFAULT_FORMAT_VERSION in incoming.format_versions
        else (incoming.format_versions[0] if incoming.format_versions else DEFAULT_FORMAT_VERSION)
    )

    ET.register_namespace("msg", MSG_NS)
    root = ET.Element("Message")
    header = ET.SubElement(root, f"{{{MSG_NS}}}Header")
    ET.SubElement(header, f"{{{MSG_NS}}}Format").text = FORMAT_URI
    ET.SubElement(header, f"{{{MSG_NS}}}CreationDate").text = created.strftime("%Y-%m-%dT%H:%M:%S")

    confirmation = ET.SubElement(header, f"{{{MSG_NS}}}Confirmation")
    ET.SubElement(confirmation, f"{{{MSG_NS}}}ExchangePlan").text = EXCHANGE_PLAN
    # Отвечаем тому, кто прислал
    ET.SubElement(confirmation, f"{{{MSG_NS}}}To").text = incoming.from_code
    ET.SubElement(confirmation, f"{{{MSG_NS}}}From").text = incoming.to_code
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

    contour = ET.SubElement(header, f"{{{MSG_NS}}}SynchronizationContour")
    node = ET.SubElement(contour, f"{{{MSG_NS}}}Node")
    ET.SubElement(node, f"{{{MSG_NS}}}Name").text = _b64_name(OUR_NODE_NAME)
    ET.SubElement(node, f"{{{MSG_NS}}}Code").text = str(state.get("node_guid"))
    ET.SubElement(node, f"{{{MSG_NS}}}Prefix").text = incoming.to_code
    ET.SubElement(node, f"{{{MSG_NS}}}LastUpdate").text = created.strftime("%Y-%m-%dT%H:%M:%S")
    corr_nodes = ET.SubElement(node, f"{{{MSG_NS}}}CorrNodes")
    corr = ET.SubElement(corr_nodes, f"{{{MSG_NS}}}CorrNode")
    ET.SubElement(corr, f"{{{MSG_NS}}}Name").text = _b64_name(
        incoming.peer_node_name or incoming.from_code
    )
    ET.SubElement(corr, f"{{{MSG_NS}}}Code").text = incoming.peer_node_guid or incoming.from_code
    ET.SubElement(corr, f"{{{MSG_NS}}}Looping").text = "false"

    body = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    # 1С пишет BOM — повторяем, чтобы не отличаться от привычного ей формата
    return b"\xef\xbb\xbf" + body


# ─── Работа с файлами обмена ───────────────────────────────────────────────


def find_incoming_files(directory: str) -> list[str]:
    """Файлы сообщений, адресованных нам (Message_<кто-то>_<наш код>)."""
    if not os.path.isdir(directory):
        return []
    found = []
    for name in sorted(os.listdir(directory)):
        match = _MESSAGE_RE.match(name)
        if not match:
            continue
        if match.group("to").upper() != OUR_NODE_CODE.upper():
            continue
        found.append(name)
    return found


def read_message_file(path: str) -> bytes:
    """Достаёт XML из файла обмена: 1С присылает zip, но бывает и голый xml."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            names = [n for n in archive.namelist() if n.lower().endswith(".xml")]
            if not names:
                raise ValueError(f"В архиве {path} нет XML-файла")
            return archive.read(names[0])
    with open(path, "rb") as fh:
        return fh.read()


def write_outgoing_message(
    directory: str,
    xml_bytes: bytes,
    *,
    from_code: str,
    to_code: str,
    compress: bool = True,
) -> str:
    """Кладёт наш ответ рядом — 1С заберёт его по FTP."""
    os.makedirs(directory, exist_ok=True)
    base = f"Message_{from_code}_{to_code}"
    if not compress:
        path = os.path.join(directory, f"{base}.xml")
        with open(path, "wb") as fh:
            fh.write(xml_bytes)
        return path

    path = os.path.join(directory, f"{base}.zip")
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{base}.xml", xml_bytes)
    with open(path, "wb") as fh:
        fh.write(buffer.getvalue())
    return path


def regenerate_outgoing_message(
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
        peer_node_guid=state.get("peer_node_guid"),
        peer_node_name=None,
    )
    xml_bytes = build_response_xml(stub, state)
    path = write_outgoing_message(
        directory,
        xml_bytes,
        from_code=OUR_NODE_CODE,
        to_code=PEER_NODE_CODE,
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


def process_exchange_directory(
    directory: Optional[str] = None,
    *,
    archive_incoming: bool = True,
) -> dict[str, Any]:
    """Разбирает входящие сообщения от 1С и формирует ответ.

    На этом этапе обрабатывается только рукопожатие: сообщения с данными
    сохраняются, но не разбираются — об этом сообщается в результате.
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
            message = parse_message(read_message_file(path), file_name=name)
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
        if message.has_body:
            files_with_body.append(name)
        else:
            latest = message
            parsed_handshake_files.append(name)
        if message.message_no > int(state.get("received_no", 0)):
            state["received_no"] = message.message_no
        # В поле ReceivedNo 1С сообщает, какое НАШЕ сообщение она приняла.
        # По нему понимаем, нужно ли увеличивать номер следующего исходящего.
        if message.received_no > int(state.get("peer_confirmed_our_no", 0)):
            state["peer_confirmed_our_no"] = message.received_no
        if message.peer_node_guid:
            state["peer_node_guid"] = message.peer_node_guid

    if files_with_body:
        result["note"] = (
            "Получено сообщение С ДАННЫМИ — разбор данных ещё не реализован; "
            "файл оставлен во входящем каталоге без подтверждения"
        )
        return result

    if latest is None:
        result["note"] = "Ни одно сообщение не удалось разобрать"
        return result

    state["sent_no"] = int(state.get("sent_no", 0)) + 1
    state["last_message_at"] = now_moscow().isoformat()

    xml_bytes = build_response_xml(latest, state)
    response_path = write_outgoing_message(
        directory,
        xml_bytes,
        from_code=latest.to_code or OUR_NODE_CODE,
        to_code=latest.from_code or PEER_NODE_CODE,
    )
    save_state(directory, state)
    result["response_file"] = os.path.basename(response_path)
    result["state"] = {
        "node_guid": state.get("node_guid"),
        "sent_no": state.get("sent_no"),
        "received_no": state.get("received_no"),
    }

    if archive_incoming:
        processed_dir = os.path.join(directory, "processed")
        os.makedirs(processed_dir, exist_ok=True)
        for name in parsed_handshake_files:
            try:
                os.replace(
                    os.path.join(directory, name),
                    os.path.join(processed_dir, name),
                )
            except OSError:
                logger.exception("Не удалось убрать обработанный файл %s", name)

    logger.info(
        "1C EnterpriseData: обработано %s, ответ %s",
        len(incoming_files),
        result["response_file"],
    )
    return result


def get_exchange_status(directory: Optional[str] = None) -> dict[str, Any]:
    directory = directory or get_exchange_dir()
    state = load_state(directory) if os.path.isdir(directory) else {}
    return {
        "directory": directory,
        "directory_exists": os.path.isdir(directory),
        "incoming_files": find_incoming_files(directory),
        "our_node_code": OUR_NODE_CODE,
        "peer_node_code": PEER_NODE_CODE,
        "declared_sending": list(SUPPORTED_SENDING),
        "declared_receiving": list(SUPPORTED_RECEIVING),
        "node_guid": state.get("node_guid"),
        "sent_no": state.get("sent_no", 0),
        "received_no": state.get("received_no", 0),
        "last_message_at": state.get("last_message_at"),
    }
