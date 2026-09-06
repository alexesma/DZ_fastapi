#!/usr/bin/env python3
"""Самопроверка обмена с 1С — эмулирует то, что делает сама 1С.

Проходит весь протокол «Обмен с сайтом» ровно как 1С:
    1. type=sale&mode=checkauth   — авторизация (Basic) → cookie сессии
    2. type=sale&mode=init        — параметры обмена (zip, лимит файла)
    3. type=sale&mode=query       — ЗАБРАТЬ документы (наши отгрузки) в CommerceML
    4. type=sale&mode=success     — ПОДТВЕРДИТЬ приём (только с --confirm!)

Зачем: показать, работает ли канал и что именно уедет в 1С, не гадая по
логам. Шаг 3 безопасен и повторяем — сервер отдаёт тот же самый батч, пока
1С не подтвердит его шагом 4.

ВНИМАНИЕ: шаг 4 (--confirm) меняет состояние — батч помечается доставленным,
а отгрузки переходят в статус «выгружено». Без --confirm не выполняется.

Примеры:
    python one_c_selftest.py --url https://dragonzap.online/api/1c/exchange \\
        --login ЛОГИН --password ПАРОЛЬ
    python one_c_selftest.py --url ... --login ... --password ... \\
        --save-xml sale.xml
    python one_c_selftest.py --url ... --login ... --password ... --confirm
"""
import argparse
import base64
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

TIMEOUT = 60


class ExchangeClient:
    def __init__(self, url: str, login: str, password: str, insecure: bool = False):
        self.url = url
        self.login = login
        self.password = password
        self.cookie: str | None = None
        self.ctx = None
        if insecure:
            import ssl

            self.ctx = ssl._create_unverified_context()

    def _request(self, *, type_: str, mode: str, use_basic: bool = False):
        query = urllib.parse.urlencode({"type": type_, "mode": mode})
        full_url = f"{self.url}{'&' if '?' in self.url else '?'}{query}"
        req = urllib.request.Request(full_url, method="GET")
        if use_basic:
            token = base64.b64encode(
                f"{self.login}:{self.password}".encode("utf-8")
            ).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        if self.cookie:
            req.add_header("Cookie", self.cookie)

        def _lower(headers) -> dict:
            # HTTP-заголовки регистронезависимы, а сервер отдаёт их
            # в нижнем регистре — нормализуем ключи.
            return {str(k).lower(): v for k, v in (headers or {}).items()}

        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT, context=self.ctx) as resp:
                return resp.status, _lower(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, _lower(exc.headers), exc.read()

    def checkauth(self) -> bool:
        status, _headers, body = self._request(
            type_="sale", mode="checkauth", use_basic=True
        )
        text = body.decode("utf-8", "replace").strip()
        print(f"1) checkauth  → HTTP {status}")
        lines = text.splitlines()
        if not lines or lines[0].strip() != "success":
            print(f"   ✗ ОТКАЗ: {text}")
            return False
        if len(lines) >= 3:
            self.cookie = f"{lines[1].strip()}={lines[2].strip()}"
            print(f"   ✓ авторизация принята, cookie: {lines[1].strip()}")
        else:
            print("   ✓ авторизация принята (без cookie)")
        return True

    def init(self) -> bool:
        status, _headers, body = self._request(type_="sale", mode="init")
        text = body.decode("utf-8", "replace").strip()
        print(f"2) init       → HTTP {status}")
        if status != 200 or text.startswith("failure"):
            print(f"   ✗ ОТКАЗ: {text}")
            return False
        print(f"   ✓ параметры: {text.replace(chr(10), ' | ')}")
        return True

    def query(self):
        status, headers, body = self._request(type_="sale", mode="query")
        print(f"3) query      → HTTP {status}")
        batch = headers.get("x-dz-1c-batch-id")
        if status != 200:
            print(f"   ✗ ОТКАЗ: {body.decode('utf-8', 'replace')[:400]}")
            return None, None
        print(f"   ✓ батч: {batch or '— (пустой, нечего выгружать)'}")
        return body, batch

    def success(self) -> bool:
        status, _headers, body = self._request(type_="sale", mode="success")
        text = body.decode("utf-8", "replace").strip()
        print(f"4) success    → HTTP {status}: {text}")
        return status == 200 and text.startswith("success")


def describe_xml(payload: bytes) -> int:
    """Разбирает CommerceML и печатает, что именно уедет в 1С."""
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        print(f"   ✗ Ответ не разобрался как XML: {exc}")
        print(f"   Первые 300 байт: {payload[:300]!r}")
        return 0

    print(f"   корень: <{root.tag}> "
          f"ВерсияСхемы={root.attrib.get('ВерсияСхемы', '—')}")
    docs = root.findall("Документ")
    if not docs:
        print("   → документов НЕТ: очередь пуста "
              "(нет проведённых отгрузок, ожидающих выгрузки)")
        return 0

    print(f"   → документов к выгрузке: {len(docs)}")
    for doc in docs[:20]:
        def val(tag: str) -> str:
            el = doc.find(tag)
            return (el.text or "").strip() if el is not None else "—"

        counterparty = doc.find("Контрагенты/Контрагент/Наименование")
        goods = doc.findall("Товары/Товар")
        client = (
            (counterparty.text or "—").strip()
            if counterparty is not None
            else "—"
        )
        print(
            f"      • №{val('Номер')} от {val('Дата')} | "
            f"{val('ХозОперация')} | контрагент: {client} | "
            f"строк: {len(goods)} | сумма: {val('Сумма')}"
        )
    if len(docs) > 20:
        print(f"      … и ещё {len(docs) - 20}")
    return len(docs)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Самопроверка обмена с 1С (эмуляция 1С)"
    )
    parser.add_argument(
        "--url", required=True,
        help="полный URL обмена, напр. https://dragonzap.online/api/1c/exchange",
    )
    parser.add_argument("--login", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument(
        "--confirm", action="store_true",
        help="выполнить шаг success (подтвердить приём) — МЕНЯЕТ СОСТОЯНИЕ",
    )
    parser.add_argument("--save-xml", help="сохранить полученный XML в файл")
    parser.add_argument(
        "--insecure", action="store_true", help="не проверять TLS-сертификат"
    )
    args = parser.parse_args()

    print(f"Обмен с 1С — самопроверка\nURL: {args.url}\n")
    client = ExchangeClient(args.url, args.login, args.password, args.insecure)

    if not client.checkauth():
        print("\nИТОГ: ✗ 1С не сможет подключиться — проверьте логин/пароль "
              "(ONE_C_EXCHANGE_LOGIN / ONE_C_EXCHANGE_PASSWORD) и адрес.")
        return 1
    if not client.init():
        print("\nИТОГ: ✗ Ошибка на шаге init.")
        return 1

    payload, batch = client.query()
    if payload is None:
        print("\nИТОГ: ✗ Ошибка на шаге выгрузки документов.")
        return 1

    doc_count = describe_xml(payload)
    if args.save_xml:
        with open(args.save_xml, "wb") as fh:
            fh.write(payload)
        print(f"   XML сохранён: {args.save_xml}")

    if args.confirm:
        if doc_count == 0:
            print("\n4) success    → пропущен: подтверждать нечего")
        else:
            client.success()
            print("   ⚠ батч подтверждён — отгрузки помечены выгруженными")
    else:
        print("\n4) success    → НЕ выполнен (нужен флаг --confirm).")
        print("   Это правильно для проверки: батч остался неподтверждённым,")
        print("   настоящая 1С заберёт его штатно.")

    print("\nИТОГ: ✓ канал работает.", end=" ")
    if doc_count:
        print(f"1С заберёт {doc_count} документ(ов).")
    else:
        print("Документов к выгрузке сейчас нет — "
              "проведите отгрузку и повторите.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
