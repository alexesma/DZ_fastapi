"""UserDataXml титулов ЭТрН (LogisticsWaybill, kl_trn_mt_05_01) и разбор Т1.

Чистые функции без сети и БД. Формат упрощённого UserDataXml и значения
атрибутов взяты из документации Диадока (раздел «Работа с ЭТрН») и XSD
`ON_TRNACLGROT_MT_05_01_UserContract` (Т1) / `ON_TRNACLGRPO_MT_05_01_01_
UserContract` (Т3). Полное XML титула Диадок собирает сам в GenerateTitleXml.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional
from xml.sax.saxutils import quoteattr

from dz_fastapi.services.diadoc_cloud_sign import split_signer_full_name

MSK = timezone(timedelta(hours=3))

# Титулы в GenerateTitleXml (по документации Диадока)
T1_FUNCTION, T1_INDEX = "reception", 0
T3_FUNCTION, T3_INDEX = "delivery", 2
T6_FUNCTION, T6_INDEX = "cost", 5
ETRN_TYPE = "LogisticsWaybill"
ETRN_VERSION = "kl_trn_mt_05_01"

T3_RESULTS = ("accepted", "partial", "rejected")
T3_RESULT_LABELS = {
    "accepted": "Груз принят",
    "partial": "Груз принят частично",
    "rejected": "Груз не принят",
}


class EtrnDataError(ValueError):
    """Не хватает данных для титула — текст показывается пользователю."""


def _q(value: Any) -> str:
    return quoteattr(str(value if value is not None else ""))


def _attrs(**kwargs: Any) -> str:
    return "".join(
        f" {key}={_q(value)}"
        for key, value in kwargs.items()
        if value not in (None, "")
    )


def _num(value: Any, places: int = 3) -> str:
    try:
        number = Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        raise EtrnDataError(f"Некорректное число: {value!r}") from None
    return f"{number:.{places}f}"


def fmt_date(value: Any) -> str:
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").strftime(
                "%d.%m.%Y"
            )
        except ValueError:
            return value
    return value.strftime("%d.%m.%Y")


def fmt_datetime(value: datetime) -> str:
    """ДД.ММ.ГГГГTЧЧ:ММ:СС+ЧЧ:ММ — формат datetimeutc из XSD."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=MSK)
    text = value.strftime("%d.%m.%YT%H:%M:%S%z")
    return f"{text[:-2]}:{text[-2:]}"


def _need(payload: dict[str, Any], key: str, label: str) -> Any:
    value = payload.get(key)
    if value in (None, "", [], {}):
        raise EtrnDataError(f"Не заполнено: {label}")
    return value


def _phones(phone: Optional[str], indent: str) -> str:
    digits = str(phone or "").strip()
    if not digits:
        return ""
    return (
        f"{indent}<Phones><Phone>{_esc(digits)}</Phone></Phones>\n"
    )


def _esc(value: Any) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _org_ref(box_id: str, phone: Optional[str], tag: str, ind: str) -> str:
    inner = _phones(phone, ind + "    ")
    body = (
        f"{ind}<{tag}>\n"
        f"{ind}  <OrganizationReference BoxId={_q(box_id)}"
    )
    if inner:
        body += f">\n{inner}{ind}  </OrganizationReference>\n"
    else:
        body += " />\n"
    return body + f"{ind}</{tag}>\n"


def address_xml(address: dict[str, Any], tag: str, ind: str) -> str:
    """AddressLogisticsWaybill/RussianAddress из записи справочника."""
    if not address:
        raise EtrnDataError(f"Не указан адрес ({tag})")
    zip_code = str(address.get("zip_code") or "").strip()
    region = str(address.get("region_code") or "").strip()
    if not zip_code or not region:
        raise EtrnDataError(
            "В адресе обязательны индекс и код региона: "
            f"{address.get('label') or tag}"
        )
    russian = "<RussianAddress" + _attrs(
        ZipCode=zip_code,
        Region=region,
        City=address.get("city"),
        Locality=address.get("locality"),
        Street=address.get("street"),
        Building=address.get("building"),
        Block=address.get("block"),
        Apartment=address.get("apartment"),
        OtherInfo=address.get("other_info"),
    ) + " />"
    return (
        f"{ind}<{tag}>\n"
        f"{ind}  <AddressLogisticsWaybill>\n"
        f"{ind}    {russian}\n"
        f"{ind}  </AddressLogisticsWaybill>\n"
        f"{ind}</{tag}>\n"
    )


def signer_xml(
    signer_full_name: str,
    signer_position: str,
    warrant: Optional[dict[str, Any]] = None,
) -> str:
    surname, first_name, patronymic = split_signer_full_name(signer_full_name)
    if not signer_position:
        raise EtrnDataError("В профиле Диадока не заполнена должность подписанта")
    out = ""
    if warrant and warrant.get("id") and warrant.get("date"):
        out += (
            "  <WarrantInfo"
            + _attrs(
                WarrantDate=fmt_date(warrant["date"]),
                WarrantNumber=warrant.get("number"),
                WarrantId=warrant["id"],
            )
            + " />\n"
        )
    out += (
        '  <Signer SignerType="1">\n'
        "    <SignerDetails"
        + _attrs(
            Position=signer_position,
            LastName=surname,
            FirstName=first_name,
            MiddleName=patronymic,
        )
        + " />\n  </Signer>\n"
    )
    return out


def build_t1_user_data(
    payload: dict[str, Any],
    *,
    signer_full_name: str,
    signer_position: str,
    shipper_box_id: str,
    shipper_inn: str,
    shipper_phone: Optional[str] = None,
) -> bytes:
    """UserDataXml Т1 (LogisticsWaybillConsignorTitle).

    payload — снимок формы (см. services/etrn.py): number, date, order_*,
    consignee{box_id, phone}, delivery_address, cargo[], carrier{box_id,
    phone}, driver{...}, vehicle{...}, loading{...}, warrant.
    """
    number = str(_need(payload, "number", "номер ЭТрН"))
    waybill_date = _need(payload, "date", "дата ЭТрН")
    consignee = _need(payload, "consignee", "грузополучатель")
    if not consignee.get("box_id"):
        raise EtrnDataError(
            "У грузополучателя нет ящика Диадока — ЭТрН отправить нельзя"
        )
    carrier = _need(payload, "carrier", "перевозчик")
    if not carrier.get("box_id"):
        raise EtrnDataError(
            "У перевозчика не указан ящик Диадока — ЭТрН отправить нельзя"
        )
    driver = _need(payload, "driver", "водитель")
    vehicle = _need(payload, "vehicle", "транспортное средство")
    cargo = _need(payload, "cargo", "груз")
    loading = _need(payload, "loading", "сведения о погрузке")
    for key, label in (
        ("last_name", "фамилия водителя"),
        ("first_name", "имя водителя"),
    ):
        _need(driver, key, label)
    for key, label in (
        ("plate_number", "госномер ТС"),
        ("brand", "марка ТС"),
        ("vehicle_type", "тип ТС"),
        ("max_weight", "грузоподъёмность ТС, т"),
    ):
        _need(vehicle, key, label)

    items = ""
    total_places = 0
    total_weight = Decimal("0")
    for line in cargo:
        places = int(line.get("places") or 0)
        weight = Decimal(str(line.get("gross_weight") or 0))
        if not line.get("name") or places <= 0:
            raise EtrnDataError(
                "В грузе нужны наименование и число мест больше нуля"
            )
        total_places += places
        total_weight += weight
        items += (
            "          <ItemDescription"
            + _attrs(
                Name=line["name"],
                Condition=line.get("condition") or "Хорошее",
                PackageMethod=line.get("package_method") or "Коробки",
                ContainerType=line.get("container_type") or "00",
                CargoSpaceQuantity=places,
                IsForStateSystemRegistration="0",
            )
            + ">\n"
            "            <Marks><Mark>Отсутствует</Mark></Marks>\n"
            f"            <PlannedMass GrossWeight={_q(_num(weight, 3))} />\n"
            "          </ItemDescription>\n"
        )

    driver_attrs = _attrs(
        LastName=driver["last_name"],
        FirstName=driver["first_name"],
        MiddleName=driver.get("middle_name"),
        LicenseNumber=driver.get("license_number"),
        LicenseSeries=driver.get("license_series"),
        LicenseDate=fmt_date(driver["license_date"])
        if driver.get("license_date")
        else None,
    )
    veh_common = {
        "Ownership": str(vehicle.get("ownership") or "1"),
        "Type": vehicle["vehicle_type"],
        "Model": vehicle["brand"],
        "MaxWeight": _num(vehicle["max_weight"], 2),
    }
    vehicle_xml = (
        "      <Vehicle"
        + _attrs(Number=vehicle["plate_number"], **veh_common)
        + _attrs(Capacity=_num(vehicle["capacity"], 2)
                 if vehicle.get("capacity") not in (None, "") else None)
        + " />\n"
    )
    if vehicle.get("trailer_number"):
        trailer = dict(veh_common, Type="прицеп")
        vehicle_xml += (
            "      <Trailers>\n        <Trailer"
            + _attrs(Number=vehicle["trailer_number"], **trailer)
            + " />\n      </Trailers>\n"
        )

    stated = fmt_datetime(loading["arrival_at"])
    departed = fmt_datetime(loading["departure_at"])
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<LogisticsWaybillConsignorTitle"
        + _attrs(
            Number=number,
            Date=fmt_date(waybill_date),
            OrderNumber=payload.get("order_number") or number,
            OrderDate=fmt_date(payload.get("order_date") or waybill_date),
        )
        + ">\n  <WaybillInfo>\n"
        '    <ShipperInfo IsFreightForwarder="0">\n'
        + _org_ref(shipper_box_id, shipper_phone, "Shipper", "      ")
        + "    </ShipperInfo>\n"
        "    <ConsigneeInfo>\n"
        + _org_ref(
            consignee["box_id"], consignee.get("phone"), "Consignee", "      "
        )
        + address_xml(
            _need(payload, "delivery_address", "адрес выгрузки"),
            "DeliveryAddres",
            "      ",
        )
        + "    </ConsigneeInfo>\n"
        "    <CargoInfo>\n      <ItemDescriptions>\n"
        + items
        + "      </ItemDescriptions>\n    </CargoInfo>\n"
        "    <ConsignorDirectives TransportationDirectives="
        f"{_q(payload.get('directives') or 'Отсутствует')}>\n"
        '      <ReaddressDetails AccountableSide="Consignor">\n'
        + _phones(shipper_phone or carrier.get("phone") or "0000000", "        ")
        + "      </ReaddressDetails>\n    </ConsignorDirectives>\n"
        + _org_ref(carrier["box_id"], carrier.get("phone"), "Carrier", "    ")
        + "    <Drivers>\n"
        f"      <DriverInfo{driver_attrs}>\n"
        + _phones(driver.get("phone"), "        ")
        + "      </DriverInfo>\n    </Drivers>\n"
        "    <VehicleInfo>\n"
        + vehicle_xml
        + "    </VehicleInfo>\n"
        "    <LoadingInfo"
        + _attrs(
            StatedArrivalDateTime=stated,
            StatedArrivalDateTimeEnablingTimeZone="1",
            ActualArrivalDateTime=stated,
            ActualArrivalDateTimeEnablingTimeZone="1",
            ActualDepartureDateTime=departed,
            ActualDepartureDateTimeEnablingTimeZone="1",
            GrossWeight=_num(loading.get("gross_weight") or total_weight, 3),
            WeighingMethod=loading.get("weighing_method") or "03",
            NumberOfPlaces=loading.get("places") or total_places,
        )
        + ">\n"
        + address_xml(
            _need(payload, "loading_address", "адрес погрузки"),
            "ActualLoadingAddress",
            "      ",
        )
        + '      <LoadingPartyDetails MatchingShipper="1">\n'
        '        <LoadingPartyEmployee Position="Сотрудник">\n'
        "          <JobResponsibilities>Должностные обязанности"
        "</JobResponsibilities>\n"
        + "          <Fio"
        + _attrs(
            LastName=split_signer_full_name(signer_full_name)[0],
            FirstName=split_signer_full_name(signer_full_name)[1],
        )
        + " />\n        </LoadingPartyEmployee>\n"
        f"        <RequisitesShipper Inn={_q(shipper_inn)} />\n"
        "      </LoadingPartyDetails>\n"
        '      <LoadingOwnerDetails Type="1">\n'
        f"        <RequisitesShipper Inn={_q(shipper_inn)} />\n"
        "      </LoadingOwnerDetails>\n"
        "    </LoadingInfo>\n  </WaybillInfo>\n"
        + signer_xml(signer_full_name, signer_position, payload.get("warrant"))
        + "</LogisticsWaybillConsignorTitle>\n"
    )
    ET.fromstring(xml.encode("utf-8"))  # самопроверка: XML well-formed
    return xml.encode("utf-8")


def build_t3_user_data(
    *,
    organization_name: str,
    signer_full_name: str,
    signer_position: str,
    result: str,
    delivery_address: Optional[dict[str, Any]] = None,
    arrival_at: Optional[datetime] = None,
    departure_at: Optional[datetime] = None,
    stated_arrival_at: Optional[datetime] = None,
    places_received: Optional[int] = None,
    gross_weight_received: Optional[Any] = None,
    cargo_status: Optional[str] = None,
    rejection_reason: Optional[str] = None,
    warrant: Optional[dict[str, Any]] = None,
) -> bytes:
    """UserDataXml Т3 (LogisticsWaybillConsigneeTitle).

    accepted/partial — ReceivingOperation=0 + CargoAcceptance; частичная
    приёмка отличается числом мест/массой и текстом состояния груза.
    rejected — ReceivingOperation=1 + RejectionReason.
    """
    if result not in T3_RESULTS:
        raise EtrnDataError(f"Неизвестный результат приёмки: {result}")
    if not organization_name:
        raise EtrnDataError("В профиле Диадока не заполнено название организации")
    if result == "rejected":
        reason = str(rejection_reason or "").strip()
        if not reason:
            raise EtrnDataError("Укажите причину отказа от груза")
        info = (
            '  <ConsigneeInfo ReceivingOperation="1"'
            + _attrs(RejectionReason=reason)
            + " />\n"
        )
    else:
        if places_received is None or gross_weight_received in (None, ""):
            raise EtrnDataError(
                "Укажите число принятых мест и массу брутто принятого груза"
            )
        status = str(cargo_status or "").strip() or (
            "Груз принят без замечаний"
            if result == "accepted"
            else ""
        )
        if not status:
            raise EtrnDataError(
                "При частичной приёмке опишите, что принято и почему"
            )
        now = datetime.now(MSK)
        arrival = arrival_at or now
        departure = departure_at or now
        stated = stated_arrival_at or arrival
        info = (
            '  <ConsigneeInfo ReceivingOperation="0">\n'
            "    <CargoAcceptance"
            + _attrs(
                ActualArrivalDateTime=fmt_datetime(arrival),
                ActualArrivalDateTimeEnablingTimeZone="1",
                ActualDepartureDateTime=fmt_datetime(departure),
                ActualDepartureDateTimeEnablingTimeZone="1",
                StatedArrivalDateTime=fmt_datetime(stated),
                StatedArrivalDateTimeEnablingTimeZone="1",
                PlaceCount=int(places_received),
                CargoActualStatus=status,
                GrossWeightReceived=_num(gross_weight_received, 3),
            )
            + ">\n"
            + address_xml(
                delivery_address or {}, "DeliveryAddress", "      "
            )
            + "    </CargoAcceptance>\n  </ConsigneeInfo>\n"
        )
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<LogisticsWaybillConsigneeTitle"
        + _attrs(DocumentCreator=organization_name)
        + ">\n"
        + info
        + signer_xml(signer_full_name, signer_position, warrant)
        + "</LogisticsWaybillConsigneeTitle>\n"
    )
    ET.fromstring(xml.encode("utf-8"))
    return xml.encode("utf-8")


def build_t6_user_data(
    *,
    signer_full_name: str,
    signer_position: str,
    department: Optional[str] = None,
    okpo: Optional[str] = None,
    info: Optional[str] = None,
    bank: Optional[dict[str, Any]] = None,
    warrant: Optional[dict[str, Any]] = None,
) -> bytes:
    """UserDataXml Т6 (LogisticsWaybillConsignorCostTitle): ответ
    грузоотправителя на стоимость перевозки (Т5 перевозчика)."""
    consignor = ""
    if any([department, okpo, info, bank]):
        bank_xml = ""
        if bank and bank.get("account"):
            bank_xml = (
                "    <BankRequisites"
                + _attrs(
                    BankAccount=bank.get("account"),
                    BankName=bank.get("name"),
                    BankId=bank.get("bik"),
                    CorrespondentAccount=bank.get("corr_account"),
                )
                + " />\n"
            )
        consignor = (
            "  <ConsignorInfo"
            + _attrs(Department=department, Okpo=okpo, Info=info)
            + (">\n" + bank_xml + "  </ConsignorInfo>\n" if bank_xml else " />\n")
        )
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<LogisticsWaybillConsignorCostTitle>\n"
        + consignor
        + signer_xml(signer_full_name, signer_position, warrant)
        + "</LogisticsWaybillConsignorCostTitle>\n"
    )
    ET.fromstring(xml.encode("utf-8"))
    return xml.encode("utf-8")


# ----------------------------------------------------------- разбор Т1


def _party(node: Optional[ET.Element]) -> dict[str, Optional[str]]:
    if node is None:
        return {"name": None, "inn": None, "kpp": None}
    legal = node.find(".//СвЮЛУч")
    if legal is not None:
        return {
            "name": legal.get("НаимОрг"),
            "inn": legal.get("ИННЮЛ"),
            "kpp": legal.get("КПП"),
        }
    person = node.find(".//СвИП")
    if person is not None:
        fio = person.find("ФИО")
        name = " ".join(
            filter(
                None,
                [
                    fio.get("Фамилия") if fio is not None else None,
                    fio.get("Имя") if fio is not None else None,
                    fio.get("Отчество") if fio is not None else None,
                ],
            )
        )
        return {
            "name": f"ИП {name}".strip(),
            "inn": person.get("ИННФЛ"),
            "kpp": None,
        }
    return {"name": None, "inn": None, "kpp": None}


def _address_text(node: Optional[ET.Element]) -> Optional[str]:
    if node is None:
        return None
    addr = node.find(".//АдресРФ")
    if addr is None:
        addr = node.find(".//АдрРФ")
    if addr is None:
        return None
    parts = [
        addr.get("Индекс"),
        addr.get("Город") or addr.get("НаселПункт"),
        addr.get("Улица"),
        f"д. {addr.get('Дом')}" if addr.get("Дом") else None,
    ]
    return ", ".join(part for part in parts if part) or None


def parse_t1_summary(content: bytes) -> dict[str, Any]:
    """Краткое содержимое Т1 (полный XML ФНС) для показа и приёмки."""
    root = ET.fromstring(content)
    body = root.find(".//СодИнфГО")
    if body is None:
        raise EtrnDataError("В файле нет сведений грузоотправителя (СодИнфГО)")
    cargo = []
    for item in body.findall(".//ОпГруз"):
        mass = item.find("ПлМасГруз")
        cargo.append(
            {
                "name": item.get("НаимГруз"),
                "places": item.get("КолМестГр"),
                "gross_weight": mass.get("МасБрутЗнач")
                if mass is not None
                else None,
            }
        )
    loading = body.find("СвПогруз")
    return {
        "number": body.get("НомерТрН"),
        "date": body.get("ДатаТрН"),
        "shipper": _party(body.find("СвГО")),
        "consignee": _party(body.find("СвГП")),
        "carrier": _party(body.find("СвПер")),
        "delivery_address": _address_text(body.find("СвГП")),
        "loading_address": _address_text(loading),
        "cargo": cargo,
        "places": loading.get("КолМестПрием") if loading is not None else None,
        "gross_weight": loading.get("МасБрутОтгр")
        if loading is not None
        else None,
        "stated_arrival": loading.get("ЗаявПогр")
        if loading is not None
        else None,
    }
