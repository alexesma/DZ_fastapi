import xml.etree.ElementTree as ET
from datetime import datetime

import pytest
from sqlalchemy import select

from dz_fastapi.models.diadoc import DiadocCloudSignTask
from dz_fastapi.models.etrn import EtrnWaybill
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.diadoc_cloud_sign import confirm_cloud_sign_task
from dz_fastapi.services.diadoc_transport import (
    extract_kl_status,
    extract_mt_id,
    find_transport_problem,
    t1_already_sent,
    t3_already_signed,
)
from dz_fastapi.services.etrn import (
    EtrnError,
    start_t1_sign,
    start_t3_sign,
    sync_etrn,
    upsert_outgoing_draft,
)
from dz_fastapi.services.etrn_xml import (
    EtrnDataError,
    build_t1_user_data,
    build_t3_user_data,
    parse_t1_summary,
)

BOX = "11111111-1111-1111-1111-111111111111"
CARRIER_BOX = "22222222-2222-2222-2222-222222222222"
CLIENT_BOX = "33333333-3333-3333-3333-333333333333"

ADDRESS = {
    "label": "Склад", "zip_code": "620050", "region_code": "66",
    "city": "Екатеринбург", "street": "Ленина", "building": "1",
}


def _t1_payload():
    return {
        "number": "ТрН-1",
        "date": "2026-11-02",
        "consignee": {"box_id": CLIENT_BOX, "phone": "9056678765"},
        "delivery_address": dict(ADDRESS),
        "loading_address": dict(ADDRESS),
        "cargo": [
            {"name": "Автозапчасти", "places": 3, "gross_weight": "12.5"}
        ],
        "carrier": {"box_id": CARRIER_BOX, "phone": "89002827262"},
        "driver": {"last_name": "Кислицын", "first_name": "Михаил"},
        "vehicle": {
            "plate_number": "В777УС116", "brand": "Газель",
            "vehicle_type": "фургон", "max_weight": "1.5",
            "trailer_number": "АА1234",
        },
        "loading": {
            "arrival_at": datetime(2026, 11, 2, 9, 0),
            "departure_at": datetime(2026, 11, 2, 9, 30),
        },
    }


def _integration(**kw):
    row = DiadocIntegrationSettings(
        environment="staging", box_id_guid=BOX,
        organization_name='ООО "Дрэгонзап"', organization_inn="7701234567",
        signer_full_name="Ермоленко Александр Сергеевич",
        signer_position="Директор", etrn_enabled=True,
    )
    for key, value in kw.items():
        setattr(row, key, value)
    return row


# ---------------------------------------------------------------- XML


def test_t1_user_data_structure():
    xml = build_t1_user_data(
        _t1_payload(), signer_full_name="Ермоленко Александр Сергеевич",
        signer_position="Директор", shipper_box_id=BOX,
        shipper_inn="7701234567",
    )
    root = ET.fromstring(xml)
    assert root.tag == "LogisticsWaybillConsignorTitle"
    assert root.get("Number") == "ТрН-1" and root.get("Date") == "02.11.2026"
    info = root.find("WaybillInfo")
    assert info.find("ShipperInfo/Shipper/OrganizationReference").get(
        "BoxId") == BOX
    assert info.find("Carrier/OrganizationReference").get(
        "BoxId") == CARRIER_BOX
    assert info.find("Drivers/DriverInfo").get("LastName") == "Кислицын"
    assert info.find("VehicleInfo/Vehicle").get("Number") == "В777УС116"
    assert info.find("VehicleInfo/Trailers/Trailer").get("Number") == "АА1234"
    address = info.find("ConsigneeInfo/DeliveryAddres/AddressLogisticsWaybill"
                        "/RussianAddress")
    assert address.get("ZipCode") == "620050" and address.get("Region") == "66"
    loading = info.find("LoadingInfo")
    assert loading.get("NumberOfPlaces") == "3"
    assert loading.get("ActualDepartureDateTime") == "02.11.2026T09:30:00+03:00"
    signer = root.find("Signer/SignerDetails")
    assert signer.get("LastName") == "Ермоленко"
    assert root.find("WarrantInfo") is None


def test_t1_warrant_is_optional_and_added():
    payload = _t1_payload()
    payload["warrant"] = {"id": "W-1", "date": datetime(2026, 1, 1).date()}
    xml = build_t1_user_data(
        payload, signer_full_name="Иванов Иван", signer_position="Менеджер",
        shipper_box_id=BOX, shipper_inn="7701234567",
    )
    assert ET.fromstring(xml).find("WarrantInfo").get("WarrantId") == "W-1"


@pytest.mark.parametrize(
    "mutate, text",
    [
        (lambda p: p["carrier"].pop("box_id"), "перевозчика"),
        (lambda p: p["consignee"].pop("box_id"), "грузополучателя"),
        (lambda p: p.pop("driver"), "водитель"),
        (lambda p: p["delivery_address"].pop("zip_code"), "индекс"),
    ],
)
def test_t1_reports_missing_data(mutate, text):
    payload = _t1_payload()
    mutate(payload)
    with pytest.raises(EtrnDataError) as err:
        build_t1_user_data(
            payload, signer_full_name="Иванов Иван",
            signer_position="Директор", shipper_box_id=BOX,
            shipper_inn="7701234567",
        )
    assert text in str(err.value)


def _t3(**kw):
    base = dict(
        organization_name='ООО "Дрэгонзап"',
        signer_full_name="Ермоленко Александр Сергеевич",
        signer_position="Директор", result="accepted",
        delivery_address=ADDRESS, places_received=3,
        gross_weight_received="12.5",
    )
    base.update(kw)
    return build_t3_user_data(**base)


def test_t3_accepted_partial_rejected():
    accepted = ET.fromstring(_t3())
    info = accepted.find("ConsigneeInfo")
    assert info.get("ReceivingOperation") == "0"
    assert info.find("CargoAcceptance").get("CargoActualStatus") == (
        "Груз принят без замечаний")
    partial = ET.fromstring(
        _t3(result="partial", places_received=2,
            cargo_status="Принято 2 места из 3, одно повреждено")
    )
    assert partial.find("ConsigneeInfo/CargoAcceptance").get("PlaceCount") == "2"
    rejected = ET.fromstring(
        _t3(result="rejected", rejection_reason="Не наш груз")
    )
    assert rejected.find("ConsigneeInfo").get("ReceivingOperation") == "1"
    assert rejected.find("ConsigneeInfo").get("RejectionReason") == "Не наш груз"
    assert rejected.find("ConsigneeInfo/CargoAcceptance") is None


def test_t3_requires_details():
    with pytest.raises(EtrnDataError):
        _t3(result="rejected", rejection_reason="")
    with pytest.raises(EtrnDataError):
        _t3(result="partial", cargo_status="")
    with pytest.raises(EtrnDataError):
        _t3(places_received=None)


def test_parse_t1_summary():
    xml = """<?xml version="1.0" encoding="windows-1251"?>
<Файл><Документ><СодИнфГО НомерТрН="N-5" ДатаТрН="02.04.2026">
<СвГО><РекИдентГО><ИдСв><СвЮЛУч НаимОрг="Отправитель" ИННЮЛ="1" КПП="2"/>
</ИдСв></РекИдентГО></СвГО>
<СвГП><РекИдентГП><ИдСв><СвЮЛУч НаимОрг="Мы" ИННЮЛ="7701234567"/></ИдСв>
</РекИдентГП><АдресДостГр><АдресРФ Индекс="620050" Город="Екб"/></АдресДостГр></СвГП>
<СвГруз><ОпГруз НаимГруз="Масло" КолМестГр="5"><ПлМасГруз МасБрутЗнач="50"/>
</ОпГруз></СвГруз>
<СвПер><ИдСв><СвЮЛУч НаимОрг="Перевозчик" ИННЮЛ="9"/></ИдСв></СвПер>
<СвПогруз КолМестПрием="5" МасБрутОтгр="50"/>
</СодИнфГО></Документ></Файл>""".encode("windows-1251")
    summary = parse_t1_summary(xml)
    assert summary["number"] == "N-5"
    assert summary["consignee"]["inn"] == "7701234567"
    assert summary["carrier"]["name"] == "Перевозчик"
    assert summary["cargo"][0] == {
        "name": "Масло", "places": "5", "gross_weight": "50"}
    assert "Екб" in summary["delivery_address"]


# ------------------------------------------------------ статусы Диадока


def test_stage_mt_id_and_problems():
    payload = {
        "CustomData": [
            {"Key": "kl-status", "Value": "d-consingee-singed"},
            {"Key": "mt-id", "Value": "UID-1"},
        ],
        "OuterDocflowInfo": [
            {"DocflowNamedId": "KlDriverConfirm",
             "Status": {"Type": "Error", "FriendlyName": "Водитель отклонил"}}
        ],
    }
    assert extract_kl_status(payload) == "d-consingee-singed"
    assert extract_mt_id(payload) == "UID-1"
    assert find_transport_problem(payload) == "Водитель отклонил"
    assert find_transport_problem(
        {"CustomData": [{"Key": "kl-status", "Value": "r-rejected"}]}
    ) == "Перевозчик отказался от груза"
    assert find_transport_problem({"CustomData": []}) is None
    assert t3_already_signed("d-carrier-signed") and not t3_already_signed(
        "on-the-way")
    assert t1_already_sent("r-carrier-signed") and not t1_already_sent(
        "r-wait-storekeeper")


def test_mt_id_from_status_details():
    payload = {
        "OuterDocflowInfo": [{
            "DocflowNamedId": "KlMt",
            "Status": {"NamedId": "4000211000", "Details": [
                {"Code": "mt-id", "Value": "UID-9"}]},
        }]
    }
    assert extract_mt_id(payload) == "UID-9"


# --------------------------------------------------------------- сервис


class FakeClient:
    def __init__(self, documents=None, document=None):
        self.documents = documents or []
        self.document = document or {}
        self.posted = []
        self.generated = []

    async def get_documents(self, **kwargs):
        category = kwargs["filter_category"]
        docs = [d for d in self.documents if d["_cat"] == category]
        number = kwargs.get("document_number")
        if number:
            docs = [d for d in docs if d.get("DocumentNumber") == number]
        return {"Documents": docs}

    async def get_document(self, **kwargs):
        return self.document

    async def get_entity_content(self, **kwargs):
        raise RuntimeError("нет содержимого")

    async def generate_title_xml(self, **kwargs):
        self.generated.append(kwargs)
        return b"<xml/>", "title.xml"

    async def cloud_sign(self, files):
        return "token-1"

    async def cloud_sign_confirm(self, token, confirmation_code):
        return ["SIG"]

    async def post_message(self, message, operation_id=None):
        self.posted.append((message, operation_id))
        return {"MessageId": "M-1", "Entities": [{"EntityId": "E-1"}]}


def _draft_data():
    p = _t1_payload()
    return {
        "number": p["number"], "date": "2026-11-02",
        "consignee_box_id": CLIENT_BOX,
        "carrier_manual": {"name": "ИП Перевозчик", "box_id": CARRIER_BOX},
        "driver_manual": p["driver"], "vehicle_manual": p["vehicle"],
        "loading_address_manual": ADDRESS,
        "delivery_address_manual": ADDRESS, "cargo": p["cargo"],
        "loading_arrival_at": "2026-11-02T09:00:00+03:00",
        "loading_departure_at": "2026-11-02T09:30:00+03:00",
    }


async def _admin(session):
    user = User(
        email="admin@example.com", password_hash="x", name="a",
        role=UserRole.ADMIN, status=UserStatus.ACTIVE,
    )
    session.add(user)
    await session.commit()


@pytest.mark.asyncio
async def test_draft_requires_enabled_flag(test_session):
    with pytest.raises(EtrnError):
        await upsert_outgoing_draft(
            test_session, integration=_integration(etrn_enabled=False),
            data=_draft_data(),
        )


@pytest.mark.asyncio
async def test_t1_sign_flow_and_double_send_guard(test_session):
    integration = _integration()
    waybill = await upsert_outgoing_draft(
        test_session, integration=integration, data=_draft_data()
    )
    assert waybill.status == "draft" and waybill.counteragent_box_id == (
        CARRIER_BOX)
    client = FakeClient()
    task = await start_t1_sign(
        test_session, client=client, integration=integration, waybill=waybill
    )
    assert waybill.status == "sign_pending"
    assert client.generated[0]["title_index"] == 0
    await confirm_cloud_sign_task(
        test_session, client=client, task=task, confirmation_code="1234"
    )
    await test_session.refresh(waybill)
    assert waybill.status == "sent" and waybill.t1_message_id == "M-1"
    assert client.posted[0][1] == f"etrn-t1-{waybill.id}"
    assert client.posted[0][0]["ToBoxId"] == CARRIER_BOX
    # повторная отправка запрещена
    with pytest.raises(EtrnError):
        await start_t1_sign(
            test_session, client=client, integration=integration,
            waybill=waybill,
        )
    other = DiadocCloudSignTask(
        operation="etrn_t1_send", state="waiting_code", box_id_guid=BOX,
        cloud_sign_token="t", files=[{"content_b64": "eA=="}],
        params={"etrn_id": waybill.id},
    )
    test_session.add(other)
    await test_session.commit()
    with pytest.raises(EtrnError):
        await confirm_cloud_sign_task(
            test_session, client=client, task=other, confirmation_code="1")
    assert len(client.posted) == 1


@pytest.mark.asyncio
async def test_t1_not_sent_when_already_signed_in_1c(test_session):
    integration = _integration()
    waybill = await upsert_outgoing_draft(
        test_session, integration=integration, data=_draft_data()
    )
    client = FakeClient(documents=[{
        "_cat": "Any.OutboundNotRecycled", "TypeNamedId": "LogisticsWaybill",
        "DocumentNumber": "ТрН-1", "MessageId": "M-9", "EntityId": "E-9",
    }])
    with pytest.raises(EtrnError) as err:
        await start_t1_sign(
            test_session, client=client, integration=integration,
            waybill=waybill,
        )
    assert "уже есть в Диадоке" in str(err.value)
    await test_session.refresh(waybill)
    assert waybill.t1_message_id == "M-9" and waybill.status == "sent"
    assert not client.generated


async def _incoming(session, kl="on-the-way", inn="7701234567"):
    row = EtrnWaybill(
        environment="staging", direction="incoming", status="in_transit",
        box_id_guid=BOX, t1_message_id="IM-1", t1_entity_id="IE-1",
        kl_status=kl,
        payload={"summary": {
            "consignee": {"inn": inn}, "carrier": {"inn": "555"},
        }},
    )
    session.add(row)
    await session.commit()
    return row


@pytest.mark.asyncio
async def test_t3_blocked_when_stage_shows_signed(test_session):
    row = await _incoming(test_session)
    client = FakeClient(document={
        "CustomData": [{"Key": "kl-status", "Value": "d-carrier-signed"}]
    })
    with pytest.raises(EtrnError) as err:
        await start_t3_sign(
            test_session, client=client, integration=_integration(),
            waybill=row, form={"result": "accepted"},
        )
    assert "уже подписан" in str(err.value)
    await test_session.refresh(row)
    assert row.t3_state == "signed_external"


@pytest.mark.asyncio
async def test_t3_requires_us_as_consignee(test_session):
    row = await _incoming(test_session, inn="999")
    with pytest.raises(EtrnError) as err:
        await start_t3_sign(
            test_session, client=FakeClient(), integration=_integration(),
            waybill=row, form={"result": "accepted"},
        )
    assert "грузополучателем" in str(err.value)


@pytest.mark.asyncio
async def test_t3_sign_flow(test_session):
    from dz_fastapi.models.etrn import TransportCarrier

    test_session.add(TransportCarrier(
        name="Перевозчик", inn="555", diadoc_box_id=CARRIER_BOX))
    await test_session.commit()
    row = await _incoming(test_session)
    integration = _integration()
    client = FakeClient(document={
        "CustomData": [{"Key": "kl-status", "Value": "on-the-way"}]
    })
    task = await start_t3_sign(
        test_session, client=client, integration=integration, waybill=row,
        form={
            "result": "accepted", "places_received": 3,
            "gross_weight_received": 12.5, "delivery_address": ADDRESS,
        },
    )
    assert client.generated[0]["title_index"] == 2
    assert client.generated[0]["letter_id"] == "IM-1"
    await confirm_cloud_sign_task(
        test_session, client=client, task=task, confirmation_code="1")
    await test_session.refresh(row)
    assert row.t3_state == "signed_here" and row.t3_message_id == "M-1"
    message = client.posted[0][0]
    assert message["ToBoxId"] == CARRIER_BOX
    assert message["DocumentAttachments"][0]["Function"] == "delivery"


@pytest.mark.asyncio
async def test_sync_creates_waybill_and_notifies_once(test_session):
    await _admin(test_session)
    client = FakeClient(
        documents=[
            {"_cat": "Any.InboundNotRecycled",
             "TypeNamedId": "LogisticsWaybill", "MessageId": "IM-7",
             "EntityId": "IE-7", "DocumentNumber": "N-7"},
            {"_cat": "Any.InboundNotRecycled", "TypeNamedId": "Invoice",
             "MessageId": "X", "EntityId": "Y"},
            {"_cat": "Any.InboundNotRecycled",
             "TypeNamedId": "LogisticsWaybill", "Function": "fwd",
             "MessageId": "FW-1", "EntityId": "FE-1",
             "CustomData": [{"Key": "mt-id", "Value": "UID-7"},
                            {"Key": "kl-func", "Value": "delivery"}]},
        ],
        document={
            "CustomData": [{"Key": "kl-status", "Value": "r-rejected"},
                           {"Key": "mt-id", "Value": "UID-7"}],
        },
    )
    integration = _integration()
    first = await sync_etrn(
        test_session, client=client, integration=integration)
    assert first["created"] == 1 and first["problems"] == 1
    assert first["linked_t3"] == 1
    row = (await test_session.execute(select(EtrnWaybill))).scalar_one()
    assert row.mt_id == "UID-7" and row.status == "refused"
    assert row.t3_message_id == "FW-1"
    second = await sync_etrn(
        test_session, client=client, integration=integration)
    assert second["created"] == 0 and second["problems"] == 0
    notes = (await test_session.execute(select(AppNotification))).scalars().all()
    assert len(notes) == 1 and "ЭТрН" in notes[0].title


def test_driver_confirm_and_t6_xml():
    from dz_fastapi.services.diadoc_transport import extract_driver_confirm
    from dz_fastapi.services.etrn_xml import build_t6_user_data

    payload = {"OuterDocflowInfo": [{
        "DocflowNamedId": "KlDriverConfirm",
        "Status": {"NamedId": "Confirm", "FriendlyName": "Водитель согласовал"},
    }]}
    assert extract_driver_confirm(payload)["confirmed"] is True
    assert extract_driver_confirm({}) is None
    xml = build_t6_user_data(
        signer_full_name="Иванов Иван", signer_position="Директор",
        info="Договор 1", bank={"account": "407", "bik": "044"},
    )
    root = ET.fromstring(xml)
    assert root.tag == "LogisticsWaybillConsignorCostTitle"
    assert root.find("ConsignorInfo").get("Info") == "Договор 1"
    assert root.find("ConsignorInfo/BankRequisites").get("BankId") == "044"
    bare = ET.fromstring(build_t6_user_data(
        signer_full_name="Иванов Иван", signer_position="Директор"))
    assert bare.find("ConsignorInfo") is None


@pytest.mark.asyncio
async def test_t5_linked_and_t6_flow(test_session):
    from dz_fastapi.services.etrn import start_t6_sign

    integration = _integration(etrn_t6_enabled=True)
    waybill = EtrnWaybill(
        environment="staging", direction="outgoing", status="in_transit",
        box_id_guid=BOX, t1_message_id="OM-1", t1_entity_id="OE-1",
        mt_id="UID-5", payload={},
    )
    test_session.add(waybill)
    await test_session.commit()
    client = FakeClient(
        documents=[{
            "_cat": "Any.InboundNotRecycled",
            "TypeNamedId": "LogisticsWaybill", "Function": "cost",
            "MessageId": "C-1", "EntityId": "CE-1",
            "CustomData": [{"Key": "mt-id", "Value": "UID-5"}],
        }],
        document={},
    )
    client.patches = []

    async def post_patch(patch, operation_id=None):
        client.patches.append((patch, operation_id))
        return {}

    client.post_message_patch = post_patch
    await sync_etrn(test_session, client=client, integration=integration)
    await test_session.refresh(waybill)
    assert waybill.payload["cost"]["t5_entity_id"] == "CE-1"
    # флаг Т6 выключен — нельзя
    with pytest.raises(EtrnError):
        await start_t6_sign(
            test_session, client=client, form={},
            integration=_integration(etrn_t6_enabled=False), waybill=waybill,
        )
    task = await start_t6_sign(
        test_session, client=client, integration=integration,
        waybill=waybill, form={"info": "Договор"},
    )
    assert client.generated[0]["title_index"] == 5
    await confirm_cloud_sign_task(
        test_session, client=client, task=task, confirmation_code="1")
    patch, op = client.patches[0]
    assert patch["RecipientTitles"][0]["ParentEntityId"] == "CE-1"
    assert op == f"etrn-t6-{waybill.id}"
    await test_session.refresh(waybill)
    assert waybill.payload["cost"]["t6_state"] == "signed_here"
    with pytest.raises(EtrnError):
        await start_t6_sign(
            test_session, client=client, integration=integration,
            waybill=waybill, form={},
        )
