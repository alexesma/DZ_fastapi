from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart, Photo
from dz_fastapi.models.partner import (
    CUSTOMER_ORDER_ITEM_STATUS,
    CUSTOMER_ORDER_STATUS,
    Customer,
    CustomerExternalReference,
    CustomerOrder,
)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services import partssoft_order_reconciliation as service
from dz_fastapi.services.partssoft_order_reconciliation import (
    _candidate_score,
    _is_order_tracking_token,
    _remote_customer,
    _remote_order_number,
    local_order_fingerprint,
    remote_order_fingerprint,
)


def test_tracking_token_recognizes_regular_and_partssoft_auto_order_ids():
    assert _is_order_tracking_token("4eaf6aa8-6f80-44cc-8ac3-cb4e2c8ff3ca")
    assert _is_order_tracking_token("ps-123-456")
    assert not _is_order_tracking_token("обычный комментарий")


def test_remote_customer_uses_embedded_legal_details():
    customer = _remote_customer(
        {
            "customer_id": 7,
            "customer": {
                "id": 7,
                "compile_name": "Контакт",
                "email": "person@example.com",
                "email_org": "office@example.com",
                "essential": {
                    "company_name": "ООО Тест",
                    "inn": "77-01-23",
                    "kpp": "770101001",
                },
            },
        }
    )
    assert customer["external_id"] == 7
    assert customer["name"] == "ООО Тест"
    assert customer["email"] == "office@example.com"
    assert customer["inn"] == "770123"
    assert customer["kpp"] == "770101001"


def test_remote_order_number_uses_client_number_first():
    assert (
        _remote_order_number(
            {
                "id": 10,
                "order_id": 11,
                "id_1c": "1C-1",
                "load_order_client_number": "CLIENT-1",
            }
        )
        == "CLIENT-1"
    )


def test_order_fingerprints_normalize_and_sort_rows():
    remote = {
        "order_items": [
            {"oem": " B-2 ", "make_name": "Brand", "qnt": 1, "cost": 20},
            {"oem": "a-1", "make_name": "BRAND", "qnt": 2, "cost": "10.0"},
        ]
    }
    local = SimpleNamespace(
        items=[
            SimpleNamespace(oem="A-1", brand="brand", requested_qty=2, requested_price=10),
            SimpleNamespace(oem="b-2", brand="BRAND", requested_qty=1, requested_price="20.00"),
        ]
    )
    assert remote_order_fingerprint(remote) == local_order_fingerprint(local)


def test_candidate_score_uses_inn_kpp_and_company_name():
    customer = SimpleNamespace(
        id=10,
        name="РМС Черноземье",
        inn="3662217593",
        kpp="366201001",
        email_contact=None,
    )
    score, basis = _candidate_score(
        customer,
        name="ООО «РМС ЧЕРНОЗЕМЬЕ»",
        inn="3662217593",
        kpp="366201001",
        email="",
    )
    assert score == 160
    assert basis == ["ИНН", "КПП", "название"]


@pytest.mark.asyncio
async def test_link_partssoft_customer_fills_all_empty_details(
    test_session,
    created_customers,
    monkeypatch,
):
    customer = created_customers[0]
    customer.email_contact = None
    customer.inn = None
    customer.kpp = None
    customer.legal_address = None
    customer.postal_address = None
    customer.credit_limit = None
    customer.payment_terms_days = 0
    await test_session.commit()

    async def fake_fetch_customer(_external_customer_id):
        return {
            "id": 501,
            "compile_name": "ООО Партнёр",
            "email_org": "partner@example.com",
            "ur_type": 0,
            "discount_type_id": 12,
            "region_id": 28,
            "user_id": 4,
            "send_sms": True,
            "send_email": True,
            "nds": 20,
            "credit_limit": "150000.00",
            "pay_delay": 14,
            "essential": {
                "company_name": "ООО Партнёр",
                "company_type": "ООО",
                "inn": "7701234567",
                "kpp": "770101001",
                "bik": "044525225",
                "bank": "ПАО Банк",
                "city": "Москва",
                "loro_account": "40702810000000000001",
                "korr_schet": "30101810400000000225",
            },
            "contact": {"phone": "+74950000000", "cell_phone": "+79990000000"},
            "official_address": {"city": "Москва", "street": "Тверская", "house": "1"},
            "delivery_address": {"city": "Москва", "street": "Складская", "house": "2"},
        }

    monkeypatch.setattr(service, "_fetch_customer", fake_fetch_customer)
    result = await service.link_partssoft_customer(
        test_session,
        external_customer_id=501,
        local_customer_id=customer.id,
    )

    await test_session.refresh(customer)
    reference = await test_session.get(
        CustomerExternalReference,
        result["reference_id"],
    )
    assert customer.inn == "7701234567"
    assert customer.legal_name == "ООО Партнёр"
    assert customer.kpp == "770101001"
    assert customer.email_contact == "partner@example.com"
    assert customer.company_type == "ООО"
    assert customer.phone == "+74950000000"
    assert customer.additional_phone == "+79990000000"
    assert customer.legal_address == "Москва, Тверская, 1"
    assert customer.postal_address == "Москва, Складская, 2"
    assert customer.credit_limit == Decimal("150000.00")
    assert customer.payment_terms_days == 14
    assert customer.vat_rate == Decimal("20.000")
    assert customer.bank_bik == "044525225"
    assert customer.bank_name == "ПАО Банк"
    assert customer.bank_city == "Москва"
    assert customer.bank_account == "40702810000000000001"
    assert customer.correspondent_account == "30101810400000000225"
    assert customer.registration_source == "dragonzap.ru"
    assert reference.external_classification == {
        "ur_type": 0,
        "discount_type_id": 12,
        "region_id": 28,
        "user_id": 4,
        "send_sms": True,
        "send_email": True,
    }
    assert reference.is_verified is True
    assert reference.match_basis == "manual"
    assert reference.external_payload["id"] == 501
    assert reference.last_synced_at is not None


@pytest.mark.asyncio
async def test_automatic_legal_customer_match_requires_confirmation(
    test_session,
    created_customers,
    monkeypatch,
):
    customer = created_customers[0]
    customer.inn = "7701234567"
    customer.kpp = "770101001"
    await test_session.commit()

    resolved, result = await service._resolve_sync_customer(
        test_session,
        {
            "customer_id": 777,
            "customer": {
                "id": 777,
                "login_or_email": "wholesale-777",
                "essential": {
                    "company_name": "ООО Партнёр",
                    "inn": "7701234567",
                    "kpp": "770101001",
                },
            },
        },
    )
    await test_session.commit()
    reference = await test_session.scalar(
        select(CustomerExternalReference).where(
            CustomerExternalReference.external_customer_id == 777
        )
    )

    assert resolved.id == customer.id
    assert result == "created"
    assert reference.customer_id == customer.id
    assert reference.is_verified is False
    assert reference.match_basis == "automatic_match"
    assert reference.external_payload["id"] == 777

    async def fake_fetch_customer(_external_customer_id):
        return {
            "id": 777,
            "login_or_email": "wholesale-777",
            "essential": {
                "company_name": "ООО Партнёр",
                "inn": "7701234567",
                "kpp": "770101001",
            },
        }

    monkeypatch.setattr(service, "_fetch_customer", fake_fetch_customer)
    corrected_customer = created_customers[1]
    await service.link_partssoft_customer(
        test_session,
        external_customer_id=777,
        local_customer_id=corrected_customer.id,
    )
    await test_session.refresh(reference)

    assert reference.customer_id == corrected_customer.id
    assert reference.is_verified is True
    assert reference.match_basis == "manual"


@pytest.mark.asyncio
async def test_sync_imports_retail_order_once_and_preserves_offer_origin(
    test_session,
    monkeypatch,
):
    remote_order = {
        "id": 9001,
        "created_at": "2026-09-09T10:15:00+03:00",
        "load_order_client_number": "WEB-9001",
        "customer_id": 701,
        "customer": {
            "id": 701,
            "login_or_email": "retail-login-701",
            "compile_name": "Иван Иванов",
            "email": "retail-701@example.com",
            "region_id": 77,
        },
        "order_items": [
            {
                "id": 8001,
                "oem": "ABC-123",
                "make_name": "HAVAL",
                "detail_name": "Фильтр",
                "qnt": 2,
                "cost": "1500.50",
                "offer_id": 4401,
                "supplier_id": 55,
                "warehouse_id": 66,
                "hash_key": "offer-hash",
                "system_hash": "system-hash",
            }
        ],
    }

    async def fake_fetch_orders(_days, *, region_id=None):
        assert region_id is None
        created = service._parse_datetime(remote_order["created_at"])
        return created, created, [remote_order]

    monkeypatch.setattr(service, "_fetch_orders", fake_fetch_orders)
    monkeypatch.setattr(service, "PARTSSOFT_API_AUTO_ORDER_ENABLED", False)

    first = await service.sync_partssoft_orders(test_session)
    second = await service.sync_partssoft_orders(test_session)

    assert first["counts"] == {"imported": 1}
    assert second["counts"] == {"already_imported": 1}
    order = await test_session.get(CustomerOrder, first["imported_order_ids"][0])
    await test_session.refresh(order, attribute_names=["items", "customer"])
    assert order.external_source == "PARTS_SOFT"
    assert order.external_order_id == "9001"
    assert order.import_origin == "partssoft_recovery"
    assert order.source_subject == "Восстановлен из Parts-Soft"
    assert order.customer.name == "retail-login-701"
    reference = await test_session.scalar(
        select(CustomerExternalReference).where(
            CustomerExternalReference.external_customer_id == 701
        )
    )
    assert reference.is_verified is True
    assert reference.match_basis == "created_from_partssoft"
    assert len(order.items) == 1
    item = order.items[0]
    assert item.external_offer_id == "4401"
    assert item.external_provider_id == "55"
    assert item.external_warehouse_id == "66"
    assert item.source_resolution_status == "api_ready"
    assert item.source_payload["hash_key"] == "offer-hash"


@pytest.mark.asyncio
async def test_api_source_items_are_sent_once_and_marked(
    test_session,
    monkeypatch,
):
    customer = Customer(name="API customer")
    user = User(
        name="Parts-Soft sync",
        email="partssoft-sync@example.com",
        password_hash="test",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    test_session.add_all([customer, user])
    await test_session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        external_source="PARTS_SOFT",
        external_order_id="9002",
        import_origin="partssoft_recovery",
    )
    test_session.add(order)
    await test_session.flush()
    item = service._remote_item_to_customer_order_item(
        {
            "id": 8002,
            "oem": "API-1",
            "make_name": "HAVAL",
            "qnt": 1,
            "cost": "900.00",
            "offer_id": 4402,
            "supplier_id": 55,
            "hash_key": "api-offer-hash",
        },
        order_id=order.id,
        row_index=1,
    )
    test_session.add(item)
    await test_session.commit()

    calls = []

    async def fake_send(**kwargs):
        calls.append(kwargs)
        position = kwargs["request"][0]
        return SimpleNamespace(
            results=[
                {
                    "request_tracking_uuid": position.tracking_uuid,
                    "status": "success",
                }
            ],
            order_id=77,
        )

    from dz_fastapi.services import site_order_sender

    monkeypatch.setattr(service, "PARTSSOFT_API_AUTO_ORDER_ENABLED", True)
    monkeypatch.setenv("KEY_FOR_WEBSITE", "test-key")
    monkeypatch.setattr(site_order_sender, "send_dragonzap_site_order", fake_send)

    counts = await service._auto_order_api_items(
        test_session,
        order_ids=[order.id],
    )
    await test_session.refresh(item)
    second_counts = await service._auto_order_api_items(
        test_session,
        order_ids=[order.id],
    )

    assert counts == {"api_ordered_items": 1}
    assert second_counts == {}
    assert len(calls) == 1
    assert calls[0]["request"][0].hash_key == "api-offer-hash"
    assert item.source_resolution_status == "api_ordered"


@pytest.mark.asyncio
async def test_partssoft_product_sync_merges_card_and_photos(
    test_session,
    monkeypatch,
):
    remote_product = {
        "id": 741717,
        "oem": "ST-MR403027",
        "make_name": "SAT",
        "detail_name": "Тяга рулевая",
        "body": "Подробное описание",
        "weight": 0.3,
        "width": 5,
        "height": 2,
        "length": 22,
        "product_category_ids": [755],
        "product_properties": [{"name": "Минимальный заказ", "value": "1"}],
        "product_photo_url": "/system/product_photo/741717/main.jpg",
        "images": [{"photo_url": "/system/image_photo/1503/extra.jpg"}],
        "updated_at": "2026-08-28T15:03:45+03:00",
    }

    async def fake_fetch_products(updated_since=None):
        return [remote_product]

    monkeypatch.setattr(service, "_fetch_products", fake_fetch_products)
    monkeypatch.setenv("V3_BASE_URL", "https://admin.dragonzap.ru/api/v3")

    first = await service.sync_partssoft_products(test_session)
    second = await service.sync_partssoft_products(test_session)

    assert first["counts"]["created"] == 1
    assert first["counts"]["photos_added"] == 2
    assert second["counts"]["updated"] == 1
    assert second["counts"]["photos_existing"] == 2
    autopart = await test_session.scalar(
        select(AutoPart).where(AutoPart.partssoft_product_id == 741717)
    )
    photos = list(
        (
            await test_session.scalars(
                select(Photo).where(Photo.autopart_id == autopart.id)
            )
        ).all()
    )
    assert autopart.oem_number == "STMR403027"
    assert autopart.description == "Подробное описание"
    assert autopart.partssoft_payload["product_category_ids"] == [755]
    assert {photo.url for photo in photos} == {
        "https://admin.dragonzap.ru/system/product_photo/741717/main.jpg",
        "https://admin.dragonzap.ru/system/image_photo/1503/extra.jpg",
    }


@pytest.mark.asyncio
async def test_partssoft_order_process_does_not_require_email_config(
    test_session,
):
    from dz_fastapi.services.customer_orders import process_manual_customer_order

    customer = Customer(name="Розничный клиент Parts-Soft")
    test_session.add(customer)
    await test_session.flush()
    order = CustomerOrder(
        customer_id=customer.id,
        external_source="PARTS_SOFT",
        external_order_id="9010",
        status=CUSTOMER_ORDER_STATUS.NEW,
    )
    test_session.add(order)
    await test_session.flush()
    item = service._remote_item_to_customer_order_item(
        {
            "id": 8010,
            "oem": "46530-50041",
            "make_name": "TOYOTA",
            "detail_name": "Колодка стояночного тормоза",
            "qnt": 3,
            "cost": "2967.00",
            "price_id": 383,
        },
        order_id=order.id,
        row_index=1,
    )
    test_session.add(item)
    await test_session.commit()

    processed = await process_manual_customer_order(test_session, order.id)
    await test_session.refresh(item)

    assert processed.status == CUSTOMER_ORDER_STATUS.PROCESSED
    assert item.status == CUSTOMER_ORDER_ITEM_STATUS.SUPPLIER
    assert item.ship_qty == 3
    assert item.source_resolution_status == "partssoft_price"
