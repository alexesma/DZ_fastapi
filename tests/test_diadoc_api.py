import base64
from datetime import timedelta
from io import BytesIO

import pandas as pd
import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.settings import crud_diadoc_integration_settings
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.inventory import (
    ReturnDocumentStatus,
    ReturnFromCustomer,
    ReturnItem,
    ReturnToSupplier,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
)
from dz_fastapi.models.partner import (
    SUPPLIER_ORDER_STATUS,
    CustomerExternalReference,
    ProviderExternalReference,
    SupplierOrder,
    SupplierOrderItem,
    SupplierOrderMessage,
    SupplierReceipt,
    SupplierReceiptItem,
    SupplierResponseConfig,
)
from dz_fastapi.models.user import UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.diadoc_oauth import build_diadoc_oauth_state


async def _create_user(
    session,
    email: str,
    password: str,
    role: UserRole = UserRole.MANAGER,
    status: UserStatus = UserStatus.PENDING,
    name: str | None = None,
):
    from dz_fastapi.models.user import User

    user = User(
        name=name,
        email=email.lower().strip(),
        password_hash=get_password_hash(password),
        role=role,
        status=status,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


async def _login_admin(async_client, test_session):
    await _create_user(
        test_session,
        email="admin@example.com",
        password="secret123",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    response = await async_client.post(
        "/auth/login",
        json={"email": "admin@example.com", "password": "secret123"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_diadoc_status_and_init_require_admin(async_client, test_session, monkeypatch):
    response = await async_client.get("/diadoc/status")
    assert response.status_code == 401

    await _login_admin(async_client, test_session)

    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.build_diadoc_auth_url",
        lambda environment: f"https://identity.example/auth?env={environment}",
    )

    response = await async_client.post(
        "/diadoc/oauth/init",
        json={"environment": "staging"},
    )
    assert response.status_code == 200
    assert response.json()["auth_url"] == ("https://identity.example/auth?env=staging")

    response = await async_client.get("/diadoc/status")
    assert response.status_code == 200
    data = response.json()
    assert data["configured"] is False
    assert data["connected"] is False
    assert data["environment"] == "staging"


@pytest.mark.asyncio
async def test_diadoc_oauth_callback_saves_tokens_and_autoselects_box(
    async_client,
    test_session,
    monkeypatch,
):
    async def fake_exchange(code: str):
        assert code == "test-code"
        return {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "openid profile email offline_access Diadoc.PublicAPI.Staging",
        }

    async def fake_get_my_user(self):
        return {
            "Id": "user-guid",
            "LastName": "Иванов",
            "FirstName": "Иван",
            "MiddleName": "Иванович",
        }

    async def fake_get_my_organizations(self, auto_register=False):
        assert auto_register is False
        return {
            "Organizations": [
                {
                    "OrgIdGuid": "org-guid",
                    "Inn": "7700000000",
                    "Kpp": "770001001",
                    "FullName": "ООО Тест",
                    "ShortName": "Тест",
                    "Boxes": [
                        {
                            "BoxId": "box@diadoc.ru",
                            "BoxIdGuid": "box-guid",
                            "Title": "Основной ящик",
                            "InvoiceFormatVersion": "v5_02",
                            "EncryptedDocumentsAllowed": False,
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.exchange_diadoc_code_for_tokens",
        fake_exchange,
    )
    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_my_user",
        fake_get_my_user,
    )
    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_my_organizations",
        fake_get_my_organizations,
    )

    state = build_diadoc_oauth_state("staging")
    response = await async_client.get(
        "/diadoc/oauth/callback",
        params={"code": "test-code", "state": state},
    )
    assert response.status_code == 200
    assert "Диадок подключен" in response.text

    integration = await crud_diadoc_integration_settings.get_or_create(test_session)
    assert integration.environment == "staging"
    assert integration.refresh_token == "refresh-token"
    assert integration.access_token == "access-token"
    assert integration.connected_user_id == "user-guid"
    assert integration.connected_user_name == "Иванов Иван Иванович"
    assert integration.organization_id == "org-guid"
    assert integration.box_id_guid == "box-guid"


@pytest.mark.asyncio
async def test_diadoc_documents_and_content_use_selected_box(
    async_client,
    test_session,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
        },
    )

    async def fake_get_documents(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        return {
            "TotalCount": 1,
            "HasMoreResults": False,
            "Documents": [
                {
                    "MessageId": "message-1",
                    "EntityId": "entity-1",
                    "FileName": "invoice.xml",
                    "DocumentDate": "11.05.2026",
                    "DocumentNumber": "UPD-1",
                }
            ],
        }

    async def fake_get_document(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        return {
            "MessageId": "message-1",
            "EntityId": "entity-1",
            "FileName": "invoice.xml",
        }

    async def fake_get_entity_content(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        return b"<xml />"

    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_documents",
        fake_get_documents,
    )
    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_document",
        fake_get_document,
    )
    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_entity_content",
        fake_get_entity_content,
    )

    response = await async_client.get("/diadoc/documents")
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 1
    assert data["documents"][0]["message_id"] == "message-1"
    assert data["documents"][0]["file_name"] == "invoice.xml"

    response = await async_client.get("/diadoc/documents/message-1/entity-1")
    assert response.status_code == 200
    assert response.json()["MessageId"] == "message-1"

    response = await async_client.get("/diadoc/documents/message-1/entity-1/content")
    assert response.status_code == 200
    assert response.content == b"<xml />"
    assert "invoice.xml" in response.headers["content-disposition"]


@pytest.mark.asyncio
async def test_diadoc_process_endpoint_registers_message_and_posts_receipt(
    async_client,
    test_session,
    created_providers,
    created_autopart,
    tmp_path,
):
    await _login_admin(async_client, test_session)

    provider = created_providers[0]
    order = SupplierOrder(
        provider_id=provider.id,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add(order)
    await test_session.flush()

    matched_item = SupplierOrderItem(
        supplier_order_id=order.id,
        autopart_id=created_autopart.id,
        oem_number=created_autopart.oem_number,
        brand_name=created_autopart.brand.name,
        autopart_name=created_autopart.name,
        quantity=5,
        price=120.0,
    )
    missing_item = SupplierOrderItem(
        supplier_order_id=order.id,
        autopart_id=created_autopart.id,
        oem_number="MISS-002",
        brand_name=created_autopart.brand.name,
        autopart_name="Missing row item",
        quantity=4,
        price=90.0,
    )
    test_session.add_all([matched_item, missing_item])
    test_session.add(
        SupplierResponseConfig(
            provider_id=provider.id,
            name="Diadoc doc parser",
            sender_emails=["docs@example.com"],
            response_type="file",
            file_payload_type="document",
            file_format="excel",
            start_row=1,
            oem_col=1,
            brand_col=2,
            qty_col=3,
            price_col=4,
            process_shipping_docs=True,
            is_active=True,
        )
    )

    frame = pd.DataFrame(
        [
            [
                created_autopart.oem_number,
                created_autopart.brand.name,
                2,
                101.0,
            ],
            ["EXTRA-999", "EXTRA-BRAND", 7, 333.0],
        ]
    )
    buffer = BytesIO()
    frame.to_excel(buffer, index=False, header=False)
    file_path = tmp_path / f"supplier_order_{order.id}_doc.xlsx"
    file_path.write_bytes(buffer.getvalue())

    document = DiadocIncomingDocument(
        environment="staging",
        box_id_guid="box-guid",
        message_id="message-77",
        entity_id="entity-77",
        provider_id=provider.id,
        file_name=file_path.name,
        local_file_path=str(file_path),
        status="synced",
    )
    test_session.add(document)
    await test_session.commit()
    await test_session.refresh(document)

    response = await async_client.post(
        f"/diadoc/inbound-documents/{document.id}/process",
        json={"register_if_needed": True},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["already_processed"] is False
    assert data["created_receipts"] == 1
    assert data["posted_receipts"] == 1
    assert data["parsed_response_files"] == 1
    assert data["supplier_order_message_id"] > 0
    assert len(data["receipt_ids"]) == 1

    await test_session.refresh(matched_item)
    await test_session.refresh(missing_item)
    await test_session.refresh(document)
    stored_document = document
    stored_message = await test_session.get(
        SupplierOrderMessage,
        data["supplier_order_message_id"],
    )
    receipt = (
        await test_session.execute(
            select(SupplierReceipt).where(SupplierReceipt.id == data["receipt_ids"][0])
        )
    ).scalar_one()
    receipt_items = (
        (
            await test_session.execute(
                select(SupplierReceiptItem).where(SupplierReceiptItem.receipt_id == receipt.id)
            )
        )
        .scalars()
        .all()
    )
    linked_rows = [row for row in receipt_items if row.supplier_order_item_id == matched_item.id]
    unlinked_rows = [row for row in receipt_items if row.supplier_order_item_id is None]

    assert stored_document is not None
    assert stored_document.status == "processed"
    assert stored_message is not None
    assert stored_message.message_type == "SHIPPING_DOC"
    assert matched_item.received_quantity == 2
    assert missing_item.confirmed_quantity == 0
    assert missing_item.response_status_raw == "автоотказ по документу"
    assert receipt.posted_at is not None
    assert len(linked_rows) == 1
    assert linked_rows[0].received_quantity == 2
    assert len(unlinked_rows) == 1
    assert unlinked_rows[0].oem_number == "EXTRA999"
    assert unlinked_rows[0].received_quantity == 7


@pytest.mark.asyncio
async def test_diadoc_sync_can_auto_process_documents(
    async_client,
    test_session,
    created_providers,
    created_autopart,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
        },
    )

    provider = created_providers[0]
    counteragent_box_id = "counteragent-box-77"
    test_session.add(
        ProviderExternalReference(
            provider_id=provider.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_supplier_name=counteragent_box_id,
            is_active=True,
        )
    )

    order = SupplierOrder(
        provider_id=provider.id,
        status=SUPPLIER_ORDER_STATUS.SENT,
    )
    test_session.add(order)
    await test_session.flush()

    matched_item = SupplierOrderItem(
        supplier_order_id=order.id,
        autopart_id=created_autopart.id,
        oem_number=created_autopart.oem_number,
        brand_name=created_autopart.brand.name,
        autopart_name=created_autopart.name,
        quantity=5,
        price=120.0,
    )
    missing_item = SupplierOrderItem(
        supplier_order_id=order.id,
        autopart_id=created_autopart.id,
        oem_number="MISS-003",
        brand_name=created_autopart.brand.name,
        autopart_name="Missing row item",
        quantity=4,
        price=90.0,
    )
    test_session.add_all([matched_item, missing_item])
    test_session.add(
        SupplierResponseConfig(
            provider_id=provider.id,
            name="Diadoc auto parser",
            sender_emails=["docs@example.com"],
            response_type="file",
            file_payload_type="document",
            file_format="excel",
            start_row=1,
            oem_col=1,
            brand_col=2,
            qty_col=3,
            price_col=4,
            process_shipping_docs=True,
            is_active=True,
        )
    )
    await test_session.commit()

    frame = pd.DataFrame(
        [
            [
                created_autopart.oem_number,
                created_autopart.brand.name,
                3,
                111.0,
            ],
            ["EXTRA-555", "EXTRA-BRAND", 6, 222.0],
        ]
    )
    buffer = BytesIO()
    frame.to_excel(buffer, index=False, header=False)
    content = buffer.getvalue()
    file_name = f"supplier_order_{order.id}_doc.xlsx"

    async def fake_get_documents(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        return {
            "TotalCount": 1,
            "HasMoreResults": False,
            "Documents": [
                {
                    "MessageId": "message-sync-1",
                    "EntityId": "entity-sync-1",
                    "IndexKey": "idx-1",
                    "CounteragentBoxId": counteragent_box_id,
                    "FileName": file_name,
                    "DocumentDate": "11.05.2026",
                    "DocumentNumber": "UPD-SYNC-1",
                }
            ],
        }

    async def fake_get_entity_content(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        assert kwargs["message_id"] == "message-sync-1"
        assert kwargs["entity_id"] == "entity-sync-1"
        return content

    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_documents",
        fake_get_documents,
    )
    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_entity_content",
        fake_get_entity_content,
    )

    response = await async_client.post(
        "/diadoc/sync/inbound",
        json={
            "count": 10,
            "download_content": True,
            "process_supplier_message": True,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_from_api"] == 1
    assert data["created"] == 1
    assert data["downloaded"] == 1
    assert data["registered_supplier_messages"] == 1
    assert data["processed_supplier_messages"] == 1
    assert data["processing_skipped"] == 0
    assert data["provider_resolved"] == 1
    assert data["errors"] == []

    synced_document = (
        await test_session.execute(
            select(DiadocIncomingDocument).where(
                DiadocIncomingDocument.message_id == "message-sync-1"
            )
        )
    ).scalar_one()
    await test_session.refresh(matched_item)
    await test_session.refresh(missing_item)
    receipt = (
        await test_session.execute(
            select(SupplierReceipt).where(
                SupplierReceipt.source_message_id == synced_document.supplier_order_message_id
            )
        )
    ).scalar_one()
    receipt_items = (
        (
            await test_session.execute(
                select(SupplierReceiptItem).where(SupplierReceiptItem.receipt_id == receipt.id)
            )
        )
        .scalars()
        .all()
    )

    assert synced_document.status == "processed"
    assert synced_document.supplier_order_message_id is not None
    assert matched_item.received_quantity == 3
    assert missing_item.response_status_raw == "автоотказ по документу"
    assert receipt.posted_at is not None
    assert any(
        row.supplier_order_item_id == matched_item.id and row.received_quantity == 3
        for row in receipt_items
    )
    assert any(
        row.supplier_order_item_id is None
        and row.oem_number == "EXTRA555"
        and row.received_quantity == 6
        for row in receipt_items
    )


@pytest.mark.asyncio
async def test_diadoc_counteragents_and_provider_binding(
    async_client,
    test_session,
    created_providers,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
        },
    )

    async def fake_get_counteragents(self, **kwargs):
        assert kwargs["my_box_id_guid"] == "box-guid"
        assert kwargs["query"] == "Авто"
        return {
            "TotalCount": 1,
            "HasMoreResults": False,
            "Counteragents": [
                {
                    "BoxIdGuid": "counteragent-guid-1",
                    "BoxId": "counteragent@diadoc.ru",
                    "FullName": "ООО Авто Контрагент",
                    "ShortName": "Авто Контрагент",
                    "Inn": "7700000000",
                    "Kpp": "770001001",
                    "CurrentStatusNamedId": "IsMyCounteragent",
                }
            ],
        }

    monkeypatch.setattr(
        "dz_fastapi.api.diadoc.DiadocClient.get_counteragents",
        fake_get_counteragents,
    )

    response = await async_client.get(
        "/diadoc/counteragents",
        params={"query": "Авто"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_count"] == 1
    assert data["counteragents"][0]["box_id_guid"] == "counteragent-guid-1"
    assert data["counteragents"][0]["mapped_provider_id"] is None

    response = await async_client.post(
        f"/diadoc/providers/{created_providers[0].id}/bind-counteragent",
        json={"counteragent_box_id": "counteragent-guid-1"},
    )
    assert response.status_code == 200
    binding = response.json()
    assert binding["provider_id"] == created_providers[0].id
    assert binding["source_system"] == "DIADOC_COUNTERAGENT_BOX"
    assert binding["external_supplier_name"] == "counteragent-guid-1"

    response = await async_client.get(
        "/diadoc/counteragents",
        params={"query": "Авто"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["counteragents"][0]["mapped_provider_id"] == created_providers[0].id
    assert data["counteragents"][0]["mapped_provider_name"] == created_providers[0].name


@pytest.mark.asyncio
async def test_diadoc_can_bind_customer_counteragent(
    async_client,
    test_session,
    created_customers,
):
    await _login_admin(async_client, test_session)

    response = await async_client.post(
        f"/diadoc/customers/{created_customers[0].id}/bind-counteragent",
        json={"counteragent_box_id": "customer-counteragent-guid-1"},
    )
    assert response.status_code == 200
    binding = response.json()
    assert binding["customer_id"] == created_customers[0].id
    assert binding["source_system"] == "DIADOC_COUNTERAGENT_BOX"
    assert binding["external_customer_name"] == "customer-counteragent-guid-1"

    reference = (
        await test_session.execute(
            select(CustomerExternalReference).where(
                CustomerExternalReference.customer_id == created_customers[0].id
            )
        )
    ).scalar_one()
    assert reference.external_customer_name == "customer-counteragent-guid-1"


@pytest.mark.asyncio
async def test_diadoc_can_create_outgoing_draft(
    async_client,
    test_session,
    created_providers,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
        },
    )

    provider = created_providers[0]
    test_session.add(
        ProviderExternalReference(
            provider_id=provider.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_supplier_name="counteragent-guid-2",
            is_active=True,
        )
    )
    await test_session.commit()

    async def fake_post_message(self, **kwargs):
        message = kwargs["message"]
        assert message["FromBoxId"] == "box-guid"
        assert message["ToBoxId"] == "counteragent-guid-2"
        assert message["IsDraft"] is True
        attachment = message["DocumentAttachments"][0]
        assert attachment["TypeNamedId"] == "Nonformalized"
        assert attachment["Metadata"][0]["Key"] == "FileName"
        return {
            "MessageId": "outgoing-message-1",
            "Entities": [
                {
                    "EntityId": "outgoing-entity-1",
                }
            ],
        }

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.post_message",
        fake_post_message,
    )

    content = b"hello diadoc draft"
    response = await async_client.post(
        "/diadoc/outbound-documents",
        json={
            "provider_id": provider.id,
            "file_name": "contract.pdf",
            "content_base64": base64.b64encode(content).decode("ascii"),
            "comment": "Черновик договора",
            "send_mode": "draft",
            "document_number": "DOG-77",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["provider_id"] == provider.id
    assert data["to_box_id_guid"] == "counteragent-guid-2"
    assert data["status"] == "draft"
    assert data["message_id"] == "outgoing-message-1"
    assert data["entity_id"] == "outgoing-entity-1"
    assert data["document_number"] == "DOG-77"


@pytest.mark.asyncio
async def test_diadoc_can_create_outgoing_draft_from_shipment_document(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
        },
    )

    customer = created_customers[0]
    test_session.add(
        CustomerExternalReference(
            customer_id=customer.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="customer-counteragent-guid-2",
            is_active=True,
        )
    )
    shipment = ShipmentDocument(
        doc_number="SHIP-42",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
        notes="Отгрузка клиенту",
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            quantity=2,
            price=123.45,
            notes="Без ГТД, товар РФ",
        )
    )
    await test_session.commit()

    async def fake_post_message(self, **kwargs):
        message = kwargs["message"]
        assert message["FromBoxId"] == "box-guid"
        assert message["ToBoxId"] == "customer-counteragent-guid-2"
        assert message["IsDraft"] is True
        attachment = message["DocumentAttachments"][0]
        assert attachment["TypeNamedId"] == "Nonformalized"
        content = base64.b64decode(attachment["SignedContent"]["Content"].encode("ascii")).decode(
            "utf-8"
        )
        assert "<ShipmentDocument>" in content
        assert "<DocumentNumber>SHIP-42</DocumentNumber>" in content
        assert "E4G163611091" in content
        assert "Без ГТД, товар РФ" not in content
        return {
            "MessageId": "outgoing-message-shipment-1",
            "Entities": [{"EntityId": "outgoing-entity-shipment-1"}],
        }

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.post_message",
        fake_post_message,
    )

    response = await async_client.post(
        f"/diadoc/outbound-documents/from-shipment/{shipment.id}",
        json={"send_mode": "draft"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["customer_id"] == customer.id
    assert data["customer_name"] == customer.name
    assert data["status"] == "draft"
    assert data["message_id"] == "outgoing-message-shipment-1"
    assert data["entity_id"] == "outgoing-entity-shipment-1"
    assert data["document_number"] == "SHIP-42"
    assert data["source_type"] == "shipment_document"
    assert data["source_id"] == shipment.id


@pytest.mark.asyncio
async def test_diadoc_can_create_formalized_utd_from_shipment_document(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
            "organization_name": "ООО ДрагонЗап",
            "organization_inn": "7700000000",
            "organization_kpp": "770001001",
            "seller_legal_address": "г. Москва, ул. Продавца, д. 5",
            "signer_full_name": "Иванов Иван Иванович",
            "signer_position": "Генеральный директор",
            "signer_basis": "Устав",
            "formalized_default_function": "ДОП",
        },
    )

    customer = created_customers[0]
    test_session.add(
        CustomerExternalReference(
            customer_id=customer.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="customer-counteragent-guid-4",
            is_active=True,
        )
    )
    shipment = ShipmentDocument(
        doc_number="SHIP-UTD-1",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
        reason="товары переданы",
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            quantity=3,
            price=200.0,
        )
    )
    await test_session.commit()

    async def fake_get_document_types(self, box_id_guid):
        assert box_id_guid == "box-guid"
        return {
            "DocumentTypes": [
                {
                    "TypeNamedId": "UniversalTransferDocument",
                    "Functions": [
                        {
                            "Name": "ДОП",
                            "Versions": [
                                {
                                    "Version": "utd970_05_03_01",
                                    "IsActual": True,
                                    "Titles": [{"Index": 0, "IsFormal": True}],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    async def fake_get_organization(self, **kwargs):
        box_id_guid = kwargs.get("box_id_guid")
        if box_id_guid == "box-guid":
            return {
                "FullName": "ООО ДрагонЗап",
                "ShortName": "ДрагонЗап",
                "Inn": "7700000000",
                "Kpp": "770001001",
                "FnsParticipantId": "2BM-7700000000-770001001-202401010000000000001",
                "Address": {
                    "RussianAddress": {
                        "Region": "77",
                        "City": "Москва",
                        "Street": "Тестовая",
                        "Building": "5",
                    }
                },
            }
        assert box_id_guid == "customer-counteragent-guid-4"
        return {
            "FullName": "ООО Покупатель",
            "ShortName": "Покупатель",
            "Inn": "7701234567",
            "Kpp": "770101001",
            "FnsParticipantId": "2BM-7701234567-770101001-202401010000000000002",
            "Address": {
                "RussianAddress": {
                    "Region": "50",
                    "City": "Химки",
                    "Street": "Покупателя",
                    "Building": "7",
                }
            },
        }

    async def fake_generate_title_xml(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        assert kwargs["document_type_named_id"] == "UniversalTransferDocument"
        assert kwargs["document_function"] == "ДОП"
        assert kwargs["document_version"] == "utd970_05_03_01"
        xml_text = kwargs["user_data_xml"].decode("utf-8")
        assert "<UniversalTransferDocument" in xml_text
        assert 'Function="ДОП"' in xml_text
        assert 'TaxRate="NoVat"' in xml_text
        assert "<Signers>" in xml_text
        return (
            ('<?xml version="1.0" encoding="windows-1251"?>' '<Файл ИдФайл="test-utd" />').encode(
                "utf-8"
            ),
            "ON_NSCHFDOPPR_test.xml",
        )

    async def fake_post_message(self, **kwargs):
        message = kwargs["message"]
        assert message["FromBoxId"] == "box-guid"
        assert message["ToBoxId"] == "customer-counteragent-guid-4"
        attachment = message["DocumentAttachments"][0]
        assert attachment["TypeNamedId"] == "UniversalTransferDocument"
        assert attachment["Function"] == "ДОП"
        assert attachment["Version"] == "utd970_05_03_01"
        content = base64.b64decode(attachment["SignedContent"]["Content"].encode("ascii")).decode(
            "utf-8"
        )
        assert "<Файл" in content
        return {
            "MessageId": "outgoing-message-utd-1",
            "Entities": [{"EntityId": "outgoing-entity-utd-1"}],
        }

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.get_document_types",
        fake_get_document_types,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.get_organization",
        fake_get_organization,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.generate_title_xml",
        fake_generate_title_xml,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.post_message",
        fake_post_message,
    )

    response = await async_client.post(
        f"/diadoc/outbound-documents/from-shipment/{shipment.id}",
        json={
            "send_mode": "draft",
            "document_format": "formalized_utd",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["customer_id"] == customer.id
    assert data["type_named_id"] == "UniversalTransferDocument"
    assert data["document_function"] == "ДОП"
    assert data["document_version"] == "utd970_05_03_01"
    assert data["file_name"] == "ON_NSCHFDOPPR_test.xml"
    assert data["status"] == "draft"
    assert data["message_id"] == "outgoing-message-utd-1"


@pytest.mark.asyncio
async def test_diadoc_shipment_readiness_reports_formalized_status(
    async_client,
    test_session,
    created_autopart,
    created_customers,
):
    await _login_admin(async_client, test_session)

    customer = created_customers[0]
    customer.inn = "7701234567"
    customer.kpp = "770101001"
    customer.legal_address = "г. Москва, ул. Тестовая, д. 1"
    customer.postal_address = "г. Москва, а/я 10"
    test_session.add(customer)

    test_session.add(
        CustomerExternalReference(
            customer_id=customer.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="customer-counteragent-guid-3",
            is_active=True,
        )
    )

    shipment = ShipmentDocument(
        doc_number="SHIP-READY-1",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
        notes="Проверка готовности",
    )
    test_session.add(shipment)
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=99.99,
        )
    )
    await test_session.commit()

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "organization_name": "ООО Тестовый продавец",
            "organization_inn": "7700000000",
            "organization_kpp": "770001001",
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
            "seller_legal_address": "г. Москва, ул. Продавца, д. 2",
            "seller_postal_address": "г. Москва, а/я 22",
            "signer_full_name": "Иванов Иван Иванович",
            "signer_position": "Генеральный директор",
            "signer_basis": "Устав",
            "formalized_default_function": "ДОП",
        },
    )

    response = await async_client.get(f"/diadoc/outbound-readiness/shipment/{shipment.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["shipment_id"] == shipment.id
    assert data["customer_id"] == customer.id
    assert data["ready_nonformalized"] is True
    assert data["ready_formalized"] is True
    assert data["missing_required_fields"] == []


@pytest.mark.asyncio
async def test_diadoc_shipment_batch_readiness_returns_multiple_shipments(
    async_client,
    test_session,
    created_autopart,
    created_customers,
):
    await _login_admin(async_client, test_session)

    customer = created_customers[0]
    test_session.add(
        CustomerExternalReference(
            customer_id=customer.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="customer-counteragent-guid-5",
            is_active=True,
        )
    )
    shipment_ready = ShipmentDocument(
        doc_number="SHIP-BATCH-READY",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
    )
    shipment_not_ready = ShipmentDocument(
        doc_number="SHIP-BATCH-DRAFT",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.DRAFT,
        customer_id=customer.id,
    )
    test_session.add_all([shipment_ready, shipment_not_ready])
    await test_session.flush()
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment_ready.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=50.0,
        )
    )
    test_session.add(
        ShipmentDocumentItem(
            document_id=shipment_not_ready.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=50.0,
        )
    )
    await test_session.commit()

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
            "signer_full_name": "Иванов Иван Иванович",
            "signer_position": "Директор",
        },
    )

    response = await async_client.get(
        "/diadoc/outbound-readiness/shipments",
        params=[
            ("shipment_ids", str(shipment_ready.id)),
            ("shipment_ids", str(shipment_not_ready.id)),
        ],
    )
    assert response.status_code == 200
    rows = {row["shipment_id"]: row for row in response.json()}
    assert rows[shipment_ready.id]["ready_nonformalized"] is True
    assert rows[shipment_ready.id]["ready_formalized"] is True
    assert rows[shipment_not_ready.id]["ready_nonformalized"] is False
    assert rows[shipment_not_ready.id]["ready_formalized"] is False


@pytest.mark.asyncio
async def test_diadoc_customer_return_readiness_requires_confirmed_status(
    async_client,
    test_session,
    created_autopart,
    created_customers,
):
    await _login_admin(async_client, test_session)

    customer = created_customers[0]
    customer.inn = "7701234567"
    customer.kpp = "770101001"
    customer.legal_address = "г. Москва, ул. Клиентская, д. 11"
    customer.postal_address = "г. Москва, а/я 11"
    test_session.add(customer)
    test_session.add(
        CustomerExternalReference(
            customer_id=customer.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="customer-counteragent-guid-return-ready",
            is_active=True,
        )
    )
    shipment = ShipmentDocument(
        doc_number="SHIP-RET-READY-1",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
    )
    test_session.add(shipment)
    await test_session.flush()
    shipment_item = ShipmentDocumentItem(
        document_id=shipment.id,
        autopart_id=created_autopart.id,
        quantity=2,
        price=150.0,
    )
    test_session.add(shipment_item)
    await test_session.flush()
    return_doc = ReturnFromCustomer(
        doc_number="RET-CUST-READY-1",
        doc_date=now_moscow(),
        status=ReturnDocumentStatus.APPROVED,
        customer_id=customer.id,
        shipment_document_id=shipment.id,
        reason="Возврат по согласованию",
    )
    test_session.add(return_doc)
    await test_session.flush()
    test_session.add(
        ReturnItem(
            return_from_customer_id=return_doc.id,
            shipment_item_id=shipment_item.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=150.0,
            oem_number=created_autopart.oem_number,
            brand_name=created_autopart.brand.name,
            autopart_name=created_autopart.name,
        )
    )
    await test_session.commit()

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
            "organization_name": "ООО ДрагонЗап",
            "organization_inn": "7700000000",
            "organization_kpp": "770001001",
            "seller_legal_address": "г. Москва, ул. Продавца, д. 5",
            "signer_full_name": "Иванов Иван Иванович",
            "signer_position": "Генеральный директор",
            "signer_basis": "Устав",
            "formalized_default_function": "ДОП",
        },
    )

    response = await async_client.get(f"/diadoc/outbound-readiness/customer-return/{return_doc.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["return_kind"] == "customer"
    assert data["document_id"] == return_doc.id
    assert data["ready_formalized"] is False
    assert any("подтверждён" in item.lower() for item in data["missing_required_fields"])


@pytest.mark.asyncio
async def test_diadoc_can_create_formalized_ukd_from_customer_return(
    async_client,
    test_session,
    created_autopart,
    created_customers,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    customer = created_customers[0]
    customer.inn = "7701234567"
    customer.kpp = "770101001"
    customer.legal_address = "г. Москва, ул. Клиентская, д. 15"
    customer.postal_address = "г. Москва, а/я 15"
    test_session.add(customer)
    test_session.add(
        CustomerExternalReference(
            customer_id=customer.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_customer_name="customer-counteragent-guid-return-1",
            is_active=True,
        )
    )
    shipment = ShipmentDocument(
        doc_number="SHIP-RET-1",
        doc_date=now_moscow(),
        status=ShipmentDocumentStatus.POSTED,
        customer_id=customer.id,
        reason="Основная отгрузка",
    )
    test_session.add(shipment)
    await test_session.flush()
    shipment_item = ShipmentDocumentItem(
        document_id=shipment.id,
        autopart_id=created_autopart.id,
        quantity=3,
        price=200.0,
    )
    test_session.add(shipment_item)
    await test_session.flush()
    return_doc = ReturnFromCustomer(
        doc_number="RET-CUST-1",
        doc_date=now_moscow(),
        status=ReturnDocumentStatus.CONFIRMED,
        customer_id=customer.id,
        shipment_document_id=shipment.id,
        reason="Возврат товара от клиента",
    )
    test_session.add(return_doc)
    await test_session.flush()
    test_session.add(
        ReturnItem(
            return_from_customer_id=return_doc.id,
            shipment_item_id=shipment_item.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=200.0,
            oem_number=created_autopart.oem_number,
            brand_name=created_autopart.brand.name,
            autopart_name=created_autopart.name,
        )
    )
    await test_session.commit()

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
            "organization_name": "ООО ДрагонЗап",
            "organization_inn": "7700000000",
            "organization_kpp": "770001001",
            "seller_legal_address": "г. Москва, ул. Продавца, д. 5",
            "signer_full_name": "Иванов Иван Иванович",
            "signer_position": "Генеральный директор",
            "signer_basis": "Устав",
            "formalized_default_function": "ДОП",
        },
    )

    async def fake_get_document_types(self, box_id_guid):
        assert box_id_guid == "box-guid"
        return {
            "DocumentTypes": [
                {
                    "TypeNamedId": "UniversalCorrectionDocument",
                    "Functions": [
                        {
                            "Name": "ДИС",
                            "Versions": [
                                {
                                    "Version": "ucd736_05_01_02",
                                    "IsActual": True,
                                    "Titles": [{"Index": 0, "IsFormal": True}],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    async def fake_get_organization(self, **kwargs):
        box_id_guid = kwargs.get("box_id_guid")
        if box_id_guid == "box-guid":
            return {
                "FullName": "ООО ДрагонЗап",
                "ShortName": "ДрагонЗап",
                "Inn": "7700000000",
                "Kpp": "770001001",
                "FnsParticipantId": "2BM-7700000000-770001001-202401010000000000001",
                "Address": {
                    "RussianAddress": {
                        "Region": "77",
                        "City": "Москва",
                        "Street": "Тестовая",
                        "Building": "5",
                    }
                },
            }
        assert box_id_guid == "customer-counteragent-guid-return-1"
        return {
            "FullName": "ООО Покупатель",
            "ShortName": "Покупатель",
            "Inn": "7701234567",
            "Kpp": "770101001",
            "FnsParticipantId": "2BM-7701234567-770101001-202401010000000000002",
            "Address": {
                "RussianAddress": {
                    "Region": "77",
                    "City": "Москва",
                    "Street": "Клиентская",
                    "Building": "15",
                }
            },
        }

    async def fake_generate_title_xml(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        assert kwargs["document_type_named_id"] == "UniversalCorrectionDocument"
        assert kwargs["document_function"] == "ДИС"
        assert kwargs["document_version"] == "ucd736_05_01_02"
        xml_text = kwargs["user_data_xml"].decode("utf-8")
        assert "<UniversalCorrectionDocument" in xml_text
        assert 'Function="ДИС"' in xml_text
        assert 'BaseDocumentNumber="SHIP-RET-1"' in xml_text
        assert "Возврат товара от клиента" in xml_text
        return (
            (
                '<?xml version="1.0" encoding="windows-1251"?>'
                '<Файл ИдФайл="test-ukd-customer" />'
            ).encode("utf-8"),
            "ON_KORR_ukd_customer.xml",
        )

    async def fake_post_message(self, **kwargs):
        message = kwargs["message"]
        assert message["FromBoxId"] == "box-guid"
        assert message["ToBoxId"] == "customer-counteragent-guid-return-1"
        attachment = message["DocumentAttachments"][0]
        assert attachment["TypeNamedId"] == "UniversalCorrectionDocument"
        assert attachment["Function"] == "ДИС"
        assert attachment["Version"] == "ucd736_05_01_02"
        content = base64.b64decode(attachment["SignedContent"]["Content"].encode("ascii")).decode(
            "utf-8"
        )
        assert "<Файл" in content
        return {
            "MessageId": "outgoing-message-ukd-customer-1",
            "Entities": [{"EntityId": "outgoing-entity-ukd-customer-1"}],
        }

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.get_document_types",
        fake_get_document_types,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.get_organization",
        fake_get_organization,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.generate_title_xml",
        fake_generate_title_xml,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.post_message",
        fake_post_message,
    )

    response = await async_client.post(
        f"/diadoc/outbound-documents/from-customer-return/{return_doc.id}",
        json={"send_mode": "draft"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["customer_id"] == customer.id
    assert data["source_type"] == "return_from_customer"
    assert data["source_id"] == return_doc.id
    assert data["type_named_id"] == "UniversalCorrectionDocument"
    assert data["document_function"] == "ДИС"
    assert data["document_version"] == "ucd736_05_01_02"
    assert data["file_name"] == "ON_KORR_ukd_customer.xml"
    assert data["status"] == "draft"
    assert data["message_id"] == "outgoing-message-ukd-customer-1"

    await test_session.refresh(return_doc)
    assert return_doc.diadoc_outgoing_document_id == data["id"]


@pytest.mark.asyncio
async def test_diadoc_can_create_formalized_ukd_from_supplier_return(
    async_client,
    test_session,
    created_autopart,
    created_providers,
    monkeypatch,
):
    await _login_admin(async_client, test_session)

    provider = created_providers[0]
    test_session.add(
        ProviderExternalReference(
            provider_id=provider.id,
            source_system="DIADOC_COUNTERAGENT_BOX",
            external_supplier_name="provider-counteragent-guid-return-1",
            is_active=True,
        )
    )
    receipt = SupplierReceipt(
        provider_id=provider.id,
        document_number="SUP-REC-1",
        document_date=now_moscow().date(),
        comment="Исходное поступление поставщика",
    )
    test_session.add(receipt)
    await test_session.flush()
    receipt_item = SupplierReceiptItem(
        receipt_id=receipt.id,
        autopart_id=created_autopart.id,
        oem_number=created_autopart.oem_number,
        brand_name=created_autopart.brand.name,
        autopart_name=created_autopart.name,
        received_quantity=2,
        price=150.0,
    )
    test_session.add(receipt_item)
    await test_session.flush()
    return_doc = ReturnToSupplier(
        doc_number="RET-SUP-1",
        doc_date=now_moscow(),
        status=ReturnDocumentStatus.SHIPPED,
        provider_id=provider.id,
        supplier_receipt_id=receipt.id,
        reason="Возврат товара поставщику",
    )
    test_session.add(return_doc)
    await test_session.flush()
    test_session.add(
        ReturnItem(
            return_to_supplier_id=return_doc.id,
            supplier_receipt_item_id=receipt_item.id,
            autopart_id=created_autopart.id,
            quantity=1,
            price=150.0,
            oem_number=created_autopart.oem_number,
            brand_name=created_autopart.brand.name,
            autopart_name=created_autopart.name,
        )
    )
    await test_session.commit()

    await crud_diadoc_integration_settings.update(
        test_session,
        {
            "environment": "staging",
            "refresh_token": "refresh-token",
            "access_token": "cached-access-token",
            "access_token_expires_at": now_moscow() + timedelta(hours=1),
            "box_id_guid": "box-guid",
            "box_id": "box@diadoc.ru",
            "organization_name": "ООО ДрагонЗап",
            "organization_inn": "7700000000",
            "organization_kpp": "770001001",
            "seller_legal_address": "г. Москва, ул. Продавца, д. 5",
            "signer_full_name": "Иванов Иван Иванович",
            "signer_position": "Генеральный директор",
            "signer_basis": "Устав",
            "formalized_default_function": "ДОП",
        },
    )

    async def fake_get_document_types(self, box_id_guid):
        assert box_id_guid == "box-guid"
        return {
            "DocumentTypes": [
                {
                    "TypeNamedId": "UniversalCorrectionDocument",
                    "Functions": [
                        {
                            "Name": "ДИС",
                            "Versions": [
                                {
                                    "Version": "ucd736_05_01_02",
                                    "IsActual": True,
                                    "Titles": [{"Index": 0, "IsFormal": True}],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    async def fake_get_organization(self, **kwargs):
        box_id_guid = kwargs.get("box_id_guid")
        if box_id_guid == "box-guid":
            return {
                "FullName": "ООО ДрагонЗап",
                "ShortName": "ДрагонЗап",
                "Inn": "7700000000",
                "Kpp": "770001001",
                "FnsParticipantId": "2BM-7700000000-770001001-202401010000000000001",
                "Address": {
                    "RussianAddress": {
                        "Region": "77",
                        "City": "Москва",
                        "Street": "Тестовая",
                        "Building": "5",
                    }
                },
            }
        assert box_id_guid == "provider-counteragent-guid-return-1"
        return {
            "FullName": "ООО Поставщик",
            "ShortName": "Поставщик",
            "Inn": "7705555555",
            "Kpp": "770501001",
            "FnsParticipantId": "2BM-7705555555-770501001-202401010000000000003",
            "Address": {
                "RussianAddress": {
                    "Region": "77",
                    "City": "Москва",
                    "Street": "Поставщика",
                    "Building": "7",
                }
            },
        }

    async def fake_generate_title_xml(self, **kwargs):
        assert kwargs["box_id_guid"] == "box-guid"
        assert kwargs["document_type_named_id"] == "UniversalCorrectionDocument"
        assert kwargs["document_function"] == "ДИС"
        assert kwargs["document_version"] == "ucd736_05_01_02"
        xml_text = kwargs["user_data_xml"].decode("utf-8")
        assert "<UniversalCorrectionDocument" in xml_text
        assert 'BaseDocumentNumber="SUP-REC-1"' in xml_text
        assert "Возврат товара поставщику" in xml_text
        return (
            (
                '<?xml version="1.0" encoding="windows-1251"?>'
                '<Файл ИдФайл="test-ukd-supplier" />'
            ).encode("utf-8"),
            "ON_KORR_ukd_supplier.xml",
        )

    async def fake_post_message(self, **kwargs):
        message = kwargs["message"]
        assert message["FromBoxId"] == "box-guid"
        assert message["ToBoxId"] == "provider-counteragent-guid-return-1"
        attachment = message["DocumentAttachments"][0]
        assert attachment["TypeNamedId"] == "UniversalCorrectionDocument"
        assert attachment["Function"] == "ДИС"
        assert attachment["Version"] == "ucd736_05_01_02"
        content = base64.b64decode(attachment["SignedContent"]["Content"].encode("ascii")).decode(
            "utf-8"
        )
        assert "<Файл" in content
        return {
            "MessageId": "outgoing-message-ukd-supplier-1",
            "Entities": [{"EntityId": "outgoing-entity-ukd-supplier-1"}],
        }

    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.get_document_types",
        fake_get_document_types,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.get_organization",
        fake_get_organization,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.generate_title_xml",
        fake_generate_title_xml,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.diadoc_outgoing.DiadocClient.post_message",
        fake_post_message,
    )

    response = await async_client.post(
        f"/diadoc/outbound-documents/from-supplier-return/{return_doc.id}",
        json={"send_mode": "draft"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["provider_id"] == provider.id
    assert data["source_type"] == "return_to_supplier"
    assert data["source_id"] == return_doc.id
    assert data["type_named_id"] == "UniversalCorrectionDocument"
    assert data["document_function"] == "ДИС"
    assert data["document_version"] == "ucd736_05_01_02"
    assert data["file_name"] == "ON_KORR_ukd_supplier.xml"
    assert data["status"] == "draft"
    assert data["message_id"] == "outgoing-message-ukd-supplier-1"

    await test_session.refresh(return_doc)
    assert return_doc.diadoc_outgoing_document_id == data["id"]
