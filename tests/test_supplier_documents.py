from datetime import date, timedelta

import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.user import UserRole, UserStatus
from dz_fastapi.services.supplier_documents import (
    classify_supplier_document,
    register_supplier_document_from_diadoc,
)
from tests.test_diadoc_api import _create_user


async def _login(async_client, test_session):
    await _create_user(
        test_session,
        email='fin@example.com',
        password='secret123',
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    response = await async_client.post(
        '/auth/login',
        json={'email': 'fin@example.com', 'password': 'secret123'},
    )
    assert response.status_code == 200


def _doc(**kwargs):
    base = dict(
        environment='staging',
        box_id_guid='box',
        message_id='m',
        entity_id='e',
    )
    base.update(kwargs)
    return DiadocIncomingDocument(**base)


def test_classify_supplier_document():
    invoice = _doc(file_name='Счет на оплату №ТМ-1 от 02.11.2026.pdf')
    act = _doc(file_name='Акт услуг.xml', document_kind='act')
    recon = _doc(
        file_name='Акт сверки №3 от 28.10.2026.pdf', document_kind='act'
    )
    upd = _doc(file_name='ON_NSCHFDOPPR_1.xml', document_kind='upd')
    assert classify_supplier_document(invoice) == 'invoice'
    assert classify_supplier_document(act) == 'act'
    assert classify_supplier_document(recon) is None
    assert classify_supplier_document(upd) is None


@pytest.mark.asyncio
async def test_register_from_diadoc_is_idempotent(
    test_session, created_providers
):
    provider = created_providers[0]
    doc = _doc(
        provider_id=provider.id,
        direction='supplier',
        file_name='Счет на оплату №ТМ-77 от 02.11.2026.pdf',
    )
    test_session.add(doc)
    await test_session.commit()
    first = await register_supplier_document_from_diadoc(test_session, doc)
    second = await register_supplier_document_from_diadoc(test_session, doc)
    assert first.id == second.id
    assert first.number == 'ТМ-77'
    assert first.doc_date == date(2026, 11, 2)
    assert first.amount is None


@pytest.mark.asyncio
async def test_registry_payments_and_creditors_report(
    async_client, test_session, created_providers
):
    await _login(async_client, test_session)
    provider = created_providers[0]
    yesterday = (now_moscow().date() - timedelta(days=1)).isoformat()

    response = await async_client.post(
        '/finance/supplier-documents',
        json={
            'provider_id': provider.id,
            'doc_type': 'invoice',
            'number': 'A-1',
            'amount': '1000.00',
            'due_date': yesterday,
        },
    )
    assert response.status_code == 201, response.text
    document = response.json()
    assert document['owed'] == '1000.00' and document['is_overdue'] is True

    response = await async_client.post(
        '/finance/supplier-documents',
        json={'provider_id': provider.id, 'doc_type': 'act'},
    )
    assert response.status_code == 201

    response = await async_client.post(
        '/finance/supplier-payments',
        json={
            'provider_id': provider.id,
            'supplier_document_id': document['id'],
            'amount': '400.00',
            'payment_date': now_moscow().date().isoformat(),
        },
    )
    assert response.status_code == 201, response.text

    response = await async_client.get(
        '/finance/supplier-documents', params={'provider_id': provider.id}
    )
    rows = {row['id']: row for row in response.json()}
    assert rows[document['id']]['paid'] == '400.00'
    assert rows[document['id']]['owed'] == '600.00'

    response = await async_client.get('/finance/creditors')
    assert response.status_code == 200
    report = next(
        r for r in response.json() if r['provider_id'] == provider.id
    )
    assert report['documents_total'] == '1000.00'
    assert report['documents_paid'] == '400.00'
    assert report['documents_owed'] == '600.00'
    assert report['documents_overdue'] == '600.00'
    assert report['documents_without_amount'] == 1

    response = await async_client.patch(
        f"/finance/supplier-documents/{document['id']}",
        json={'status': 'paid'},
    )
    assert response.status_code == 200
    assert response.json()['owed'] == '0.00'
