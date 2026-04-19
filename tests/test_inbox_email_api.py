import pytest

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.inbox_email import InboxEmail
from dz_fastapi.models.partner import (Customer, CustomerOrderConfig, Provider,
                                       SupplierResponseConfig)
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash


async def _create_user(session, email: str) -> User:
    user = User(
        email=email,
        password_hash=get_password_hash('secret123'),
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


async def _login(async_client, email: str) -> None:
    response = await async_client.post(
        '/auth/login',
        json={'email': email, 'password': 'secret123'},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_inbox_emails_support_search_filters(async_client, test_session):
    await _create_user(test_session, 'admin.inbox.filters@example.com')
    await _login(async_client, 'admin.inbox.filters@example.com')

    account = EmailAccount(
        name='Inbox Filters',
        email='inbox.filters@example.com',
        password='secret',
        imap_host='imap.example.com',
        purposes=['orders_in'],
        is_active=True,
    )
    test_session.add(account)
    await test_session.flush()

    customer = Customer(
        name='Customer Filter',
        email_contact='customer.filter@example.com',
    )
    provider = Provider(
        name='Provider Filter',
        email_contact='provider.filter@example.com',
        email_incoming_price='price@supplier.ru',
    )
    test_session.add_all([customer, provider])
    await test_session.flush()

    customer_cfg = CustomerOrderConfig(
        customer_id=customer.id,
        order_email='zakaz@client.ru',
        order_emails=['orders2@client.ru'],
        email_account_id=account.id,
        oem_col=1,
        brand_col=2,
        qty_col=3,
        is_active=True,
    )
    provider_cfg = SupplierResponseConfig(
        provider_id=provider.id,
        name='Provider response cfg',
        sender_emails=['reply@supplier.ru'],
        inbox_email_account_id=account.id,
        is_active=True,
    )
    test_session.add_all([customer_cfg, provider_cfg])
    await test_session.flush()

    messages = [
        InboxEmail(
            email_account_id=account.id,
            uid='1',
            folder='INBOX',
            from_email='zakaz@client.ru',
            subject='Заказ 100',
            body_preview='x',
            has_attachments=False,
            attachment_info=[],
            received_at=now_moscow(),
            fetched_at=now_moscow(),
        ),
        InboxEmail(
            email_account_id=account.id,
            uid='2',
            folder='INBOX',
            from_email='orders2@client.ru',
            subject='Уточнение заказа',
            body_preview='x',
            has_attachments=False,
            attachment_info=[],
            received_at=now_moscow(),
            fetched_at=now_moscow(),
        ),
        InboxEmail(
            email_account_id=account.id,
            uid='3',
            folder='INBOX',
            from_email='reply@supplier.ru',
            subject='Ответ по заказу',
            body_preview='x',
            has_attachments=False,
            attachment_info=[],
            received_at=now_moscow(),
            fetched_at=now_moscow(),
        ),
        InboxEmail(
            email_account_id=account.id,
            uid='4',
            folder='INBOX',
            from_email='price@supplier.ru',
            subject='Прайс на сегодня',
            body_preview='x',
            has_attachments=False,
            attachment_info=[],
            received_at=now_moscow(),
            fetched_at=now_moscow(),
        ),
        InboxEmail(
            email_account_id=account.id,
            uid='5',
            folder='INBOX',
            from_email='misc@other.ru',
            subject='Прочее письмо',
            body_preview='x',
            has_attachments=False,
            attachment_info=[],
            received_at=now_moscow(),
            fetched_at=now_moscow(),
        ),
    ]
    test_session.add_all(messages)
    await test_session.commit()

    response = await async_client.get(
        '/inbox/emails',
        params={
            'days': 7,
            'page': 1,
            'page_size': 200,
            'subject_contains': 'заказ',
        },
    )
    assert response.status_code == 200
    subject_ids = {item['uid'] for item in response.json()['items']}
    assert subject_ids == {'1', '2', '3'}

    response = await async_client.get(
        '/inbox/emails',
        params={
            'days': 7,
            'page': 1,
            'page_size': 200,
            'sender_contains': 'supplier.ru',
        },
    )
    assert response.status_code == 200
    sender_ids = {item['uid'] for item in response.json()['items']}
    assert sender_ids == {'3', '4'}

    response = await async_client.get(
        '/inbox/emails',
        params={
            'days': 7,
            'page': 1,
            'page_size': 200,
            'customer_id': customer.id,
        },
    )
    assert response.status_code == 200
    customer_ids = {item['uid'] for item in response.json()['items']}
    assert customer_ids == {'1', '2'}

    response = await async_client.get(
        '/inbox/emails',
        params={
            'days': 7,
            'page': 1,
            'page_size': 200,
            'provider_id': provider.id,
        },
    )
    assert response.status_code == 200
    provider_ids = {item['uid'] for item in response.json()['items']}
    assert provider_ids == {'3', '4'}

    response = await async_client.get(
        '/inbox/emails',
        params={
            'days': 7,
            'page': 1,
            'page_size': 200,
            'customer_id': customer.id,
            'provider_id': provider.id,
        },
    )
    assert response.status_code == 200
    combined_ids = {item['uid'] for item in response.json()['items']}
    assert combined_ids == {'1', '2', '3', '4'}
