import pytest

from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services.auth import get_password_hash
from dz_fastapi.services.company_profile import load_company_requisites, save_profile


async def _login(async_client, test_session, role, email):
    user = User(
        email=email,
        password_hash=get_password_hash("secret123"),
        role=role,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()
    response = await async_client.post(
        "/auth/login", json={"email": email, "password": "secret123"}
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_company_profile_roundtrip_and_validation(
    async_client, test_session
):
    await _login(async_client, test_session, UserRole.ADMIN,
                 "company-admin@example.com")
    initial = await async_client.get("/settings/company-profile")
    assert initial.status_code == 200
    assert initial.json()["prefilled"] is True

    bad = await async_client.put(
        "/settings/company-profile", json={"inn": "123", "vat_payer": True}
    )
    assert bad.status_code == 422

    saved = await async_client.put(
        "/settings/company-profile",
        json={
            "short_name": "  ООО «Тест»  ",
            "inn": "7701234567",
            "kpp": "770101001",
            "bank_bik": "044525225",
            "bank_account": "40702810000000000001",
            "director_name": "Иванов И.И.",
            "accountant_name": "Петрова П.П.",
            "vat_payer": True,
        },
    )
    assert saved.status_code == 200, saved.text
    data = saved.json()
    assert data["prefilled"] is False
    assert data["short_name"] == "ООО «Тест»"
    assert data["inn"] == "7701234567"

    again = await async_client.get("/settings/company-profile")
    assert again.json()["bank_bik"] == "044525225"


@pytest.mark.no_auth_override
@pytest.mark.asyncio
async def test_company_profile_update_requires_admin(
    async_client, test_session
):
    await _login(async_client, test_session, UserRole.MANAGER,
                 "company-manager@example.com")
    assert (await async_client.get("/settings/company-profile")).status_code == 200
    denied = await async_client.put(
        "/settings/company-profile", json={"short_name": "X"}
    )
    assert denied.status_code == 403


@pytest.mark.asyncio
async def test_documents_use_saved_company_requisites(
    async_client, test_session
):
    await save_profile(test_session, {
        "short_name": "ООО «Реквизит»", "inn": "7701234567",
        "director_name": "Сидоров С.С.", "accountant_name": "Кузнецова К.К.",
        "shipping_address": "г. Тверь, склад 1",
        "is_individual_entrepreneur": False,
    })
    seller = await load_company_requisites(test_session)
    assert seller["name"] == "ООО «Реквизит»"
    assert seller["signer_name"] == "Сидоров С.С."
    assert seller["accountant_name"] == "Кузнецова К.К."
    assert seller["shipping_address"] == "г. Тверь, склад 1"
