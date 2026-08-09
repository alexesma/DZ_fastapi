import pytest

from dz_fastapi.models.user import User, UserRole, UserStatus


@pytest.mark.no_auth_override
@pytest.mark.asyncio
async def test_process_architecture_requires_authentication(async_client):
    response = await async_client.get("/process-architecture/annotations")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_process_architecture_comment_thread_and_drawing(async_client, test_session):
    user = User(
        id=1,
        name="Test Admin",
        email="test-admin@example.com",
        password_hash="not-used",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    test_session.add(user)
    await test_session.commit()

    comment_response = await async_client.post(
        "/process-architecture/annotations",
        json={
            "page_key": "dragonzap-operating-model",
            "section_key": "labels",
            "kind": "comment",
            "anchor_x": 0.25,
            "anchor_y": 0.4,
            "content": "Нужно проверить формат штрихкода.",
        },
    )
    assert comment_response.status_code == 201
    comment = comment_response.json()
    assert comment["created_by"]["name"] == "Test Admin"

    reply_response = await async_client.post(
        "/process-architecture/annotations",
        json={
            "page_key": "dragonzap-operating-model",
            "section_key": "labels",
            "kind": "comment",
            "parent_id": comment["id"],
            "content": "Используем внутренний ID строки заказа.",
        },
    )
    assert reply_response.status_code == 201

    drawing_response = await async_client.post(
        "/process-architecture/annotations",
        json={
            "page_key": "dragonzap-operating-model",
            "section_key": "labels",
            "kind": "drawing",
            "drawing_data": {
                "strokes": [{"color": "#e4572e", "width": 3, "points": [[0.1, 0.2], [0.3, 0.4]]}]
            },
        },
    )
    assert drawing_response.status_code == 201

    resolve_response = await async_client.patch(
        f"/process-architecture/annotations/{comment['id']}",
        json={"is_resolved": True},
    )
    assert resolve_response.status_code == 200
    assert resolve_response.json()["is_resolved"] is True

    list_response = await async_client.get(
        "/process-architecture/annotations",
        params={"page_key": "dragonzap-operating-model"},
    )
    assert list_response.status_code == 200
    annotations = list_response.json()
    assert len(annotations) == 3
    assert {item["kind"] for item in annotations} == {"comment", "drawing"}


@pytest.mark.asyncio
async def test_process_architecture_rejects_invalid_drawing(async_client, test_session):
    test_session.add(
        User(
            id=1,
            name="Test Admin",
            email="test-admin@example.com",
            password_hash="not-used",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    await test_session.commit()

    response = await async_client.post(
        "/process-architecture/annotations",
        json={
            "section_key": "system-map",
            "kind": "drawing",
            "drawing_data": {
                "strokes": [{"points": [[1.4, 0.2]]}],
            },
        },
    )
    assert response.status_code == 422
