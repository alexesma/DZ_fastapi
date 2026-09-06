import unittest
from unittest.mock import AsyncMock, MagicMock

from dz_fastapi.http.dz_v3_client import V3Client


class V3ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_uses_get_with_query_parameters(self):
        client = V3Client("https://example.com/api/v3/", "user", "password")
        self.assertIsNone(client._session)
        response = MagicMock()
        response.json = AsyncMock(return_value={"customers": [{"id": 1}]})
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=None)
        client._session = MagicMock()
        client._session.get.return_value = context

        result = await client.get("/customers.json", params={"per_page": 1})

        self.assertEqual(result, {"customers": [{"id": 1}]})
        client._session.get.assert_called_once_with(
            "https://example.com/api/v3/customers.json", params={"per_page": 1}
        )
        client._session.post.assert_not_called()

    async def test_get_failure_does_not_fall_back_to_post(self):
        client = V3Client("https://example.com/api/v3", "user", "password")
        client._session = MagicMock()
        client._session.get.side_effect = RuntimeError("unavailable")
        self.assertIsNone(await client.get("/products.json"))
        client._session.post.assert_not_called()
