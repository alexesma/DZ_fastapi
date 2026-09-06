from types import SimpleNamespace

from dz_fastapi.services.partssoft_order_reconciliation import (
    _candidate_score,
    _remote_customer,
    _remote_order_number,
    local_order_fingerprint,
    remote_order_fingerprint,
)


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
    assert customer == {
        "external_id": 7,
        "name": "ООО Тест",
        "email": "office@example.com",
        "inn": "770123",
        "kpp": "770101001",
    }


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
