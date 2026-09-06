from dz_fastapi.services.partssoft_reconciliation import (
    CustomerMatcher,
    compare_fields,
    flatten_remote_customer,
    normalize_digits,
    normalize_name,
)


def test_normalize_digits_preserves_scientific_tax_identifiers():
    assert normalize_digits("5.0232771813E+11") == "502327718130"
    assert normalize_digits(221003000000.0) == "221003000000"
    assert normalize_digits("77-01-23") == "770123"


def test_normalize_name_ignores_legal_form_and_quotes():
    assert normalize_name("ООО «РМС ЧЕРНОЗЕМЬЕ»") == normalize_name('"РМС Черноземье"')


def test_flatten_remote_customer_prefers_company_and_organization_email():
    row = flatten_remote_customer(
        {
            "id": 884,
            "compile_name": "Иванов Иван",
            "email": "person@example.com",
            "email_org": "office@example.com",
            "pay_delay": 7,
            "essential": {"company_name": 'ООО "Ромашка"', "inn": "7701", "kpp": "770101001"},
            "official_address": {"city": "Москва", "street": "Тверская", "house": "1"},
        }
    )
    assert row["name"] == 'ООО "Ромашка"'
    assert row["email"] == "office@example.com"
    assert row["legal_address"] == "Москва, Тверская, 1"


def test_matcher_prioritizes_external_reference_then_inn_kpp():
    matcher = CustomerMatcher(
        [
            {"id": 1, "external_id": 500, "inn": "111", "kpp": "222"},
            {"id": 2, "external_id": None, "inn": "333", "kpp": "444"},
        ]
    )
    linked = matcher.match({"external_id": 500, "inn": "333", "kpp": "444"})
    assert (linked.classification, linked.local_id, linked.basis) == ("linked", 1, "external_id")
    exact = matcher.match({"external_id": 501, "inn": "333", "kpp": "444"})
    assert (exact.classification, exact.local_id, exact.basis) == ("auto_match", 2, "inn_kpp")


def test_matcher_marks_shared_email_as_ambiguous():
    matcher = CustomerMatcher(
        [
            {"id": 1, "email": "shared@example.com"},
            {"id": 2, "email": "SHARED@example.com"},
        ]
    )
    result = matcher.match({"external_id": 9, "email": "shared@example.com"})
    assert result.classification == "ambiguous"
    assert result.candidate_ids == (1, 2)


def test_compare_fields_separates_missing_values_and_conflicts():
    missing, conflicts = compare_fields(
        {"name": "ООО Ромашка", "email": "new@example.com", "inn": "7701"},
        {"name": "ООО Ромашка", "email": None, "inn": "7702"},
    )
    assert missing == ["email"]
    assert conflicts == ["inn"]
