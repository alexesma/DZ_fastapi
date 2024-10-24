#test/test_constants.py
from dz_fastapi.models.brand import Brand

TEST_BRAND = {
    'name': r'Test-Brand_*?/\\|<>,.()[]{};:!@#$%^&àáâãäåçèéêëìíîðñòôõöö[|]\'~<!--@/*$%^&#*/()?>,.*/\\',
    'country_of_origin': 'USA',
    'description': 'A test brand',
    'website': 'https://example.com',
}
MAX_FILE_SIZE = 1024 * 1024  # 1 МБ

TEST_AUTOPART = {
    "oem_number": "1205011xkz16a",
    "name": "test autopart name ТЕСТ",
    "description": "this is a test auto part.",
    "width": 10.0,
    "height": 5.0,
    "length": 15.0,
    "weight": 2.5,
    "purchase_price": 100.00,
    "retail_price": 150.00,
    "wholesale_price": 120.00,
    "multiplicity": 1,
    "minimum_balance": 5,
    "min_balance_auto": True,
    "min_balance_user": False,
    "comment": "Test autopart comment"
}

