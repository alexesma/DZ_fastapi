#test/test_constants.py
from dz_fastapi.models.brand import Brand

TEST_BRAND = {
    'name': r'Test-Brand_*?/\\|<>,.()[]{};:!@#$%^&àáâãäåçèéêëìíîðñòôõöö[|]\'~<!--@/*$%^&#*/()?>,.*/\\',
    'country_of_origin': 'USA',
    'description': 'A test brand',
    'website': 'https://example.com',
}
MAX_FILE_SIZE = 1024 * 1024  # 1 МБ
