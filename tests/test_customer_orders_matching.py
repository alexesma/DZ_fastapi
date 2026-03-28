from dz_fastapi.api.validators import normalize_brand_name
from dz_fastapi.services.customer_orders import (_canonicalize_brand_key,
                                                 _normalize_key,
                                                 _normalize_oem_key)


def test_normalize_oem_key_matches_autopart_storage_rules():
    assert _normalize_oem_key('90119-08419') == '9011908419'
    assert _normalize_oem_key(' 90 119/08419 ') == '9011908419'


def test_normalize_brand_name_matches_existing_rules():
    assert normalize_brand_name('Toyota') == 'TOYOTA'
    assert normalize_brand_name('  lexus  ') == 'LEXUS'


def test_normalize_key_uses_brand_aliases_for_synonyms():
    brand_aliases = {
        'TOYOTA': 'TOYOTA',
        'LEXUS': 'TOYOTA',
    }

    assert _canonicalize_brand_key('Lexus', brand_aliases) == 'TOYOTA'
    assert _normalize_key('90119-08419', 'Toyota', brand_aliases) == (
        '9011908419',
        'TOYOTA',
    )
    assert _normalize_key('9011908419', 'Lexus', brand_aliases) == (
        '9011908419',
        'TOYOTA',
    )
