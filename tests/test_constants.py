# test/test_constants.py

TEST_BRAND = {
    'name': r'Test-Brand_*?/\\|<>,.()[]{};:!@#$%^&àáâãäåçèéê'
    r'ëìíîðñòôõöö[|]\'~<!--@/*$%^&#*/()?>,.*/\\',
    'country_of_origin': 'USA',
    'description': 'A test brand',
    'website': 'https://example.com',
}
MAX_FILE_SIZE = 1024 * 1024  # 1 МБ

TEST_AUTOPART = {
    'oem_number': '1205011xkz16a',
    'name': 'test autopart name ТЕСТ',
    'description': 'this is a test auto part.',
    'width': 10.0,
    'height': 5.0,
    'length': 15.0,
    'weight': 2.5,
    'purchase_price': 100.00,
    'retail_price': 150.00,
    'wholesale_price': 120.00,
    'multiplicity': 1,
    'minimum_balance': 5,
    'min_balance_auto': True,
    'min_balance_user': False,
    'comment': 'Test autopart comment',
}

TEST_PROVIDER = {
    'name': 'Test-Provider_*?/\\|<>,.()[]{};:!@#$%^&à'
    'áâãäåçèéêëìíîðñòôõöö[|]\'~<!--@/*$%^&#*/()?>,.*/\\',
    'type_prices': 'Retail',
    'email_contact': 'test2@example.com',
    'comment': 'Test comment',
    'description': 'A test provider',
    'email_incoming_price': 'test3@example.com',
}

TEST_CUSTOMER = {
    'name': 'Test-Customer_*?/\\|<>,.()[]{};:!@#$%^&'
    'àáâãäåçèéêëìíîðñòôõöö[|]\'~<!--@/*$%^&#*/()?>,.*/\\',
    'type_prices': 'Retail',
    'email_contact': 'testcustomer@example.com',
    'comment': 'Test comment',
    'description': 'A test customer',
    'email_outgoing_price': 'testcustomer@example.com',
}

CONFIG_DATA = {
    'start_row': 1,
    'oem_col': 0,
    'brand_col': 1,
    'name_col': 2,
    'qty_col': 3,
    'price_col': 4,
}
