import pytest
from dz_fastapi.services.process import assign_brand

BRAND_TEST_CASES = {
    'CHERY_HAVAL': [
        ('SMW299932', ['CHERY', 'HAVAL']),
        ('SMD103044', ['CHERY', 'HAVAL']),
    ],
    'FAW': [
        ('3705100K7', ['FAW']),
        ('FA3034210', ['FAW']),
        ('B5CA03328ZAP1', ['FAW']),
        ('1105110M01A00', ['FAW']),
        ('FA014373XP1', ['FAW'])
    ],
    'GEELY': [
        ('3502140005', ['GEELY']),
        ('160204118001', ['GEELY']),
        ('10160001520', ['GEELY']),
        ('E020300601', ['GEELY']),
        ('LP0W20AFS4L', ['GEELY']),
        ('1400616180', ['GEELY']),
        ('1014001633', ['GEELY']),
        ('101200033502', ['GEELY']),
        ('380211000503', ['GEELY']),
        ('6010008600661', ['GEELY'])
    ],
    'CHERY': [
        ('M112905010', ['CHERY']),
        ('S21XLB3AH2203030A', ['CHERY']),
        ('3721007011', ['CHERY']),
        ('519MHA1602501', ['CHERY']),
        ('480EE1008052', ['CHERY']),
        ('A115605010BA', ['CHERY']),
        ('018CHA1502202', ['CHERY']),
        ('019CHA1500010CA', ['CHERY']),
        ('025CHA1508206', ['CHERY']),
        ('015141165AA', ['CHERY']),
        ('0CF18A1501111GA', ['CHERY']),
        ('10051425', ['CHERY']),
        ('110100019AA', ['CHERY']),
        ('1200015756', ['CHERY']),
        ('123000081AA10', ['CHERY']),
        ('2030044001J', ['CHERY']),
        ('2510117801', ['CHERY']),
        ('30V0QU015', ['CHERY']),
        ('351HHA4004011AA10', ['CHERY']),
        ('401000070ABABK', ['CHERY']),
        ('Q330C10', ['CHERY'])
    ],
    'BYD': [
        ('17042500F3001', ['BYD']),
        ('BYDF32912011', ['BYD']),
        ('1001102300', ['BYD']),
        ('1024272100', ['BYD'])
    ],
    'LIFAN': [
        ('LF479Q13818100A', ['LIFAN']),
        ('LBA3407100', ['LIFAN']),
        ('F8108130', ['LIFAN']),
        ('F4116100A2', ['LIFAN']),
        ('B8202200B2', ['LIFAN']),
        ('1891237T13F6', ['LIFAN']),
        ('2311030TF30', ['LIFAN']),
        ('S1001210', ['LIFAN']),
        ('101B1212T13F3', ['LIFAN']),
        ('SS22003', ['LIFAN'])
    ],
    'CHANGAN': [
        ('H150010300', ['CHANGAN']),
        ('S1010350200', ['CHANGAN']),
        ('S111F2603030200', ['CHANGAN']),
        ('08020150601061', ['CHANGAN']),
        ('K0010401', ['CHANGAN']),
        ('YJ003120', ['CHANGAN']),
        ('CD569F2801032700', ['CHANGAN'])
    ],
    'HAIMA': [
        ('B25D67482AL1', ['HAIMA']),
    ],
    'DONGFENG': [
        ('3502500VD01', ['DONGFENG']),
    ],
    'HAVAL': [
        ('8402700K00', ['HAVAL']),
        ('2804135XKQ00A8L', ['HAVAL']),
        ('1109101XGW01A', ['HAVAL']),
        ('1104029S08', ['HAVAL']),
        ('09820019', ['HAVAL']),
        ('1701543S', ['HAVAL']),
        ('4408640001', ['HAVAL']),
        ('6006ERSN', ['HAVAL'])
    ]
}

# Генерация общего списка тест-кейсов для параметризации
ALL_TESTS = []
for brand, cases in BRAND_TEST_CASES.items():
    for (oem_code, expected) in cases:
        ALL_TESTS.append((oem_code, expected))


@pytest.mark.parametrize('oem_code, expected_brand', ALL_TESTS)
def test_assign_brand(oem_code, expected_brand):
    assert assign_brand(oem_code) == expected_brand, f'OEM: {oem_code} | Expected: {expected_brand} | Got: {assign_brand(oem_code)}'
