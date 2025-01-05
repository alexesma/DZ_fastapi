import os

MAX_NAME_BRAND = 256
MAX_NAME_PARTNER = 256
MAX_LIGHT_OEM = 256
MAX_NAME_CATEGORY = 256
MAX_LIGHT_BARCODE = 256
MAX_LIGHT_NAME_LOCATION = 20
MAX_LIGHT_NAME_CAR_MODEL = 56
MAX_LIGHT_NAME_BODY = 256
MAX_LIGHT_NAME_ENGINE = 256
MAX_LEN_WEBSITE = 1056
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', 5 * 1024 * 1024))  # 5 МБ
ERROR_MESSAGE_FORMAT_DATE = '{key} должен быть в формате "YYYY" или "MM.YYYY"'
ERROR_MESSAGE_RANGE_DATE = ('{key} не может быть меньше '
                            '1980 и больше текущей даты')
FORMAT_YEAR_FOR_CAR_1 = '%Y'
FORMAT_YEAR_FOR_CAR_2 = '%m.%Y'
UPLOAD_DIR = 'uploads/logos'

ORIGINAL_BRANDS = ['CHERY', 'HAVAL', 'CHANGAN', 'GREAT WALL', 'JAC', 'GEELY']
PROVIDER = 'DRAGONZAP'
CUSTOMER = 'Zzap'
PROVIDER_IN = {
    'name': 'Dragonzap Provider',
    'email_contact': 'admin@dragonzap.ru',
    'email_incoming_price': 'masterzapprice@gmail.com',
    'type_prices': 'Wholesale'
}
CONFIG_DATA_PROVIDER = {
    'start_row': 2,
    'oem_col': 2,
    'brand_col': 0,
    'name_col': 1,
    'qty_col': 3,
    'price_col': 4,
    'name_price': 'zzap_kross.xlsx',
    'name_mail': 'Прайс лист'
}
CUSTOMER_IN = {
    'name': 'Zzap',
    'type_prices': 'Retail',
    'email_contact': 'support@zzap.ru',
    'email_outgoing_price': 'info@dragonzap.ru'
}
CONFIG_DATA_CUSTOMER = {
    'name': 'Price Dragonzap for zzap',
    'general_markup': 1,
    'additional_filters': {'ZZAP': True},
}

# Invoice name
PRICELIST_DRAGONZAP = 'dragonzap.xlsx'
PRICELIST_HOT_PARTS = 'Прайс Хотпартс.xls'

# Brand indicators
INDICATOR_BYD_FIRST_FIVE = [
    '10237', '10242', '10259', '10375'
]
INDICATOR_DONGFENG_FULL = [
    '4151700', '3502500VD01', '4151200', '8226006'
]
INDICATOR_CHANGAN_FIRST_THREE = [
    'C20', 'H15', 'H16', 'PA0', 'S10',
    'S20', 'S30', 'YA0', 'YJ0', 'CD1', 'PA1', 'CD5'
]
INDICATOR_LIFAN_FIRST_THREE_2 = [
    'S10'
]
INDICATOR_CHANGAN_FIRST_FOUR = [
    'S111',
]
INDICATOR_JAC = [
    '1026090GH500', '1017100GH500', '8114010U8520', '8126100U851025',
    '1041200GG010', '1109130V5070', '3503100U7300F011', 'S1700L2106980017'
]
INDICATOR_CHANGAN_FIRST_SEVEN = [
    '0802015',
]
INDICATOR_CHANGAN_END_THREE = [
    'M06'
]
INDICATOR_CHANGAN_FIRST_TWO = [
    'K0'
]
INDICATOR_CHERY_FIRST_THREE = [
    '473', '475', '480', '481', '484', '513', '525', 'A11',
    'A13', 'A15', 'A18', 'A21', 'B11', 'B13', 'B14', 'E4G',
    'F4J', 'FQ1', 'J42', 'J43', 'J52', 'J60', 'J68', 'J69',
    'M11', 'M31', 'P11', 'Q18', 'Q32', 'Q33', 'QR5', 'S11',
    'S12', 'S18', 'S21', 'SND', 'T11', 'T15', 'T19', 'T1E',
    'T21', '015', '372', '477', '020', '519', '019', '018',
    '0CF', '025', '30V', '351', 'M12', '372', '371', '416',
    '515', '5MF', '7A1', 'D4G', 'D4T', 'E4T', 'FJ1', 'FQ3',
    'FQ2', 'FQ4', 'FQ6', 'J15', 'J26', 'J51', 'M1D', 'M1E',
    'M36', 'Q14', 'Q21', '465', '472', 'J11', 'T1C', 'T22'
]
INDICATOR_CHERY_FULL = [
    'Q320B12', '10010242', '10016312', '10021773', '10049491',
    '10051410', '10051421', '10051425', '10051430',
    '10051436', '101904', '10350305', '10350306', '10393684',
    '1040052001Z', '10405053', '1080111', '1080121',
    '1080131', '1080311', '1080311H', '1080411', '1080421',
    '109026', '10940301', '110010', '110057', '2030044001J',
    '20522', '20524', '20527', '20528', '20533', '47700000', 'Q330C10'
]
INDICATOR_CHERY_10_11_POSITION = [
    'AA', 'AB', 'AD', 'AC',
]
INDICATOR_CHERY_FIRST_THREE_LEN_10 = [
    '120', '251', '130',
]
INDICATOR_LIFAN_FIRST_THREE = [
    'AAB', 'B61', 'BAC', 'LAL', 'LAX', 'LBA', 'LBU', 'LBV',
    'LCA', 'LF4', 'LFB', 'PBA', 'Q15', 'Q55', 'L5M', 'SB3'
]
INDICATOR_HAIMA_FULL = [
    'HC0020660BM1', 'SA0034156M1', 'B25D67482AL1'
]
INDICATOR_LIFAN_END_TWO = [
    'C1', 'B1', 'A2', 'B2', 'A4', 'A1',
]
INDICATOR_LIFAN_END_THREE = [
    'B32', 'TF3',
]
INDICATOR_LIFAN_LEN_TEN = [
    'BBF', 'SCA'
]
INDICATOR_LIFAN_LEN_SEVEN = [
    'SA2', 'SB3', 'SF3', 'SS2'
]
CUMMINS_OEM = [
    '5255313'
]
INDICATOR_LIFAN_LEN_NINE = [
    'SBAC', 'SF22', 'SLAL', 'SLBA', 'X60F', 'Q401',
]
INDICATOR_LIFAN_END_FIVE = [
    'T13F6', '00TF3', 'T13F3'
]
INDICATOR_LIFAN_END_FOUR = [
    'TF30', '0TF3'
]
INDICATOR_FOTON = [
    'E049343000008', '1106911800005', 'T2666108B', 'P700000181'
]
INDICATOR_GEELY_FIRST_TWO = [
    'LP'
]
GEELY_NOT_OEM = [
    '4408640001',
]
INDICATOR_GEELY_FIRST_THREE = [
    'E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E08', 'E09', 'E10',
    'E12', 'E15', 'E20', 'GA7', 'JQ1', 'JQ3', 'E11'
]
INDICATOR_GEELY_FIRST_FOUR = [
    '1136', '1602', '1016', '1012', '1014', '1017', '1018', '1086',
    '1400', '1401', '1402', '3170'
]
INDICATOR_CHERY_GW_FIRST_TWO = [
    'MD',
]
INDICATOR_CHERY_GW_FIRST_THREE = [
    'SMD', 'SMF', 'SMS', 'SMW',
]
INDICATOR_CHERY_GW_FULL = [
    'S1258A003',
]
INDICATOR_LIFAN_WHISOUT = [
    '6007RSNR', '6006ERSN', 'HK301600'
]
INDICATOR_END_IS_NOT_LIFAN = [
    'S', 'E'
]
INDICATOR_LIFAN_WHISOUT_FIRST = [
    '0'
]
INDICATOR_BYD_FIRST_THREE = ['BYD']
INDICATOR_BYD = [
    '1001102300', '17042500F3001', '1001896500', '1013339300', '1013561900',
    '1013763600', '3600090E', 'BS151602004', 'F32906103', 'BS151602004'
                 ]
INDICATOR_FAW_PREFIXES = [
    'FA', '5CA', 'B5CA', 'FC', 'C230', 'L06A'
]
INDICATOR_FAW_OTHER_PATTERNS = [
    '27060', '52576', '85310', '67861', '90080'
]
FAW_OEM = [
    '1105110M01A00', '19130T2A20', '2911052HL', '101201037KP1', '3705100K7',
    '43420TKA40', 'RF4F13Z40H', '52576TKA00', '5CA034156', 'B5CA02648Z',
    'BFA0122530', 'FA014373XP1', '2902011HL', 'BFA0122530'
]
INDICATOR_HAVAL = [
    'K00', 'F04', 'S08', 'K46', 'E03', 'E00', 'D01', 'F00', 'E02', 'P00'
]
BRILLIANCE_OEM = [
    '3437007', '3483012', 'DAMD194294', 'SHZ2200026'
]


NAME_COLUMNS = ['Numer_column', 'OEM', 'Name', 'Amount', 'Sum']
SEPARATE_CSV = ';'


def create_supplier_params(
        doc_type,
        name_invoice,
        name_sheet,
        title_line,
        start_line,
        column_ids,
        default_title_line=0,
        default_start_line=0):
    return {
        'doc_type': doc_type,
        'name_invoice': name_invoice,
        'name_sheet': name_sheet,
        'title_line': title_line if title_line is not None
        else default_title_line,
        'start_line': start_line if start_line is not None
        else default_start_line,
        'column_ids': column_ids
    }


SUPPLIER_PARAM = {
    '1c': create_supplier_params(
        'xlsx',
        '1c_invoice.xlsx',
        'Sheet1',
        0,
        0,
        [0, 1, 2, 3, 4]
    ),
    'reline': create_supplier_params(
        'csv',
        'relines.csv',
        None,
        0,
        17,
        [1, 5, 9, 26, 57]
    ),
    'autotorg': create_supplier_params(
        'xls',
        'партс.xls',
        'TDSheet',
        13,
        17,
        [1, 5, 9, 26, 57]
    ),
    'apex': create_supplier_params(
        'xls',
        'apex.xls',
        'TDSheet',
        20,
        0,
        [3, 20, 5, 33, 49]
    ),
    'hot-parts': create_supplier_params(
        'xls',
        '71977.xls',
        'Report',
        27,
        0,
        [0, 1, 22, 58]
    ),
    'redline': create_supplier_params(
        'xls',
        'redline.xls',
        'TDSheet',
        18,
        0,
        [1, 9, 13, 35, 64]
    ),
    'techo': create_supplier_params(
        'xls',
        'nk2023111600027.xls',
        'Лист1',
        20,
        0,
        [1, 5, 6, 10, 18]
    ),
    'froza': create_supplier_params(
        'xls',
        'froza.xls',
        'TDSheet',
        18,
        0,
        [1, 9, 13, 35, 67, 88]
    ),
    'autopiter': create_supplier_params(
        'xls',
        'piter.xls',
        'TDSheet',
        24,
        0,
        [1, 5, 9, 26, 57]
    ),
}


def get_max_file_size():
    return int(os.getenv('MAX_FILE_SIZE', 5 * 1024 * 1024))


def get_upload_dir():
    upload_dir = os.getenv('UPLOAD_DIR', 'uploads')
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


BRANDS = [
    '555', 'ACQ', 'ADVICS', 'A-GRESSOR', 'AIKO', 'AISAN', 'AISIN', 'AJUSA',
    'AKEBONO', 'AKITAKA', 'ALFI PARTS', 'ALL4MOTORS', 'AMD', 'AP', 'ASHIKA',
    'ASIN', 'ASP', 'ASPACO', 'ASPARTS', 'ASVA', 'ATIHO', 'ATS', 'AUTOFAMILY',
    'AUTO-GUR', 'AUTOWELT', 'AVANTECH', 'BAPCO', 'BAW', 'BGA', 'BM', 'BOSAL',
    'BOSCH', 'BRAVE', 'BRITPART', 'BSV', 'CHANGAN', 'CHERY',
    'CITROEN-PEUGEOT', 'COMI ONE', 'CONTITECH', 'CTR', 'CUMMINS', 'DAR',
    'DAYCO', 'DEKO', 'DELLO', 'DELPHI', 'DENSO', 'DEPO', 'DOCAR', 'DODA',
    'DOLZ', 'DOMINANT', 'DRAGONZAP', 'FA1', 'FAE', 'FAG', 'FAW', 'FEBEST',
    'FINWHALE', 'FLAG', 'FORD', 'FORTLUFT', 'FORWARD', 'FOTON', 'FRENKIT',
    'FREX', 'FREY', 'FRICTION MASTER', 'GUD', 'GATES', 'GAZPROMNEFT',
    'G-BRAKE', 'GEELY', 'GENERAL MOTORS', 'GMB', 'GNV', 'GOETZE', 'GOODWILL',
    'GPARTS', 'GREAT WALL', 'GT', 'GUFU PARTS', 'HANS PRIES', 'HAVAL',
    'HENGST', 'HERZOG', 'HONDA', 'HQ', 'HYUNDAI-KIA', 'JAC', 'JAPAN CARS',
    'JAPANPARTS', 'JEENICE', 'JIKIU', 'JNBK', 'JUST DRIVE', 'KAIZEN',
    'KASHIYAMA', 'KG', 'KLAKSON', 'KOLBENSCHMIDT', 'KORTEX', 'KOYO', 'KRAUF',
    'KYB', 'LEDO', 'LIFAN', 'LIQUI MOLY', 'LOBRO', 'LYNXAUTO', 'MAHLE',
    'MARKON', 'MARSHALL', 'MASTERKIT', 'MASUMA', 'MAZDA', 'MCB',
    'MERCEDES-BENZ', 'METACO', 'METELLI', 'MEYLE', 'MILES', 'MITSUBISHI',
    'MITSUBOSHI', 'MIYACO', 'MOLY-GREEN', 'MOSKVICH', 'MUSASHI', 'NACHI',
    'NAKAMOTO', 'NARICHIN', 'NDC', 'NIPPON MOTORS', 'NISSAN', 'NITTO',
    'NK', 'NOK', 'NORDFIL', 'NPW', 'NSK', 'NTN', 'NTY', 'OBK', 'OEM',
    'OPTIMAL', 'PARTS-MALL', 'PASCAL', 'PATRON', 'PAYEN', 'PILENGA', 'PULLMAN',
    'QUARTZ', 'QUATTRO FRENI', 'R8', 'RBI', 'RENAULT',
    'ROADRUNNER', 'ROCKY', 'ROSTECO', 'SAILING', 'SAKURA', 'SANGSIN',
    'SAT', 'SCT GERMANY', 'SEIKEN', 'SEIWA', 'SH', 'SSANGYONG', 'STARMANN',
    'STARTVOLT', 'STELLOX', 'SUN', 'TAIHO', 'TCL', 'TEIKIN', 'TENACITY',
    'THG', 'TOKICO', 'TONG HONG', 'TOP DRIVE', 'TORCH', 'TORR', 'TOYO',
    'TOYOTA', 'TPR', 'TRANSMASTER UNIVERSAL', 'TRIALLI', 'VAG', 'VIC',
    'VICTOR REINZ', 'VTR', 'WOG', 'YAMAHA', 'YEC', 'ZEKKERT', 'ZUIKO',
    'ZX SHOCK', 'ZZVF'
]

JANAP_BRAND = [
    '555', 'ADVICS', 'AISAN', 'AISIN', 'AKEBONO', 'DENSO', 'GMB', 'HONDA',
    'JNBK', 'KASHIYAMA', 'KOYO', 'KYB', 'LIQUI MOLY', 'MAHLE', 'MAZDA',
    'MITSUBISHI', 'MITSUBOSHI', 'MIYACO', 'MOLY-GREEN', 'MUSASHI', 'NACHI',
    'NIPPON MOTORS', 'NISSAN', 'NITTO', 'NOK', 'NPW', 'NSK', 'NTN', 'OBK',
    'ROCKY', 'SEIKEN', 'SEIWA', 'SUN', 'TAIHO', 'TCL', 'TEIKIN', 'TOKICO',
    'TOYO', 'TOYOTA', 'TPR', 'VIC', 'VICTOR REINZ', 'YAMAHA', 'YEC', 'ZUIKO'
]
GERMANY_BRAND = [
    'BOSCH', 'CONTITECH', 'DELLO', 'FAG', 'FLAG', 'GOETZE', 'HANS PRIES',
    'HENGST', 'KOLBENSCHMIDT', 'MERCEDES-BENZ', 'MEYLE', 'NK', 'OPTIMAL',
    'VAG'
]
SPANISH_BRAND = [
    'AJUSA', 'DOLZ', 'FAE', 'FRENKIT'
]
SOUTH_KOREA = [
    'ASIN', 'AVANTECH', 'CTR', 'HYUNDAI-KIA', 'PARTS-MALL', 'ROADRUNNER',
    'SANGSIN', 'SSANGYONG'
]
BELGIUM = [
    'BOSAL', 'GATES'
]
UK = [
    'BGA', 'BRITPART'
]
FRANCE = [
    'CITROEN-PEUGEOT', 'RENAULT'
]
USA = [
    'CUMMINS', 'DAYCO', 'DELPHI', 'FORD', 'FRICTION MASTER', 'GENERAL MOTORS',
    'PAYEN',
]
TAIWAN = [
    'DEPO', 'NAKAMOTO', 'RBI', 'SH', 'TENACITY'
]
POLAND = [
    'FA1', 'NTY'
]
RUSSIA = [
    'GAZPROMNEFT', 'MOSKVICH', 'ROSTECO'
]
ITALY = [
    'METELLI'
]


def create_brands(brands: list) -> list:
    brands_for_create = []
    for brand in brands:
        if brand in JANAP_BRAND:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Japan'
                }
            )
        elif brand in GERMANY_BRAND:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Germany'
                }
            )
        elif brand in SPANISH_BRAND:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Spain'
                }
            )
        elif brand in SOUTH_KOREA:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'South Korea'
                }
            )
        elif brand in BELGIUM:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Belgium'
                }
            )
        elif brand in UK:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'UK'
                }
            )
        elif brand in FRANCE:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'France'
                }
            )
        elif brand in USA:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'USA'
                }
            )
        elif brand in TAIWAN:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Taiwan'
                }
            )
        elif brand in POLAND:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Poland'
                }
            )
        elif brand in RUSSIA:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Russia'
                }
            )
        elif brand in ITALY:
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'Italy'
                }
            )
        else:
            # Default case for brands not in any specific country list
            brands_for_create.append(
                {
                    'name': brand,
                    'main_brand': True,
                    'country_of_origin': 'China'
                }
            )

    return brands_for_create
