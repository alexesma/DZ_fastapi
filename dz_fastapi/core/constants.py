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
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 МБ
ERROR_MESSAGE_FORMAT_DATE ='{key} должен быть в формате "YYYY" или "MM.YYYY"'
ERROR_MESSAGE_RANGE_DATE='{key} не может быть меньше 1980 и больше текущей даты'
FORMAT_YEAR_FOR_CAR_1 = '%Y'
FORMAT_YEAR_FOR_CAR_2 = '%m.%Y'
UPLOAD_DIR = 'uploads/logos'

# Invoice name
PRICELIST_DRAGONZAP = 'dragonzap.xlsx'
PRICELIST_HOT_PARTS = 'Прайс Хотпартс.xls'

#Brand indicators
INDICATOR_FAW_FULL = [
    '52576TKA00', '5CA034156', 'B5CA02648Z', 'BFA0122530', 'FA014373XP1',
]
INDICATOR_BYD_FULL = [
    'BS151602004', 'F32906103',
]
INDICATOR_DONGFENG_FULL = [
    '4151700', '3502500VD01',
]
INDICATOR_CHANGAN_FIRST_THREE = [
    'C20', 'H15', 'H16', 'PA0', 'S10', 'S20', 'S30', 'YA0', 'YJ0',
]
INDICATOR_CHANGAN_FIRST_FOUR = [
    'S111',
]
INDICATOR_CHANGAN_FIRST_TWO = [
    'K0'
]
INDICATOR_CHERY_FIRST_THREE = [
    '473', '475', '480', '481', '484', '513', '525', 'A11', 'A13', 'A15', 'A18', 'A21', 'B11', 'B13', 'B14', 'E4G',
    'F4J', 'FQ1', 'J42', 'J43', 'J52', 'J60', 'J68', 'J69', 'M11', 'M31', 'P11', 'Q18', 'Q32', 'Q33', 'QR5', 'S11',
    'S12', 'S18', 'S21', 'SND', 'T11', 'T15', 'T19', 'T1E', 'T21', '015', '372', '477', '020', '519',
]
INDICATOR_CHERY_FULL = [
    'Q320B12',
]
INDICATOR_LIFAN_FIRST_THREE = [
    'AAB', 'B61', 'BAC', 'LAL', 'LAX', 'LBA', 'LBU', 'LBV', 'LCA', 'LF4', 'LFB', 'PBA', 'Q15', 'Q55'
]
INDICATOR_HAIMA_FULL = [
    'HC0020660BM1', 'SA0034156M1', 'B25D67482AL1'
]
INDICATOR_LIFAN_END_TWO = [
    'C1', 'B1', 'A2', 'B2', 'A4', 'A1',
]
INDICATOR_LIFAN_END_THREE = [
    'B32',
]
INDICATOR_LIFAN_LEN_SEVEN = [
    'SA2', 'SB3', 'SF3', 'SS2',
]
INDICATOR_LIFAN_LEN_NINE = [
    'SBAC', 'SF22', 'SLAL', 'SLBA', 'X60F', 'Q401',
]
INDICATOR_FOTON = [
    'E049343000008',
]
INDICATOR_GEELY_FIRST_TWO = [
    'LP'
]
INDICATOR_GEELY_FIRST_THREE = [
    'E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E08', 'E09', 'E10', 'E12', 'E15', 'E20', 'GA7', 'JQ1', 'JQ3',
]
INDICATOR_GEELY_FIRST_FOUR = [
    '1136', '1602', '1016',
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
    '6007RSNR'
]
INDICATOR_LIFAN_WHISOUT_FIRST = [
    '0'
]
INDICATOR_BYD_FIRST_TWO = ['17']
INDICATOR_BYD_FIRST_THREE = ['102', '101', '100', 'BYD']
INDICATOR_BYD_COMMON_PATTERNS = ['101', '100', '102']
INDICATOR_BYD_HYPHEN_POSITION = [14]
INDICATOR_FAW_PREFIXES = ['FA', '5CA', 'B5CA', 'FC', 'C230', 'L06A']
INDICATOR_FAW_OTHER_PATTERNS = ['3705100', '31250', '27060', '52576', '85310', '67861', '90080']

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
        'title_line': title_line if title_line is not None else default_title_line,
        'start_line': start_line if start_line is not None else default_start_line,
        'column_ids': column_ids
    }


SUPPLIER_PARAM = {
    '1c': create_supplier_params('xlsx', '1c_invoice.xlsx', 'Sheet1', 0, 0, [0, 1, 2, 3, 4]),
    'reline': create_supplier_params('csv', 'relines.csv', None, 0, 17, [1, 5, 9, 26, 57]),
    'autotorg': create_supplier_params('xls', 'партс.xls', 'TDSheet', 13, 17, [1, 5, 9, 26, 57]),
    'apex': create_supplier_params('xls', 'apex.xls', 'TDSheet', 20, 0, [3, 20, 5, 33, 49]),
    'hot-parts': create_supplier_params('xls', '71977.xls', 'Report', 27, 0, [0, 1, 22, 58]),
    'redline': create_supplier_params('xls', 'redline.xls', 'TDSheet', 18, 0, [1, 9, 13, 35, 64]),
    'techo': create_supplier_params('xls', 'nk2023111600027.xls', 'Лист1', 20, 0, [1, 5, 6, 10, 18]),
    'froza': create_supplier_params('xls', 'froza.xls', 'TDSheet', 18, 0, [1, 9, 13, 35, 67, 88]),
    'autopiter': create_supplier_params('xls', 'piter.xls', 'TDSheet', 24, 0, [1, 5, 9, 26, 57]),
}

