from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Допустимые типы правил
# ---------------------------------------------------------------------------
RuleType = Literal[
    # --- Полностью реализованы ---
    'price_list',        # Прайс-лист поставщика
    'order_reply',       # Ответ поставщика на заказ
                         # (подтверждение / отказ / частично)
    # --- Реализованы (существующие сервисы) ---
    'customer_order',    # Входящий заказ от клиента
    'document',          # Документ от поставщика
                         # (накладная / счёт / акт / счёт-фактура)
    'shipment_notice',   # Уведомление об отгрузке / трекинг
    'claim',             # Претензия / рекламация от клиента
    # --- Заглушки: уведомление менеджера, логика позже ---
    'error_report',      # Сообщение об ошибке
    'inquiry',           # Вопрос
    'proposal',          # Коммерческое предложение
    'spam',              # Спам (скрыть без обработки)
    # --- Служебное ---
    'ignore',            # Нерелевантное, не трогать
]

# Метаданные для UI и логов
RULE_META: dict[str, dict] = {
    'price_list': {
        'label': 'Прайс-лист', 'group': 'auto', 'color': 'blue'},
    'order_reply': {
        'label': 'Ответ на заказ',
        'group': 'auto', 'color': 'green'},
    'customer_order': {
        'label': 'Заказ от клиента',
        'group': 'auto', 'color': 'cyan'},
    'document': {
        'label': 'Документ', 'group': 'auto', 'color': 'purple'},
    'shipment_notice': {
        'label': 'Уведомление об отгрузке',
        'group': 'auto', 'color': 'geekblue'},
    'claim': {
        'label': 'Претензия / рекламация',
        'group': 'notify', 'color': 'red'},
    'error_report': {
        'label': 'Ошибка', 'group': 'notify', 'color': 'orange'},
    'inquiry': {
        'label': 'Вопрос', 'group': 'notify', 'color': 'gold'},
    'proposal': {
        'label': 'Предложение', 'group': 'notify', 'color': 'lime'},
    'spam': {
        'label': 'Спам', 'group': 'service', 'color': 'default'},
    'ignore': {
        'label': 'Игнорировать',
        'group': 'service', 'color': 'default'},
}


# ---------------------------------------------------------------------------
# InboxEmail schemas
# ---------------------------------------------------------------------------

class AttachmentInfo(BaseModel):
    name: str
    size: Optional[int] = None
    path: Optional[str] = None


class InboxEmailBrief(BaseModel):
    """Краткое представление письма для списка."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    email_account_id: int
    uid: Optional[str] = None
    folder: Optional[str] = None
    from_email: str
    from_name: Optional[str] = None
    subject: Optional[str] = None
    body_preview: Optional[str] = None
    has_attachments: bool = False
    attachment_info: List[AttachmentInfo] = []
    received_at: Optional[datetime] = None
    fetched_at: Optional[datetime] = None

    rule_type: Optional[str] = None
    rule_set_at: Optional[datetime] = None
    rule_set_by_id: Optional[int] = None
    rule_auto_detected: bool = False
    processed: bool = False
    processed_at: Optional[datetime] = None
    processing_error: Optional[str] = None


class InboxEmailDetail(InboxEmailBrief):
    """Полное представление письма
    (включая body_full и результат обработки).
    """

    body_full: Optional[str] = None
    processing_result: Optional[dict] = None


class InboxEmailListResponse(BaseModel):
    items: List[InboxEmailBrief]
    total: int
    page: int
    page_size: int


# ---------------------------------------------------------------------------
# Assign rule request
# ---------------------------------------------------------------------------

class AssignRuleRequest(BaseModel):
    rule_type: RuleType
    # Если True — сохранить паттерн для будущей авто-разметки
    save_pattern: bool = True


class AssignRuleResponse(BaseModel):
    id: int
    rule_type: str
    processed: bool
    processing_result: Optional[dict] = None
    processing_error: Optional[str] = None


class ForceProcessRequest(BaseModel):
    # True: разрешить повторную обработку уже импортированного письма
    allow_reprocess: bool = True


class ForceProcessResponse(BaseModel):
    id: int
    rule_type: str
    processed: bool
    processing_result: Optional[dict] = None
    processing_error: Optional[str] = None


# ---------------------------------------------------------------------------
# EmailRulePattern schemas
# ---------------------------------------------------------------------------

class EmailRulePatternBase(BaseModel):
    email_account_id: Optional[int] = None
    from_email_pattern: Optional[str] = None
    from_domain_pattern: Optional[str] = None
    subject_keywords: List[str] = []
    requires_attachments: Optional[bool] = None
    attachment_extensions: List[str] = []
    rule_type: RuleType
    is_active: bool = True


class EmailRulePatternCreate(EmailRulePatternBase):
    pass


class EmailRulePatternUpdate(BaseModel):
    from_email_pattern: Optional[str] = None
    from_domain_pattern: Optional[str] = None
    subject_keywords: Optional[List[str]] = None
    requires_attachments: Optional[bool] = None
    attachment_extensions: Optional[List[str]] = None
    rule_type: Optional[RuleType] = None
    is_active: Optional[bool] = None


class EmailRulePatternOut(EmailRulePatternBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    times_applied: int = 0
    times_confirmed: int = 0
    created_by_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Setup wizard schemas — создание/привязка реальных конфигов системы
# ---------------------------------------------------------------------------

class ProviderSetupConfig(BaseModel):
    """Настройка привязки письма к поставщику
    (прайс / ответ / документ / отгрузка).
    """
    provider_id: int
    # Паттерны для авто-определения будущих писем
    subject_pattern: Optional[str] = None   # name_mail in pricelist config
    filename_pattern: Optional[str] = None  # name_price / filename_pattern

    # Режим конфигурации столбцов файла
    # 'existing' — обновить выбранную конфигурацию
    # 'new'      — создать новую конфигурацию
    # 'skip'     — только привязать email, не трогать конфиги
    config_mode: Literal['existing', 'new', 'skip'] = 'skip'
    config_id: Optional[int] = None        # ID ProviderPriceListConfig или
    #                                         SupplierResponseConfig
    config_name: Optional[str] = None  # имя для новой SupplierResponseConfig

    # Общие поля столбцов (для price_list, order_reply, document)
    start_row: Optional[int] = None
    oem_col: Optional[int] = None
    qty_col: Optional[int] = None
    price_col: Optional[int] = None
    brand_col: Optional[int] = None
    name_col: Optional[int] = None         # только для price_list

    # Дополнительно для order_reply
    response_type: Optional[Literal['file', 'text']] = None
    status_col: Optional[int] = None
    comment_col: Optional[int] = None
    confirm_keywords: Optional[List[str]] = None
    reject_keywords: Optional[List[str]] = None
    value_after_article_type: Optional[
        Literal['number', 'text', 'both']
    ] = None

    # Дополнительно для document
    document_number_col: Optional[int] = None
    document_date_col: Optional[int] = None


class CustomerSetupConfig(BaseModel):
    """Настройка привязки письма к клиенту (входящий заказ)."""
    customer_id: int
    config_mode: Literal['existing', 'new'] = 'existing'
    config_id: Optional[int] = None
    subject_pattern: Optional[str] = None   # order_subject_pattern (regex)
    filename_pattern: Optional[str] = None  # order_filename_pattern (regex)
    order_config: Optional[dict] = None


class InboxSetupRequest(BaseModel):
    """
    Запрос мастера настройки правила.
    Дополнительно к rule_type — содержит данные для создания/обновления
    реальных конфигов системы (ProviderPricelistConfig, CustomerOrderConfig).
    """
    rule_type: RuleType
    save_pattern: bool = True
    # Заполняется для price_list / order_reply / document / shipment_notice
    provider_config: Optional[ProviderSetupConfig] = None
    # Заполняется только для customer_order
    customer_config: Optional[CustomerSetupConfig] = None


class ConfigSetupInfo(BaseModel):
    """Информация о созданном/обновлённом конфиге в результате настройки."""
    entity_type: str      # 'provider' | 'customer'
    entity_id: int
    entity_name: str
    # 'linked' | 'updated' | 'already_linked' | 'no_config'
    action: str
    note: Optional[str] = None


class InboxSetupResponse(BaseModel):
    email_id: int
    rule_type: str
    processed: bool
    processing_result: Optional[dict] = None
    processing_error: Optional[str] = None
    configs_set: List[ConfigSetupInfo] = []


class SetupOption(BaseModel):
    """Элемент выпадающего списка поставщиков / клиентов."""
    id: int
    name: str
    email: Optional[str] = None


class InboxSetupOptions(BaseModel):
    """Данные для выпадающих списков в мастере настройки."""
    providers: List[SetupOption]
    customers: List[SetupOption]


# ---------------------------------------------------------------------------
# Fetch request/response
# ---------------------------------------------------------------------------

class FetchInboxRequest(BaseModel):
    email_account_id: Optional[int] = None  # None = все активные ящики
    days: int = 3  # 1..7

    model_config = ConfigDict(
        json_schema_extra={
            'example': {'email_account_id': 1, 'days': 3}
        }
    )


class FetchInboxResponse(BaseModel):
    fetched: int       # сколько писем загружено с сервера
    stored: int        # сколько новых записей добавлено в БД
    auto_processed: int  # сколько автоматически обработано по паттернам
