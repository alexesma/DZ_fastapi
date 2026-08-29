from typing import Optional

from pydantic import BaseModel


class RegulatoryImportResponse(BaseModel):
    file_name: Optional[str] = None
    columns: list[str] = []
    dry_run: bool = True
    rows: int = 0
    matched: int = 0
    unmatched: int = 0
    updated: int = 0
    skipped_manual: int = 0
    unchanged: int = 0
    # Сколько раз заполнилось каждое поле — видно, что именно дал файл.
    fields_filled: dict[str, int] = {}
    # Бренды, которых у нас нет: главный повод не сопоставившихся строк.
    unmatched_brands: dict[str, int] = {}
    certificates: int = 0
    links_created: int = 0
    certificate_links_rejected: int = 0
    certificate_rejections: dict[str, int] = {}
    cache_refreshed: int = 0
    honest_sign_linked: int = 0
    honest_sign_flag_only: int = 0
    honest_sign_unknown: dict[str, int] = {}


class CoverageRow(BaseModel):
    positions: int
    missing: int


class BrandCoverageRow(CoverageRow):
    brand: str


class ProviderCoverageRow(CoverageRow):
    provider: str


class RegulatoryCoverageResponse(BaseModel):
    positions: int = 0
    expired_certificates: int = 0
    # Документ выгружается, но срок не заполнен и потому не проверен.
    undated_certificates: int = 0
    tnved_pct: Optional[float] = None
    okpd2_pct: Optional[float] = None
    certificate_pct: Optional[float] = None
    complete_pct: Optional[float] = None
    brands: list[BrandCoverageRow] = []
    providers: list[ProviderCoverageRow] = []


class RegulatoryRulesResponse(BaseModel):
    dry_run: bool = True
    rules_created: int = 0
    rules_total: int = 0
    rules: int = 0
    checked: int = 0
    matched: int = 0
    updated: int = 0
    exempted: int = 0
    required: int = 0
    top_patterns: list[tuple[str, int]] = []


class RegistryRefreshResponse(BaseModel):
    dry_run: bool = False
    # Документов, подходящих под фильтр, и из них — с разбираемой ссылкой.
    candidates: int = 0
    supported: int = 0
    answered: int = 0
    updated: int = 0
    dated: int = 0
    # Реестр говорит, что документ приостановлен, прекращён или в архиве.
    not_active: int = 0
    cards_refreshed: int = 0
    # Реестр не отвечал подряд — сверка остановлена, не дойдя до конца.
    aborted: bool = False


class SuspiciousLinkRow(BaseModel):
    autopart_id: int
    oem_number: str
    name: Optional[str] = None
    brand_name: Optional[str] = None
    certificate_id: int
    number: str
    # brand_mismatch и not_active — блокирующие: в прайс такая связь
    # не попадёт. brand_unknown — у документа не проставлен бренд.
    problems: list[str] = []
    blocking: bool = False


class SuspiciousLinksResponse(BaseModel):
    links: int = 0
    with_problems: int = 0
    blocking: int = 0
    # Документов, ни разу не сверенных с реестром: пока сверка не
    # запускалась, это верно для всех, поэтому счётчик отдельный.
    unverified_certificates: int = 0
    by_problem: dict[str, int] = {}
    items: list[SuspiciousLinkRow] = []


class TnvedOkpd2ImportResponse(BaseModel):
    rows: int = 0
    created: int = 0
    existing: int = 0


class Okpd2FromTnvedResponse(BaseModel):
    # Строк в загруженной таблице соответствия. Ноль означает, что
    # таблицу ещё не загружали и проставлять не из чего.
    table_rows: int = 0
    positions: int = 0
    updated: int = 0
    # Одному ТН ВЭД отвечает несколько ОКПД 2 — выбор за человеком.
    ambiguous: int = 0
    no_match: int = 0
