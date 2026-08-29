"""Обязательные реквизиты прайса: загрузка файлов, правила, покрытие.

До этого импорт и отчёт запускались только скриптом, поэтому новая
номенклатура реквизитов не получала, а состояние покрытия никто не
видел. Здесь те же функции, что и в services/regulatory, но доступные
из интерфейса.
"""
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.schemas.regulatory import (
    RegistryRefreshResponse,
    RegulatoryCoverageResponse,
    RegulatoryImportResponse,
    RegulatoryRulesResponse,
    SuspiciousLinksResponse,
)
from dz_fastapi.services.certification_rules import apply_exemption_rules, sync_exemption_rules
from dz_fastapi.services.registry_lookup import refresh_certificates_from_registry
from dz_fastapi.services.regulatory import (
    import_supplier_regulatory,
    parse_supplier_regulatory_file,
    regulatory_coverage,
    suspicious_certificate_links,
)

router = APIRouter()

# Файлы поставщиков доходят до десятков мегабайт; больше этого — почти
# наверняка не тот файл, и разбирать его в память не нужно.
MAX_UPLOAD_BYTES = 64 * 1024 * 1024


@router.post(
    "/regulatory/import/",
    tags=["regulatory"],
    response_model=RegulatoryImportResponse,
    dependencies=[Depends(require_admin)],
)
async def import_regulatory_file(
    file: UploadFile = File(...),
    dry_run: bool = Query(
        default=True,
        description="Только посчитать, ничего не записывая",
    ),
    overwrite_manual: bool = Query(
        default=False,
        description="Перетереть позиции с ручным вводом",
    ),
    session: AsyncSession = Depends(get_session),
):
    """Переносит реквизиты из прайса поставщика в карточки.

    По умолчанию это предпросмотр: сначала видно, сколько строк
    сопоставилось, и только потом имеет смысл записывать.
    """
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Файл пустой")
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Файл слишком большой")
    try:
        rows, columns = parse_supplier_regulatory_file(content)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    if not rows:
        raise HTTPException(
            status_code=400,
            detail="В файле не нашлось строк с брендом и артикулом",
        )

    stats = await import_supplier_regulatory(
        session,
        rows,
        dry_run=dry_run,
        overwrite_manual=overwrite_manual,
    )
    # Не сопоставившихся брендов бывают тысячи — в ответ отдаём только
    # верхушку, по ней и видно, чего не хватает в каталоге.
    unmatched_brands = dict(
        sorted(
            stats.pop("unmatched_brands", {}).items(),
            key=lambda item: -item[1],
        )[:20]
    )
    return RegulatoryImportResponse(
        file_name=file.filename,
        columns=sorted(columns),
        dry_run=dry_run,
        unmatched_brands=unmatched_brands,
        **stats,
    )


@router.get(
    "/regulatory/coverage/",
    tags=["regulatory"],
    response_model=RegulatoryCoverageResponse,
)
async def get_regulatory_coverage(
    only_in_stock: bool = Query(
        default=True,
        description="Считать только позиции с остатком",
    ),
    session: AsyncSession = Depends(get_session),
):
    """Чего не хватает в позициях, которые уходят клиентам."""
    return RegulatoryCoverageResponse(
        **await regulatory_coverage(session, only_in_stock=only_in_stock)
    )


@router.post(
    "/regulatory/rules/apply/",
    tags=["regulatory"],
    response_model=RegulatoryRulesResponse,
    dependencies=[Depends(require_admin)],
)
async def apply_certification_rules(
    dry_run: bool = Query(default=True),
    only_unset: bool = Query(
        default=True,
        description="Трогать только позиции без признака",
    ),
    limit: Optional[int] = Query(default=None, ge=1),
    session: AsyncSession = Depends(get_session),
):
    """Проставляет признак сертификации по наименованию позиции."""
    synced = await sync_exemption_rules(session)
    applied = await apply_exemption_rules(
        session, dry_run=dry_run, only_unset=only_unset, limit=limit
    )
    return RegulatoryRulesResponse(
        dry_run=dry_run,
        rules_created=synced["created"],
        rules_total=synced["total"],
        **applied,
    )


@router.post(
    "/regulatory/registry-refresh/",
    tags=["regulatory"],
    response_model=RegistryRefreshResponse,
    dependencies=[Depends(require_admin)],
)
async def refresh_from_registry(
    only_linked: bool = Query(
        default=True,
        description="Только документы, покрывающие позиции",
    ),
    only_unchecked: bool = Query(
        default=True,
        description="Пропустить уже сверенные",
    ),
    limit: Optional[int] = Query(default=50, ge=1, le=500),
    dry_run: bool = Query(default=False),
    session: AsyncSession = Depends(get_session),
):
    """Берёт срок и состояние документа из реестра ФГИС.

    Реестр отвечает медленно, поэтому за один вызов сверяем ограниченную
    порцию: кнопку можно нажать несколько раз подряд.
    """
    return RegistryRefreshResponse(
        **await refresh_certificates_from_registry(
            session,
            only_linked=only_linked,
            only_unchecked=only_unchecked,
            limit=limit,
            dry_run=dry_run,
        )
    )


@router.get(
    "/regulatory/suspicious-links/",
    tags=["regulatory"],
    response_model=SuspiciousLinksResponse,
)
async def get_suspicious_links(
    limit: int = Query(default=200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
):
    """Связи позиция-документ, не прошедшие проверку."""
    return SuspiciousLinksResponse(
        **await suspicious_certificate_links(session, limit=limit)
    )
