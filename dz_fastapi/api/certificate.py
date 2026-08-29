"""Справочник сертификатов и деклараций соответствия."""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import delete, func, insert, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.certificate import Certificate, autopart_certificate_association
from dz_fastapi.schemas.certificate import (
    ApplyBrandRequest,
    ApplyBrandResponse,
    CertificateAutoPartRow,
    CertificateAutoPartsResponse,
    CertificateCreate,
    CertificateLinkRequest,
    CertificateListResponse,
    CertificateOut,
    CertificateUpdate,
)
from dz_fastapi.services.regulatory import (
    BLOCKING_LINK_PROBLEMS,
    apply_brand_certificate,
    autopart_ids_for_certificate,
    backfill_certificate_brands,
    certificate_link_problems,
    load_brand_groups,
    normalize_certificate_number,
    refresh_autopart_certificate_cache,
)

router = APIRouter()


async def _brands_by_certificate(
    session: AsyncSession,
    certificate_ids: list[int],
) -> dict[int, list[str]]:
    """Бренды связанных позиций по каждому сертификату.

    Отдельным запросом на страницу, а не подзапросом в основном:
    агрегировать массивы вместе с постраничной выборкой дороже и хуже
    читается, а страница здесь не больше двухсот строк.
    """
    if not certificate_ids:
        return {}
    rows = (
        await session.execute(
            select(
                autopart_certificate_association.c.certificate_id,
                Brand.name,
            )
            .join(
                AutoPart,
                AutoPart.id == autopart_certificate_association.c.autopart_id,
            )
            .join(Brand, Brand.id == AutoPart.brand_id)
            .where(
                autopart_certificate_association.c.certificate_id.in_(
                    certificate_ids
                )
            )
            .distinct()
        )
    ).all()
    result: dict[int, list[str]] = {}
    for certificate_id, brand_name in rows:
        result.setdefault(int(certificate_id), []).append(brand_name)
    for names in result.values():
        names.sort()
    return result


def _to_out(
    row,
    autopart_count: int,
    brands: Optional[list[str]] = None,
) -> CertificateOut:
    certificate, brand_name = row
    return CertificateOut(
        brands=brands or [],
        id=certificate.id,
        number=certificate.number,
        url=certificate.url,
        brand_id=certificate.brand_id,
        brand_name=brand_name,
        covers_whole_brand=certificate.covers_whole_brand,
        valid_from=certificate.valid_from,
        valid_until=certificate.valid_until,
        applicant=certificate.applicant,
        manufacturer=certificate.manufacturer,
        scope=certificate.scope,
        source_brand=certificate.source_brand,
        source=certificate.source,
        autopart_count=autopart_count,
        is_expired=bool(
            certificate.valid_until and certificate.valid_until < date.today()
        ),
        created_at=certificate.created_at,
    )


@router.get(
    "/certificates/",
    tags=["certificates"],
    response_model=CertificateListResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def list_certificates(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    search: str | None = Query(default=None),
    brand_id: int | None = Query(default=None),
    only_expiring: bool = Query(
        default=False,
        description="Только истекшие и истекающие в ближайшие 60 дней",
    ),
    only_linked: bool = Query(
        default=True,
        description=(
            "Только документы, привязанные хотя бы к одной позиции. "
            "Из прайсов поставщиков приходят сотни номеров на чужой "
            "ассортимент — без фильтра они забивают справочник."
        ),
    ),
    session: AsyncSession = Depends(get_session),
):
    counts = (
        select(
            autopart_certificate_association.c.certificate_id.label("cid"),
            func.count().label("cnt"),
        )
        .group_by(autopart_certificate_association.c.certificate_id)
        .subquery()
    )
    stmt = (
        select(Certificate, Brand.name, func.coalesce(counts.c.cnt, 0))
        .outerjoin(Brand, Brand.id == Certificate.brand_id)
        .outerjoin(counts, counts.c.cid == Certificate.id)
    )
    if search:
        raw = search.strip()
        # Номер в базе лежит кириллицей, а из бланка его копируют с
        # латинскими двойниками. Ищем по обоим написаниям, иначе документ
        # не находится по тому же тексту, которым его завели.
        normalized = normalize_certificate_number(raw)
        patterns = {f"%{raw}%", f"%{normalized}%"}
        number_match = or_(
            *[Certificate.number.ilike(item) for item in patterns]
        )
        # Бренд ищем и по объявленному полю, и по связанным позициям:
        # у документов из прайсов поставщиков поле часто пустое, а бренд
        # виден только через привязку.
        linked_brand = (
            select(autopart_certificate_association.c.certificate_id)
            .join(
                AutoPart,
                AutoPart.id == autopart_certificate_association.c.autopart_id,
            )
            .join(Brand, Brand.id == AutoPart.brand_id)
            .where(
                autopart_certificate_association.c.certificate_id
                == Certificate.id,
                Brand.name.ilike(f"%{raw}%"),
            )
        )
        stmt = stmt.where(
            number_match
            | Certificate.applicant.ilike(f"%{raw}%")
            | Certificate.scope.ilike(f"%{raw}%")
            | Brand.name.ilike(f"%{raw}%")
            # Бренд из прайса поставщика: его нет в нашем каталоге,
            # но искать документ по этому названию нужно.
            | Certificate.source_brand.ilike(f"%{raw}%")
            | linked_brand.exists()
        )
    if brand_id:
        stmt = stmt.where(Certificate.brand_id == brand_id)
    if only_linked:
        stmt = stmt.where(func.coalesce(counts.c.cnt, 0) > 0)
    if only_expiring:
        # Бессрочные (valid_until пуст) сюда не попадают: тревожить по ним
        # нечем, а список должен показывать то, с чем нужно работать.
        stmt = stmt.where(
            Certificate.valid_until.is_not(None),
            Certificate.valid_until
            <= date.fromordinal(date.today().toordinal() + 60),
        )

    total = (
        await session.execute(
            select(func.count()).select_from(stmt.subquery())
        )
    ).scalar_one()
    rows = (
        await session.execute(
            stmt.order_by(
                Certificate.covers_whole_brand.desc(),
                func.coalesce(counts.c.cnt, 0).desc(),
                Certificate.number.asc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    brands_map = await _brands_by_certificate(
        session, [int(row[0].id) for row in rows]
    )
    return CertificateListResponse(
        items=[
            _to_out(
                (row[0], row[1]),
                int(row[2]),
                brands_map.get(int(row[0].id)),
            )
            for row in rows
        ],
        total=int(total),
        page=page,
        page_size=page_size,
    )


@router.post(
    "/certificates/backfill-brands/",
    tags=["certificates"],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def backfill_brands(
    dry_run: bool = Query(default=True),
    session: AsyncSession = Depends(get_session),
):
    """Дозаполняет бренд по связанным позициям, где он однозначен."""
    return await backfill_certificate_brands(session, dry_run=dry_run)


@router.post(
    "/certificates/",
    tags=["certificates"],
    response_model=CertificateOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin)],
)
async def create_certificate(
    payload: CertificateCreate,
    session: AsyncSession = Depends(get_session),
):
    # Номер приводим к кириллице: с латинскими двойниками документ не
    # найдётся в реестре, когда клиент пойдёт его проверять.
    number = normalize_certificate_number(payload.number)
    if not number:
        raise HTTPException(status_code=400, detail="Пустой номер")
    exists = (
        await session.execute(
            select(Certificate).where(Certificate.number == number)
        )
    ).scalar_one_or_none()
    if exists:
        raise HTTPException(
            status_code=409, detail="Такой сертификат уже заведён"
        )
    certificate = Certificate(
        **{**payload.model_dump(), "number": number, "source": "manual"}
    )
    session.add(certificate)
    await session.commit()
    await session.refresh(certificate)
    brand_name = (
        await session.execute(
            select(Brand.name).where(Brand.id == certificate.brand_id)
        )
    ).scalar_one_or_none()
    return _to_out((certificate, brand_name), 0)


@router.patch(
    "/certificates/{certificate_id}/",
    tags=["certificates"],
    response_model=CertificateOut,
    dependencies=[Depends(require_admin)],
)
async def update_certificate(
    certificate_id: int,
    payload: CertificateUpdate,
    session: AsyncSession = Depends(get_session),
):
    certificate = await session.get(Certificate, certificate_id)
    if not certificate:
        raise HTTPException(status_code=404, detail="Сертификат не найден")
    data = payload.model_dump(exclude_unset=True)
    if "number" in data and data["number"]:
        data["number"] = normalize_certificate_number(data["number"])
    for key, value in data.items():
        setattr(certificate, key, value)
    session.add(certificate)
    await session.commit()
    await session.refresh(certificate)
    # Прайс читает кэш на карточке: без пересборки там останется старый
    # номер, ссылка или срок.
    await refresh_autopart_certificate_cache(
        session, await autopart_ids_for_certificate(session, certificate.id)
    )
    brand_name = (
        await session.execute(
            select(Brand.name).where(Brand.id == certificate.brand_id)
        )
    ).scalar_one_or_none()
    count = (
        await session.execute(
            select(func.count()).where(
                autopart_certificate_association.c.certificate_id
                == certificate.id
            )
        )
    ).scalar_one()
    brands = await _brands_by_certificate(session, [certificate.id])
    return _to_out(
        (certificate, brand_name), int(count), brands.get(certificate.id)
    )


@router.delete(
    "/certificates/{certificate_id}/",
    tags=["certificates"],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def delete_certificate(
    certificate_id: int,
    session: AsyncSession = Depends(get_session),
):
    certificate = await session.get(Certificate, certificate_id)
    if not certificate:
        raise HTTPException(status_code=404, detail="Сертификат не найден")
    # Кэш на карточках чистим, чтобы номер удалённого документа не уехал
    # в прайс клиенту. Список берём до удаления — после каскад унесёт связи.
    affected = await autopart_ids_for_certificate(session, certificate.id)
    await session.delete(certificate)
    await session.commit()
    await refresh_autopart_certificate_cache(session, affected)
    return {"status": "ok"}


@router.get(
    "/certificates/{certificate_id}/autoparts/",
    tags=["certificates"],
    response_model=CertificateAutoPartsResponse,
    dependencies=[Depends(require_admin)],
)
async def list_certificate_autoparts(
    certificate_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    base = (
        select(AutoPart, Brand.name)
        .join(
            autopart_certificate_association,
            autopart_certificate_association.c.autopart_id == AutoPart.id,
        )
        .outerjoin(Brand, Brand.id == AutoPart.brand_id)
        .where(
            autopart_certificate_association.c.certificate_id
            == certificate_id
        )
    )
    total = (
        await session.execute(
            select(func.count()).select_from(base.subquery())
        )
    ).scalar_one()
    rows = (
        await session.execute(
            base.order_by(AutoPart.oem_number.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    return CertificateAutoPartsResponse(
        items=[
            CertificateAutoPartRow(
                autopart_id=part.id,
                oem_number=part.oem_number,
                name=part.name,
                brand_name=brand_name,
                regulatory_source=part.regulatory_source,
            )
            for part, brand_name in rows
        ],
        total=int(total),
        page=page,
        page_size=page_size,
    )


@router.post(
    "/certificates/{certificate_id}/autoparts/",
    tags=["certificates"],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def link_autoparts(
    certificate_id: int,
    payload: CertificateLinkRequest,
    session: AsyncSession = Depends(get_session),
):
    certificate = await session.get(Certificate, certificate_id)
    if not certificate:
        raise HTTPException(status_code=404, detail="Сертификат не найден")

    # Привязка руками — единственный путь, который раньше писал в таблицу
    # связей без проверок. Документ на чужой бренд отсюда пройти не должен.
    _, brand_groups = await load_brand_groups(session)
    parts = {
        part.id: part
        for part in (
            await session.execute(
                select(AutoPart).where(
                    AutoPart.id.in_(payload.autopart_ids)
                )
            )
        ).scalars()
    }

    linked = 0
    rejected: list[dict] = []
    for autopart_id in payload.autopart_ids:
        part = parts.get(autopart_id)
        if part is None:
            rejected.append({"autopart_id": autopart_id, "reason": "no_part"})
            continue
        blocking = set(
            certificate_link_problems(part, certificate, brand_groups)
        ) & BLOCKING_LINK_PROBLEMS
        if blocking:
            rejected.append(
                {"autopart_id": autopart_id, "reason": sorted(blocking)[0]}
            )
            continue
        exists = (
            await session.execute(
                select(autopart_certificate_association).where(
                    autopart_certificate_association.c.autopart_id
                    == autopart_id,
                    autopart_certificate_association.c.certificate_id
                    == certificate_id,
                )
            )
        ).first()
        if exists:
            continue
        await session.execute(
            insert(autopart_certificate_association).values(
                autopart_id=autopart_id, certificate_id=certificate_id
            )
        )
        linked += 1
    await session.commit()
    await refresh_autopart_certificate_cache(session, payload.autopart_ids)
    return {"linked": linked, "rejected": rejected}


@router.delete(
    "/certificates/{certificate_id}/autoparts/{autopart_id}/",
    tags=["certificates"],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_admin)],
)
async def unlink_autopart(
    certificate_id: int,
    autopart_id: int,
    session: AsyncSession = Depends(get_session),
):
    await session.execute(
        delete(autopart_certificate_association).where(
            autopart_certificate_association.c.autopart_id == autopart_id,
            autopart_certificate_association.c.certificate_id
            == certificate_id,
        )
    )
    await session.commit()
    await refresh_autopart_certificate_cache(session, [autopart_id])
    return {"status": "ok"}


@router.post(
    "/certificates/{certificate_id}/apply-brand/",
    tags=["certificates"],
    response_model=ApplyBrandResponse,
    dependencies=[Depends(require_admin)],
)
async def apply_certificate_to_brand(
    certificate_id: int,
    payload: ApplyBrandRequest,
    session: AsyncSession = Depends(get_session),
):
    """Привязывает сертификат ко всему бренду.

    dry_run=true возвращает количество затрагиваемых позиций без записи —
    интерфейс показывает это как предпросмотр.
    """
    certificate = await session.get(Certificate, certificate_id)
    if not certificate:
        raise HTTPException(status_code=404, detail="Сертификат не найден")
    try:
        result = await apply_brand_certificate(
            session,
            brand_id=payload.brand_id,
            number=certificate.number,
            url=certificate.url,
            dry_run=payload.dry_run,
            only_undetermined=payload.only_undetermined,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    return ApplyBrandResponse(**result)
