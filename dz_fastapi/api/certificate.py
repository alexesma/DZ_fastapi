"""Справочник сертификатов и деклараций соответствия."""
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import delete, func, insert, select
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
from dz_fastapi.services.regulatory import apply_brand_certificate, normalize_certificate_number

router = APIRouter()


def _to_out(row, autopart_count: int) -> CertificateOut:
    certificate, brand_name = row
    return CertificateOut(
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
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(
            Certificate.number.ilike(pattern)
            | Certificate.applicant.ilike(pattern)
            | Certificate.scope.ilike(pattern)
        )
    if brand_id:
        stmt = stmt.where(Certificate.brand_id == brand_id)
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
    return CertificateListResponse(
        items=[_to_out((row[0], row[1]), int(row[2])) for row in rows],
        total=int(total),
        page=page,
        page_size=page_size,
    )


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
    return _to_out((certificate, brand_name), int(count))


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
    # в прайс клиенту.
    await session.execute(
        AutoPart.__table__.update()
        .where(AutoPart.eac_cert_number == certificate.number)
        .values(eac_cert_number=None, eac_cert_url=None)
    )
    await session.delete(certificate)
    await session.commit()
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
    linked = 0
    for autopart_id in payload.autopart_ids:
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
    return {"linked": linked}


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
    result = await apply_brand_certificate(
        session,
        brand_id=payload.brand_id,
        number=certificate.number,
        url=certificate.url,
        dry_run=payload.dry_run,
        only_undetermined=payload.only_undetermined,
    )
    return ApplyBrandResponse(**result)
