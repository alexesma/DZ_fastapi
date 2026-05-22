import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import get_session
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross, AutoPartInvalidCross
from dz_fastapi.schemas.autopart import (
    CrossAdminCreate,
    CrossAdminOut,
    CrossAdminUpdate,
    InvalidCrossAdminCreate,
    InvalidCrossAdminOut,
    InvalidCrossAdminUpdate,
    InvalidCrossCreate,
    InvalidCrossOut,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["crosses", "catalog"])


async def _ensure_autopart_exists(
    session: AsyncSession, autopart_id: int
) -> AutoPart:
    autopart = await session.get(AutoPart, autopart_id)
    if autopart is None:
        raise HTTPException(status_code=404, detail="Запчасть не найдена")
    return autopart


async def _ensure_brand_exists(
    session: AsyncSession, brand_id: int
) -> Brand:
    brand = await session.get(Brand, brand_id)
    if brand is None:
        raise HTTPException(status_code=404, detail="Бренд не найден")
    return brand


async def _resolve_autopart_match_id(
    session: AsyncSession,
    *,
    brand_id: int,
    oem_number: str,
) -> Optional[int]:
    normalized_oem = preprocess_oem_number(oem_number)
    match = (
        await session.execute(
            select(AutoPart.id).where(
                AutoPart.brand_id == brand_id,
                AutoPart.oem_number == normalized_oem,
            )
        )
    ).scalar_one_or_none()
    return match


async def _get_cross_by_id(
    session: AsyncSession, cross_id: int
) -> Optional[AutoPartCross]:
    return (
        await session.execute(
            select(AutoPartCross)
            .where(AutoPartCross.id == cross_id)
            .options(
                selectinload(AutoPartCross.source_autopart).selectinload(
                    AutoPart.brand
                ),
                selectinload(AutoPartCross.cross_brand),
                selectinload(AutoPartCross.cross_autopart),
            )
        )
    ).scalar_one_or_none()


async def _get_invalid_cross_by_id(
    session: AsyncSession, invalid_cross_id: int
) -> Optional[AutoPartInvalidCross]:
    return (
        await session.execute(
            select(AutoPartInvalidCross)
            .where(AutoPartInvalidCross.id == invalid_cross_id)
            .options(
                selectinload(AutoPartInvalidCross.source_autopart).selectinload(
                    AutoPart.brand
                ),
                selectinload(AutoPartInvalidCross.invalid_brand),
                selectinload(AutoPartInvalidCross.invalid_autopart),
            )
        )
    ).scalar_one_or_none()


def _cross_to_out(cross: AutoPartCross) -> CrossAdminOut:
    source_autopart = getattr(cross, "source_autopart", None)
    source_brand = getattr(source_autopart, "brand", None)
    cross_autopart = getattr(cross, "cross_autopart", None)
    return CrossAdminOut(
        id=cross.id,
        source_autopart_id=cross.source_autopart_id,
        source_brand_id=(source_autopart.brand_id if source_autopart else None),
        source_brand_name=(source_brand.name if source_brand else None),
        source_oem_number=(source_autopart.oem_number if source_autopart else ""),
        source_name=(source_autopart.name if source_autopart else None),
        cross_brand_id=cross.cross_brand_id,
        cross_brand_name=(cross.cross_brand.name if cross.cross_brand else None),
        cross_oem_number=cross.cross_oem_number,
        cross_autopart_id=cross.cross_autopart_id,
        cross_autopart_name=(cross_autopart.name if cross_autopart else None),
        priority=cross.priority,
        comment=cross.comment,
    )


def _invalid_cross_to_out(
    invalid_cross: AutoPartInvalidCross,
) -> InvalidCrossAdminOut:
    source_autopart = getattr(invalid_cross, "source_autopart", None)
    source_brand = getattr(source_autopart, "brand", None)
    invalid_autopart = getattr(invalid_cross, "invalid_autopart", None)
    return InvalidCrossAdminOut(
        id=invalid_cross.id,
        source_autopart_id=invalid_cross.source_autopart_id,
        source_brand_id=(source_autopart.brand_id if source_autopart else None),
        source_brand_name=(source_brand.name if source_brand else None),
        source_oem_number=(source_autopart.oem_number if source_autopart else ""),
        source_name=(source_autopart.name if source_autopart else None),
        invalid_brand_id=invalid_cross.invalid_brand_id,
        invalid_brand_name=(
            invalid_cross.invalid_brand.name
            if invalid_cross.invalid_brand is not None
            else None
        ),
        invalid_oem_number=invalid_cross.invalid_oem_number,
        invalid_autopart_id=invalid_cross.invalid_autopart_id,
        invalid_autopart_name=(
            invalid_autopart.name if invalid_autopart is not None else None
        ),
        comment=invalid_cross.comment,
    )


@router.get("/crosses/", response_model=list[CrossAdminOut])
async def list_crosses(
    q: Optional[str] = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(AutoPartCross).options(
        selectinload(AutoPartCross.source_autopart).selectinload(
            AutoPart.brand
        ),
        selectinload(AutoPartCross.cross_brand),
        selectinload(AutoPartCross.cross_autopart),
    )
    search = str(q or "").strip()
    if search:
        pattern = f"%{search}%"
        stmt = stmt.where(
            or_(
                AutoPartCross.cross_oem_number.ilike(pattern),
                AutoPartCross.cross_brand.has(Brand.name.ilike(pattern)),
                AutoPartCross.source_autopart.has(
                    or_(
                        AutoPart.oem_number.ilike(pattern),
                        AutoPart.name.ilike(pattern),
                        AutoPart.brand.has(Brand.name.ilike(pattern)),
                    )
                ),
            )
        )
    stmt = stmt.order_by(AutoPartCross.id.desc())
    rows = (await session.execute(stmt)).scalars().all()
    return [_cross_to_out(row) for row in rows]


@router.post(
    "/crosses/",
    response_model=CrossAdminOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_cross(
    payload: CrossAdminCreate,
    session: AsyncSession = Depends(get_session),
):
    await _ensure_autopart_exists(session, payload.source_autopart_id)
    await _ensure_brand_exists(session, payload.cross_brand_id)
    normalized_oem = preprocess_oem_number(payload.cross_oem_number)
    cross = AutoPartCross(
        source_autopart_id=payload.source_autopart_id,
        cross_brand_id=payload.cross_brand_id,
        cross_oem_number=normalized_oem,
        cross_autopart_id=await _resolve_autopart_match_id(
            session,
            brand_id=payload.cross_brand_id,
            oem_number=normalized_oem,
        ),
        priority=payload.priority,
        comment=payload.comment,
    )
    session.add(cross)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Такой кросс уже существует",
        )
    stored = await _get_cross_by_id(session, cross.id)
    return _cross_to_out(stored)


@router.put("/crosses/{cross_id}", response_model=CrossAdminOut)
async def update_cross(
    cross_id: int,
    payload: CrossAdminUpdate,
    session: AsyncSession = Depends(get_session),
):
    cross = await session.get(AutoPartCross, cross_id)
    if cross is None:
        raise HTTPException(status_code=404, detail="Кросс не найден")

    data = payload.model_dump(exclude_unset=True)
    if "source_autopart_id" in data:
        await _ensure_autopart_exists(session, data["source_autopart_id"])
    if "cross_brand_id" in data:
        await _ensure_brand_exists(session, data["cross_brand_id"])
    if "cross_oem_number" in data:
        data["cross_oem_number"] = preprocess_oem_number(
            data["cross_oem_number"]
        )

    for key, value in data.items():
        setattr(cross, key, value)

    cross.cross_autopart_id = await _resolve_autopart_match_id(
        session,
        brand_id=cross.cross_brand_id,
        oem_number=cross.cross_oem_number,
    )

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Такой кросс уже существует",
        )
    stored = await _get_cross_by_id(session, cross.id)
    return _cross_to_out(stored)


@router.delete("/crosses/{cross_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cross(
    cross_id: int,
    session: AsyncSession = Depends(get_session),
):
    cross = await session.get(AutoPartCross, cross_id)
    if cross is None:
        raise HTTPException(status_code=404, detail="Кросс не найден")
    await session.delete(cross)
    await session.commit()


@router.get(
    "/autoparts/{autopart_id:int}/invalid-crosses/",
    response_model=list[InvalidCrossOut],
)
async def list_autopart_invalid_crosses(
    autopart_id: int,
    session: AsyncSession = Depends(get_session),
):
    await _ensure_autopart_exists(session, autopart_id)
    rows = (
        await session.execute(
            select(AutoPartInvalidCross)
            .where(AutoPartInvalidCross.source_autopart_id == autopart_id)
            .options(selectinload(AutoPartInvalidCross.invalid_brand))
            .order_by(AutoPartInvalidCross.id.desc())
        )
    ).scalars().all()
    return [
        InvalidCrossOut(
            id=row.id,
            invalid_brand_id=row.invalid_brand_id,
            invalid_brand_name=(
                row.invalid_brand.name if row.invalid_brand is not None else None
            ),
            invalid_oem_number=row.invalid_oem_number,
            invalid_autopart_id=row.invalid_autopart_id,
            comment=row.comment,
        )
        for row in rows
    ]


@router.post(
    "/autoparts/{autopart_id:int}/invalid-crosses/",
    response_model=InvalidCrossOut,
    status_code=status.HTTP_201_CREATED,
)
async def add_autopart_invalid_cross(
    autopart_id: int,
    payload: InvalidCrossCreate,
    session: AsyncSession = Depends(get_session),
):
    await _ensure_autopart_exists(session, autopart_id)
    await _ensure_brand_exists(session, payload.invalid_brand_id)
    normalized_oem = preprocess_oem_number(payload.invalid_oem_number)
    invalid_cross = AutoPartInvalidCross(
        source_autopart_id=autopart_id,
        invalid_brand_id=payload.invalid_brand_id,
        invalid_oem_number=normalized_oem,
        invalid_autopart_id=await _resolve_autopart_match_id(
            session,
            brand_id=payload.invalid_brand_id,
            oem_number=normalized_oem,
        ),
        comment=payload.comment,
    )
    session.add(invalid_cross)
    try:
        await session.commit()
        await session.refresh(invalid_cross)
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Такой неверный кросс уже существует",
        )
    brand = await session.get(Brand, invalid_cross.invalid_brand_id)
    return InvalidCrossOut(
        id=invalid_cross.id,
        invalid_brand_id=invalid_cross.invalid_brand_id,
        invalid_brand_name=brand.name if brand is not None else None,
        invalid_oem_number=invalid_cross.invalid_oem_number,
        invalid_autopart_id=invalid_cross.invalid_autopart_id,
        comment=invalid_cross.comment,
    )


@router.get("/invalid-crosses/", response_model=list[InvalidCrossAdminOut])
async def list_invalid_crosses(
    q: Optional[str] = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(AutoPartInvalidCross).options(
        selectinload(AutoPartInvalidCross.source_autopart).selectinload(
            AutoPart.brand
        ),
        selectinload(AutoPartInvalidCross.invalid_brand),
        selectinload(AutoPartInvalidCross.invalid_autopart),
    )
    search = str(q or "").strip()
    if search:
        pattern = f"%{search}%"
        stmt = stmt.where(
            or_(
                AutoPartInvalidCross.invalid_oem_number.ilike(pattern),
                AutoPartInvalidCross.invalid_brand.has(Brand.name.ilike(pattern)),
                AutoPartInvalidCross.source_autopart.has(
                    or_(
                        AutoPart.oem_number.ilike(pattern),
                        AutoPart.name.ilike(pattern),
                        AutoPart.brand.has(Brand.name.ilike(pattern)),
                    )
                ),
            )
        )
    stmt = stmt.order_by(AutoPartInvalidCross.id.desc())
    rows = (await session.execute(stmt)).scalars().all()
    return [_invalid_cross_to_out(row) for row in rows]


@router.post(
    "/invalid-crosses/",
    response_model=InvalidCrossAdminOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_invalid_cross(
    payload: InvalidCrossAdminCreate,
    session: AsyncSession = Depends(get_session),
):
    await _ensure_autopart_exists(session, payload.source_autopart_id)
    await _ensure_brand_exists(session, payload.invalid_brand_id)
    normalized_oem = preprocess_oem_number(payload.invalid_oem_number)
    invalid_cross = AutoPartInvalidCross(
        source_autopart_id=payload.source_autopart_id,
        invalid_brand_id=payload.invalid_brand_id,
        invalid_oem_number=normalized_oem,
        invalid_autopart_id=await _resolve_autopart_match_id(
            session,
            brand_id=payload.invalid_brand_id,
            oem_number=normalized_oem,
        ),
        comment=payload.comment,
    )
    session.add(invalid_cross)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Такой неверный кросс уже существует",
        )
    stored = await _get_invalid_cross_by_id(session, invalid_cross.id)
    return _invalid_cross_to_out(stored)


@router.put(
    "/invalid-crosses/{invalid_cross_id}",
    response_model=InvalidCrossAdminOut,
)
async def update_invalid_cross(
    invalid_cross_id: int,
    payload: InvalidCrossAdminUpdate,
    session: AsyncSession = Depends(get_session),
):
    invalid_cross = await session.get(AutoPartInvalidCross, invalid_cross_id)
    if invalid_cross is None:
        raise HTTPException(
            status_code=404,
            detail="Неверный кросс не найден",
        )

    data = payload.model_dump(exclude_unset=True)
    if "source_autopart_id" in data:
        await _ensure_autopart_exists(session, data["source_autopart_id"])
    if "invalid_brand_id" in data:
        await _ensure_brand_exists(session, data["invalid_brand_id"])
    if "invalid_oem_number" in data:
        data["invalid_oem_number"] = preprocess_oem_number(
            data["invalid_oem_number"]
        )

    for key, value in data.items():
        setattr(invalid_cross, key, value)

    invalid_cross.invalid_autopart_id = await _resolve_autopart_match_id(
        session,
        brand_id=invalid_cross.invalid_brand_id,
        oem_number=invalid_cross.invalid_oem_number,
    )

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Такой неверный кросс уже существует",
        )
    stored = await _get_invalid_cross_by_id(session, invalid_cross.id)
    return _invalid_cross_to_out(stored)


@router.delete(
    "/invalid-crosses/{invalid_cross_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_invalid_cross(
    invalid_cross_id: int,
    session: AsyncSession = Depends(get_session),
):
    invalid_cross = await session.get(AutoPartInvalidCross, invalid_cross_id)
    if invalid_cross is None:
        raise HTTPException(
            status_code=404,
            detail="Неверный кросс не найден",
        )
    await session.delete(invalid_cross)
    await session.commit()
