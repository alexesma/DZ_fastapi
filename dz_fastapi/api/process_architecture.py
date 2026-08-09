import json

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.process_architecture import ProcessArchitectureAnnotation
from dz_fastapi.models.user import User, UserRole
from dz_fastapi.schemas.process_architecture import (
    ProcessAnnotationCreate,
    ProcessAnnotationOut,
    ProcessAnnotationUpdate,
)

router = APIRouter(prefix="/process-architecture", tags=["process-architecture"])

MAX_DRAWING_BYTES = 500_000
MAX_STROKES = 200
MAX_POINTS = 5_000


def _validate_drawing(data: dict | None) -> None:
    if not data:
        raise HTTPException(status_code=422, detail="Рисунок не может быть пустым")
    try:
        encoded = json.dumps(data, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Некорректные данные рисунка") from exc
    if len(encoded.encode("utf-8")) > MAX_DRAWING_BYTES:
        raise HTTPException(status_code=413, detail="Рисунок слишком большой")
    strokes = data.get("strokes")
    if not isinstance(strokes, list) or not strokes or len(strokes) > MAX_STROKES:
        raise HTTPException(status_code=422, detail="Некорректные штрихи рисунка")
    point_count = 0
    for stroke in strokes:
        if not isinstance(stroke, dict) or not isinstance(stroke.get("points"), list):
            raise HTTPException(status_code=422, detail="Некорректный штрих рисунка")
        for point in stroke["points"]:
            if (
                not isinstance(point, list)
                or len(point) != 2
                or not all(isinstance(value, (int, float)) for value in point)
                or not all(0 <= value <= 1 for value in point)
            ):
                raise HTTPException(status_code=422, detail="Некорректная точка рисунка")
        point_count += len(stroke["points"])
    if point_count > MAX_POINTS:
        raise HTTPException(status_code=413, detail="В рисунке слишком много точек")


def _can_manage(annotation: ProcessArchitectureAnnotation, user: User) -> bool:
    return annotation.created_by_id == user.id or user.role == UserRole.ADMIN


async def _get_annotation(
    session: AsyncSession, annotation_id: int
) -> ProcessArchitectureAnnotation:
    result = await session.execute(
        select(ProcessArchitectureAnnotation)
        .options(
            selectinload(ProcessArchitectureAnnotation.created_by),
            selectinload(ProcessArchitectureAnnotation.resolved_by),
        )
        .where(ProcessArchitectureAnnotation.id == annotation_id)
    )
    annotation = result.scalar_one_or_none()
    if annotation is None:
        raise HTTPException(status_code=404, detail="Аннотация не найдена")
    return annotation


@router.get("/annotations", response_model=list[ProcessAnnotationOut])
async def list_process_annotations(
    page_key: str = Query(default="dragonzap-operating-model", min_length=1, max_length=64),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    result = await session.execute(
        select(ProcessArchitectureAnnotation)
        .options(
            selectinload(ProcessArchitectureAnnotation.created_by),
            selectinload(ProcessArchitectureAnnotation.resolved_by),
        )
        .where(ProcessArchitectureAnnotation.page_key == page_key)
        .order_by(ProcessArchitectureAnnotation.created_at, ProcessArchitectureAnnotation.id)
    )
    return list(result.scalars().all())


@router.post(
    "/annotations",
    response_model=ProcessAnnotationOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_process_annotation(
    payload: ProcessAnnotationCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    parent = None
    if payload.parent_id is not None:
        parent = await _get_annotation(session, payload.parent_id)
        if parent.kind != "comment":
            raise HTTPException(status_code=422, detail="Отвечать можно только на комментарий")
        if parent.parent_id is not None:
            raise HTTPException(status_code=422, detail="Поддерживается один уровень ответов")
        if parent.page_key != payload.page_key or parent.section_key != payload.section_key:
            raise HTTPException(status_code=422, detail="Ответ относится к другому разделу")
    if payload.kind == "drawing":
        _validate_drawing(payload.drawing_data)

    annotation_data = payload.model_dump()
    annotation_data["content"] = (payload.content or "").strip() or None
    annotation = ProcessArchitectureAnnotation(
        **annotation_data,
        created_by_id=current_user.id,
    )
    session.add(annotation)
    await session.commit()
    return await _get_annotation(session, annotation.id)


@router.patch("/annotations/{annotation_id}", response_model=ProcessAnnotationOut)
async def update_process_annotation(
    annotation_id: int,
    payload: ProcessAnnotationUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    annotation = await _get_annotation(session, annotation_id)
    if not _can_manage(annotation, current_user):
        raise HTTPException(status_code=403, detail="Недостаточно прав для изменения")

    update_data = payload.model_dump(exclude_unset=True)
    if "content" in update_data:
        if annotation.kind != "comment":
            raise HTTPException(status_code=422, detail="У рисунка нельзя изменить текст")
        content = (update_data["content"] or "").strip()
        if not content:
            raise HTTPException(status_code=422, detail="Комментарий не может быть пустым")
        annotation.content = content
    if "is_resolved" in update_data:
        if annotation.kind != "comment" or annotation.parent_id is not None:
            raise HTTPException(status_code=422, detail="Закрыть можно только обсуждение")
        annotation.is_resolved = bool(update_data["is_resolved"])
        annotation.resolved_by_id = current_user.id if annotation.is_resolved else None
        annotation.resolved_at = now_moscow() if annotation.is_resolved else None

    await session.commit()
    return await _get_annotation(session, annotation.id)


@router.delete(
    "/annotations/{annotation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_process_annotation(
    annotation_id: int,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    annotation = await _get_annotation(session, annotation_id)
    if not _can_manage(annotation, current_user):
        raise HTTPException(status_code=403, detail="Недостаточно прав для удаления")
    await session.delete(annotation)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
