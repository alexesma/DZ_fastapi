"""Shared, administrator managed label layouts."""

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user, require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.models.settings import PrintFormTemplate

router = APIRouter(
    prefix="/print-forms",
    tags=["print-forms"],
    dependencies=[Depends(get_current_user)],
)

FORM_KEYS = {
    "product", "location", "production_wave", "cross_docking",
    "own_stock", "shipment_box", "stock_order_box",
}


class PrintBlock(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    kind: Literal["text", "barcode"] = "text"
    field: str = Field(default="", max_length=100, pattern=r"^[A-Za-z0-9_.]*$")
    text: str = Field(default="", max_length=200)
    x: float = Field(ge=0, le=300)
    y: float = Field(ge=0, le=300)
    width: float = Field(gt=0, le=300)
    height: float = Field(gt=0, le=300)
    font_size: float = Field(default=9, ge=4, le=40)
    bold: bool = False
    align: Literal["left", "center", "right"] = "left"

    @model_validator(mode="after")
    def validate_content(self):
        if self.kind == "barcode" and not self.field:
            raise ValueError("Для штрихкода нужно выбрать поле")
        if self.kind == "text" and not (self.field or self.text):
            raise ValueError("Укажите поле или постоянный текст")
        return self


class PrintFormInput(BaseModel):
    width_mm: float = Field(ge=20, le=210)
    height_mm: float = Field(ge=15, le=297)
    blocks: list[PrintBlock] = Field(max_length=40)

    @model_validator(mode="after")
    def validate_layout(self):
        ids = [block.id for block in self.blocks]
        if len(ids) != len(set(ids)):
            raise ValueError("Идентификаторы блоков должны быть уникальными")
        for block in self.blocks:
            if block.x + block.width > self.width_mm + 0.001:
                raise ValueError(f"Блок {block.id} выходит за правый край")
            if block.y + block.height > self.height_mm + 0.001:
                raise ValueError(f"Блок {block.id} выходит за нижний край")
        return self


def _out(row: PrintFormTemplate) -> dict:
    return {
        "key": row.key,
        "width_mm": row.width_mm,
        "height_mm": row.height_mm,
        "blocks": row.blocks,
        "updated_at": row.updated_at,
    }


@router.get("")
async def list_print_forms(
    response: Response, session: AsyncSession = Depends(get_session)
):
    # Макет должен быть всегда свежим: иначе печать идёт по старому.
    response.headers["Cache-Control"] = "no-store"
    rows = (await session.execute(
        select(PrintFormTemplate).order_by(PrintFormTemplate.key)
    )).scalars().all()
    return [_out(row) for row in rows]


@router.put("/{key}")
async def save_print_form(
    key: str,
    data: PrintFormInput,
    session: AsyncSession = Depends(get_session),
    _admin=Depends(require_admin),
):
    if key not in FORM_KEYS:
        raise HTTPException(status_code=404, detail="Неизвестный формат этикетки")
    row = (await session.execute(
        select(PrintFormTemplate).where(PrintFormTemplate.key == key)
    )).scalar_one_or_none()
    if row is None:
        row = PrintFormTemplate(key=key)
        session.add(row)
    row.width_mm = data.width_mm
    row.height_mm = data.height_mm
    row.blocks = [block.model_dump() for block in data.blocks]
    await session.commit()
    await session.refresh(row)
    return _out(row)
