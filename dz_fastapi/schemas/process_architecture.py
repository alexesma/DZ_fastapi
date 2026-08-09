from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ProcessAnnotationAuthor(BaseModel):
    id: int
    name: str | None = None
    email: str

    model_config = ConfigDict(from_attributes=True)


class ProcessAnnotationCreate(BaseModel):
    page_key: str = Field(default="dragonzap-operating-model", min_length=1, max_length=64)
    section_key: str = Field(min_length=1, max_length=96)
    kind: Literal["comment", "drawing"]
    anchor_x: float | None = Field(default=None, ge=0, le=1)
    anchor_y: float | None = Field(default=None, ge=0, le=1)
    content: str | None = Field(default=None, max_length=4000)
    drawing_data: dict[str, Any] | None = None
    parent_id: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_annotation(self):
        if self.kind == "comment" and not (self.content or "").strip():
            raise ValueError("Комментарий не может быть пустым")
        if self.kind == "drawing" and not self.drawing_data:
            raise ValueError("Рисунок не может быть пустым")
        if self.kind == "drawing" and self.parent_id is not None:
            raise ValueError("Рисунок не может быть ответом на комментарий")
        return self


class ProcessAnnotationUpdate(BaseModel):
    content: str | None = Field(default=None, max_length=4000)
    is_resolved: bool | None = None


class ProcessAnnotationOut(BaseModel):
    id: int
    page_key: str
    section_key: str
    kind: Literal["comment", "drawing"]
    anchor_x: float | None = None
    anchor_y: float | None = None
    content: str | None = None
    drawing_data: dict[str, Any] | None = None
    parent_id: int | None = None
    created_by_id: int
    created_by: ProcessAnnotationAuthor
    is_resolved: bool
    resolved_by: ProcessAnnotationAuthor | None = None
    resolved_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
