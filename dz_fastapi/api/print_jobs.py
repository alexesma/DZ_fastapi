"""Printer routing and a durable queue for the Windows relay."""

import re
from datetime import timedelta
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from dz_fastapi.api.deps import get_current_user, require_admin, require_email_relay
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.printing import PrintAssignment, PrintDevice, PrintJob
from dz_fastapi.models.user import User

router = APIRouter(prefix="/print-jobs", tags=["print-jobs"])
PRINT_KINDS = ("label", "a4")


class PrinterRegistration(BaseModel):
    machine_id: str = Field(min_length=1, max_length=128)
    printers: list[str] = Field(max_length=100)

    @field_validator("printers")
    @classmethod
    def normalize_printers(cls, values):
        names = [str(value).strip() for value in values]
        if any(not name or len(name) > 255 for name in names):
            raise ValueError("Некорректное имя принтера")
        return list(dict.fromkeys(names))


class PrintAssignmentInput(BaseModel):
    user_id: int
    device_id: int
    kind: Literal["label", "a4"]
    is_default: bool = True


class PrintJobInput(BaseModel):
    request_key: str = Field(min_length=8, max_length=64)
    kind: Literal["label", "a4"]
    title: str = Field(min_length=1, max_length=160)
    html: str = Field(min_length=20, max_length=2_000_000)
    device_id: int | None = None
    width_mm: float | None = Field(default=None, ge=20, le=210)
    height_mm: float | None = Field(default=None, ge=15, le=297)

    @field_validator("html")
    @classmethod
    def reject_external_content(cls, value):
        lowered = value.lower()
        for forbidden in (
            "file://", "<iframe", "<object", "<embed", "<script src",
            "@import", "url(http", "url(//",
        ):
            if forbidden in lowered:
                raise ValueError("Печатная форма содержит внешний ресурс")
        if re.search(
            r'''\b(?:src|href)\s*=\s*["']?\s*(?:https?:|//|file:)''',
            value, re.IGNORECASE,
        ) or re.search(
            r'''url\(\s*["']?\s*(?:https?:|//|file:)''',
            value, re.IGNORECASE,
        ):
            raise ValueError("Печатная форма содержит внешний ресурс")
        return value


class PrintErrorInput(BaseModel):
    error: str = Field(min_length=1, max_length=2000)


def _device_out(device: PrintDevice) -> dict:
    seen = device.last_seen_at
    online = bool(
        device.active and seen
        and now_moscow() - seen < timedelta(minutes=3)
    )
    return {
        "id": device.id, "machine_id": device.machine_id,
        "system_name": device.system_name,
        "display_name": device.display_name,
        "active": device.active, "online": online,
        "last_seen_at": seen,
    }


def _job_out(job: PrintJob) -> dict:
    return {
        "id": job.id, "request_key": job.request_key,
        "title": job.title, "kind": job.kind, "status": job.status,
        "device_id": job.device_id,
        "printer_name": job.device.display_name if job.device else None,
        "user_id": job.user_id, "attempts": job.attempts,
        "error": job.error, "created_at": job.created_at,
        "claimed_at": job.claimed_at, "printed_at": job.printed_at,
    }


@router.get("/me")
async def my_printers(
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    assignments = (await session.execute(
        select(PrintAssignment)
        .options(joinedload(PrintAssignment.device))
        .where(PrintAssignment.user_id == user.id)
        .order_by(PrintAssignment.kind, PrintAssignment.is_default.desc())
    )).scalars().all()
    return {
        kind: [
            {**_device_out(row.device), "is_default": row.is_default}
            for row in assignments if row.kind == kind and row.device.active
        ] for kind in PRINT_KINDS
    }


@router.post("")
async def create_print_job(
    data: PrintJobInput,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    existing = (await session.execute(
        select(PrintJob).options(joinedload(PrintJob.device))
        .where(PrintJob.request_key == data.request_key)
    )).scalar_one_or_none()
    if existing:
        if existing.user_id != user.id:
            raise HTTPException(status_code=409, detail="Ключ задания уже используется")
        return _job_out(existing)

    stmt = select(PrintAssignment).options(joinedload(PrintAssignment.device)).where(
        PrintAssignment.user_id == user.id,
        PrintAssignment.kind == data.kind,
    )
    if data.device_id:
        stmt = stmt.where(PrintAssignment.device_id == data.device_id)
    else:
        stmt = stmt.where(PrintAssignment.is_default.is_(True))
    assignment = (await session.execute(stmt)).scalars().first()
    if assignment is None or not assignment.device.active:
        raise HTTPException(
            status_code=409, detail="Для сотрудника не назначен принтер этого типа"
        )
    job = PrintJob(
        request_key=data.request_key,
        user_id=user.id,
        device_id=assignment.device_id,
        kind=data.kind,
        title=data.title,
        html=data.html,
        width_mm=data.width_mm,
        height_mm=data.height_mm,
        status="pending",
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = (await session.execute(
            select(PrintJob).options(joinedload(PrintJob.device))
            .where(PrintJob.request_key == data.request_key)
        )).scalar_one_or_none()
        if existing and existing.user_id == user.id:
            return _job_out(existing)
        raise HTTPException(status_code=409, detail="Ключ задания уже используется")
    saved = (await session.execute(
        select(PrintJob).options(joinedload(PrintJob.device))
        .where(PrintJob.id == job.id)
    )).scalar_one()
    return _job_out(saved)


@router.get("")
async def list_print_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    rows = (await session.execute(
        select(PrintJob).options(joinedload(PrintJob.device))
        .where(PrintJob.user_id == user.id)
        .order_by(PrintJob.id.desc()).limit(limit)
    )).scalars().all()
    return [_job_out(row) for row in rows]


@router.get("/{job_id}")
async def get_print_job(
    job_id: int,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    job = (await session.execute(
        select(PrintJob).options(joinedload(PrintJob.device))
        .where(PrintJob.id == job_id, PrintJob.user_id == user.id)
    )).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Задание печати не найдено")
    return _job_out(job)


@router.get("/admin/devices")
async def admin_devices(
    _admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    rows = (await session.execute(
        select(PrintDevice).order_by(PrintDevice.machine_id, PrintDevice.system_name)
    )).scalars().all()
    return [_device_out(row) for row in rows]


@router.get("/admin/assignments")
async def admin_assignments(
    _admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    rows = (await session.execute(
        select(PrintAssignment).order_by(
            PrintAssignment.user_id, PrintAssignment.kind, PrintAssignment.id,
        )
    )).scalars().all()
    return [{
        "id": row.id, "user_id": row.user_id,
        "device_id": row.device_id, "kind": row.kind,
        "is_default": row.is_default,
    } for row in rows]


@router.put("/admin/assignments")
async def save_assignment(
    data: PrintAssignmentInput,
    _admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    user = await session.get(User, data.user_id)
    device = await session.get(PrintDevice, data.device_id)
    if user is None or device is None or not device.active:
        raise HTTPException(status_code=404, detail="Сотрудник или принтер не найден")
    row = (await session.execute(
        select(PrintAssignment).where(
            PrintAssignment.user_id == data.user_id,
            PrintAssignment.device_id == data.device_id,
            PrintAssignment.kind == data.kind,
        )
    )).scalar_one_or_none()
    if data.is_default:
        await session.execute(
            update(PrintAssignment).where(
                PrintAssignment.user_id == data.user_id,
                PrintAssignment.kind == data.kind,
            ).values(is_default=False)
        )
    if row is None:
        row = PrintAssignment(
            user_id=data.user_id, device_id=data.device_id, kind=data.kind,
        )
        session.add(row)
    row.is_default = data.is_default
    await session.commit()
    return {
        "id": row.id, "user_id": row.user_id,
        "device_id": row.device_id, "kind": row.kind,
        "is_default": row.is_default,
    }


@router.delete("/admin/assignments/{assignment_id}")
async def delete_assignment(
    assignment_id: int,
    _admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    row = await session.get(PrintAssignment, assignment_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Назначение не найдено")
    was_default = row.is_default
    user_id, kind = row.user_id, row.kind
    await session.delete(row)
    await session.flush()
    if was_default:
        replacement = (await session.execute(
            select(PrintAssignment)
            .join(PrintDevice, PrintAssignment.device_id == PrintDevice.id)
            .where(
                PrintAssignment.user_id == user_id,
                PrintAssignment.kind == kind,
                PrintDevice.active.is_(True),
            )
            .order_by(PrintAssignment.id).limit(1)
        )).scalar_one_or_none()
        if replacement:
            replacement.is_default = True
    await session.commit()
    return {"deleted": assignment_id}


@router.get("/admin/jobs")
async def admin_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    _admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    rows = (await session.execute(
        select(PrintJob).options(joinedload(PrintJob.device))
        .order_by(PrintJob.id.desc()).limit(limit)
    )).scalars().all()
    return [_job_out(row) for row in rows]


@router.post("/admin/jobs/{job_id}/retry")
async def retry_job(
    job_id: int,
    _admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    job = await session.get(PrintJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Задание не найдено")
    if job.status not in {"error", "claimed"}:
        raise HTTPException(status_code=409, detail="Повтор здесь не требуется")
    job.status = "pending"
    job.claimed_by = None
    job.claimed_at = None
    job.error = None
    await session.commit()
    return _job_out(job)


@router.post("/relay/register")
async def register_printers(
    data: PrinterRegistration,
    _token: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    rows = (await session.execute(
        select(PrintDevice).where(PrintDevice.machine_id == data.machine_id)
    )).scalars().all()
    existing = {row.system_name: row for row in rows}
    now = now_moscow()
    for row in rows:
        row.active = row.system_name in data.printers
    for name in data.printers:
        row = existing.get(name)
        if row is None:
            row = PrintDevice(
                machine_id=data.machine_id, system_name=name,
                display_name=name,
            )
            session.add(row)
        row.active = True
        row.last_seen_at = now
    await session.commit()
    return {"machine_id": data.machine_id, "printers": data.printers}


@router.post("/relay/claim")
async def claim_print_jobs(
    machine_id: str = Query(..., min_length=1, max_length=128),
    limit: int = Query(default=10, ge=1, le=20),
    _token: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    rows = (await session.execute(
        select(PrintJob).options(joinedload(PrintJob.device))
        .join(PrintDevice, PrintDevice.id == PrintJob.device_id)
        .where(
            PrintDevice.machine_id == machine_id,
            PrintDevice.active.is_(True),
            PrintJob.status == "pending",
        )
        .order_by(PrintJob.id).limit(limit)
        .with_for_update(of=PrintJob, skip_locked=True)
    )).scalars().all()
    result = []
    for job in rows:
        job.status = "claimed"
        job.claimed_by = machine_id
        job.claimed_at = now_moscow()
        job.attempts += 1
        result.append({
            "id": job.id, "kind": job.kind, "title": job.title,
            "html": job.html, "printer_name": job.device.system_name,
            "width_mm": job.width_mm, "height_mm": job.height_mm,
        })
    await session.commit()
    return result


async def _claimed_job(session, job_id, machine_id):
    job = await session.get(PrintJob, job_id)
    if job is None or job.status != "claimed" or job.claimed_by != machine_id:
        raise HTTPException(status_code=409, detail="Задание не закреплено за этим компьютером")
    return job


@router.post("/relay/{job_id}/printed")
async def mark_printed(
    job_id: int,
    machine_id: str = Query(..., min_length=1, max_length=128),
    _token: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    job = await session.get(PrintJob, job_id)
    if job and job.status == "printed" and job.claimed_by == machine_id:
        return {"id": job.id, "status": job.status}
    job = await _claimed_job(session, job_id, machine_id)
    job.status = "printed"
    job.printed_at = now_moscow()
    job.html = ""
    await session.commit()
    return {"id": job.id, "status": job.status}


@router.post("/relay/{job_id}/error")
async def mark_print_error(
    job_id: int,
    data: PrintErrorInput,
    machine_id: str = Query(..., min_length=1, max_length=128),
    _token: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    job = await session.get(PrintJob, job_id)
    if job and job.status == "error" and job.claimed_by == machine_id:
        return {"id": job.id, "status": job.status}
    job = await _claimed_job(session, job_id, machine_id)
    job.status = "error"
    job.error = data.error
    await session.commit()
    return {"id": job.id, "status": job.status}
