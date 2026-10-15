"""Durable queue for manual generation; the HTTP process only enqueues and polls."""

import asyncio
import logging
from datetime import timedelta
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import raiseload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import crud_customer, crud_customer_pricelist_config
from dz_fastapi.models.partner import Customer, CustomerPriceList, CustomerPriceListConfig
from dz_fastapi.models.pricelist_job import CustomerPricelistJob
from dz_fastapi.schemas.partner import CustomerPriceListCreate
from dz_fastapi.services.process import (
    CUSTOMER_PRICELIST_ARTIFACT_ROOT,
    process_customer_pricelist,
    send_pricelist,
)

logger = logging.getLogger("dz_fastapi")
ACTIVE_STATUSES = ("queued", "running")
WORKER_LOCK = 685543211


def artifact_exists(pricelist):
    if not pricelist or not pricelist.artifact_path:
        return False
    path = Path(pricelist.artifact_path).resolve()
    return CUSTOMER_PRICELIST_ARTIFACT_ROOT.resolve() in path.parents and path.is_file()


async def get_config(session, customer_id, config_id):
    config = await crud_customer_pricelist_config.get_by_id(
        customer_id=customer_id, config_id=config_id, session=session, load_relations=False
    )
    if config is None:
        raise HTTPException(404, "Конфигурация клиента не найдена")
    return config


async def enqueue_job(session, *, customer_id, config_id, action, user_id, pricelist_id=None):
    config = await get_config(session, customer_id, config_id)
    existing = await session.scalar(
        select(CustomerPricelistJob).where(
            CustomerPricelistJob.config_id == config_id,
            CustomerPricelistJob.status.in_(ACTIVE_STATUSES),
        )
    )
    if existing:
        return existing
    if action == "send":
        fallback = await session.scalar(
            select(Customer.email_outgoing_price).where(Customer.id == customer_id)
        )
        if not any(str(email or "").strip() for email in (config.emails or [fallback])):
            raise HTTPException(400, "Не указаны получатели прайса. Заполните их в конфигурации")
    if pricelist_id is not None:
        if action != "send":
            raise HTTPException(400, "Для обновления прайса не нужен ID готового файла")
        pricelist = await session.scalar(
            select(CustomerPriceList)
            .options(raiseload("*"))
            .where(
                CustomerPriceList.id == pricelist_id,
                CustomerPriceList.customer_id == customer_id,
                CustomerPriceList.customer_config_id == config_id,
            )
        )
        if not artifact_exists(pricelist):
            raise HTTPException(404, "Готовый файл не найден. Обновите прайс")
        if pricelist.generation_status not in ("draft", "sent", "queued"):
            raise HTTPException(409, "Этот прайс нельзя отправить. Сформируйте новую версию")
    job = CustomerPricelistJob(
        customer_id=customer_id,
        config_id=config_id,
        action=action,
        requested_by_id=user_id,
        pricelist_id=pricelist_id,
        use_saved_file=pricelist_id is not None,
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        # Two browser tabs can submit together: the partial UNIQUE index is authoritative.
        await session.rollback()
        existing = await session.scalar(
            select(CustomerPricelistJob).where(
                CustomerPricelistJob.config_id == config_id,
                CustomerPricelistJob.status.in_(ACTIVE_STATUSES),
            )
        )
        if existing:
            return existing
        raise
    await session.refresh(job)
    return job


def job_response(job):
    if job is None:
        return None
    return {
        field: getattr(job, field)
        for field in (
            "id",
            "config_id",
            "action",
            "status",
            "stage",
            "progress",
            "pricelist_id",
            "delivery_status",
            "error",
            "created_at",
            "started_at",
            "updated_at",
            "finished_at",
        )
    }


async def get_job_state(session, customer_id, config_id):
    config = await get_config(session, customer_id, config_id)
    job = await session.scalar(
        select(CustomerPricelistJob)
        .where(
            CustomerPricelistJob.config_id == config_id,
        )
        .order_by(CustomerPricelistJob.id.desc())
        .limit(1)
    )
    latest = await session.scalar(
        select(CustomerPriceList)
        .options(raiseload("*"))
        .where(
            CustomerPriceList.customer_id == customer_id,
            CustomerPriceList.customer_config_id == config_id,
            CustomerPriceList.generated_at.is_not(None),
            CustomerPriceList.generation_status.in_(("draft", "sent", "queued", "send_failed")),
            CustomerPriceList.artifact_path.is_not(None),
        )
        .order_by(CustomerPriceList.generated_at.desc(), CustomerPriceList.id.desc())
        .limit(1)
    )
    latest_info = None
    if artifact_exists(latest):
        latest_info = {
            "id": latest.id,
            "generated_at": latest.generated_at,
            "filename": latest.artifact_filename,
            "positions_count": latest.positions_count,
            "generation_status": latest.generation_status,
        }
    return {
        "job": job_response(job),
        "latest": latest_info,
        "generation_in_progress": bool(
            config.generation_lock_token
            and config.generation_locked_at
            and config.generation_locked_at > now_moscow() - timedelta(hours=2)
        ),
    }


async def _send_saved(session, job, token, progress):
    config_id, customer_id = job.config_id, job.customer_id
    claimed = await session.scalar(
        update(CustomerPriceListConfig)
        .where(
            CustomerPriceListConfig.id == config_id,
            CustomerPriceListConfig.generation_lock_token.is_(None),
        )
        .values(generation_lock_token=token, generation_locked_at=now_moscow())
        .returning(CustomerPriceListConfig.id)
    )
    await session.commit()
    if claimed is None:
        raise HTTPException(409, "Прайс занят другой сборкой. Дождитесь её окончания")
    try:
        config = await get_config(session, customer_id, config_id)
        customer = await crud_customer.get_by_id(customer_id, session, load_relations=False)
        pricelist = await session.scalar(
            select(CustomerPriceList)
            .options(raiseload("*"))
            .where(
                CustomerPriceList.id == job.pricelist_id,
                CustomerPriceList.customer_id == customer_id,
                CustomerPriceList.customer_config_id == config_id,
            )
        )
        if not artifact_exists(pricelist):
            raise HTTPException(404, "Файл больше недоступен. Обновите прайс")
        if pricelist.generation_status not in ("draft", "sent", "queued"):
            raise HTTPException(409, "Версия прайса больше недоступна для отправки")
        quality = (pricelist.generation_summary or {}).get("quality_control") or {}
        if quality.get("enabled") and quality.get("status") == "failed":
            raise HTTPException(409, "Отправка заблокирована контролем качества прайса")
        recipients = config.emails or (
            [customer.email_outgoing_price] if customer.email_outgoing_price else []
        )
        if not recipients:
            raise HTTPException(400, "Не указаны получатели прайса")
        await progress("Отправка готового файла", 95)
        try:
            delivery = await send_pricelist(
                session=session,
                customer=customer,
                config=config,
                to_emails=recipients,
                df_excel=None,
                attachment_bytes=await asyncio.to_thread(Path(pricelist.artifact_path).read_bytes),
                attachment_filename=pricelist.artifact_filename,
                attachment_local_path=pricelist.artifact_path,
                attachment_content_type=pricelist.artifact_content_type,
                customer_pricelist_id=pricelist.id,
                subject=f"Прайс лист {pricelist.date}",
                body="Добрый день, высылаем Вам наш прайс-лист",
            )
        except Exception:
            pricelist.generation_status = "send_failed"
            pricelist.send_error = (
                "Ошибка отправки; перед повтором проверьте очередь и почтовый сервис"
            )
            await session.commit()
            raise
        pricelist.generation_status = delivery
        if delivery == "sent":
            pricelist.sent_at = now_moscow()
            pricelist.published_at = pricelist.sent_at
            config.last_sent_at = pricelist.sent_at
        else:
            pricelist.sent_at = None
        pricelist.send_error = None
        await session.commit()
        return pricelist.id, delivery
    finally:
        await session.rollback()
        await session.execute(
            update(CustomerPriceListConfig)
            .where(
                CustomerPriceListConfig.id == config_id,
                CustomerPriceListConfig.generation_lock_token == token,
            )
            .values(generation_lock_token=None, generation_locked_at=None)
        )
        await session.commit()


async def run_pricelist_jobs(session_factory):
    """One manual build across scheduler processes. Never replay an interrupted send."""
    engine = session_factory.kw["bind"]
    async with engine.connect() as connection:
        acquired = await connection.scalar(
            text("SELECT pg_try_advisory_lock(:key)"), {"key": WORKER_LOCK}
        )
        await connection.commit()
        if not acquired:
            return
        try:
            # Owning this session-level lock proves no older worker is alive.
            async with session_factory() as session:
                interrupted = (
                    await session.scalars(
                        select(CustomerPricelistJob).where(
                            CustomerPricelistJob.status == "running",
                        )
                    )
                ).all()
                for old in interrupted:
                    old.status = "failed"
                    old.stage = "Обработка прервана"
                    old.error = (
                        "Планировщик остановился во время обработки. Перед повторной отправкой "
                        "проверьте готовые прайсы, очередь relay и почтовый сервис."
                    )
                    old.finished_at = old.updated_at = now_moscow()
                    await session.execute(
                        update(CustomerPriceListConfig)
                        .where(
                            CustomerPriceListConfig.id == old.config_id,
                            CustomerPriceListConfig.generation_lock_token
                            == f"pricelist-job-{old.id}",
                        )
                        .values(generation_lock_token=None, generation_locked_at=None)
                    )
                await session.commit()
                job = await session.scalar(
                    select(CustomerPricelistJob)
                    .where(
                        CustomerPricelistJob.status == "queued",
                    )
                    .order_by(CustomerPricelistJob.id)
                    .limit(1)
                )
                if job is None:
                    return
                job.status, job.stage, job.progress = "running", "Подготовка", 2
                job.started_at = job.updated_at = now_moscow()
                await session.commit()
                job_id, config_id = job.id, job.config_id

                async def progress(stage, percent):
                    # Separate transaction: progress must not commit partially built price rows.
                    async with session_factory() as progress_session:
                        await progress_session.execute(
                            update(CustomerPricelistJob)
                            .where(
                                CustomerPricelistJob.id == job_id,
                                CustomerPricelistJob.status == "running",
                            )
                            .values(
                                stage=stage[:255],
                                progress=min(99, percent),
                                updated_at=now_moscow(),
                            )
                        )
                        await progress_session.commit()

                try:
                    token = f"pricelist-job-{job_id}"
                    if job.use_saved_file and job.action == "send":
                        result_id, delivery_status = await _send_saved(
                            session, job, token, progress
                        )
                    else:
                        customer = await crud_customer.get_by_id(
                            job.customer_id, session, load_relations=False
                        )
                        result = await process_customer_pricelist(
                            customer=customer,
                            request=CustomerPriceListCreate(
                                customer_id=job.customer_id, config_id=config_id, items=[]
                            ),
                            session=session,
                            include_autoparts_response=False,
                            delivery_mode="send" if job.action == "send" else "draft",
                            progress_callback=progress,
                            lock_token=token,
                        )
                        result_id, delivery_status = result.id, result.generation_status
                    await session.execute(
                        update(CustomerPricelistJob)
                        .where(
                            CustomerPricelistJob.id == job_id,
                        )
                        .values(
                            status="succeeded",
                            progress=100,
                            stage="Готово",
                            pricelist_id=result_id,
                            delivery_status=delivery_status,
                            finished_at=now_moscow(),
                            updated_at=now_moscow(),
                        )
                    )
                    await session.commit()
                except Exception as error:
                    await session.rollback()
                    logger.exception("Manual pricelist job failed: job_id=%s", job_id)
                    detail = (
                        str(error.detail)
                        if isinstance(error, HTTPException)
                        else "Ошибка обработки. Подробности в журнале планировщика. "
                        "Перед повторной отправкой проверьте очередь и почтовый сервис."
                    )
                    await session.execute(
                        update(CustomerPricelistJob)
                        .where(
                            CustomerPricelistJob.id == job_id,
                        )
                        .values(
                            status="failed",
                            stage="Не удалось завершить",
                            error=detail[:2000],
                            updated_at=now_moscow(),
                            finished_at=now_moscow(),
                        )
                    )
                    await session.commit()
        finally:
            await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": WORKER_LOCK})
            await connection.commit()
