import asyncio
import logging
import os
from datetime import timedelta

from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.crud.partner import crud_provider, crud_provider_pricelist_config
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.partner import ProviderPricelistReview
from dz_fastapi.services.notifications import create_notification
from dz_fastapi.services.process import process_provider_pricelist

logger = logging.getLogger("dz_fastapi")

PRICELIST_REVIEW_ROOT = os.path.realpath(os.path.join("uploads", "pricelist_reviews"))
PRICELIST_REVIEW_PROCESSING_STALE_HOURS = 6


def pricelist_review_file_path(review: ProviderPricelistReview) -> str:
    file_path = os.path.realpath(review.file_path)
    if file_path != PRICELIST_REVIEW_ROOT and not file_path.startswith(
        PRICELIST_REVIEW_ROOT + os.sep
    ):
        raise HTTPException(
            status_code=400,
            detail="Некорректный путь файла проверки",
        )
    if not os.path.isfile(file_path):
        raise HTTPException(
            status_code=404,
            detail="Сохранённый файл проверки не найден",
        )
    return file_path


def read_pricelist_review_file(file_path: str) -> bytes:
    with open(file_path, "rb") as file_handle:
        return file_handle.read()


async def mark_pricelist_review_notifications_read(
    session: AsyncSession,
    review: ProviderPricelistReview,
) -> None:
    """Закрывает эту проверку и устаревшую очередь той же конфигурации."""
    decided_at = now_moscow()
    await session.execute(
        update(ProviderPricelistReview)
        .where(
            ProviderPricelistReview.provider_config_id == review.provider_config_id,
            ProviderPricelistReview.id < review.id,
            ProviderPricelistReview.status == "pending",
        )
        .values(
            status="superseded",
            decision_reason="Заменён более свежим прайсом этой конфигурации",
            decided_at=decided_at,
            processing_error=None,
        )
    )
    review_ids = list(
        (
            await session.execute(
                select(ProviderPricelistReview.id).where(
                    ProviderPricelistReview.provider_config_id == review.provider_config_id,
                    ProviderPricelistReview.id <= review.id,
                )
            )
        ).scalars()
    )
    if not review_ids:
        return
    links = [
        f"/providers/{review.provider_id}/edit?pricelist_review={review_id}"
        for review_id in review_ids
    ]
    await session.execute(
        update(AppNotification)
        .where(
            AppNotification.link.in_(links),
            AppNotification.read_at.is_(None),
        )
        .values(read_at=decided_at)
    )


async def _restore_stale_processing_reviews(session: AsyncSession) -> int:
    stale_before = now_moscow() - timedelta(hours=PRICELIST_REVIEW_PROCESSING_STALE_HOURS)
    result = await session.execute(
        update(ProviderPricelistReview)
        .where(
            ProviderPricelistReview.status == "processing",
            (
                ProviderPricelistReview.decided_at.is_(None)
                | (ProviderPricelistReview.decided_at < stale_before)
            ),
        )
        .values(
            status="pending",
            processing_error=(
                "Предыдущая фоновая публикация была прервана. " "Запустите публикацию повторно."
            ),
            decision_reason=None,
            decided_at=None,
            decided_by_user_id=None,
        )
    )
    restored = int(result.rowcount or 0)
    if restored:
        await session.commit()
        logger.warning(
            "Restored stale provider pricelist reviews: count=%s",
            restored,
        )
    return restored


async def process_next_provider_pricelist_review(
    session: AsyncSession,
) -> int | None:
    """Claims and publishes one admin-approved provider pricelist."""
    await _restore_stale_processing_reviews(session)
    review = (
        await session.execute(
            select(ProviderPricelistReview)
            .where(ProviderPricelistReview.status == "queued")
            .order_by(
                ProviderPricelistReview.created_at.asc(),
                ProviderPricelistReview.id.asc(),
            )
            .with_for_update(skip_locked=True)
            .limit(1)
        )
    ).scalar_one_or_none()
    if review is None:
        return None

    review_id = int(review.id)
    provider_id = int(review.provider_id)
    provider_config_id = int(review.provider_config_id)
    decided_by_user_id = review.decided_by_user_id
    review.status = "processing"
    review.processing_error = None
    session.add(review)
    await session.commit()

    try:
        file_path = pricelist_review_file_path(review)
        provider = await crud_provider.get_by_id(
            provider_id=provider_id,
            session=session,
        )
        provider_config = await crud_provider_pricelist_config.get_by_id(
            config_id=provider_config_id,
            session=session,
        )
        if provider is None or provider_config is None:
            raise RuntimeError("Поставщик или конфигурация прайса удалены")
        file_content = await asyncio.to_thread(
            read_pricelist_review_file,
            file_path,
        )
        pricelist, _ = await process_provider_pricelist(
            provider=provider,
            file_content=file_content,
            file_extension=review.file_extension,
            provider_list_conf=provider_config,
            use_stored_params=True,
            start_row=None,
            oem_col=None,
            brand_col=None,
            name_col=None,
            multiplicity_col=None,
            qty_col=None,
            price_col=None,
            session=session,
            return_stats=True,
            include_autoparts_response=False,
            enforce_anomaly_guard=False,
            source_filename=review.source_filename,
        )

        approved_review = await session.get(
            ProviderPricelistReview,
            review_id,
            populate_existing=True,
            with_for_update=True,
        )
        if approved_review is None:
            raise RuntimeError("Запись проверки прайса удалена во время обработки")
        approved_review.status = "approved"
        approved_review.published_pricelist_id = int(pricelist.id)
        approved_review.processing_error = None
        session.add(approved_review)
        await mark_pricelist_review_notifications_read(
            session,
            approved_review,
        )
        await session.commit()
        if decided_by_user_id is not None:
            try:
                await create_notification(
                    session,
                    user_id=int(decided_by_user_id),
                    title="Прайс опубликован",
                    message=(
                        f"{approved_review.source_filename}: фоновая публикация "
                        f"завершена, создан прайс #{pricelist.id}."
                    ),
                    level="success",
                    link=(
                        f"/providers/{provider_id}/edit?pricelist_review="
                        f"{review_id}"
                    ),
                    payload={
                        "notification_type": "provider_pricelist_review_result",
                        "review_id": review_id,
                        "published_pricelist_id": int(pricelist.id),
                    },
                )
            except Exception:
                await session.rollback()
                logger.exception(
                    "Failed to notify user about published pricelist review: "
                    "review_id=%s",
                    review_id,
                )
        logger.info(
            "Completed queued provider pricelist review: review_id=%s " "pricelist_id=%s",
            review_id,
            pricelist.id,
        )
        return review_id
    except Exception as exc:
        await session.rollback()
        failed_review = await session.get(
            ProviderPricelistReview,
            review_id,
            populate_existing=True,
            with_for_update=True,
        )
        if failed_review is not None:
            error_text = str(exc).strip() or type(exc).__name__
            failed_review.status = "pending"
            failed_review.processing_error = error_text[:4000]
            failed_review.decision_reason = None
            failed_review.decided_at = None
            failed_review.decided_by_user_id = None
            session.add(failed_review)
            await session.commit()
            if decided_by_user_id is not None:
                try:
                    await create_notification(
                        session,
                        user_id=int(decided_by_user_id),
                        title="Не удалось опубликовать прайс",
                        message=(
                            f"{failed_review.source_filename}: {error_text[:1000]}. "
                            "Проверьте ошибку и запустите публикацию повторно."
                        ),
                        level="error",
                        link=(
                            f"/providers/{provider_id}/edit?pricelist_review="
                            f"{review_id}"
                        ),
                        payload={
                            "notification_type": "provider_pricelist_review_result",
                            "review_id": review_id,
                        },
                    )
                except Exception:
                    await session.rollback()
                    logger.exception(
                        "Failed to notify user about failed pricelist review: "
                        "review_id=%s",
                        review_id,
                    )
        logger.exception(
            "Failed queued provider pricelist review: review_id=%s",
            review_id,
        )
        return review_id
