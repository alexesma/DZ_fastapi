from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import Provider, ProviderPriceListConfig, ProviderPricelistReview
from dz_fastapi.services import pricelist_review_queue


@pytest.mark.asyncio
async def test_failed_background_review_returns_to_pending(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    provider = created_providers[0]
    review_file = tmp_path / "failed-price.xlsx"
    review_file.write_bytes(b"broken price")
    review = ProviderPricelistReview(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
        source_filename=review_file.name,
        file_path=str(review_file),
        file_extension="xlsx",
        file_sha256="f" * 64,
        status="queued",
        reasons=["Тестовая аномалия"],
        metrics={},
        examples=[],
        decision_reason="Подтверждено администратором",
        decided_at=now_moscow(),
    )
    test_session.add(review)
    await test_session.commit()

    async def fail_processing(**kwargs):
        raise RuntimeError("Файл повреждён")

    monkeypatch.setattr(
        pricelist_review_queue,
        "pricelist_review_file_path",
        lambda item: str(review_file),
    )
    monkeypatch.setattr(
        pricelist_review_queue,
        "process_provider_pricelist",
        fail_processing,
    )

    processed_id = (
        await pricelist_review_queue.process_next_provider_pricelist_review(
            test_session
        )
    )

    assert processed_id == review.id
    await test_session.refresh(review)
    assert review.status == "pending"
    assert review.processing_error == "Файл повреждён"
    assert review.decision_reason is None
    assert review.decided_at is None


@pytest.mark.asyncio
async def test_cleanup_completed_review_files_keeps_active_and_recent(
    test_session: AsyncSession,
    created_providers: list[Provider],
    created_pricelist_config: ProviderPriceListConfig,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    provider = created_providers[0]
    now = now_moscow()
    review_specs = (
        ("superseded", now - timedelta(days=2), now - timedelta(days=2), False),
        ("approved", now - timedelta(days=40), now - timedelta(days=40), False),
        ("rejected", now - timedelta(days=2), now - timedelta(days=2), True),
        ("pending", now - timedelta(days=40), None, True),
    )
    files = []
    reviews = []
    for index, (status, created_at, decided_at, _kept) in enumerate(review_specs):
        review_file = tmp_path / f"review-{index}.xlsx"
        review_file.write_bytes(b"price")
        files.append(review_file)
        reviews.append(
            ProviderPricelistReview(
                provider_id=provider.id,
                provider_config_id=created_pricelist_config.id,
                source_filename=review_file.name,
                file_path=str(review_file),
                file_extension="xlsx",
                file_sha256=str(index) * 64,
                status=status,
                reasons=[],
                metrics={},
                examples=[],
                created_at=created_at,
                decided_at=decided_at,
            )
        )
    test_session.add_all(reviews)
    await test_session.commit()
    monkeypatch.setattr(
        pricelist_review_queue,
        "PRICELIST_REVIEW_ROOT",
        str(tmp_path),
    )

    deleted = await pricelist_review_queue.cleanup_completed_pricelist_review_files(
        test_session,
        retention_days=30,
    )

    assert deleted == 2
    assert [path.exists() for path in files] == [False, False, True, True]
