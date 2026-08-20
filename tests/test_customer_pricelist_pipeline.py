from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartCross
from dz_fastapi.models.partner import (
    Customer,
    CustomerPriceList,
    CustomerPriceListConfig,
    CustomerPriceListExportRow,
    CustomerPriceListSource,
    PriceList,
    PriceListAutoPartAssociation,
    Provider,
    ProviderPriceListConfig,
)
from dz_fastapi.schemas.partner import CustomerPriceListCreate
from dz_fastapi.services import process as process_service
from dz_fastapi.services.process import (
    CUSTOMER_PRICELIST_PIPELINE_DEFAULT,
    _apply_product_labels,
    _apply_source_filters,
    _collapse_output_records,
    _transform_dragonzap_records,
    customer_pricelist_pipeline,
)


def test_transform_only_restores_dragonzap_removed_by_brand_filter():
    source = SimpleNamespace(
        brand_filters={"type": "exclude", "brands": [54]},
        position_filters={},
        min_price=None,
        max_price=None,
        min_quantity=None,
        max_quantity=None,
        additional_filters={},
    )
    source_df = pd.DataFrame(
        [
            {
                "autopart_id": 1,
                "brand_id": 54,
                "brand": "DRAGONZAP",
                "oem_number": "DZ1064001701",
                "price": 450,
                "quantity": 4,
            },
            {
                "autopart_id": 2,
                "brand_id": 74,
                "brand": "GEELY",
                "oem_number": "1064001701",
                "price": 600,
                "quantity": 2,
            },
        ]
    )

    result = _apply_source_filters(
        source_df,
        source,
        dragonzap_mode="transform_only",
    )

    dragonzap = result[result["brand"] == "DRAGONZAP"].iloc[0]
    assert len(result) == 2
    assert bool(dragonzap["__transform_only"]) is True


def test_dragonzap_transform_retains_physical_item_and_labels_output():
    transformed = _transform_dragonzap_records(
        [
            {
                "autopart_id": 197705,
                "brand": "DRAGONZAP",
                "oem_number": "DZ1064001701",
                "name": "Подшипник",
                "price": 450,
                "quantity": 4,
                "__transform_only": True,
            }
        ],
        keep_source=False,
    )
    labelled = _apply_product_labels(
        transformed,
        label_original=True,
        label_transformed=True,
        original_label=">>Оригинал<<",
        transformed_label=">>Неоригинал<<",
    )

    assert labelled == [
        {
            "autopart_id": 197705,
            "brand": "GEELY",
            "oem_number": "1064001701",
            "name": ">>Неоригинал<< Подшипник",
            "price": 450,
            "quantity": 4,
            "__transform_only": True,
            "__row_type": "zzap_transform",
            "__source_oem": "DZ1064001701",
            "__source_brand": "DRAGONZAP",
            "__origin_type": "dragonzap_transform",
        }
    ]


def test_duplicate_policy_uses_price_then_stock_then_real_original():
    common = {
        "brand": "GEELY",
        "oem_number": "1064001701",
        "price": 500,
        "quantity": 7,
    }
    result = _collapse_output_records(
        [
            {**common, "autopart_id": 3, "__origin_type": "dragonzap_transform"},
            {**common, "autopart_id": 2, "__origin_type": "original_source"},
            {**common, "autopart_id": 1, "price": 490, "quantity": 1},
        ]
    )

    assert len(result) == 1
    assert result[0]["autopart_id"] == 1

    tied = _collapse_output_records(
        [
            {**common, "autopart_id": 3, "__origin_type": "dragonzap_transform"},
            {**common, "autopart_id": 2, "__origin_type": "original_source"},
        ]
    )
    assert tied[0]["autopart_id"] == 2


def test_pipeline_order_is_complete_and_respects_required_dependencies():
    config = SimpleNamespace(
        additional_filters={
            "PIPELINE_ORDER": [
                "quality_control",
                "deduplication",
                "source_filters",
            ]
        }
    )

    result = customer_pricelist_pipeline(config)

    assert set(result) == set(CUSTOMER_PRICELIST_PIPELINE_DEFAULT)
    assert result.index("source_filters") < result.index("dragonzap_transform")
    assert result.index("dragonzap_transform") < result.index("deduplication")
    assert result[-1] == "quality_control"


@pytest.mark.asyncio
async def test_v2_pipeline_transforms_filtered_dragonzap_into_original_draft(
    test_session: AsyncSession,
    async_client: AsyncClient,
    created_customers: list[Customer],
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    customer = created_customers[0]
    dragonzap = Brand(name="DRAGONZAP")
    geely = Brand(name="GEELY")
    provider = Provider(
        name="Own DragonZap Pipeline",
        email_contact="pipeline@example.com",
        email_incoming_price="pipeline-price@example.com",
        description="Own stock",
        comment="",
        type_prices="Wholesale",
        is_own_price=True,
    )
    test_session.add_all([dragonzap, geely, provider])
    await test_session.flush()
    provider_config = ProviderPriceListConfig(
        provider_id=provider.id,
        start_row=1,
        oem_col=0,
        qty_col=1,
        price_col=2,
        name_price="OWN_DZ_PIPELINE",
        name_mail="OWN_DZ_PIPELINE",
    )
    physical = AutoPart(
        brand_id=dragonzap.id,
        oem_number="DZ1064001701",
        name="Подшипник передней ступицы",
    )
    cross = AutoPart(
        brand_id=dragonzap.id,
        oem_number="DZ1014003218",
        name="Подшипник передней ступицы",
    )
    test_session.add_all([provider_config, physical, cross])
    await test_session.flush()
    test_session.add(
        AutoPartCross(
            source_autopart_id=physical.id,
            cross_brand_id=dragonzap.id,
            cross_oem_number=cross.oem_number,
            cross_autopart_id=cross.id,
            is_bidirectional=True,
        )
    )
    pricelist = PriceList(
        date=date.today(),
        provider_id=provider.id,
        provider_config_id=provider_config.id,
        is_active=True,
    )
    test_session.add(pricelist)
    await test_session.flush()
    test_session.add(
        PriceListAutoPartAssociation(
            pricelist_id=pricelist.id,
            autopart_id=physical.id,
            quantity=4,
            price=450,
        )
    )
    config = CustomerPriceListConfig(
        customer_id=customer.id,
        name="ZZap independent profile",
        general_markup=1,
        own_price_list_markup=1,
        third_party_markup=1,
        emails=["price@example.com"],
        additional_filters={
            "PIPELINE_V2_ENABLED": True,
            "PROFILE_TEMPLATE": "zzap",
            "DZ_ORIGINAL_TRANSFORM_ENABLED": True,
            "DZ_TRANSFORM_INCLUDE_CROSSES": True,
            "DZ_TRANSFORM_KEEP_DRAGONZAP": False,
            "PUBLISH_CONFIRMED_DZ_CROSSES": False,
            "PRODUCT_LABELS_ENABLED": True,
            "LABEL_TRANSFORMED_ENABLED": True,
            "LABEL_TRANSFORMED_TEXT": ">>Неоригинал<<",
            "PRICE_CONTROL_ENABLED": False,
            "QUALITY_CONTROL_ENABLED": True,
            "REQUIRE_DRAFT_APPROVAL": True,
        },
    )
    test_session.add(config)
    await test_session.flush()
    test_session.add(
        CustomerPriceListSource(
            customer_config_id=config.id,
            provider_config_id=provider_config.id,
            enabled=True,
            markup=1,
            brand_filters={"type": "exclude", "brands": [dragonzap.id]},
            position_filters={},
            additional_filters={},
        )
    )
    await test_session.commit()
    monkeypatch.setattr(
        process_service,
        "CUSTOMER_PRICELIST_ARTIFACT_ROOT",
        tmp_path,
    )

    response = await process_service.process_customer_pricelist(
        customer=customer,
        request=CustomerPriceListCreate(
            customer_id=customer.id,
            config_id=config.id,
            items=[],
        ),
        session=test_session,
        include_autoparts_response=False,
        delivery_mode="draft",
    )
    rows = list(
        (
            await test_session.execute(
                select(CustomerPriceListExportRow)
                .where(CustomerPriceListExportRow.customer_pricelist_id == response.id)
                .order_by(CustomerPriceListExportRow.advertised_oem)
            )
        )
        .scalars()
        .all()
    )

    assert [(row.advertised_brand, row.advertised_oem) for row in rows] == [
        ("GEELY", "1014003218"),
        ("GEELY", "1064001701"),
    ]
    assert {row.source_autopart_id for row in rows} == {physical.id}
    assert all(row.advertised_name.startswith(">>Неоригинал<<") for row in rows)
    assert response.generation_status == "draft"
    generated = await test_session.get(CustomerPriceList, response.id)
    assert generated.generation_summary["quality_control"]["status"] == "passed"

    diagnostic_response = await async_client.get(
        f"/customers/{customer.id}/pricelist-configs/{config.id}"
        f"/drafts/{response.id}/diagnostics",
        params={"search": "DZ1064001701"},
    )
    assert diagnostic_response.status_code == 200, diagnostic_response.text
    diagnostic = diagnostic_response.json()
    assert diagnostic["items"][0]["status"] == "transformed"
    assert "GEELY 1064001701" in diagnostic["items"][0]["reason"]

    config.additional_filters = {
        **config.additional_filters,
        "PRICE_CONTROL_ENABLED": True,
        "PRICE_CONTROL_PROVIDER_CONFIG_IDS": [],
    }
    test_session.add(config)
    await test_session.commit()
    failed_response = await process_service.process_customer_pricelist(
        customer=customer,
        request=CustomerPriceListCreate(
            customer_id=customer.id,
            config_id=config.id,
            items=[],
        ),
        session=test_session,
        include_autoparts_response=False,
        delivery_mode="draft",
    )
    failed_draft = await test_session.get(CustomerPriceList, failed_response.id)
    assert failed_draft.generation_summary["quality_control"]["status"] == "failed"

    approve_response = await async_client.post(
        f"/customers/{customer.id}/pricelist-configs/{config.id}"
        f"/drafts/{failed_response.id}/approve"
    )
    assert approve_response.status_code == 409, approve_response.text
    assert "Контроль качества не пройден" in approve_response.json()["detail"]
