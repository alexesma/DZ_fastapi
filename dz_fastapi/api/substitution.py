import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import get_async_session
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand
from dz_fastapi.models.cross import AutoPartSubstitution

router = APIRouter(prefix="/substitutions", tags=['substitutions'])


class SubstitutionCreate(BaseModel):
    source_autopart_id: int
    substitution_brand_id: int
    substitution_oem_number: str
    priority: int = 1
    min_source_quantity: int = 4
    quantity_reduction: int = 1
    customer_config_id: int | None = None


class SubstitutionResponse(BaseModel):
    id: int
    source_autopart_id: int
    substitution_brand_id: int
    substitution_oem_number: str
    priority: int
    min_source_quantity: int
    quantity_reduction: int
    is_active: bool
    model_config = ConfigDict(from_attributes=True)


@router.post("/", response_model=SubstitutionResponse)
async def create_substitution(
    data: SubstitutionCreate,
    session: AsyncSession = Depends(get_async_session),
):
    """Создать подмену для прайс-листа"""

    substitution = AutoPartSubstitution(**data.model_dump())
    session.add(substitution)
    await session.commit()
    await session.refresh(substitution)

    return substitution


@router.post("/upload-from-1c")
async def upload_substitutions_from_1c(
    file: UploadFile = File(...),
    customer_config_id: int | None = None,
    session: AsyncSession = Depends(get_async_session),
):
    """
    Загрузка подмен из Excel файла 1С.

    Формат файла:
    |source_brand|source_oem|sub_brand|sub_oem|priority|min_qty|reduction|
    |DRAGONZAP   |12345     |TOYOTA   |90915-YZZD3|1   |4      |1        |
    |DRAGONZAP   |12345     |GEELY    |1234567    |2   |4      |2        |
    """

    df = pd.read_excel(file.file)

    required_columns = ['source_brand', 'source_oem', 'sub_brand', 'sub_oem']
    if not all(col in df.columns for col in required_columns):
        raise HTTPException(
            status_code=400,
            detail=f'File must contain columns: {required_columns}'
        )

    added = 0
    skipped = 0
    errors = []

    for idx, row in df.iterrows():
        try:
            # Найти source autopart
            result = await session.execute(
                select(AutoPart.id)
                .join(Brand)
                .where(
                    Brand.name == row['source_brand'],
                    AutoPart.oem_number == str(row['source_oem']).upper()
                )
            )
            source_id = result.scalar_one_or_none()

            if not source_id:
                errors.append(
                    f"Row {idx}: Source autopart not found: "
                    f"{row['source_brand']} {row['source_oem']}"
                )
                skipped += 1
                continue

            # Найти substitution brand_id
            result = await session.execute(
                select(Brand.id).where(Brand.name == row['sub_brand'])
            )
            sub_brand_id = result.scalar_one_or_none()

            if not sub_brand_id:
                errors.append(
                    f"Row {idx}: Substitution brand not found: "
                    f"{row['sub_brand']}"
                )
                skipped += 1
                continue

            # Создать подмену
            substitution = AutoPartSubstitution(
                source_autopart_id=source_id,
                substitution_brand_id=sub_brand_id,
                substitution_oem_number=str(row['sub_oem']).upper(),
                priority=int(row.get('priority', 1)),
                min_source_quantity=int(row.get('min_qty', 4)),
                quantity_reduction=int(row.get('reduction', 1)),
                customer_config_id=customer_config_id,
            )

            session.add(substitution)
            added += 1

        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")
            skipped += 1

    await session.commit()

    return {
        'added': added,
        'skipped': skipped,
        'errors': errors[:10]  # Первые 10 ошибок
    }


@router.get("/{autopart_id}", response_model=list[SubstitutionResponse])
async def get_substitutions(
    autopart_id: int,
    session: AsyncSession = Depends(get_async_session),
):
    """Получить все подмены для детали"""

    result = await session.execute(
        select(AutoPartSubstitution)
        .where(AutoPartSubstitution.source_autopart_id == autopart_id)
        .order_by(AutoPartSubstitution.priority)
    )

    return result.scalars().all()
