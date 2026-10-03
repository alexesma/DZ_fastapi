"""Удаление позиции номенклатуры.

Позицию можно удалить, только пока на неё не ссылаются документы и склад:
заказы, приёмки, остатки, движения, партии, резервы и т.п. Всё, что
«принадлежит» самой позиции (фото, кроссы, цены из прайсов, история цен,
привязки к категориям), удаляется вместе с ней.

Таблицы, не перечисленные в ``OWNED_TABLES``, считаются блокирующими: если в
проекте появится новая ссылка на позицию, удаление по умолчанию будет
запрещено, а не потихоньку сотрёт чужие данные.
"""

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.db import Base
from dz_fastapi.models.autopart import AutoPart

OWNED_TABLES = {
    "photo": "Фото",
    "autopart_category_association": "Привязки к категориям",
    "autopart_storage_association": "Привязки к местам хранения",
    "autopart_applicability_association": "Применимость",
    "autopart_certificate_association": "Привязки к сертификатам",
    "autopart_honest_sign_association": "Категории Честного знака",
    "autopartcross": "Кроссы",
    "autopartinvalidcross": "Неверные кроссы",
    "autopartsubstitution": "Замены",
    "autopartturnoversummary": "Сводка оборота",
    "autopartpricehistory": "История цен",
    "autopartrestockdecision": "Решения о пополнении",
    "autopurchaseexcludeditem": "Исключения автозакупки",
    "autopurchaseforecastsnapshot": "Прогнозы автозакупки",
    "autopurchasetopitem": "Топ автозакупки",
    "pricelistautopartassociation": "Строки прайсов поставщиков",
    "customerpricelistautopartassociation": "Строки прайсов клиентов",
    "customerpricelistoverride": "Ручные цены клиентских прайсов",
    "customerpricelistpublicationrule": "Правила публикации прайсов",
    "customerpricelistpublicationruletarget": "Цели правил публикации",
    "customerpricelistpublishedalias": "Опубликованные псевдонимы",
    "customerpricelistexportrow": "Строки выгрузок прайсов",
    "providerinventoryrolerule": "Правила ролей поставщиков",
    "adhoclabelprintevent": "Печать этикеток",
}

BLOCKER_LABELS = {
    "customerorderitem": "Позиции заказов клиентов",
    "supplierorderitem": "Позиции заказов поставщикам",
    "orderitem": "Позиции заказов",
    "stockorderitem": "Позиции складских заказов",
    "supplierreceiptitem": "Приёмки от поставщиков",
    "stockbylocation": "Остатки на складе",
    "stocklot": "Партии на складе",
    "stockmovement": "Движения по складу",
    "stockreserve": "Резервы",
    "stockdocumentitem": "Складские документы",
    "shipmentdocumentitem": "Отгрузки",
    "inventoryitem": "Инвентаризация",
    "returnitem": "Возвраты",
    "reclamationitem": "Рекламации",
    "paymentinvoiceitem": "Счета на оплату",
    "autopurchaserunitem": "Запуски автозакупки",
    "pricecontrolrecommendation": "Рекомендации контроля цен",
    "productmarkingcode": "Коды маркировки",
    "productmarkingcodemovement": "Движения кодов маркировки",
}


@dataclass
class UsageRow:
    table: str
    label: str
    count: int


def _autopart_references():
    """Колонки всех таблиц, ссылающиеся на autopart.id (кроме самой autopart)."""
    target = Base.metadata.tables["autopart"]
    for table in Base.metadata.sorted_tables:
        if table is target:
            continue
        for fk in table.foreign_keys:
            if fk.column.table is target:
                yield table, fk.parent, fk.ondelete


async def _count(session: AsyncSession, column, autopart_id: int) -> int:
    return (
        await session.execute(
            select(func.count()).select_from(column.table).where(column == autopart_id)
        )
    ).scalar_one()


async def autopart_delete_check(
    session: AsyncSession, autopart_id: int
) -> tuple[list[UsageRow], list[UsageRow]]:
    """Возвращает (блокирующие ссылки, данные, которые удалятся вместе с позицией)."""
    blockers: dict[str, UsageRow] = {}
    owned: dict[str, UsageRow] = {}
    for table, column, _ondelete in _autopart_references():
        # Ссылки «кросс на эту позицию» из чужих кроссов не мешают: связь
        # просто обнуляется, а сам кросс остаётся по артикулу.
        if table.name in ("autopartcross", "autopartinvalidcross") and column.name in (
            "cross_autopart_id",
            "invalid_autopart_id",
        ):
            continue
        count = await _count(session, column, autopart_id)
        if not count:
            continue
        if table.name in OWNED_TABLES:
            bucket, label = owned, OWNED_TABLES[table.name]
        else:
            bucket, label = blockers, BLOCKER_LABELS.get(table.name, table.name)
        row = bucket.setdefault(table.name, UsageRow(table.name, label, 0))
        row.count += count
    return list(blockers.values()), list(owned.values())


async def delete_autopart(session: AsyncSession, autopart: AutoPart) -> list[str]:
    """Удаляет позицию и её собственные данные. Блокировки проверяет вызывающий.

    Возвращает URL фото, файлы которых нужно удалить с диска после commit.
    """
    autopart_id = autopart.id
    # Чистим явно, не полагаясь на ON DELETE в конкретной базе: собственные
    # строки удаляем, а «мягкие» ссылки (кросс на эту позицию и т.п.) обнуляем.
    for table, column, ondelete in _autopart_references():
        if table.name not in OWNED_TABLES or table.name == "photo":
            continue
        if ondelete == "SET NULL":
            await session.execute(
                update(table).where(column == autopart_id).values({column.name: None})
            )
        else:
            await session.execute(delete(table).where(column == autopart_id))
    photo_table = Base.metadata.tables["photo"]
    owned_photos = photo_table.c.autopart_id == autopart_id
    url_rows = await session.execute(select(photo_table.c.url).where(owned_photos))
    photo_urls = list(url_rows.scalars())
    await session.execute(delete(photo_table).where(owned_photos))
    part_table = AutoPart.__table__
    await session.execute(delete(part_table).where(part_table.c.id == autopart_id))
    return photo_urls


def describe_blockers(blockers: list[UsageRow]) -> Optional[str]:
    if not blockers:
        return None
    return "; ".join(f"{row.label}: {row.count}" for row in blockers)
