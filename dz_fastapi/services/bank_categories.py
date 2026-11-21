"""Статьи не торговых банковских операций: правила, ручная разноска, аналитика.

Налоги, зарплата, аренда, сайт и т.п. не относятся к клиентам и поставщикам,
но их нужно видеть и анализировать. Каждая такая операция получает статью
(``BankTransaction.category``), а контрагенту можно «запомнить» статью — тогда
прошлые и будущие операции с ним попадут туда автоматически.
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import extract, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.finance import (
    BankCategory,
    BankCategoryRule,
    BankTransaction,
    BankTxnStatus,
)

KINDS = ("expense", "income", "transfer")
RULE_TYPES = ("inn", "name", "purpose")
DEPOSIT_CODES = ("deposit_open", "deposit_return", "deposit_interest")

# code, name, kind, color, is_system, sort — то же, что кладёт миграция.
DEFAULT_CATEGORIES = [
    ("tax", "Налоги и сборы", "expense", "#c0392b", True, 10),
    ("salary", "Зарплата", "expense", "#2a78d6", True, 20),
    ("rent", "Аренда", "expense", "#8e5ea2", False, 30),
    ("website", "Сайт и хостинг", "expense", "#16a085", False, 40),
    ("advertising", "Реклама", "expense", "#e67e22", False, 50),
    ("transport", "Доставка и транспорт", "expense", "#b7950b", False, 60),
    ("software", "Софт и сервисы", "expense", "#5d6d7e", False, 70),
    ("office", "Офис и хозрасходы", "expense", "#7d6608", False, 80),
    ("bank_fee", "Банковская комиссия", "expense", "#7f8c8d", True, 90),
    ("other_expense", "Прочие расходы", "expense", "#95a5a6", True, 900),
    ("bank_interest", "Проценты банка", "income", "#27ae60", True, 100),
    ("deposit_interest", "Депозит: проценты", "income", "#1e8449", True, 110),
    ("other_income", "Прочие доходы", "income", "#58d68d", True, 910),
    ("deposit_open", "Депозит: открытие", "transfer", "#aab7b8", True, 200),
    ("deposit_return", "Депозит: возврат", "transfer", "#aab7b8", True, 210),
]
_PALETTE = [
    "#2a78d6",
    "#e67e22",
    "#16a085",
    "#8e5ea2",
    "#c0392b",
    "#b7950b",
    "#5d6d7e",
    "#d35498",
]


async def ensure_default_categories(session: AsyncSession) -> None:
    """Пустая таблица статей (чистая база, тесты) — заполняем по умолчанию."""
    existing = await session.scalar(select(func.count(BankCategory.id)))
    if existing:
        return
    for code, name, kind, color, system, sort in DEFAULT_CATEGORIES:
        session.add(
            BankCategory(
                code=code,
                name=name,
                kind=kind,
                color=color,
                is_system=system,
                is_active=True,
                sort_order=sort,
            )
        )
    await session.flush()


@dataclass
class CategoryContext:
    labels: dict[str, str] = field(default_factory=dict)
    kinds: dict[str, str] = field(default_factory=dict)
    colors: dict[str, str] = field(default_factory=dict)
    rules: list[tuple[str, str, str]] = field(default_factory=list)

    def label(self, code: Optional[str]) -> str:
        from dz_fastapi.services.bank_reconciliation import NON_TRADE_LABELS

        if not code:
            return ""
        return self.labels.get(code) or NON_TRADE_LABELS.get(code) or code


async def load_context(session: AsyncSession) -> CategoryContext:
    categories = (await session.execute(select(BankCategory))).scalars().all()
    rules = (
        (
            await session.execute(
                select(BankCategoryRule).order_by(BankCategoryRule.id)
            )
        )
        .scalars()
        .all()
    )
    return CategoryContext(
        labels={c.code: c.name for c in categories},
        kinds={c.code: c.kind for c in categories},
        colors={c.code: c.color for c in categories if c.color},
        rules=[(r.match_type, r.pattern, r.category_code) for r in rules],
    )


def rule_pattern(match_type: str, txn: BankTransaction) -> Optional[str]:
    """Что запоминаем у операции для правила данного типа."""
    from dz_fastapi.services.bank_reconciliation import (
        normalize_inn,
        normalize_name,
    )

    if match_type == "inn":
        return normalize_inn(txn.counterparty_inn) or None
    if match_type == "name":
        return normalize_name(txn.counterparty_name) or None
    return None


def match_rule(ctx: CategoryContext, txn: BankTransaction) -> Optional[str]:
    """Статья по правилам пользователя: ИНН → название → текст назначения."""
    from dz_fastapi.services.bank_reconciliation import (
        normalize_inn,
        normalize_name,
    )

    inn = normalize_inn(txn.counterparty_inn)
    name = normalize_name(txn.counterparty_name)
    purpose = (txn.purpose or "").casefold()
    for wanted in RULE_TYPES:
        for match_type, pattern, code in ctx.rules:
            if match_type != wanted:
                continue
            if wanted == "inn" and inn and inn == pattern:
                return code
            if wanted == "name" and name and name == pattern:
                return code
            if wanted == "purpose" and pattern and pattern in purpose:
                return code
    return None


def apply_category(
    txn: BankTransaction,
    code: str,
    ctx: CategoryContext,
    *,
    deal_ref: Optional[str] = None,
) -> str:
    txn.status = BankTxnStatus.IGNORED
    txn.category = code
    txn.deal_ref = deal_ref
    txn.customer_payment_id = None
    txn.supplier_payment_id = None
    note = f"Не торговая операция: {ctx.label(code)}"
    if deal_ref:
        note += f" № {deal_ref}"
    txn.match_note = note
    return note


async def refresh_statement_counts(
    session: AsyncSession, statement_ids: set[int]
) -> None:
    from dz_fastapi.services.bank_reconciliation import _refresh_matched_count

    for statement_id in statement_ids:
        await _refresh_matched_count(session, statement_id)


# ── Ручная разноска на статью ──────────────────────────────────────────────────


async def categorize_transaction(
    session: AsyncSession,
    txn: BankTransaction,
    code: Optional[str],
    *,
    remember: Optional[str] = None,
    apply_to_past: bool = True,
) -> dict:
    """Назначает статью операции (``code=None`` — снять статью).

    ``remember`` — ``inn`` / ``name``: создаёт правило, и тогда остальные
    операции этого контрагента (при ``apply_to_past``) тоже переносятся на
    статью. Депозитные операции правилами не перебиваются.
    """
    if txn.status == BankTxnStatus.MATCHED:
        # Явное действие пользователя: платёж клиента/поставщика снимается.
        from dz_fastapi.services.bank_reconciliation import _unpost

        await _unpost(session, txn)
    await ensure_default_categories(session)
    ctx = await load_context(session)
    touched = {txn.statement_id}

    if code is None:
        txn.status = BankTxnStatus.UNMATCHED
        txn.category = None
        txn.deal_ref = None
        txn.match_note = None
        await session.flush()
        await refresh_statement_counts(session, touched)
        await session.commit()
        return {"also_updated": 0, "rule_created": False}

    if code not in ctx.labels:
        raise ValueError("Такой статьи нет")
    apply_category(
        txn,
        code,
        ctx,
        deal_ref=txn.deal_ref if code in DEPOSIT_CODES else None,
    )

    also = 0
    rule_created = False
    if remember:
        if remember not in ("inn", "name"):
            raise ValueError("Запомнить можно по ИНН или по названию")
        pattern = rule_pattern(remember, txn)
        if not pattern:
            raise ValueError(
                "У операции нет ИНН"
                if remember == "inn"
                else "У операции нет названия контрагента"
            )
        rule_created = await upsert_rule(session, remember, pattern, code)
        if apply_to_past:
            also, other_statements = await _apply_rule_to_past(
                session, txn, remember, pattern, code, ctx
            )
            touched |= other_statements
    await session.flush()
    await refresh_statement_counts(session, touched)
    await session.commit()
    return {"also_updated": also, "rule_created": rule_created}


async def upsert_rule(
    session: AsyncSession, match_type: str, pattern: str, code: str
) -> bool:
    """Создаёт правило либо переназначает статью у такого же. True — новое."""
    existing = (
        (
            await session.execute(
                select(BankCategoryRule).where(
                    BankCategoryRule.match_type == match_type,
                    BankCategoryRule.pattern == pattern,
                )
            )
        )
        .scalars()
        .first()
    )
    if existing:
        existing.category_code = code
        await session.flush()
        return False
    session.add(
        BankCategoryRule(
            category_code=code, match_type=match_type, pattern=pattern
        )
    )
    await session.flush()
    return True


async def _apply_rule_to_past(
    session: AsyncSession,
    source: BankTransaction,
    match_type: str,
    pattern: str,
    code: str,
    ctx: CategoryContext,
) -> tuple[int, set[int]]:
    from dz_fastapi.services.bank_reconciliation import (
        normalize_inn,
        normalize_name,
    )

    q = select(BankTransaction).where(
        BankTransaction.id != source.id,
        BankTransaction.status.in_(
            [BankTxnStatus.UNMATCHED, BankTxnStatus.IGNORED]
        ),
    )
    rows = (await session.execute(q)).scalars().all()
    count = 0
    statements: set[int] = set()
    for row in rows:
        if row.category in DEPOSIT_CODES or row.category == code:
            continue
        if match_type == "inn":
            same = normalize_inn(row.counterparty_inn) == pattern
        else:
            same = normalize_name(row.counterparty_name) == pattern
        if not same:
            continue
        apply_category(row, code, ctx)
        statements.add(row.statement_id)
        count += 1
    return count, statements


async def reapply_rules(session: AsyncSession) -> dict:
    """Прогоняет правила по всем нераспознанным операциям."""
    await ensure_default_categories(session)
    ctx = await load_context(session)
    rows = (
        (
            await session.execute(
                select(BankTransaction).where(
                    BankTransaction.status == BankTxnStatus.UNMATCHED
                )
            )
        )
        .scalars()
        .all()
    )
    updated = 0
    statements: set[int] = set()
    for row in rows:
        code = match_rule(ctx, row)
        if code:
            apply_category(row, code, ctx)
            statements.add(row.statement_id)
            updated += 1
    await session.flush()
    await refresh_statement_counts(session, statements)
    await session.commit()
    return {"updated": updated}


# ── Справочник статей ──────────────────────────────────────────────────────────


def make_code(name: str, taken: set[str]) -> str:
    base = "".join(
        ch if ch.isascii() and ch.isalnum() else "_" for ch in name.lower()
    )
    base = "_".join(part for part in base.split("_") if part)[:20]
    base = f"c_{base}" if base else "c"
    code = base
    n = 2
    while code in taken:
        code = f"{base}_{n}"
        n += 1
    return code


async def create_category(
    session: AsyncSession,
    *,
    name: str,
    kind: str = "expense",
    color: Optional[str] = None,
) -> BankCategory:
    await ensure_default_categories(session)
    name = name.strip()
    if not name:
        raise ValueError("Название обязательно")
    if kind not in KINDS:
        raise ValueError("Тип статьи: расход, доход или перемещение")
    rows = (await session.execute(select(BankCategory))).scalars().all()
    if any(r.name.casefold() == name.casefold() for r in rows):
        raise ValueError("Статья с таким названием уже есть")
    category = BankCategory(
        code=make_code(name, {r.code for r in rows}),
        name=name,
        kind=kind,
        color=color or _PALETTE[len(rows) % len(_PALETTE)],
        is_system=False,
        is_active=True,
        sort_order=max([r.sort_order for r in rows if r.sort_order < 800] + [0])
        + 10,
    )
    session.add(category)
    await session.commit()
    await session.refresh(category)
    return category


async def delete_category(
    session: AsyncSession, category: BankCategory
) -> None:
    if category.is_system:
        raise ValueError("Системную статью удалить нельзя")
    used = await session.scalar(
        select(func.count(BankTransaction.id)).where(
            BankTransaction.category == category.code
        )
    )
    if used:
        raise ValueError(
            f"В статье {used} операций: перенесите их или скройте статью"
        )
    rules = (
        (
            await session.execute(
                select(BankCategoryRule).where(
                    BankCategoryRule.category_code == category.code
                )
            )
        )
        .scalars()
        .all()
    )
    for rule in rules:
        await session.delete(rule)
    await session.delete(category)
    await session.commit()


# ── Аналитика ──────────────────────────────────────────────────────────────────


def _month_range(start: date, end: date) -> list[str]:
    months = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            year, month = year + 1, 1
    return months


def _signed(kind: str, direction: str, amount: Decimal) -> Decimal:
    """Расход: списание плюс, возврат минус. Доход и перемещения — наоборот."""
    outgoing = str(direction) == "outgoing"
    if kind == "expense":
        return amount if outgoing else -amount
    return -amount if outgoing else amount


async def _category_rows(
    session: AsyncSession, date_from: date, date_to: date
) -> list[tuple]:
    ym = (
        extract("year", BankTransaction.value_date) * 100
        + extract("month", BankTransaction.value_date)
    ).label("ym")
    result = await session.execute(
        select(
            BankTransaction.category,
            BankTransaction.direction,
            ym,
            func.sum(BankTransaction.amount),
            func.count(BankTransaction.id),
        )
        .where(
            BankTransaction.value_date >= date_from,
            BankTransaction.value_date <= date_to,
            BankTransaction.status == BankTxnStatus.IGNORED,
            BankTransaction.category.is_not(None),
        )
        .group_by(BankTransaction.category, BankTransaction.direction, ym)
    )
    return list(result.all())


def default_period(today: date) -> tuple[date, date]:
    """Последние 6 месяцев, считая текущий."""
    year, month = today.year, today.month - 5
    while month < 1:
        month += 12
        year -= 1
    return date(year, month, 1), today


async def build_category_analytics(
    session: AsyncSession,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> dict:
    """Расходы и доходы по статьям: итог, динамика по месяцам, к прошлому периоду."""
    await ensure_default_categories(session)
    if date_to is None and date_from is None:
        date_from, date_to = default_period(date.today())
    date_to = date_to or date.today()
    date_from = date_from or date(date_to.year, date_to.month, 1)
    if date_from > date_to:
        date_from, date_to = date_to, date_from

    ctx = await load_context(session)
    categories = (
        (
            await session.execute(
                select(BankCategory).order_by(
                    BankCategory.sort_order, BankCategory.id
                )
            )
        )
        .scalars()
        .all()
    )
    meta = {c.code: c for c in categories}

    months = _month_range(date_from, date_to)
    span = (date_to - date_from).days + 1
    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=span - 1)

    def collect(rows):
        data: dict[str, dict] = {}
        for code, direction, ym, total, count in rows:
            kind = (
                meta[code].kind
                if code in meta
                else ("expense" if str(direction) == "outgoing" else "income")
            )
            item = data.setdefault(
                code,
                {
                    "total": Decimal("0"),
                    "count": 0,
                    "by_month": {},
                    "kind": kind,
                },
            )
            value = _signed(kind, direction, Decimal(str(total or 0)))
            item["total"] += value
            item["count"] += int(count or 0)
            key = f"{int(ym) // 100:04d}-{int(ym) % 100:02d}"
            item["by_month"][key] = (
                item["by_month"].get(key, Decimal("0")) + value
            )
        return data

    current = collect(await _category_rows(session, date_from, date_to))
    previous = collect(await _category_rows(session, prev_from, prev_to))

    totals = {
        "expense": Decimal("0"),
        "income": Decimal("0"),
        "transfer": Decimal("0"),
    }
    for item in current.values():
        totals[item["kind"]] += item["total"]

    out = []
    for code in sorted(
        set(current) | set(previous),
        key=lambda c: (meta[c].sort_order if c in meta else 999, c),
    ):
        item = current.get(code)
        prev_total = previous.get(code, {}).get("total", Decimal("0"))
        total = item["total"] if item else Decimal("0")
        kind = (item or previous[code])["kind"]
        base = totals.get(kind) or Decimal("0")
        change = None
        if prev_total:
            change = ((total - prev_total) / abs(prev_total) * 100).quantize(
                Decimal("0.1")
            )
        out.append(
            {
                "code": code,
                "name": ctx.label(code),
                "kind": kind,
                "color": ctx.colors.get(code),
                "total": total,
                "count": item["count"] if item else 0,
                "share_percent": (
                    (total / base * 100).quantize(Decimal("0.1"))
                    if base and kind != "transfer"
                    else None
                ),
                "prev_total": prev_total,
                "change_percent": change,
                "by_month": [
                    (
                        item["by_month"].get(m, Decimal("0"))
                        if item
                        else Decimal("0")
                    )
                    for m in months
                ],
            }
        )

    # Контекст: что ещё не разнесено и каковы торговые обороты.
    def _by_direction(status):
        return (
            select(
                BankTransaction.direction,
                func.count(BankTransaction.id),
                func.coalesce(func.sum(BankTransaction.amount), 0),
            )
            .where(
                BankTransaction.value_date >= date_from,
                BankTransaction.value_date <= date_to,
                BankTransaction.status == status,
            )
            .group_by(BankTransaction.direction)
        )

    unmatched = {
        str(d): (int(c), Decimal(str(s)))
        for d, c, s in (
            await session.execute(_by_direction(BankTxnStatus.UNMATCHED))
        ).all()
    }
    trade = {
        str(d): Decimal(str(s))
        for d, _c, s in (
            await session.execute(_by_direction(BankTxnStatus.MATCHED))
        ).all()
    }
    zero = (0, Decimal("0"))
    return {
        "date_from": date_from,
        "date_to": date_to,
        "months": months,
        "categories": out,
        "totals": {
            "expense_total": totals["expense"],
            "income_total": totals["income"],
            "net": totals["income"] - totals["expense"],
            "transfer_total": totals["transfer"],
            "trade_income": trade.get("incoming", Decimal("0")),
            "trade_expense": trade.get("outgoing", Decimal("0")),
            "unmatched_count": sum(c for c, _ in unmatched.values()),
            "unmatched_outgoing": unmatched.get("outgoing", zero)[1],
            "unmatched_incoming": unmatched.get("incoming", zero)[1],
        },
    }


async def list_category_operations(
    session: AsyncSession,
    *,
    category: Optional[str] = None,
    uncategorized: bool = False,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 300,
) -> list[BankTransaction]:
    q = select(BankTransaction).order_by(
        BankTransaction.value_date.desc(), BankTransaction.id.desc()
    )
    if uncategorized:
        q = q.where(BankTransaction.status == BankTxnStatus.UNMATCHED)
    elif category:
        q = q.where(BankTransaction.category == category)
    else:
        q = q.where(BankTransaction.category.is_not(None))
    if date_from:
        q = q.where(BankTransaction.value_date >= date_from)
    if date_to:
        q = q.where(BankTransaction.value_date <= date_to)
    return list((await session.execute(q.limit(limit))).scalars().all())
