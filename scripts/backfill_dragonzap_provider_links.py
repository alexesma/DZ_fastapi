import argparse
import asyncio
import re
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.db import get_async_session
from dz_fastapi.crud.partner import crud_provider
from dz_fastapi.models.partner import Provider


SOURCE_SYSTEM = 'DRAGONZAP'


@dataclass(slots=True)
class MergeProposal:
    source_provider_id: int
    source_provider_name: str
    target_provider_id: int
    target_provider_name: str
    matched_aliases: list[str]


def _normalize_alias(value: object) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    text = re.sub(r'\(https?://[^)]*\)', ' ', text, flags=re.I)
    text = re.sub(r'\b(ооо|ооо\.|ип)\b', ' ', text, flags=re.I)
    text = (
        text.replace('«', ' ')
        .replace('»', ' ')
        .replace('"', ' ')
        .replace("'", ' ')
    )
    text = re.sub(r'[^0-9a-zа-яё]+', ' ', text, flags=re.I)
    text = re.sub(r'\s+', ' ', text).strip().casefold()
    return text


def _provider_aliases(provider: Provider) -> set[str]:
    aliases: set[str] = set()
    normalized_name = _normalize_alias(provider.name)
    if normalized_name:
        aliases.add(normalized_name)
    for abbreviation in (provider.abbreviations or []):
        normalized_abbr = _normalize_alias(abbreviation.abbreviation)
        if normalized_abbr:
            aliases.add(normalized_abbr)
    for reference in (provider.external_references or []):
        if str(reference.source_system or '').strip().upper() != SOURCE_SYSTEM:
            continue
        normalized_ref_name = _normalize_alias(
            reference.external_supplier_name
        )
        if normalized_ref_name:
            aliases.add(normalized_ref_name)
    return aliases


async def _load_providers(session: AsyncSession) -> list[Provider]:
    rows = await session.execute(
        select(Provider)
        .options(
            selectinload(Provider.external_references),
            selectinload(Provider.abbreviations),
        )
        .order_by(Provider.id.asc())
    )
    return list(rows.scalars().all())


def _build_merge_proposals(providers: list[Provider]) -> list[MergeProposal]:
    manual_providers = [provider for provider in providers if not provider.is_virtual]
    virtual_providers = []
    for provider in providers:
        if not provider.is_virtual:
            continue
        has_dragonzap_ref = any(
            str(reference.source_system or '').strip().upper() == SOURCE_SYSTEM
            for reference in (provider.external_references or [])
        )
        if has_dragonzap_ref:
            virtual_providers.append(provider)

    alias_to_target_ids: dict[str, set[int]] = {}
    target_by_id = {provider.id: provider for provider in manual_providers}
    for provider in manual_providers:
        for alias in _provider_aliases(provider):
            alias_to_target_ids.setdefault(alias, set()).add(int(provider.id))

    proposals: list[MergeProposal] = []
    for source in virtual_providers:
        source_aliases = _provider_aliases(source)
        candidate_ids: set[int] = set()
        matched_aliases: set[str] = set()
        for alias in source_aliases:
            alias_candidates = alias_to_target_ids.get(alias, set())
            if alias_candidates:
                matched_aliases.add(alias)
                candidate_ids.update(alias_candidates)
        if len(candidate_ids) != 1:
            continue
        target_id = next(iter(candidate_ids))
        target = target_by_id.get(target_id)
        if target is None or int(target.id) == int(source.id):
            continue
        proposals.append(
            MergeProposal(
                source_provider_id=int(source.id),
                source_provider_name=str(source.name or ''),
                target_provider_id=int(target.id),
                target_provider_name=str(target.name or ''),
                matched_aliases=sorted(matched_aliases),
            )
        )
    return proposals


async def _merge_pair(
    session: AsyncSession,
    *,
    source_provider_id: int,
    target_provider_id: int,
) -> bool:
    return await crud_provider.merge_providers(
        source_provider_id=source_provider_id,
        target_provider_id=target_provider_id,
        session=session,
    )


async def _run(
    *,
    apply_changes: bool,
    source_provider_id: int | None,
    target_provider_id: int | None,
) -> None:
    Session = get_async_session()
    async with Session() as session:
        if source_provider_id is not None or target_provider_id is not None:
            if not source_provider_id or not target_provider_id:
                raise ValueError(
                    'Для ручного merge нужны оба параметра: '
                    '--source-provider-id и --target-provider-id'
                )
            print(
                f'Manual merge: source={source_provider_id} '
                f'-> target={target_provider_id}'
            )
            if apply_changes:
                merged = await _merge_pair(
                    session,
                    source_provider_id=source_provider_id,
                    target_provider_id=target_provider_id,
                )
                print(f'Applied: merged={merged}')
            else:
                print('Dry-run only. Add --apply to execute merge.')
            return

        providers = await _load_providers(session)
        proposals = _build_merge_proposals(providers)
        if not proposals:
            print('No unambiguous DRAGONZAP provider merge proposals found.')
            return

        print(f'Found {len(proposals)} merge proposal(s):')
        for proposal in proposals:
            print(
                f'  source #{proposal.source_provider_id} '
                f'"{proposal.source_provider_name}" -> '
                f'target #{proposal.target_provider_id} '
                f'"{proposal.target_provider_name}" '
                f'(aliases: {", ".join(proposal.matched_aliases) or "-"})'
            )

        if not apply_changes:
            print('Dry-run only. Add --apply to perform merges.')
            return

        applied = 0
        for proposal in proposals:
            merged = await _merge_pair(
                session,
                source_provider_id=proposal.source_provider_id,
                target_provider_id=proposal.target_provider_id,
            )
            if merged:
                applied += 1
                print(
                    f'Applied merge: source #{proposal.source_provider_id} '
                    f'-> target #{proposal.target_provider_id}'
                )
        print(f'Completed. Applied merges: {applied}')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Backfill and merge DRAGONZAP provider duplicates'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='perform merges; without this flag the script runs in dry-run mode',
    )
    parser.add_argument(
        '--source-provider-id',
        type=int,
        default=None,
        help='explicit source provider id to merge from',
    )
    parser.add_argument(
        '--target-provider-id',
        type=int,
        default=None,
        help='explicit target provider id to merge into',
    )
    args = parser.parse_args()
    asyncio.run(
        _run(
            apply_changes=bool(args.apply),
            source_provider_id=args.source_provider_id,
            target_provider_id=args.target_provider_id,
        )
    )


if __name__ == '__main__':
    main()
