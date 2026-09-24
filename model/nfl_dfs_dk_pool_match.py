"""Join a live DraftKings pool observation to a saved slate upload.

## Why this is not a foreign key

Our `nfl_dfs_slate_players.dk_player_id` is DraftKings' **draftable** id
(44,191,122 on the week-3 NYG@LAR showdown). The live pool endpoint carries
DraftKings' **player** id (`pid`, 1,228,244) and no draftable id at all. The one
endpoint that bridges the two namespaces --
`api.draftkings.com/draftgroups/v1/draftgroups/{id}/draftables` -- returns 403
without a session. Measured 2026-09-23, not assumed.

So the join is by name and team, which this project has been bitten by before
(`AZ`/`ARI`, `Kenny`/`Kenneth Gainwell`). Two things make it survivable here
that do not hold in the general case: both sides are DraftKings' own spelling of
the same player in the same week, and there is an independent check available --
the salary. A slate and a pool that agree on team set, format and price are the
same slate; if they disagree on price they are not, and nothing is applied.

## The rules, in order of how much they matter

1.  **Ambiguity changes nothing.** A normalized name appearing twice on either
    side is dropped from the join rather than resolved by a tiebreak. Two
    players sharing a name is exactly the case where guessing is worst.
2.  **A pool must prove it is the same slate** before any of it is used:
    identical team set, identical format, and salary agreement on a
    supermajority of matched players.
3.  **Newer wins, and only newer.** An observation captured before the upload
    tells us nothing the upload did not already say.
4.  **Nothing here mutates the slate.** The result is an overlay the read layer
    applies; the stored row is what the workspace showed when a lineup was
    built and stays that way.
"""

from __future__ import annotations

from dataclasses import dataclass, field

VERSION = "nfl-dfs-dk-pool-match-v1"

# Below this share of matched players agreeing on salary, the pool is a
# different slate (or a different contest type for the same games) and none of
# it is applied. Salaries are frozen once DraftKings posts a draft group, so
# real agreement is total; the allowance covers a late-added player, not drift.
SALARY_AGREEMENT_FLOOR = 0.95
# ...and a handful of matched players is not evidence either way.
MIN_MATCHED_FOR_SALARY_CHECK = 10

# DraftKings tags we treat as "not playing". Same set the salary-file parser
# uses, kept deliberately separate from Doubtful and Questionable, which are
# judgement calls the optimizer owns rather than facts the parser can assert.
OUT_STATUSES = frozenset({"O", "OUT", "IR", "PUP", "SUSP", "NA"})


def normalize_name(name: str) -> str:
    """Mirror of `normalizeName` in `web/src/app/dfs/nfl/actions.ts`.

    It must be the same rule on both sides of the join, character for
    character, because the slate's stored `normalized_name` is what a live
    observation is looked up against. Note that DIGITS SURVIVE -- the field
    audit's normalizer strips them, which would turn the 49ers defense into
    "ers" and quietly lose one team every week. `test:nfl-dk-pool-match` pins
    the two implementations together.
    """
    import re
    import unicodedata

    decomposed = unicodedata.normalize("NFKD", str(name or ""))
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()
    stripped = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", stripped)
    return re.sub(r"[^a-z0-9]+", "", stripped)


@dataclass
class LivePlayerStatus:
    normalized_name: str
    name: str
    team: str | None
    status: str | None
    is_disabled: bool
    salary: int | None
    captured_at: object


@dataclass
class MatchResult:
    """What, if anything, a live pool says about a saved slate."""

    applied: bool
    reason: str
    version: str = VERSION
    draft_group_id: int | None = None
    captured_at: object = None
    # normalized name -> live status, only for unambiguous matches
    statuses: dict[str, LivePlayerStatus] = field(default_factory=dict)
    matched: int = 0
    salary_agreement: float | None = None
    ambiguous_names: list[str] = field(default_factory=list)
    unmatched_slate_players: int = 0


def _unique_by_name(rows, name_key, salary_key):
    """Index rows by normalized name, discarding every name that repeats."""
    index, duplicates = {}, set()
    for row in rows:
        key = row[name_key]
        if not key:
            continue
        if key in index:
            duplicates.add(key)
        index[key] = row
    for key in duplicates:
        index.pop(key, None)
    return index, sorted(duplicates)


def match_pool_to_slate(
    slate_players,
    slate_format: str,
    slate_teams,
    pool_players,
    pool_format: str,
    pool_teams,
    *,
    draft_group_id: int | None = None,
    captured_at=None,
    upload_captured_at=None,
) -> MatchResult:
    """Resolve a live pool against a saved slate, or refuse and say why.

    `slate_players` and `pool_players` are dicts carrying at least
    `normalized_name`, `salary`, and (pool only) `status`/`is_disabled`.
    """
    if pool_format != slate_format:
        return MatchResult(False, f"Pool is {pool_format}; this slate is {slate_format}.")
    if sorted(pool_teams or []) != sorted(slate_teams or []):
        return MatchResult(False, "Pool covers a different set of teams than this slate.")
    if captured_at is not None and upload_captured_at is not None and captured_at <= upload_captured_at:
        return MatchResult(False, "The live pool is no newer than the uploaded salary file.")

    slate_index, slate_dupes = _unique_by_name(slate_players, "normalized_name", "salary")
    pool_index, pool_dupes = _unique_by_name(pool_players, "normalized_name", "salary")
    shared = sorted(set(slate_index) & set(pool_index))
    if not shared:
        return MatchResult(False, "No player on this slate could be matched to the live pool.")

    agree = sum(
        1 for key in shared
        if slate_index[key].get("salary") is not None
        and slate_index[key].get("salary") == pool_index[key].get("salary")
    )
    agreement = agree / len(shared)
    if len(shared) >= MIN_MATCHED_FOR_SALARY_CHECK and agreement < SALARY_AGREEMENT_FLOOR:
        return MatchResult(
            False,
            f"Salaries disagree on {len(shared) - agree} of {len(shared)} matched players "
            "— this is a different slate, so none of it was applied.",
            draft_group_id=draft_group_id, matched=len(shared), salary_agreement=agreement,
        )

    statuses = {
        key: LivePlayerStatus(
            normalized_name=key,
            name=pool_index[key].get("name") or key,
            team=pool_index[key].get("team"),
            status=pool_index[key].get("status"),
            is_disabled=bool(pool_index[key].get("is_disabled")),
            salary=pool_index[key].get("salary"),
            captured_at=captured_at,
        )
        for key in shared
    }
    return MatchResult(
        True,
        f"Matched {len(shared)} players against DraftKings' live pool.",
        draft_group_id=draft_group_id,
        captured_at=captured_at,
        statuses=statuses,
        matched=len(shared),
        salary_agreement=agreement,
        ambiguous_names=sorted(set(slate_dupes) | set(pool_dupes)),
        unmatched_slate_players=len(slate_index) - len(shared),
    )


def is_out_status(status: str | None) -> bool:
    return (status or "").strip().upper() in OUT_STATUSES
