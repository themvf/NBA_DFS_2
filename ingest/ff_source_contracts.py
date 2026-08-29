"""Immutable, as-of-safe source contracts for Fantasy Football roster-aware V2.

This module owns the machine-readable contract for football inputs and the
single persistence path for ``ff_source_snapshots``. It deliberately contains
no projection logic: a snapshot can be stored without being model eligible,
and missing inputs produce an explicit fallback tier instead of numeric zeroes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from psycopg2.extras import Json


SNAPSHOT_STATUSES = frozenset({"success", "partial", "failed"})
FALLBACK_CONFIDENCE = {"A": 1.0, "B": 0.8, "C": 0.6}
CORE_SOURCE_KEYS = frozenset({
    "weekly-rosters",
    "weekly-stats",
    "play-by-play",
    "schedule",
})


class AsOfCutoffError(ValueError):
    """Raised when a source was not available by a simulated decision cutoff."""


@dataclass(frozen=True)
class SourceContract:
    key: str
    source: str
    dataset: str
    license: str
    required_fields: tuple[str, ...]
    cadence: str
    historical_availability: str
    fallback_behavior: str
    source_url: str


SOURCE_CONTRACTS: dict[str, SourceContract] = {
    "weekly-rosters": SourceContract(
        key="weekly-rosters",
        source="nflverse",
        dataset="weekly-roster",
        license="CC BY 4.0; attribute nflverse-data",
        required_fields=("season", "week", "team", "gsis_id", "position", "status"),
        cadence="Daily in season; freeze the response used by each model run",
        historical_availability="Week-level rosters from 2002 onward",
        fallback_behavior="Use effective-dated prior roster plus Sleeper enrichment; tier B and lower confidence",
        source_url="https://github.com/nflverse/nflverse-data/releases/tag/weekly_rosters",
    ),
    "weekly-stats": SourceContract(
        key="weekly-stats",
        source="nflverse",
        dataset="weekly-player-stats",
        license="CC BY 4.0; attribute nflverse-data",
        required_fields=(
            "player_id", "season", "week", "season_type", "team", "position",
            "attempts", "carries", "targets", "receptions", "passing_tds",
            "rushing_tds", "receiving_tds",
        ),
        cadence="After games and upstream corrections; snapshot every consumed release",
        historical_availability="Regular-season player statistics from 1999 onward",
        fallback_behavior="Aggregate eligible play-by-play; tier B. If both are absent, use prior/league rates at tier C",
        source_url="https://github.com/nflverse/nflverse-data/releases/tag/stats_player",
    ),
    "play-by-play": SourceContract(
        key="play-by-play",
        source="nflverse",
        dataset="play-by-play",
        license="CC BY 4.0; attribute nflverse-data",
        required_fields=(
            "game_id", "season", "week", "posteam", "defteam", "play_type",
            "pass_attempt", "rush_attempt", "sack", "qb_kneel", "qb_scramble",
            "complete_pass", "touchdown", "yardline_100", "score_differential",
        ),
        cadence="Nightly/incremental during the season; corrections create a new response hash",
        historical_availability="Play-by-play from 1999 onward",
        fallback_behavior="Use weekly team/player facts without red-zone or game-script detail; tier B, or league priors at tier C",
        source_url="https://github.com/nflverse/nflverse-data/releases/tag/pbp",
    ),
    "participation": SourceContract(
        key="participation",
        source="nflverse-ftn",
        dataset="participation",
        license="CC BY-SA 4.0; attribute FTN Data via nflverse for 2023+, NFL NextGenStats via nflverse for 2022 and earlier",
        required_fields=("nflverse_game_id", "play_id", "offense_players", "defense_players"),
        cadence="Historical release; 2023+ data is generally available after the postseason",
        historical_availability="2016 onward; provider and release timing vary by season",
        fallback_behavior="Use snaps/depth/weekly usage without route-level evidence; tier B and wider role uncertainty",
        source_url="https://github.com/nflverse/nflreadr/blob/main/R/load_participation.R",
    ),
    "schedule": SourceContract(
        key="schedule",
        source="nflverse",
        dataset="schedule",
        license="CC BY 4.0; attribute nflverse-data",
        required_fields=(
            "game_id", "season", "game_type", "week", "gameday", "gametime",
            "away_team", "home_team", "location", "stadium",
        ),
        cadence="Daily and whenever the league changes dates, venues, or game status",
        historical_availability="Historical schedules and results from 1999 onward",
        fallback_behavior="Use the last eligible schedule revision; missing opponent/venue context forces tier C",
        source_url="https://github.com/nflverse/nflverse-data/releases/tag/schedules",
    ),
    "transactions": SourceContract(
        key="transactions",
        source="nflverse-sleeper",
        dataset="transactions",
        license="nflverse data: CC BY 4.0; Sleeper enrichment: API terms, no redistribution license asserted",
        required_fields=("player_id", "effective_at", "transaction_type", "from_team", "to_team"),
        cadence="At least daily in preseason/in-season; persist effective and observed times separately",
        historical_availability="nflverse trades plus differences in weekly rosters; Sleeper is current-context enrichment only",
        fallback_behavior="Infer only effective-dated roster changes from adjacent eligible weekly rosters; tier B and flag transaction detail missing",
        source_url="https://github.com/nflverse/nflverse-data/releases",
    ),
}


@dataclass(frozen=True)
class FallbackDecision:
    tier: str
    confidence_multiplier: float
    missing_sources: tuple[str, ...]


def select_fallback(
    required_sources: Iterable[str],
    available_sources: Iterable[str],
) -> FallbackDecision:
    """Return an explicit A/B/C decision; never synthesize missing data as zero."""

    required = set(required_sources)
    available = set(available_sources)
    unknown = required.difference(SOURCE_CONTRACTS)
    if unknown:
        raise KeyError(f"Unknown fantasy-football source contracts: {sorted(unknown)}")
    missing = tuple(sorted(required - available))
    if not missing:
        tier = "A"
    elif CORE_SOURCE_KEYS.intersection(missing):
        tier = "C"
    else:
        tier = "B"
    return FallbackDecision(tier, FALLBACK_CONFIDENCE[tier], missing)


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def assert_as_of_eligible(
    *,
    as_of_at: datetime,
    source_published_at: datetime | None,
    fetched_at: datetime,
) -> datetime:
    """Return the conservative availability timestamp or reject future data."""

    cutoff = _utc(as_of_at, "as_of_at")
    fetched = _utc(fetched_at, "fetched_at")
    available = _utc(source_published_at, "source_published_at") if source_published_at else fetched
    if available > cutoff:
        raise AsOfCutoffError(
            f"Source available at {available.isoformat()} is newer than simulated cutoff {cutoff.isoformat()}"
        )
    return available


@dataclass(frozen=True)
class SnapshotProvenance:
    source: str
    dataset: str
    season: int
    response_hash: str
    row_count: int
    request_params: Mapping[str, Any] = field(default_factory=dict)
    scoring: str | None = None
    ranking_type: str | None = None
    week: int | None = None
    contract_key: str | None = None
    source_published_at: datetime | None = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    as_of_at: datetime | None = None
    matched_count: int = 0
    unmatched_count: int = 0
    missingness: Mapping[str, Any] = field(default_factory=dict)
    status: str = "success"
    fallback_tier: str | None = "A"
    confidence_multiplier: float = 1.0
    model_eligible: bool = True
    eligibility_reason: str | None = None
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if self.status not in SNAPSHOT_STATUSES:
            raise ValueError(f"Unsupported snapshot status: {self.status}")
        if self.contract_key is not None and self.contract_key not in SOURCE_CONTRACTS:
            raise KeyError(f"Unknown fantasy-football source contract: {self.contract_key}")
        if len(self.response_hash) != 64 or any(char not in "0123456789abcdef" for char in self.response_hash.lower()):
            raise ValueError("response_hash must be a 64-character SHA-256 hex digest")
        if min(self.row_count, self.matched_count, self.unmatched_count) < 0:
            raise ValueError("snapshot counts cannot be negative")
        if self.week is not None and not 1 <= self.week <= 22:
            raise ValueError("week must be between 1 and 22")
        if self.fallback_tier not in FALLBACK_CONFIDENCE:
            raise ValueError("fallback_tier must be A, B, or C")
        expected_confidence = FALLBACK_CONFIDENCE[self.fallback_tier]
        if not 0 <= self.confidence_multiplier <= expected_confidence:
            raise ValueError(
                f"confidence_multiplier for tier {self.fallback_tier} must be between 0 and {expected_confidence}"
            )
        if self.missingness and self.fallback_tier == "A":
            raise ValueError("missing inputs require fallback tier B or C")
        if self.status == "failed" and self.model_eligible:
            raise ValueError("failed snapshots cannot be model eligible")
        _utc(self.fetched_at, "fetched_at")
        if self.source_published_at:
            _utc(self.source_published_at, "source_published_at")
        if self.as_of_at:
            assert_as_of_eligible(
                as_of_at=self.as_of_at,
                source_published_at=self.source_published_at,
                fetched_at=self.fetched_at,
            )

    @property
    def available_at(self) -> datetime:
        return self.source_published_at or self.fetched_at


def persist_source_snapshot(db: Any, snapshot: SnapshotProvenance) -> int:
    """Insert immutable response provenance and return its stable snapshot id.

    A repeated response hash returns the original row without changing its
    fetch time, cutoff, request parameters, or eligibility assessment.
    """

    row = db.execute_one(
        """WITH inserted AS (
             INSERT INTO ff_source_snapshots
               (source,dataset,season,scoring,ranking_type,week,contract_key,
                request_params,source_updated_at,source_published_at,fetched_at,
                available_at,as_of_at,response_hash,row_count,matched_count,
                unmatched_count,missingness,status,fallback_tier,
                confidence_multiplier,model_eligible,eligibility_reason,error_summary)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
             ON CONFLICT(source,dataset,response_hash) DO NOTHING
             RETURNING id
           )
           SELECT id FROM inserted
           UNION ALL
           SELECT id FROM ff_source_snapshots
           WHERE source=%s AND dataset=%s AND response_hash=%s
           LIMIT 1""",
        (
            snapshot.source,
            snapshot.dataset,
            snapshot.season,
            snapshot.scoring,
            snapshot.ranking_type,
            snapshot.week,
            snapshot.contract_key,
            Json(dict(snapshot.request_params)),
            snapshot.source_published_at,
            snapshot.source_published_at,
            snapshot.fetched_at,
            snapshot.available_at,
            snapshot.as_of_at,
            snapshot.response_hash.lower(),
            snapshot.row_count,
            snapshot.matched_count,
            snapshot.unmatched_count,
            Json(dict(snapshot.missingness)),
            snapshot.status,
            snapshot.fallback_tier,
            snapshot.confidence_multiplier,
            snapshot.model_eligible,
            snapshot.eligibility_reason,
            snapshot.error_summary,
            snapshot.source,
            snapshot.dataset,
            snapshot.response_hash.lower(),
        ),
    )
    if not row:
        raise RuntimeError("Could not persist or resolve fantasy-football source snapshot")
    return int(row["id"])


def validate_source_contract_registry() -> None:
    required = {"weekly-rosters", "weekly-stats", "play-by-play", "participation", "schedule", "transactions"}
    missing = required.difference(SOURCE_CONTRACTS)
    if missing:
        raise ValueError(f"Missing required source contracts: {sorted(missing)}")
    for key, contract in SOURCE_CONTRACTS.items():
        if contract.key != key or not all((
            contract.source,
            contract.dataset,
            contract.license,
            contract.required_fields,
            contract.cadence,
            contract.historical_availability,
            contract.fallback_behavior,
            contract.source_url,
        )):
            raise ValueError(f"Incomplete source contract: {key}")


validate_source_contract_registry()
