"""Point-in-time NFL game availability resolution.

This module is the model boundary between append-only provider observations and
projection decisions.  It deliberately knows nothing about DraftKings slate
eligibility: platform restrictions are a separate, slate-scoped decision.

``available_at`` means the observation was durably usable by the system.  The
SQL reader supplies the later of source retrieval and normalized-row storage.
An observation can affect a decision only when::

    available_at <= as_of_at < kickoff

Raw and ineligible observations are retained in the returned audit, but cannot
zero, clear, transfer, or create a conflict that changes the projection.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping


VERSION = "player-game-availability-v1"
OUT_CLASS = frozenset({"OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"})
ACTIVE_CLASS = frozenset({"ACTIVE", "HEALTHY"})
KNOWN_STATES = OUT_CLASS | ACTIVE_CLASS | frozenset({"QUESTIONABLE", "DOUBTFUL", "UNKNOWN"})
SOURCE_AUTHORITY = {"fantasypros": 1, "sleeper": 2, "nfl_official": 3}
MAX_SOURCE_AGE = {"fantasypros": timedelta(hours=24), "sleeper": timedelta(hours=72), "nfl_official": timedelta(hours=3)}


@dataclass(frozen=True)
class AvailabilityDecision:
    version: str
    state: str
    projection_status: str | None
    source: str | None
    observation_id: int | None
    source_snapshot_id: int | None
    available_at: str | None
    as_of_at: str
    kickoff: str | None
    reason: str
    qualifying_observation_ids: tuple[int, ...]
    display_only_observation_ids: tuple[int, ...]
    qualifying_source_snapshot_ids: tuple[int, ...]
    display_only_source_snapshot_ids: tuple[int, ...]

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["qualifying_observation_ids"] = list(self.qualifying_observation_ids)
        result["display_only_observation_ids"] = list(self.display_only_observation_ids)
        result["qualifying_source_snapshot_ids"] = list(self.qualifying_source_snapshot_ids)
        result["display_only_source_snapshot_ids"] = list(self.display_only_source_snapshot_ids)
        return result


def _aware(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None


def _status(value: Any) -> str:
    normalized = str(value or "UNKNOWN").strip().upper()
    return normalized if normalized in KNOWN_STATES else "UNKNOWN"


def _timestamp(value: Any) -> datetime | None:
    if _aware(value):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if _aware(parsed) else None
        except ValueError:
            return None
    return None


def _id(row: Mapping[str, Any], key: str) -> int | None:
    value = row.get(key)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def resolve_game_availability(
    observations: Iterable[Mapping[str, Any]] | None,
    *,
    as_of_at: datetime,
    kickoff: datetime | None,
) -> AvailabilityDecision:
    """Resolve one player-game without consulting mutable current state."""
    if not _aware(as_of_at):
        raise ValueError("as_of_at must be timezone-aware")
    rows = list(observations or ())
    display_only: list[int] = []
    display_only_source_snapshots: list[int] = []
    qualifying: list[Mapping[str, Any]] = []
    stale_candidate = False

    if not _aware(kickoff) or as_of_at >= kickoff:
        return AvailabilityDecision(
            VERSION, "UNKNOWN", None, None, None, None, None,
            as_of_at.isoformat(), kickoff.isoformat() if _aware(kickoff) else None,
            "Kickoff is missing or the requested decision time is not pregame.",
            (), tuple(i for row in rows if (i := _id(row, "observation_id")) is not None),
            (), tuple(sorted({i for row in rows if (i := _id(row, "source_snapshot_id")) is not None})),
        )

    for row in rows:
        observation_id = _id(row, "observation_id")
        source = str(row.get("source") or "").lower()
        available_at = row.get("available_at") or row.get("captured_at")
        source_ok = source in SOURCE_AUTHORITY
        time_ok = _aware(available_at) and available_at <= as_of_at and available_at < kickoff
        complete = str(row.get("snapshot_status") or "success").lower() == "success"
        model_eligible = row.get("model_eligible") is True
        scope_ok = row.get("game_scope_valid", True) is True
        if source == "nfl_official":
            scope_ok = scope_ok and _timestamp(row.get("observation_kickoff")) == kickoff
        age_ok = bool(_aware(available_at) and as_of_at - available_at <= MAX_SOURCE_AGE.get(source, timedelta(0)))
        if source_ok and time_ok and complete and model_eligible and scope_ok and not age_ok:
            stale_candidate = True
        if source_ok and time_ok and complete and model_eligible and scope_ok and age_ok:
            qualifying.append(row)
        elif observation_id is not None:
            display_only.append(observation_id)
            source_snapshot_id = _id(row, "source_snapshot_id")
            if source_snapshot_id is not None:
                display_only_source_snapshots.append(source_snapshot_id)

    if not qualifying:
        return AvailabilityDecision(
            VERSION, "STALE" if stale_candidate else "UNKNOWN", None, None, None, None, None,
            as_of_at.isoformat(), kickoff.isoformat(),
            ("Eligible evidence existed but exceeded the source freshness limit."
             if stale_candidate else
             "No complete, eligible, fresh observation was usable by the requested decision time."),
            (), tuple(display_only),
            (), tuple(sorted({i for row in rows if (i := _id(row, "source_snapshot_id")) is not None})),
        )

    # One current statement per source. Newer same-source ACTIVE can clear an
    # earlier OUT only because both came from complete, qualified snapshots.
    latest: dict[str, Mapping[str, Any]] = {}
    for row in qualifying:
        source = str(row["source"]).lower()
        prior = latest.get(source)
        if prior is None or row["available_at"] > prior["available_at"]:
            latest[source] = row

    official = latest.get("nfl_official")
    if official is not None:
        status = _status(official.get("status"))
        # Official ACTIVE confirms dress status only. It does not erase injury
        # context or assert a normal role, so other evidence remains visible.
        if status == "INACTIVE":
            return _decision("OUT_CONFIRMED", "OUT", official, as_of_at, kickoff, qualifying, display_only,
                             display_only_source_snapshots, "Exact-game official inactive report.")

    nonofficial = [row for source, row in latest.items() if source != "nfl_official"]
    meaningful = [row for row in nonofficial if _status(row.get("status")) != "UNKNOWN"]
    status_groups = {"out" if _status(row.get("status")) in OUT_CLASS else
                     "active" if _status(row.get("status")) in ACTIVE_CLASS else
                     _status(row.get("status")).lower() for row in meaningful}
    if len(status_groups) > 1:
        return AvailabilityDecision(
            VERSION, "CONFLICT", None, None, None, None, None,
            as_of_at.isoformat(), kickoff.isoformat(),
            "Qualified sources disagree; baseline projection is retained and redistribution is blocked.",
            tuple(sorted(i for row in qualifying if (i := _id(row, "observation_id")) is not None)),
            tuple(sorted(display_only)),
            tuple(sorted({i for row in qualifying if (i := _id(row, "source_snapshot_id")) is not None})),
            tuple(sorted({i for row in rows if row not in qualifying and (i := _id(row, "source_snapshot_id")) is not None})),
        )

    if not meaningful:
        return AvailabilityDecision(
            VERSION, "UNKNOWN", None, None, None, None, None,
            as_of_at.isoformat(), kickoff.isoformat(), "Qualified observations do not establish availability.",
            tuple(sorted(i for row in qualifying if (i := _id(row, "observation_id")) is not None)),
            tuple(sorted(display_only)),
            tuple(sorted({i for row in qualifying if (i := _id(row, "source_snapshot_id")) is not None})),
            tuple(sorted({i for row in rows if row not in qualifying and (i := _id(row, "source_snapshot_id")) is not None})),
        )

    chosen = max(meaningful, key=lambda row: (SOURCE_AUTHORITY[str(row["source"]).lower()], row["available_at"]))
    status = _status(chosen.get("status"))
    if status in OUT_CLASS:
        return _decision("OUT_CONFIRMED", status, chosen, as_of_at, kickoff, qualifying, display_only,
                         display_only_source_snapshots, "Qualified structured source reports an OUT-class status.")
    if status in ACTIVE_CLASS:
        return _decision("EXPECTED_ACTIVE", None, chosen, as_of_at, kickoff, qualifying, display_only,
                         display_only_source_snapshots, "Qualified structured source reports active/healthy; normal workload is not implied.")
    return _decision(status, None, chosen, as_of_at, kickoff, qualifying, display_only,
                     display_only_source_snapshots,
                     f"Qualified structured source reports {status}; v1 retains the baseline projection.")


def _decision(
    state: str,
    projection_status: str | None,
    row: Mapping[str, Any],
    as_of_at: datetime,
    kickoff: datetime,
    qualifying: list[Mapping[str, Any]],
    display_only: list[int],
    display_only_source_snapshots: list[int],
    reason: str,
) -> AvailabilityDecision:
    available_at = row.get("available_at")
    return AvailabilityDecision(
        VERSION, state, projection_status, str(row.get("source") or "").lower(),
        _id(row, "observation_id"), _id(row, "source_snapshot_id"),
        available_at.isoformat() if _aware(available_at) else None,
        as_of_at.isoformat(), kickoff.isoformat(), reason,
        tuple(sorted(i for item in qualifying if (i := _id(item, "observation_id")) is not None)),
        tuple(sorted(display_only)),
        tuple(sorted({i for item in qualifying if (i := _id(item, "source_snapshot_id")) is not None})),
        tuple(sorted(set(display_only_source_snapshots))),
    )
