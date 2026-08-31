"""Deterministic NFL injury observations, episodes, and change events.

The player directory remains mutable current state. These tables preserve the
source-attributable history needed to distinguish a new injury from a cleared
one. Rich timeline fields are shadow data and do not alter live projections.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

from psycopg2.extras import Json


CANONICAL_STATUSES = {
    "HEALTHY",
    "QUESTIONABLE",
    "DOUBTFUL",
    "OUT",
    "IR",
    "PUP",
    "NFI",
    "SUSPENDED",
    "UNKNOWN",
}
HEALTHY_ALIASES = {"", "ACTIVE", "AVAILABLE", "CLEARED", "HEALTHY", "NONE", "NULL"}
STATUS_ALIASES = {
    "Q": "QUESTIONABLE",
    "QUESTIONABLE": "QUESTIONABLE",
    "D": "DOUBTFUL",
    "DOUBTFUL": "DOUBTFUL",
    "O": "OUT",
    "OUT": "OUT",
    "IR": "IR",
    "INJURED RESERVE": "IR",
    "INJURED_RESERVE": "IR",
    "PUP": "PUP",
    "PUP-R": "PUP",
    "PUP-P": "PUP",
    "NFI": "NFI",
    "SUS": "SUSPENDED",
    "SUSPENDED": "SUSPENDED",
}


@dataclass(frozen=True)
class NormalizedInjuryObservation:
    source_status: str | None
    normalized_status: str
    body_part: str | None
    injury_type: str | None
    description: str | None
    practice_status: str | None
    injury_started_at: datetime | None
    provider_updated_at: datetime | None
    expected_return_min: date | None
    expected_return_max: date | None
    weeks_out_min: float | None
    weeks_out_max: float | None
    availability_probability: float | None
    estimate_basis: str


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _float(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    numeric = _float(value)
    if numeric is not None and numeric > 10_000_000:
        if numeric > 10_000_000_000:
            numeric /= 1000
        return datetime.fromtimestamp(numeric, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _date(value: Any) -> date | None:
    parsed = _datetime(value)
    return parsed.date() if parsed else None


def normalize_injury_status(value: Any) -> str:
    raw = str(value or "").strip().upper().replace("-", "-")
    if raw in HEALTHY_ALIASES:
        return "HEALTHY"
    if raw in STATUS_ALIASES:
        return STATUS_ALIASES[raw]
    for token, normalized in (
        ("INJURED RESERVE", "IR"),
        ("RESERVE/PUP", "PUP"),
        ("QUESTION", "QUESTIONABLE"),
        ("DOUBT", "DOUBTFUL"),
        ("OUT", "OUT"),
        ("SUSP", "SUSPENDED"),
    ):
        if token in raw:
            return normalized
    return "UNKNOWN"


def normalize_injury_observation(source: str, row: dict[str, Any]) -> NormalizedInjuryObservation:
    source_status = _text(_first(row, "injury_status", "status", "status_short", "designation"))
    normalized_status = normalize_injury_status(source_status)
    body_part = _text(_first(row, "injury_body_part", "body_part", "body_part_name"))
    injury_type = _text(_first(row, "injury_type", "type", "type_name"))
    description = _text(_first(row, "injury_notes", "description", "comment", "notes"))
    practice_status = _text(_first(row, "practice_status", "practice", "practice_participation"))
    injury_started_at = _datetime(_first(row, "injury_start_date", "injury_started_at", "start_date"))
    provider_updated_at = _datetime(_first(row, "updated_at", "last_updated", "last_updated_ts", "news_updated"))
    expected_return_min = _date(_first(row, "expected_return_min", "return_date_min", "estimated_return_date"))
    expected_return_max = _date(_first(row, "expected_return_max", "return_date_max", "expected_return_date", "return_date"))
    weeks_out_min = _float(_first(row, "weeks_out_min", "ir_weeks_min"))
    weeks_out_max = _float(_first(row, "weeks_out_max", "ir_weeks", "ir_weeks_max", "weeks_out"))
    if weeks_out_min is None and weeks_out_max is not None:
        weeks_out_min = weeks_out_max
    if weeks_out_max is None and weeks_out_min is not None:
        weeks_out_max = weeks_out_min
    probability = _float(_first(row, "availability_probability", "playing_probability", "probability_playing", "probability"))
    if probability is not None and probability > 1:
        probability /= 100
    if probability is not None:
        probability = max(0.0, min(1.0, probability))
    estimate_basis = "provider" if any(
        value is not None for value in (expected_return_min, expected_return_max, weeks_out_min, weeks_out_max)
    ) else "unknown"
    return NormalizedInjuryObservation(
        source_status=source_status,
        normalized_status=normalized_status,
        body_part=body_part,
        injury_type=injury_type,
        description=description,
        practice_status=practice_status,
        injury_started_at=injury_started_at,
        provider_updated_at=provider_updated_at,
        expected_return_min=expected_return_min,
        expected_return_max=expected_return_max,
        weeks_out_min=weeks_out_min,
        weeks_out_max=weeks_out_max,
        availability_probability=probability,
        estimate_basis=estimate_basis,
    )


def _json(value: Any) -> Json:
    """psycopg2 Json adapter that survives dates and datetimes.

    `_state()` reads straight off a database row, so it carries whatever the
    column types are -- `expected_return_min`/`expected_return_max` come back as
    `date` objects, and the raw Sleeper payload can carry timestamps. Plain
    `Json()` uses `json.dumps` with no fallback and raises
    "Object of type datetime is not JSON serializable", which aborts the whole
    refresh mid-transaction: the scheduled job failed 13 consecutive times
    between 2026-08-26 and 2026-08-31 on exactly this, leaving the draft board
    frozen while NFL rosters churned through final cuts.

    The two hash helpers in this file already pass `default=str`; this makes the
    write path agree with them, so a value that can be hashed can also be
    stored.
    """
    return Json(value, dumps=lambda obj: json.dumps(obj, default=str))


def _payload_hash(source: str, row: dict[str, Any]) -> str:
    raw = json.dumps({"source": source, "row": row}, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _state(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    keys = (
        "status",
        "body_part",
        "injury_type",
        "expected_return_min",
        "expected_return_max",
        "weeks_out_min",
        "weeks_out_max",
        "estimate_basis",
        "primary_source",
        "source_conflict",
        "active",
    )
    return {key: row.get(key) for key in keys}


def _event_key(player_id: int, event_type: str, observation_hash: str, new_state: dict[str, Any]) -> str:
    raw = json.dumps(
        {"player_id": player_id, "event_type": event_type, "observation": observation_hash, "state": new_state},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def _insert_event(
    db: Any,
    *,
    injury_id: int | None,
    player_id: int,
    observation_id: int,
    event_type: str,
    previous_state: dict[str, Any],
    new_state: dict[str, Any],
    source: str,
    observation_hash: str,
) -> None:
    db.execute(
        """INSERT INTO ff_injury_events
           (injury_id,player_id,observation_id,event_type,previous_state,new_state,source,event_key)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(event_key) DO NOTHING""",
        (
            injury_id,
            player_id,
            observation_id,
            event_type,
            _json(previous_state),
            _json(new_state),
            source,
            _event_key(player_id, event_type, observation_hash, new_state),
        ),
    )


def persist_injury_observation(
    db: Any,
    *,
    player_id: int,
    season: int,
    source: str,
    source_snapshot_id: int,
    row: dict[str, Any],
    reconcile_current: bool = True,
) -> dict[str, Any]:
    """Persist one provider observation and reconcile the player's active episode."""
    normalized = normalize_injury_observation(source, row)
    response_hash = _payload_hash(source, row)
    observation = db.execute_one(
        """INSERT INTO ff_player_injury_observations
           (player_id,season,source,source_snapshot_id,source_status,normalized_status,
            body_part,injury_type,description,practice_status,injury_started_at,
            provider_updated_at,expected_return_min,expected_return_max,weeks_out_min,
            weeks_out_max,availability_probability,raw_payload,response_hash)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(source_snapshot_id,player_id,source) DO NOTHING
           RETURNING id,observed_at""",
        (
            player_id,
            season,
            source,
            source_snapshot_id,
            normalized.source_status,
            normalized.normalized_status,
            normalized.body_part,
            normalized.injury_type,
            normalized.description,
            normalized.practice_status,
            normalized.injury_started_at,
            normalized.provider_updated_at,
            normalized.expected_return_min,
            normalized.expected_return_max,
            normalized.weeks_out_min,
            normalized.weeks_out_max,
            normalized.availability_probability,
            _json(row),
            response_hash,
        ),
    )
    if not observation:
        existing = db.execute_one(
            """SELECT id,observed_at FROM ff_player_injury_observations
               WHERE source_snapshot_id=%s AND player_id=%s AND source=%s""",
            (source_snapshot_id, player_id, source),
        )
        return {"observation_id": int(existing["id"]) if existing else None, "event": None, "duplicate": True}

    observation_id = int(observation["id"])
    observed_at = observation.get("observed_at") or datetime.now(timezone.utc)
    if not reconcile_current:
        return {"observation_id": observation_id, "event": None, "duplicate": False}
    active = db.execute_one(
        "SELECT * FROM ff_player_injuries WHERE player_id=%s AND active ORDER BY first_seen_at DESC LIMIT 1",
        (player_id,),
    )
    previous_state = _state(active)

    if normalized.normalized_status == "HEALTHY":
        db.execute("UPDATE ff_players SET injury_status=NULL WHERE id=%s", (player_id,))
        if not active:
            return {"observation_id": observation_id, "event": None, "duplicate": False}
        db.execute(
            """UPDATE ff_player_injuries SET active=FALSE,status=%s,last_confirmed_at=%s,
               cleared_at=%s,primary_source=%s WHERE id=%s""",
            (active["status"], observed_at, observed_at, source, int(active["id"])),
        )
        new_state = {**previous_state, "active": False, "cleared_at": observed_at, "primary_source": source}
        _insert_event(
            db,
            injury_id=int(active["id"]),
            player_id=player_id,
            observation_id=observation_id,
            event_type="CLEARED",
            previous_state=previous_state,
            new_state=new_state,
            source=source,
            observation_hash=response_hash,
        )
        return {"observation_id": observation_id, "event": "CLEARED", "duplicate": False}

    canonical_status = normalized.normalized_status
    new_values = {
        "status": canonical_status,
        "body_part": normalized.body_part,
        "injury_type": normalized.injury_type,
        "expected_return_min": normalized.expected_return_min,
        "expected_return_max": normalized.expected_return_max,
        "weeks_out_min": normalized.weeks_out_min,
        "weeks_out_max": normalized.weeks_out_max,
        "estimate_basis": normalized.estimate_basis,
        "primary_source": source,
        "source_conflict": False,
        "active": True,
    }
    if not active:
        injury = db.execute_one(
            """INSERT INTO ff_player_injuries
               (player_id,season,status,body_part,injury_type,first_seen_at,last_confirmed_at,
                expected_return_min,expected_return_max,weeks_out_min,weeks_out_max,
                estimate_basis,confidence,primary_source,source_conflict,active)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,TRUE)
               RETURNING id""",
            (
                player_id,
                season,
                canonical_status,
                normalized.body_part,
                normalized.injury_type,
                observed_at,
                observed_at,
                normalized.expected_return_min,
                normalized.expected_return_max,
                normalized.weeks_out_min,
                normalized.weeks_out_max,
                normalized.estimate_basis,
                normalized.availability_probability,
                source,
            ),
        )
        injury_id = int(injury["id"])
        _insert_event(
            db,
            injury_id=injury_id,
            player_id=player_id,
            observation_id=observation_id,
            event_type="NEW_INJURY",
            previous_state={},
            new_state=new_values,
            source=source,
            observation_hash=response_hash,
        )
        event_type = "NEW_INJURY"
    else:
        injury_id = int(active["id"])
        timeline_keys = ("expected_return_min", "expected_return_max", "weeks_out_min", "weeks_out_max")
        event_type = None
        if active.get("status") != canonical_status:
            event_type = "STATUS_CHANGED"
        elif any(active.get(key) != new_values[key] and new_values[key] is not None for key in timeline_keys):
            event_type = "TIMELINE_CHANGED"
        db.execute(
            """UPDATE ff_player_injuries SET status=%s,
               body_part=COALESCE(%s,body_part),injury_type=COALESCE(%s,injury_type),
               last_confirmed_at=%s,expected_return_min=COALESCE(%s,expected_return_min),
               expected_return_max=COALESCE(%s,expected_return_max),
               weeks_out_min=COALESCE(%s,weeks_out_min),weeks_out_max=COALESCE(%s,weeks_out_max),
               estimate_basis=CASE WHEN %s<>'unknown' THEN %s ELSE estimate_basis END,
               confidence=COALESCE(%s,confidence),primary_source=%s WHERE id=%s""",
            (
                canonical_status,
                normalized.body_part,
                normalized.injury_type,
                observed_at,
                normalized.expected_return_min,
                normalized.expected_return_max,
                normalized.weeks_out_min,
                normalized.weeks_out_max,
                normalized.estimate_basis,
                normalized.estimate_basis,
                normalized.availability_probability,
                source,
                injury_id,
            ),
        )
        if event_type:
            _insert_event(
                db,
                injury_id=injury_id,
                player_id=player_id,
                observation_id=observation_id,
                event_type=event_type,
                previous_state=previous_state,
                new_state={**previous_state, **{key: value for key, value in new_values.items() if value is not None}},
                source=source,
                observation_hash=response_hash,
            )

    db.execute("UPDATE ff_players SET injury_status=%s WHERE id=%s", (canonical_status, player_id))
    return {"observation_id": observation_id, "event": event_type, "duplicate": False}


def persist_sleeper_injury_observations(
    db: Any,
    *,
    season: int,
    source_snapshot_id: int,
    players: Iterable[dict[str, Any]],
) -> dict[str, int]:
    counts = {"observations": 0, "events": 0, "duplicates": 0}
    for player in players:
        if player.get("position") == "DST":
            continue
        raw = dict((player.get("metadata") or {}).get("sleeper") or {})
        if not raw:
            raw = {"injury_status": player.get("injury_status")}
        result = persist_injury_observation(
            db,
            player_id=int(player["player_id"]),
            season=season,
            source="sleeper",
            source_snapshot_id=source_snapshot_id,
            row=raw,
        )
        counts["observations"] += 1
        counts["duplicates"] += int(result["duplicate"])
        counts["events"] += int(result["event"] is not None)
    return counts


def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    text = re.sub(r"\b(jr|sr|ii|iii|iv)\.?\b", "", text, flags=re.I)
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def persist_fantasypros_injury_observations(
    db: Any,
    *,
    season: int,
    source_snapshot_id: int,
    rows: Iterable[dict[str, Any]],
) -> dict[str, int]:
    players = db.execute(
        """SELECT id,normalized_name,position,team_abbrev,fantasypros_player_id
           FROM ff_players WHERE season=%s""",
        (season,),
    )
    by_fp = {
        int(player["fantasypros_player_id"]): player
        for player in players
        if player.get("fantasypros_player_id") is not None
    }
    counts = {"source_rows": 0, "matched": 0, "unmatched": 0, "ambiguous": 0, "events": 0}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        counts["source_rows"] += 1
        fp_id = _float(_first(raw, "player_id", "fpid"))
        candidate = by_fp.get(int(fp_id)) if fp_id is not None else None
        if candidate is None:
            name = _normalize_name(str(_first(raw, "player_name", "name") or ""))
            position = str(_first(raw, "position_id", "position") or "").upper()
            team = str(_first(raw, "team_id", "team") or "").upper()
            candidates = [
                player for player in players
                if player.get("normalized_name") == name
                and str(player.get("position") or "").upper() == position
                and (not team or not player.get("team_abbrev") or str(player.get("team_abbrev")).upper() == team)
            ]
            if len(candidates) == 1:
                candidate = candidates[0]
            elif len(candidates) > 1:
                counts["ambiguous"] += 1
                continue
        if candidate is None:
            counts["unmatched"] += 1
            continue
        result = persist_injury_observation(
            db,
            player_id=int(candidate["id"]),
            season=season,
            source="fantasypros",
            source_snapshot_id=source_snapshot_id,
            row=raw,
            # Phase 1 preserves the richer FantasyPros evidence without letting
            # an optional/list-style feed clear or reopen Sleeper's canonical
            # current state. Cross-source reconciliation is a separate gate.
            reconcile_current=False,
        )
        counts["matched"] += 1
        counts["events"] += int(result["event"] is not None)
    return counts
