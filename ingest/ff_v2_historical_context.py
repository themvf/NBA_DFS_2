"""Populate immutable 2020-2025 roster and team-week context for FF V2.

The output is append-only by deterministic run id. Raw nflverse assets are
cached by response hash under ``data/ff_v2_sources`` so every aggregate can be
replayed from the exact bytes named by its source snapshots.

Usage:
    python -m ingest.ff_v2_historical_context --start-season 2020 --end-season 2025
    python -m ingest.ff_v2_historical_context --verify artifacts/ff_v2_historical_context_2020_2025.json
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import uuid
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_independent import normalize_team
from ingest.ff_source_contracts import (
    CORE_SOURCE_KEYS,
    FALLBACK_CONFIDENCE,
    SOURCE_CONTRACTS,
    SnapshotProvenance,
    persist_source_snapshot,
)


TRANSFORM_VERSION = "ff-v2-context-v1"
RUN_NAMESPACE = uuid.UUID("dbd7d338-0a36-44c6-af17-9d5634769219")
EASTERN = ZoneInfo("America/New_York")
DEFAULT_CACHE_ROOT = Path("data/ff_v2_sources")
DEFAULT_ARTIFACT = Path("artifacts/ff_v2_historical_context_2020_2025.json")
USER_AGENT = "NBADFS-v2-roster-aware/1.0"
FANTASY_POSITIONS = frozenset({"QB", "RB", "FB", "WR", "TE", "K"})

SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"
SEASON_URLS = {
    "weekly-rosters": (
        "https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/"
        "roster_weekly_{season}.parquet"
    ),
    "weekly-stats": (
        "https://github.com/nflverse/nflverse-data/releases/download/stats_player/"
        "stats_player_week_{season}.parquet"
    ),
    "play-by-play": (
        "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
        "play_by_play_{season}.parquet"
    ),
    "participation": (
        "https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/"
        "pbp_participation_{season}.parquet"
    ),
}


@dataclass(frozen=True)
class LoadedSource:
    contract_key: str
    season: int
    url: str
    cache_path: str
    response_hash: str
    fetched_at: datetime
    source_published_at: datetime | None
    frame: pd.DataFrame
    snapshot_id: int | None = None


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _clean(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple, set)):
        return [_clean(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return round(value, 8)
    if hasattr(value, "item"):
        return _clean(value.item())
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def canonical_digest(value: Any) -> str:
    raw = json.dumps(_clean(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_frame(payload: bytes, suffix: str) -> pd.DataFrame:
    if suffix == ".csv":
        return pd.read_csv(io.BytesIO(payload), low_memory=False)
    return pd.read_parquet(io.BytesIO(payload))


def load_source(
    contract_key: str,
    season: int,
    url: str,
    *,
    cache_root: Path,
    refresh: bool,
) -> LoadedSource:
    suffix = ".csv" if url.lower().endswith(".csv") else ".parquet"
    source_dir = cache_root / contract_key / str(season)
    pointer_path = source_dir / "latest.json"
    metadata: dict[str, Any] | None = None
    payload: bytes | None = None
    cache_path: Path | None = None

    if not refresh and pointer_path.exists():
        metadata = json.loads(pointer_path.read_text(encoding="utf-8"))
        candidate = Path(metadata["cache_path"])
        if candidate.exists():
            payload = candidate.read_bytes()
            cache_path = candidate

    if payload is None:
        response = requests.get(url, timeout=120, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        payload = response.content
        digest = hashlib.sha256(payload).hexdigest()
        fetched_at = datetime.now(timezone.utc)
        published_at = _parse_http_datetime(response.headers.get("Last-Modified"))
        source_dir.mkdir(parents=True, exist_ok=True)
        cache_path = source_dir / f"{digest}{suffix}"
        if not cache_path.exists():
            cache_path.write_bytes(payload)
        metadata = {
            "url": url,
            "cache_path": str(cache_path),
            "response_hash": digest,
            "fetched_at": fetched_at.isoformat(),
            "source_published_at": published_at.isoformat() if published_at else None,
        }
        pointer_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    assert metadata is not None and cache_path is not None and payload is not None
    digest = hashlib.sha256(payload).hexdigest()
    if digest != metadata["response_hash"]:
        raise RuntimeError(f"Cached {contract_key} {season} bytes do not match metadata hash")
    frame = _read_frame(payload, suffix)
    return LoadedSource(
        contract_key=contract_key,
        season=season,
        url=url,
        cache_path=str(cache_path),
        response_hash=digest,
        fetched_at=datetime.fromisoformat(metadata["fetched_at"]),
        source_published_at=(
            datetime.fromisoformat(metadata["source_published_at"])
            if metadata.get("source_published_at") else None
        ),
        frame=frame,
    )


def persist_loaded_source(db: RefreshDatabase, loaded: LoadedSource, *, requested_seasons: list[int]) -> LoadedSource:
    contract = SOURCE_CONTRACTS[loaded.contract_key]
    missing_fields = sorted(set(contract.required_fields) - set(loaded.frame.columns))
    status = "partial" if missing_fields else "success"
    fallback_tier = "C" if loaded.contract_key in CORE_SOURCE_KEYS and missing_fields else ("B" if missing_fields else "A")
    snapshot_id = persist_source_snapshot(
        db,
        SnapshotProvenance(
            source=contract.source,
            dataset=contract.dataset,
            contract_key=loaded.contract_key,
            season=loaded.season,
            response_hash=loaded.response_hash,
            row_count=len(loaded.frame),
            request_params={
                "url": loaded.url,
                "cache_path": loaded.cache_path,
                "requested_seasons": requested_seasons,
                "transform_version": TRANSFORM_VERSION,
                "license": contract.license,
            },
            source_published_at=loaded.source_published_at,
            fetched_at=loaded.fetched_at,
            missingness={"required_fields": missing_fields} if missing_fields else {},
            status=status,
            fallback_tier=fallback_tier,
            confidence_multiplier=FALLBACK_CONFIDENCE[fallback_tier],
            model_eligible=not missing_fields,
            eligibility_reason=(
                "required historical context fields present"
                if not missing_fields
                else f"missing required fields: {', '.join(missing_fields)}"
            ),
        ),
    )
    return replace(loaded, snapshot_id=snapshot_id)


def _kickoff_at(row: Mapping[str, Any]) -> datetime:
    game_day = pd.to_datetime(row.get("gameday"), errors="coerce")
    if pd.isna(game_day):
        raise ValueError(f"Schedule row lacks gameday: {row.get('game_id')}")
    time_text = str(row.get("gametime") or "13:00")
    try:
        hour, minute = (int(part) for part in time_text.split(":")[:2])
    except (TypeError, ValueError):
        hour, minute = 13, 0
    local = datetime(game_day.year, game_day.month, game_day.day, hour, minute, tzinfo=EASTERN)
    return local.astimezone(timezone.utc)


def build_schedule_context(
    schedule: pd.DataFrame,
    seasons: Iterable[int],
    source_snapshot_id: int,
) -> tuple[list[dict[str, Any]], dict[tuple[int, int], datetime], dict[tuple[str, str], dict[str, Any]]]:
    requested = set(int(season) for season in seasons)
    regular = schedule[(schedule["season"].isin(requested)) & (schedule["game_type"] == "REG")].copy()
    regular["home_team"] = regular["home_team"].map(normalize_team)
    regular["away_team"] = regular["away_team"].map(normalize_team)
    contexts: list[dict[str, Any]] = []
    week_starts: dict[tuple[int, int], datetime] = {}
    game_team_context: dict[tuple[str, str], dict[str, Any]] = {}

    for season in sorted(requested):
        season_games = regular[regular["season"] == season].copy()
        teams = sorted(set(season_games["home_team"]) | set(season_games["away_team"]))
        max_week = int(season_games["week"].max())
        for week in range(1, max_week + 1):
            week_games = season_games[season_games["week"] == week]
            kickoffs = [_kickoff_at(row) for row in week_games.to_dict("records")]
            week_start = min(kickoffs) if kickoffs else datetime(season, 9, 1, tzinfo=timezone.utc) + timedelta(weeks=week - 1)
            week_starts[(season, week)] = week_start
            by_team: dict[str, dict[str, Any]] = {}
            for row in week_games.to_dict("records"):
                kickoff = _kickoff_at(row)
                for is_home, team_field, opponent_field, qb_id_field, qb_name_field, coach_field in (
                    (True, "home_team", "away_team", "home_qb_id", "home_qb_name", "home_coach"),
                    (False, "away_team", "home_team", "away_qb_id", "away_qb_name", "away_coach"),
                ):
                    team = normalize_team(row[team_field])
                    context = {
                        "season": season,
                        "week": week,
                        "team": team,
                        "is_bye": False,
                        "game_id": str(row["game_id"]),
                        "game_date": str(row["gameday"]),
                        "kickoff_at": kickoff,
                        "opponent": normalize_team(row[opponent_field]),
                        "is_home": is_home,
                        "location": _clean(row.get("location")),
                        "stadium": _clean(row.get("stadium")),
                        "stadium_id": _clean(row.get("stadium_id")),
                        "roof": _clean(row.get("roof")),
                        "surface": _clean(row.get("surface")),
                        "quarterback_gsis_id": _clean(row.get(qb_id_field)),
                        "quarterback_name": _clean(row.get(qb_name_field)),
                        "head_coach": _clean(row.get(coach_field)),
                        "play_caller_id": None,
                        "source_snapshot_id": source_snapshot_id,
                        "observed_at": kickoff,
                    }
                    context["row_digest"] = canonical_digest(context)
                    by_team[team] = context
                    game_team_context[(str(row["game_id"]), team)] = context
            for team in teams:
                if team in by_team:
                    contexts.append(by_team[team])
                    continue
                bye = {
                    "season": season,
                    "week": week,
                    "team": team,
                    "is_bye": True,
                    "game_id": None,
                    "game_date": None,
                    "kickoff_at": None,
                    "opponent": None,
                    "is_home": None,
                    "location": None,
                    "stadium": None,
                    "stadium_id": None,
                    "roof": None,
                    "surface": None,
                    "quarterback_gsis_id": None,
                    "quarterback_name": None,
                    "head_coach": None,
                    "play_caller_id": None,
                    "source_snapshot_id": source_snapshot_id,
                    "observed_at": week_start,
                }
                bye["row_digest"] = canonical_digest(bye)
                contexts.append(bye)
    return contexts, week_starts, game_team_context


def _position_lookup(stats: pd.DataFrame, roster: pd.DataFrame) -> dict[tuple[int, int, str], str]:
    lookup: dict[tuple[int, int, str], str] = {}
    stats_regular = stats[stats["season_type"] == "REG"] if "season_type" in stats else stats
    for row in stats_regular[["season", "week", "player_id", "position"]].dropna(subset=["player_id"]).to_dict("records"):
        lookup[(int(row["season"]), int(row["week"]), str(row["player_id"]))] = str(row.get("position") or "")
    for row in roster[["season", "week", "gsis_id", "position"]].dropna(subset=["gsis_id"]).to_dict("records"):
        player_id = str(row.get("gsis_id") or "").strip()
        if player_id:
            lookup.setdefault((int(row["season"]), int(row["week"]), player_id), str(row.get("position") or ""))
    return lookup


def build_roster_rows(
    roster: pd.DataFrame,
    stats: pd.DataFrame,
    *,
    source_snapshot_id: int,
    week_starts: Mapping[tuple[int, int], datetime],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    regular = roster[roster["game_type"] == "REG"].copy() if "game_type" in roster else roster.copy()
    regular = regular[regular["position"].isin(FANTASY_POSITIONS)].copy()
    regular["team"] = regular["team"].map(normalize_team)
    regular["gsis_id"] = regular["gsis_id"].fillna("").astype(str).str.strip()
    regular = regular[regular["gsis_id"] != ""]
    stats_team: dict[tuple[int, int, str], str] = {}
    if all(column in stats for column in ("season", "week", "player_id", "team")):
        for row in stats.dropna(subset=["player_id", "team"]).to_dict("records"):
            stats_team[(int(row["season"]), int(row["week"]), str(row["player_id"]))] = normalize_team(row["team"])

    rows: list[dict[str, Any]] = []
    conflict_count = 0
    skipped_conflicts = 0
    group_columns = ["season", "week", "gsis_id"]
    for (season, week, player_id), group in regular.groupby(group_columns, sort=True, dropna=False):
        candidates = group.to_dict("records")
        teams = sorted({normalize_team(row.get("team")) for row in candidates if row.get("team")})
        resolution_method = "single_weekly_roster_row"
        selected: dict[str, Any] | None = None
        if len(teams) == 1:
            selected = candidates[-1]
            resolution_method = "same_team_duplicate" if len(candidates) > 1 else resolution_method
        else:
            conflict_count += 1
            played_for = stats_team.get((int(season), int(week), str(player_id)))
            matching = [row for row in candidates if normalize_team(row.get("team")) == played_for]
            if played_for and matching:
                selected = matching[-1]
                resolution_method = "weekly_stats_team"
            else:
                skipped_conflicts += 1
                continue
        effective_at = week_starts[(int(season), int(week))]
        row = {
            "season": int(season),
            "week": int(week),
            "player_gsis_id": str(player_id),
            "player_name": str(selected.get("full_name") or selected.get("football_name") or player_id),
            "position": _clean(selected.get("position")),
            "depth_chart_position": _clean(selected.get("depth_chart_position")),
            "team": normalize_team(selected.get("team")),
            "roster_status": _clean(selected.get("status")),
            "resolution_method": resolution_method,
            "effective_at": effective_at,
            "source_snapshot_id": source_snapshot_id,
            "observed_at": effective_at,
        }
        row["row_digest"] = canonical_digest(row)
        rows.append(row)
    return rows, {"multi_team_conflicts": conflict_count, "skipped_multi_team_conflicts": skipped_conflicts}


def build_transactions(roster_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_player: dict[str, list[dict[str, Any]]] = {}
    for row in roster_rows:
        by_player.setdefault(row["player_gsis_id"], []).append(row)
    transactions: list[dict[str, Any]] = []
    for player_id, history in sorted(by_player.items()):
        ordered = sorted(history, key=lambda row: (row["effective_at"], row["season"], row["week"]))
        previous: dict[str, Any] | None = None
        for current in ordered:
            if previous is not None and current["team"] != previous["team"]:
                transaction = {
                    "player_gsis_id": player_id,
                    "player_name": current["player_name"],
                    "from_team": previous["team"],
                    "to_team": current["team"],
                    "effective_at": current["effective_at"],
                    "transaction_type": "weekly_roster_team_change",
                    "source_snapshot_id": current["source_snapshot_id"],
                    "evidence": {
                        "prior_season": previous["season"],
                        "prior_week": previous["week"],
                        "new_season": current["season"],
                        "new_week": current["week"],
                        "resolution_method": current["resolution_method"],
                    },
                    "observed_at": current["observed_at"],
                }
                transaction["row_digest"] = canonical_digest(transaction)
                transactions.append(transaction)
            previous = current
    return transactions


def _flag(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(0, index=frame.index, dtype="int64")
    return pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)


def _seconds_per_play(frame: pd.DataFrame) -> float | None:
    samples: list[float] = []
    drive_column = "fixed_drive" if "fixed_drive" in frame else "drive"
    for _, drive in frame.dropna(subset=[drive_column]).groupby(drive_column):
        ordered = drive.sort_values(["game_seconds_remaining", "play_id"], ascending=[False, True])
        seconds = pd.to_numeric(ordered["game_seconds_remaining"], errors="coerce").dropna().tolist()
        for previous, current in zip(seconds, seconds[1:]):
            delta = float(previous) - float(current)
            if 0 < delta <= 120:
                samples.append(delta)
    return round(sum(samples) / len(samples), 4) if samples else None


def build_team_week_facts(
    pbp: pd.DataFrame,
    stats: pd.DataFrame,
    roster: pd.DataFrame,
    *,
    season: int,
    source_snapshot_ids: Mapping[str, int],
    game_team_context: Mapping[tuple[str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    positions = _position_lookup(stats, roster)
    regular = pbp[(pbp["season"] == season) & (pbp["season_type"] == "REG") & pbp["posteam"].notna()].copy()
    regular["posteam"] = regular["posteam"].map(normalize_team)
    regular["defteam"] = regular["defteam"].map(normalize_team)
    facts: list[dict[str, Any]] = []
    unknown_rusher_positions = 0
    unknown_receiver_positions = 0

    for (game_id, team), group in regular.groupby(["game_id", "posteam"], sort=True):
        group = group.copy()
        valid = group["play_type"].fillna("") != "no_play"
        pass_flag = (_flag(group, "pass_attempt") == 1) & valid
        sack_flag = (_flag(group, "sack") == 1) & valid
        kneel_flag = (_flag(group, "qb_kneel") == 1) & valid
        rush_flag = (_flag(group, "rush_attempt") == 1) & valid & ~kneel_flag
        target_flag = pass_flag & ~sack_flag & group["receiver_player_id"].notna()
        two_point = _flag(group, "two_point_attempt") == 1
        context = game_team_context.get((str(game_id), str(team)))
        if context is None:
            continue

        rb_carry = []
        for index in group.index[rush_flag & group["rusher_player_id"].notna()]:
            player_id = str(group.at[index, "rusher_player_id"])
            position = positions.get((season, int(group.at[index, "week"]), player_id))
            if not position:
                unknown_rusher_positions += 1
            rb_carry.append(position in {"RB", "FB"})
        rb_target = []
        for index in group.index[target_flag]:
            player_id = str(group.at[index, "receiver_player_id"])
            position = positions.get((season, int(group.at[index, "week"]), player_id))
            if not position:
                unknown_receiver_positions += 1
            rb_target.append(position in {"RB", "FB"})

        scrimmage = pass_flag | rush_flag
        neutral = scrimmage & pd.to_numeric(group["score_differential"], errors="coerce").between(-7, 7) & (pd.to_numeric(group["qtr"], errors="coerce") <= 3)
        neutral_plays = int(neutral.sum())
        neutral_passes = int((neutral & pass_flag).sum())
        score = pd.to_numeric(group.loc[scrimmage, "score_differential"], errors="coerce")
        drive_column = "fixed_drive" if "fixed_drive" in group else "drive"
        red_zone = scrimmage & (pd.to_numeric(group["yardline_100"], errors="coerce") <= 20)
        goal_line = rush_flag & (pd.to_numeric(group["yardline_100"], errors="coerce") <= 5)
        air_yards = pd.to_numeric(group["air_yards"], errors="coerce")
        yardline = pd.to_numeric(group["yardline_100"], errors="coerce")
        end_zone = target_flag & air_yards.notna() & yardline.notna() & (air_yards >= yardline)

        official_pass_attempts = int(pass_flag.sum() - sack_flag.sum())
        rush_attempts = int(rush_flag.sum())
        plays = int(pass_flag.sum() + rush_flag.sum())
        fact = {
            "season": season,
            "week": int(group["week"].iloc[0]),
            "game_id": str(game_id),
            "game_date": str(context["game_date"]),
            "team": str(team),
            "opponent": str(context["opponent"]),
            "plays": plays,
            "drives": int(group.loc[scrimmage, drive_column].nunique()),
            "pass_attempts": official_pass_attempts,
            "dropbacks": int(_flag(group.loc[valid], "qb_dropback").sum()),
            "sacks": int(sack_flag.sum()),
            "allocatable_targets": int(target_flag.sum()),
            "rush_attempts": rush_attempts,
            "rb_carries": int(sum(rb_carry)),
            "rb_targets": int(sum(rb_target)),
            "pass_touchdowns": int(((_flag(group, "pass_touchdown") == 1) & valid & ~two_point).sum()),
            "rush_touchdowns": int(((_flag(group, "rush_touchdown") == 1) & valid & ~two_point).sum()),
            "red_zone_trips": int(group.loc[red_zone, drive_column].nunique()),
            "goal_line_carries": int(goal_line.sum()),
            "end_zone_targets": int(end_zone.sum()),
            "neutral_pass_rate": round(neutral_passes / neutral_plays, 6) if neutral_plays else None,
            "seconds_per_play": _seconds_per_play(group.loc[scrimmage]),
            "score_state_features": {
                "leading_by_8_plus": int((score > 7).sum()),
                "neutral_within_7": int(score.between(-7, 7).sum()),
                "trailing_by_8_plus": int((score < -7).sum()),
            },
            "quarterback_gsis_id": context.get("quarterback_gsis_id"),
            "quarterback_name": context.get("quarterback_name"),
            "play_caller_id": None,
            "source_snapshot_ids": dict(source_snapshot_ids),
            "derivation": {
                "semantics_version": TRANSFORM_VERSION,
                "plays": "pass_attempt flag (includes sacks) plus non-kneel rush_attempt; no_play rows excluded",
                "pass_attempts": "pass_attempt minus sacks; spikes retained; throwaways have no allocatable target",
                "rush_attempts": "rush_attempt excluding quarterback kneels; scrambles retained",
                "rb_opportunity": "rusher/receiver position RB or FB from same-season weekly stats, then weekly roster",
                "red_zone_trips": "unique offensive drive with a valid scrimmage snap at yardline_100 <= 20",
                "goal_line_carries": "non-kneel rush at yardline_100 <= 5",
                "end_zone_targets": "named target with air_yards >= yardline_100",
                "raw_pass_flag_count": int(pass_flag.sum()),
                "raw_rush_flag_count": int(((_flag(group, "rush_attempt") == 1) & valid).sum()),
                "kneels_removed": int(kneel_flag.sum()),
                "targetless_official_attempts": max(0, official_pass_attempts - int(target_flag.sum())),
            },
            "observed_at": context["kickoff_at"] + timedelta(hours=6),
        }
        fact["fact_digest"] = canonical_digest(fact)
        facts.append(fact)
    return facts, {
        "unknown_rusher_positions": unknown_rusher_positions,
        "unknown_receiver_positions": unknown_receiver_positions,
    }


def build_coverage_report(
    *,
    seasons: list[int],
    contexts: list[dict[str, Any]],
    roster_rows: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    facts: list[dict[str, Any]],
    schedule: pd.DataFrame,
    roster_conflicts: Mapping[int, Mapping[str, int]],
    fact_missingness: Mapping[int, Mapping[str, int]],
    source_partitions: Mapping[str, Any],
) -> dict[str, Any]:
    report: dict[str, Any] = {"seasons": {}, "source_partitions": source_partitions}
    for season in seasons:
        season_schedule = schedule[(schedule["season"] == season) & (schedule["game_type"] == "REG")]
        season_contexts = [row for row in contexts if row["season"] == season]
        season_rosters = [row for row in roster_rows if row["season"] == season]
        season_facts = [row for row in facts if row["season"] == season]
        season_transactions = [row for row in transactions if row["evidence"].get("new_season") == season]
        expected_games = int(len(season_schedule))
        fact_keys = {(row["game_id"], row["team"]) for row in season_facts}
        expected_fact_keys = {
            (str(row["game_id"]), normalize_team(team))
            for row in season_schedule.to_dict("records")
            for team in (row["home_team"], row["away_team"])
        }
        teams = sorted({row["team"] for row in season_contexts})
        weeks = sorted({row["week"] for row in season_contexts})
        report["seasons"][str(season)] = {
            "teams": len(teams),
            "missing_teams": sorted(set(teams) ^ set(normalize_team(team) for team in set(season_schedule["home_team"]) | set(season_schedule["away_team"]))),
            "weeks": len(weeks),
            "week_range": [min(weeks), max(weeks)] if weeks else [],
            "scheduled_games": expected_games,
            "team_week_context_rows": len(season_contexts),
            "bye_rows": sum(row["is_bye"] for row in season_contexts),
            "team_week_fact_rows": len(season_facts),
            "missing_game_team_facts": [list(key) for key in sorted(expected_fact_keys - fact_keys)],
            "extra_game_team_facts": [list(key) for key in sorted(fact_keys - expected_fact_keys)],
            "roster_rows": len(season_rosters),
            "unique_roster_players": len({row["player_gsis_id"] for row in season_rosters}),
            "transactions_effective": len(season_transactions),
            "roster_identity": dict(roster_conflicts.get(season, {})),
            "fact_missingness": dict(fact_missingness.get(season, {})),
            "missing_play_caller_rows": len(season_facts),
        }
    report["totals"] = {
        "context_rows": len(contexts),
        "roster_rows": len(roster_rows),
        "transactions": len(transactions),
        "fact_rows": len(facts),
        "unique_players": len({row["player_gsis_id"] for row in roster_rows}),
    }
    return report


def artifact_digest(
    source_hashes: Mapping[str, str],
    contexts: Iterable[Mapping[str, Any]],
    roster_rows: Iterable[Mapping[str, Any]],
    transactions: Iterable[Mapping[str, Any]],
    facts: Iterable[Mapping[str, Any]],
) -> str:
    return canonical_digest({
        "transform_version": TRANSFORM_VERSION,
        "source_hashes": dict(sorted(source_hashes.items())),
        "context_digests": sorted(row["row_digest"] for row in contexts),
        "roster_digests": sorted(row["row_digest"] for row in roster_rows),
        "transaction_digests": sorted(row["row_digest"] for row in transactions),
        "fact_digests": sorted(row["fact_digest"] for row in facts),
    })


def _insert_values(cursor: Any, statement: str, rows: list[tuple[Any, ...]], *, page_size: int = 2000) -> None:
    if rows:
        execute_values(cursor, statement, rows, page_size=page_size)


def persist_context_run(
    db: RefreshDatabase,
    *,
    run_id: str,
    seasons: list[int],
    source_snapshot_ids: Mapping[str, int],
    coverage: Mapping[str, Any],
    digest: str,
    contexts: list[dict[str, Any]],
    roster_rows: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    facts: list[dict[str, Any]],
    transform_version: str = TRANSFORM_VERSION,
) -> None:
    cursor = db.conn.cursor()
    cursor.execute(
        """INSERT INTO ff_v2_context_runs
           (run_id,transform_version,seasons,source_snapshot_ids,coverage_report,artifact_digest)
           VALUES (%s,%s,%s,%s,%s,%s)
           ON CONFLICT(run_id) DO NOTHING""",
        (run_id, transform_version, Json(seasons), Json(dict(source_snapshot_ids)), Json(dict(coverage)), digest),
    )
    _insert_values(
        cursor,
        """INSERT INTO ff_v2_team_week_context
           (run_id,season,week,team,is_bye,game_id,game_date,kickoff_at,opponent,is_home,
            location,stadium,stadium_id,roof,surface,quarterback_gsis_id,quarterback_name,
            head_coach,play_caller_id,source_snapshot_id,row_digest,observed_at)
           VALUES %s ON CONFLICT(run_id,season,week,team) DO NOTHING""",
        [(
            run_id, row["season"], row["week"], row["team"], row["is_bye"], row["game_id"],
            row["game_date"], row["kickoff_at"], row["opponent"], row["is_home"], row["location"],
            row["stadium"], row["stadium_id"], row["roof"], row["surface"],
            row["quarterback_gsis_id"], row["quarterback_name"], row["head_coach"],
            row["play_caller_id"], row["source_snapshot_id"], row["row_digest"], row["observed_at"],
        ) for row in contexts],
    )
    _insert_values(
        cursor,
        """INSERT INTO ff_v2_roster_weeks
           (run_id,season,week,player_gsis_id,player_name,position,depth_chart_position,
            team,roster_status,resolution_method,effective_at,source_snapshot_id,row_digest,observed_at)
           VALUES %s ON CONFLICT(run_id,season,week,player_gsis_id) DO NOTHING""",
        [(
            run_id, row["season"], row["week"], row["player_gsis_id"], row["player_name"],
            row["position"], row["depth_chart_position"], row["team"], row["roster_status"],
            row["resolution_method"], row["effective_at"], row["source_snapshot_id"],
            row["row_digest"], row["observed_at"],
        ) for row in roster_rows],
    )
    _insert_values(
        cursor,
        """INSERT INTO ff_v2_transactions
           (run_id,player_gsis_id,player_name,from_team,to_team,effective_at,transaction_type,
            source_snapshot_id,evidence,row_digest,observed_at)
           VALUES %s ON CONFLICT(run_id,player_gsis_id,effective_at,to_team) DO NOTHING""",
        [(
            run_id, row["player_gsis_id"], row["player_name"], row["from_team"], row["to_team"],
            row["effective_at"], row["transaction_type"], row["source_snapshot_id"],
            Json(row["evidence"]), row["row_digest"], row["observed_at"],
        ) for row in transactions],
    )
    _insert_values(
        cursor,
        """INSERT INTO ff_v2_team_week_facts
           (run_id,season,week,game_id,game_date,team,opponent,plays,drives,pass_attempts,
            dropbacks,sacks,allocatable_targets,rush_attempts,rb_carries,rb_targets,
            pass_touchdowns,rush_touchdowns,red_zone_trips,goal_line_carries,end_zone_targets,
            neutral_pass_rate,seconds_per_play,score_state_features,quarterback_gsis_id,
            quarterback_name,play_caller_id,source_snapshot_ids,derivation,fact_digest,observed_at)
           VALUES %s ON CONFLICT(run_id,game_id,team) DO NOTHING""",
        [(
            run_id, row["season"], row["week"], row["game_id"], row["game_date"], row["team"],
            row["opponent"], row["plays"], row["drives"], row["pass_attempts"], row["dropbacks"],
            row["sacks"], row["allocatable_targets"], row["rush_attempts"], row["rb_carries"],
            row["rb_targets"], row["pass_touchdowns"], row["rush_touchdowns"],
            row["red_zone_trips"], row["goal_line_carries"], row["end_zone_targets"],
            row["neutral_pass_rate"], row["seconds_per_play"], Json(row["score_state_features"]),
            row["quarterback_gsis_id"], row["quarterback_name"], row["play_caller_id"],
            Json(row["source_snapshot_ids"]), Json(row["derivation"]), row["fact_digest"],
            row["observed_at"],
        ) for row in facts],
    )


def _source_entry(loaded: LoadedSource) -> dict[str, Any]:
    return {
        "contractKey": loaded.contract_key,
        "season": loaded.season,
        "url": loaded.url,
        "cachePath": loaded.cache_path,
        "responseHash": loaded.response_hash,
        "fetchedAt": loaded.fetched_at.isoformat(),
        "sourcePublishedAt": loaded.source_published_at.isoformat() if loaded.source_published_at else None,
        "rowCount": len(loaded.frame),
        "sourceSnapshotId": loaded.snapshot_id,
    }


def _load_cached_entry(entry: Mapping[str, Any]) -> LoadedSource:
    path = Path(str(entry["cachePath"]))
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != entry["responseHash"]:
        raise RuntimeError(f"Cached source hash mismatch: {path}")
    return LoadedSource(
        contract_key=str(entry["contractKey"]),
        season=int(entry["season"]),
        url=str(entry["url"]),
        cache_path=str(path),
        response_hash=digest,
        fetched_at=datetime.fromisoformat(str(entry["fetchedAt"])),
        source_published_at=(
            datetime.fromisoformat(str(entry["sourcePublishedAt"]))
            if entry.get("sourcePublishedAt") else None
        ),
        frame=_read_frame(payload, path.suffix.lower()),
        snapshot_id=int(entry["sourceSnapshotId"]),
    )


def build_context_bundle(sources: Mapping[str, LoadedSource], seasons: list[int]) -> dict[str, Any]:
    schedule_source = sources["schedule:all"]
    if schedule_source.snapshot_id is None:
        raise ValueError("schedule source must have a persisted snapshot id")
    contexts, week_starts, game_team_context = build_schedule_context(
        schedule_source.frame,
        seasons,
        schedule_source.snapshot_id,
    )
    roster_rows: list[dict[str, Any]] = []
    facts: list[dict[str, Any]] = []
    roster_conflicts: dict[int, dict[str, int]] = {}
    fact_missingness: dict[int, dict[str, int]] = {}

    for season in seasons:
        roster_source = sources[f"weekly-rosters:{season}"]
        stats_source = sources[f"weekly-stats:{season}"]
        pbp_source = sources[f"play-by-play:{season}"]
        participation_source = sources[f"participation:{season}"]
        if any(source.snapshot_id is None for source in (roster_source, stats_source, pbp_source, participation_source)):
            raise ValueError(f"season {season} sources must have persisted snapshot ids")
        season_rosters, conflicts = build_roster_rows(
            roster_source.frame,
            stats_source.frame,
            source_snapshot_id=int(roster_source.snapshot_id),
            week_starts=week_starts,
        )
        roster_rows.extend(season_rosters)
        roster_conflicts[season] = conflicts
        season_facts, missingness = build_team_week_facts(
            pbp_source.frame,
            stats_source.frame,
            roster_source.frame,
            season=season,
            source_snapshot_ids={
                "play_by_play": int(pbp_source.snapshot_id),
                "weekly_stats": int(stats_source.snapshot_id),
                "weekly_rosters": int(roster_source.snapshot_id),
                "schedule": int(schedule_source.snapshot_id),
                "participation": int(participation_source.snapshot_id),
            },
            game_team_context=game_team_context,
        )
        facts.extend(season_facts)
        fact_missingness[season] = missingness

    transactions = build_transactions(roster_rows)
    source_partitions = {
        key: {
            "snapshot_id": source.snapshot_id,
            "response_hash": source.response_hash,
            "row_count": len(source.frame),
            "source_published_at": source.source_published_at.isoformat() if source.source_published_at else None,
            "cache_path": source.cache_path,
        }
        for key, source in sorted(sources.items())
    }
    coverage = build_coverage_report(
        seasons=seasons,
        contexts=contexts,
        roster_rows=roster_rows,
        transactions=transactions,
        facts=facts,
        schedule=schedule_source.frame,
        roster_conflicts=roster_conflicts,
        fact_missingness=fact_missingness,
        source_partitions=source_partitions,
    )
    source_hashes = {key: source.response_hash for key, source in sources.items()}
    digest = artifact_digest(source_hashes, contexts, roster_rows, transactions, facts)
    input_digest = canonical_digest({"transform_version": TRANSFORM_VERSION, "sources": source_hashes})
    run_id = str(uuid.uuid5(RUN_NAMESPACE, input_digest))
    representative_facts: list[dict[str, Any]] = []
    for season in seasons:
        candidates = sorted(
            (row for row in facts if row["season"] == season),
            key=lambda row: (row["week"], row["game_id"], row["team"]),
        )
        representative_facts.extend(candidates[:1])
    return {
        "run_id": run_id,
        "input_digest": input_digest,
        "artifact_digest": digest,
        "contexts": contexts,
        "roster_rows": roster_rows,
        "transactions": transactions,
        "facts": facts,
        "coverage": coverage,
        "source_hashes": source_hashes,
        "source_snapshot_ids": {key: int(source.snapshot_id) for key, source in sources.items() if source.snapshot_id is not None},
        "representative_facts": representative_facts,
    }


def _manifest(bundle: Mapping[str, Any], sources: Mapping[str, LoadedSource], seasons: list[int]) -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "artifactType": "fantasy-football-v2-historical-context",
        "transformVersion": TRANSFORM_VERSION,
        "runId": bundle["run_id"],
        "seasons": seasons,
        "inputDigest": bundle["input_digest"],
        "artifactDigest": bundle["artifact_digest"],
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "sources": {key: _source_entry(source) for key, source in sorted(sources.items())},
        "coverage": bundle["coverage"],
        "representativeFacts": [_clean(row) for row in bundle["representative_facts"]],
    }


def run(
    *,
    start_season: int,
    end_season: int,
    cache_root: Path = DEFAULT_CACHE_ROOT,
    artifact_path: Path = DEFAULT_ARTIFACT,
    refresh_sources: bool = False,
) -> dict[str, Any]:
    if start_season > end_season:
        raise ValueError("start_season cannot exceed end_season")
    seasons = list(range(start_season, end_season + 1))
    config = load_config()
    DatabaseManager(config.database_url)
    sources: dict[str, LoadedSource] = {}
    schedule = load_source(
        "schedule", end_season, SCHEDULE_URL,
        cache_root=cache_root, refresh=refresh_sources,
    )
    sources["schedule:all"] = schedule
    for season in seasons:
        for contract_key, template in SEASON_URLS.items():
            sources[f"{contract_key}:{season}"] = load_source(
                contract_key,
                season,
                template.format(season=season),
                cache_root=cache_root,
                refresh=refresh_sources,
            )

    # Source persistence is its own short transaction. The CPU-heavy
    # derivation below must not hold an idle Neon connection open for minutes.
    source_db = RefreshDatabase(config.database_url)
    try:
        sources = {
            key: persist_loaded_source(source_db, loaded, requested_seasons=seasons)
            for key, loaded in sources.items()
        }
        source_db.close()
    except Exception:
        if not source_db.conn.closed:
            source_db.close(error=True)
        raise

    bundle = build_context_bundle(sources, seasons)
    run_db = RefreshDatabase(config.database_url)
    try:
        persist_context_run(
            run_db,
            run_id=bundle["run_id"],
            seasons=seasons,
            source_snapshot_ids=bundle["source_snapshot_ids"],
            coverage=bundle["coverage"],
            digest=bundle["artifact_digest"],
            contexts=bundle["contexts"],
            roster_rows=bundle["roster_rows"],
            transactions=bundle["transactions"],
            facts=bundle["facts"],
        )
        manifest = _manifest(bundle, sources, seasons)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        run_db.close()
        return manifest
    except Exception:
        if not run_db.conn.closed:
            run_db.close(error=True)
        raise


def verify(artifact_path: Path) -> dict[str, Any]:
    manifest = json.loads(artifact_path.read_text(encoding="utf-8"))
    if manifest.get("transformVersion") != TRANSFORM_VERSION:
        raise ValueError(
            f"Artifact transform {manifest.get('transformVersion')} does not match {TRANSFORM_VERSION}"
        )
    sources = {key: _load_cached_entry(entry) for key, entry in manifest["sources"].items()}
    seasons = [int(season) for season in manifest["seasons"]]
    bundle = build_context_bundle(sources, seasons)
    if bundle["run_id"] != manifest["runId"]:
        raise RuntimeError("Historical context run id did not reproduce")
    if bundle["artifact_digest"] != manifest["artifactDigest"]:
        raise RuntimeError("Historical context artifact digest did not reproduce")

    config = load_config()
    manager = DatabaseManager(config.database_url)
    row = manager.execute_one(
        """SELECT artifact_digest,
                  (SELECT COUNT(*) FROM ff_v2_team_week_context WHERE run_id=%s) AS context_rows,
                  (SELECT COUNT(*) FROM ff_v2_roster_weeks WHERE run_id=%s) AS roster_rows,
                  (SELECT COUNT(*) FROM ff_v2_transactions WHERE run_id=%s) AS transactions,
                  (SELECT COUNT(*) FROM ff_v2_team_week_facts WHERE run_id=%s) AS fact_rows
           FROM ff_v2_context_runs WHERE run_id=%s""",
        (manifest["runId"],) * 5,
    )
    if not row or row["artifact_digest"] != manifest["artifactDigest"]:
        raise RuntimeError("Persisted historical context run is missing or has a different digest")
    expected = manifest["coverage"]["totals"]
    observed = {
        "context_rows": int(row["context_rows"]),
        "roster_rows": int(row["roster_rows"]),
        "transactions": int(row["transactions"]),
        "fact_rows": int(row["fact_rows"]),
    }
    if observed != {key: int(expected[key]) for key in observed}:
        raise RuntimeError(f"Persisted historical context counts differ: {observed} vs {expected}")
    return {
        "status": "verified",
        "runId": manifest["runId"],
        "artifactDigest": manifest["artifactDigest"],
        "counts": observed,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-season", type=int, default=2020)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument("--refresh-sources", action="store_true")
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    result = (
        verify(args.verify)
        if args.verify
        else run(
            start_season=args.start_season,
            end_season=args.end_season,
            cache_root=args.cache_root,
            artifact_path=args.artifact,
            refresh_sources=args.refresh_sources,
        )
    )
    print(json.dumps(_clean(result), indent=2, sort_keys=True))
