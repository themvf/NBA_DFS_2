"""FantasyPros + nflverse ingestion for the Fantasy Football draft assistant.

The API key is read only from FANTASYPROS_API_KEY and is never logged. Browser
traffic does not call FantasyPros; scheduled jobs materialize immutable ranking
sets consumed by the web app.

Usage:
    python -m ingest.ff_fantasypros --season 2026
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from psycopg2.extras import Json
from psycopg2.extras import RealDictCursor

from config import load_config
from db.database import DatabaseManager

BASE_URL = "https://api.fantasypros.com/public/v2/json"
NFLVERSE_SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"
POSITIONS = {"QB", "RB", "WR", "TE", "K", "DST"}
SCORING_TYPES = ("STD", "HALF", "PPR")


@dataclass(frozen=True)
class FantasyProsEndpointContract:
    dataset: str
    path: str
    params: dict[str, Any]
    row_key: str
    minimum_rows: int
    required: bool = True


def fantasypros_endpoint_contracts(season: int) -> list[FantasyProsEndpointContract]:
    """Return the server-side FantasyPros datasets used by the draft product.

    Minimums are deliberately conservative. Their purpose is to reject sample
    responses (for example, a ten-player free-tier payload), not to enforce an
    exact player count that could change during the offseason.
    """
    contracts = [
        FantasyProsEndpointContract(
            dataset="players",
            path="nfl/players",
            params={"ecr": "included", "show": "pos_rank", "external_ids": "yahoo:espn:cbs:nfl:mfl:draftkings"},
            row_key="players",
            minimum_rows=100,
        ),
        FantasyProsEndpointContract(
            dataset="projections",
            path=f"nfl/{season}/projections",
            params={"week": 0, "positions": "QB:RB:WR:TE:K:DST"},
            row_key="players",
            minimum_rows=100,
        ),
        FantasyProsEndpointContract(
            dataset="injuries",
            path="nfl/injuries",
            params={"year": season, "week": 0, "include_probabilities": "true"},
            row_key="injuries",
            minimum_rows=0,
            required=False,
        ),
    ]
    for scoring in SCORING_TYPES:
        contracts.extend([
            FantasyProsEndpointContract(
                dataset=f"draft-rankings-{scoring.lower()}",
                path=f"nfl/{season}/consensus-rankings",
                params={"position": "ALL", "type": "DRAFT", "scoring": scoring, "week": 0},
                row_key="players",
                minimum_rows=100,
            ),
            FantasyProsEndpointContract(
                dataset=f"adp-{scoring.lower()}",
                path=f"nfl/{season}/consensus-rankings",
                params={"position": "ALL", "type": "ADP", "scoring": scoring, "week": 0},
                row_key="players",
                minimum_rows=100,
            ),
        ])
    return contracts


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    text = re.sub(r"\b(jr|sr|ii|iii|iv)\.?\b", "", text, flags=re.I)
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def as_float(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    number = as_float(value)
    return int(number) if number is not None else None


def position_rank(value: Any) -> int | None:
    match = re.search(r"(\d+)$", str(value or ""))
    return int(match.group(1)) if match else None


def projection_stats(value: Any) -> dict[str, float]:
    """Normalize both the documented list and legacy object stats shapes."""
    objects = value if isinstance(value, list) else [value]
    merged: dict[str, float] = {}
    for item in objects:
        if not isinstance(item, dict):
            continue
        for key, raw in item.items():
            number = as_float(raw)
            if number is not None:
                merged[str(key)] = number
    return merged


def source_points(stats: dict[str, float], scoring: str) -> float | None:
    key = {"STD": "points", "HALF": "points_half", "PPR": "points_ppr"}[scoring]
    return stats.get(key)


def historical_points(row: dict[str, Any], scoring: str) -> float | None:
    std = as_float(row.get("fantasy_points_std"))
    ppr = as_float(row.get("fantasy_points_ppr"))
    if scoring == "STD":
        return std
    if scoring == "PPR":
        return ppr
    return (std + ppr) / 2 if std is not None and ppr is not None else None


@dataclass(frozen=True)
class ModelProjection:
    points: float | None
    expected_games: float
    confidence: float
    explanation: dict[str, Any]


def build_model_projection(
    fantasypros_points: float | None,
    history: dict[str, Any] | None,
    *,
    scoring: str,
    rookie: bool,
    injured: bool,
) -> ModelProjection:
    """Transparent v1 blend: market prior plus availability-adjusted history."""
    games = as_int((history or {}).get("games")) or 0
    expected_games = 15.8 if games >= 14 else 15.0 if games >= 10 else 14.2
    if rookie:
        expected_games = 15.2
    if injured:
        expected_games = min(expected_games, 13.5)

    prior_points = historical_points(history or {}, scoring)
    history_season = None
    if prior_points is not None and games > 0:
        history_season = prior_points / games * expected_games

    if fantasypros_points is not None and history_season is not None and not rookie:
        points = 0.60 * fantasypros_points + 0.40 * history_season
        confidence = min(0.92, 0.62 + games / 60)
    elif fantasypros_points is not None:
        points = fantasypros_points
        confidence = 0.56 if rookie else 0.60
    else:
        points = history_season
        confidence = 0.42 if history_season is not None else 0.15

    if injured:
        confidence = max(0.15, confidence - 0.12)
    return ModelProjection(
        round(points, 2) if points is not None else None,
        expected_games,
        round(confidence, 3),
        {
            "model": "ff-v1-market-history-blend",
            "fantasypros_weight": 0.60 if history_season is not None and not rookie else 1.0,
            "historical_weight": 0.40 if history_season is not None and not rookie else 0.0,
            "history_games": games,
            "history_season_points": round(history_season, 2) if history_season is not None else None,
        },
    )


class FantasyProsClient:
    def __init__(self, api_key: str, timeout: int = 30) -> None:
        if not api_key:
            raise ValueError("FANTASYPROS_API_KEY is required")
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}/{path.lstrip('/')}"
        for attempt in range(4):
            response = self.session.get(
                url,
                params=params,
                headers={"x-api-key": self.api_key, "Accept": "application/json"},
                timeout=self.timeout,
            )
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == 3:
                    response.raise_for_status()
                wait = as_float(response.headers.get("Retry-After")) or 2**attempt
                time.sleep(min(wait, 30))
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Unexpected FantasyPros response for {path}")
            return payload
        raise RuntimeError(f"FantasyPros request failed for {path}")


def audit_fantasypros_endpoints(
    client: FantasyProsClient,
    season: int,
) -> dict[str, Any]:
    """Verify entitlement and response shape without retaining player data.

    The report is safe for a CI artifact: it contains request parameters,
    counts, hashes, timestamps, and error classifications, but never the API
    key or raw player records.
    """
    checked_at = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []
    for contract in fantasypros_endpoint_contracts(season):
        try:
            payload = client.get(contract.path, contract.params)
            rows = payload.get(contract.row_key)
            row_count = len(rows) if isinstance(rows, list) else 0
            passed = isinstance(rows, list) and row_count >= contract.minimum_rows
            status = "pass" if passed else "partial" if row_count else "fail"
            row_dicts = [row for row in (rows or []) if isinstance(row, dict)] if isinstance(rows, list) else []
            results.append({
                "dataset": contract.dataset,
                "path": contract.path,
                "request_params": contract.params,
                "row_key": contract.row_key,
                "row_count": row_count,
                "minimum_rows": contract.minimum_rows,
                "required": contract.required,
                "status": status,
                "response_hash": response_hash(payload),
                "source_updated_at": (
                    datetime.fromtimestamp(payload["last_updated_ts"], tz=timezone.utc).isoformat()
                    if as_int(payload.get("last_updated_ts"))
                    else None
                ),
                "top_level_keys": sorted(str(key) for key in payload),
                "row_field_keys": sorted({str(key) for row in row_dicts[:25] for key in row}),
                "identifier_field_counts": {
                    key: sum(as_int(row.get(key)) is not None for row in row_dicts)
                    for key in ("player_id", "fpid", "id")
                },
                "error_type": None,
                "http_status": None,
            })
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            results.append({
                "dataset": contract.dataset,
                "path": contract.path,
                "request_params": contract.params,
                "row_key": contract.row_key,
                "row_count": 0,
                "minimum_rows": contract.minimum_rows,
                "required": contract.required,
                "status": "unavailable",
                "response_hash": None,
                "source_updated_at": None,
                "top_level_keys": [],
                "row_field_keys": [],
                "identifier_field_counts": {},
                "error_type": type(exc).__name__,
                "http_status": getattr(response, "status_code", None),
            })
        except (TypeError, ValueError) as exc:
            results.append({
                "dataset": contract.dataset,
                "path": contract.path,
                "request_params": contract.params,
                "row_key": contract.row_key,
                "row_count": 0,
                "minimum_rows": contract.minimum_rows,
                "required": contract.required,
                "status": "invalid",
                "response_hash": None,
                "source_updated_at": None,
                "top_level_keys": [],
                "row_field_keys": [],
                "identifier_field_counts": {},
                "error_type": type(exc).__name__,
                "http_status": None,
            })

    required = [result for result in results if result["required"]]
    return {
        "source": "fantasypros",
        "season": season,
        "checked_at": checked_at,
        "contracts": results,
        "required_contracts": len(required),
        "passed_required_contracts": sum(result["status"] == "pass" for result in required),
        "all_required_contracts_pass": all(result["status"] == "pass" for result in required),
    }


def parse_season_range(value: str) -> list[int]:
    """Parse one season, a comma list, or an inclusive range."""
    text = value.strip()
    if not text:
        raise ValueError("At least one historical season is required")
    if ":" in text:
        start_text, end_text = text.split(":", 1)
        start, end = int(start_text), int(end_text)
        if start > end:
            raise ValueError("Historical season range must be ascending")
        return list(range(start, end + 1))
    seasons = sorted({int(item.strip()) for item in text.split(",") if item.strip()})
    if not seasons:
        raise ValueError("At least one historical season is required")
    return seasons


def first_regular_season_cutoffs(
    schedule: pd.DataFrame,
    seasons: list[int],
) -> dict[int, datetime]:
    """Return the first scheduled regular-season kickoff for each season.

    nflverse publishes `gameday` and `gametime` in US Eastern time. The first
    kickoff is the latest timestamp a preseason draft snapshot may use without
    seeing regular-season results.
    """
    required = {"season", "game_type", "week", "gameday", "gametime"}
    missing = required.difference(schedule.columns)
    if missing:
        raise ValueError(f"nflverse schedule missing fields: {sorted(missing)}")

    eastern = ZoneInfo("America/New_York")
    result: dict[int, datetime] = {}
    for season in seasons:
        rows = schedule[
            (schedule["season"] == season)
            & (schedule["game_type"] == "REG")
            & (schedule["week"] == 1)
        ]
        kickoffs: list[datetime] = []
        for row in rows.to_dict("records"):
            gameday = str(row.get("gameday") or "")
            gametime = str(row.get("gametime") or "")
            if not gameday or not gametime or gametime.lower() == "nan":
                continue
            local = datetime.fromisoformat(f"{gameday}T{gametime}").replace(tzinfo=eastern)
            kickoffs.append(local.astimezone(timezone.utc))
        if not kickoffs:
            raise ValueError(f"No dated Week 1 regular-season kickoff found for {season}")
        result[season] = min(kickoffs)
    return result


def audit_fantasypros_history(
    client: FantasyProsClient,
    seasons: list[int],
    schedule: pd.DataFrame,
) -> dict[str, Any]:
    """Audit historical contracts and enforce draft-time ADP eligibility.

    Endpoint access and draft-time eligibility are intentionally separate.
    A historical response can remain accessible today while its provider
    timestamp is too late for a simulated preseason decision.
    """
    normalized_seasons = sorted(set(seasons))
    cutoffs = first_regular_season_cutoffs(schedule, normalized_seasons)
    reports: list[dict[str, Any]] = []
    for season in normalized_seasons:
        report = audit_fantasypros_endpoints(client, season)
        adp = next(
            (item for item in report["contracts"] if item["dataset"] == "adp-ppr"),
            None,
        )
        source_updated_at = (
            datetime.fromisoformat(adp["source_updated_at"])
            if adp and adp.get("source_updated_at")
            else None
        )
        cutoff = cutoffs[season]
        adp_eligible = bool(
            adp
            and adp.get("status") == "pass"
            and source_updated_at is not None
            and source_updated_at <= cutoff
        )
        reports.append({
            **report,
            "decision_cutoff_at": cutoff.isoformat(),
            "ppr_adp_source_updated_at": adp.get("source_updated_at") if adp else None,
            "ppr_adp_rows": adp.get("row_count", 0) if adp else 0,
            "ppr_adp_response_hash": adp.get("response_hash") if adp else None,
            "ppr_adp_cutoff_eligible": adp_eligible,
        })

    return {
        "source": "fantasypros",
        "audit_type": "historical-draft-contracts",
        "cutoff_policy": "first scheduled regular-season kickoff from nflverse schedule",
        "seasons": normalized_seasons,
        "season_reports": reports,
        "all_required_contracts_pass": all(
            report["all_required_contracts_pass"] for report in reports
        ),
        "all_ppr_adp_cutoffs_eligible": all(
            report["ppr_adp_cutoff_eligible"] for report in reports
        ),
    }


def write_audit_report(report: dict[str, Any], output: str | None = None) -> None:
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if output:
        Path(output).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


def _fantasypros_player_identity(row: dict[str, Any]) -> tuple[int | None, str, str, str | None]:
    fp_id = as_int(row.get("player_id") or row.get("fpid"))
    name = normalize_name(str(row.get("player_name") or row.get("name") or ""))
    position = str(
        row.get("position_id")
        or row.get("player_position_id")
        or row.get("player_positions")
        or ""
    ).split(",")[0]
    team = str(row.get("team_id") or row.get("player_team_id") or "") or None
    return fp_id, name, position, team


def link_fantasypros_players(
    db: DatabaseManager | RefreshDatabase,
    season: int,
    payload: dict[str, Any],
) -> dict[str, int]:
    """Attach FantasyPros IDs to the independent canonical player rows.

    No FantasyPros-only player row is inserted here. That boundary prevents a
    partial or differently scoped vendor directory from replacing the
    nflverse/Sleeper universe used by the Best Ball board.
    """
    existing = db.execute(
        """SELECT id,normalized_name,position,team_abbrev,fantasypros_player_id
           FROM ff_players WHERE season=%s""",
        (season,),
    )
    by_fp = {
        int(row["fantasypros_player_id"]): row
        for row in existing
        if as_int(row.get("fantasypros_player_id")) is not None
    }
    by_identity: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in existing:
        by_identity.setdefault((str(row["normalized_name"]), str(row["position"])), []).append(row)

    matched = 0
    linked = 0
    ambiguous = 0
    unsupported = 0
    seen_fp_ids: set[int] = set()
    for source_row in payload.get("players", []):
        if not isinstance(source_row, dict):
            unsupported += 1
            continue
        fp_id, name, position, team = _fantasypros_player_identity(source_row)
        if fp_id is None or not name or position not in POSITIONS:
            unsupported += 1
            continue
        seen_fp_ids.add(fp_id)
        if fp_id in by_fp:
            matched += 1
            continue
        candidates = by_identity.get((name, position), [])
        if team:
            same_team = [row for row in candidates if row.get("team_abbrev") == team]
            if len(same_team) == 1:
                candidates = same_team
        if len(candidates) != 1:
            ambiguous += 1 if candidates else 0
            continue
        canonical = candidates[0]
        db.execute(
            """UPDATE ff_players SET fantasypros_player_id=%s,fetched_at=NOW()
               WHERE id=%s AND fantasypros_player_id IS NULL""",
            (fp_id, canonical["id"]),
        )
        canonical["fantasypros_player_id"] = fp_id
        by_fp[fp_id] = canonical
        matched += 1
        linked += 1
    return {
        "source_rows": len(payload.get("players", [])),
        "matched": matched,
        "linked": linked,
        "unmatched": max(0, len(seen_fp_ids) - matched),
        "ambiguous": ambiguous,
        "unsupported": unsupported,
    }


def count_fantasypros_payload_matches(
    db: DatabaseManager | RefreshDatabase,
    season: int,
    rows: list[dict[str, Any]],
) -> int:
    """Count source rows that resolve by vendor ID or canonical identity."""
    players = db.execute(
        """SELECT normalized_name,position,team_abbrev,fantasypros_player_id
           FROM ff_players WHERE season=%s""",
        (season,),
    )
    known_ids = {
        int(player["fantasypros_player_id"])
        for player in players
        if as_int(player.get("fantasypros_player_id")) is not None
    }
    identities = {
        (str(player["normalized_name"]), str(player["position"]), str(player.get("team_abbrev") or ""))
        for player in players
    }
    identity_without_team: dict[tuple[str, str], int] = {}
    for name, position, _team in identities:
        identity_without_team[(name, position)] = identity_without_team.get((name, position), 0) + 1

    matched = 0
    for row in rows:
        fp_id, name, position, team = _fantasypros_player_identity(row)
        if fp_id is not None and fp_id in known_ids:
            matched += 1
        elif (name, position, str(team or "")) in identities:
            matched += 1
        elif name and position and identity_without_team.get((name, position)) == 1:
            matched += 1
    return matched


def persist_fantasypros_projections(
    db: DatabaseManager | RefreshDatabase,
    season: int,
    snapshot_id: int,
    payload: dict[str, Any],
) -> dict[str, int]:
    """Persist source-attributable projections without letting the source own the board."""
    players = db.execute(
        """SELECT id,normalized_name,position,team_abbrev,fantasypros_player_id,
                  sleeper_player_id,gsis_id
           FROM ff_players WHERE season=%s""",
        (season,),
    )
    by_fp = {
        int(player["fantasypros_player_id"]): player
        for player in players
        if as_int(player.get("fantasypros_player_id")) is not None
    }
    by_identity: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for player in players:
        by_identity.setdefault(
            (str(player["normalized_name"]), str(player["position"])),
            [],
        ).append(player)

    matched_players = 0
    unmatched_players = 0
    values_written = 0
    score_counts = {scoring: 0 for scoring in SCORING_TYPES}
    for source_row in payload.get("players", []):
        if not isinstance(source_row, dict):
            unmatched_players += 1
            continue
        fp_id, name, position, team = _fantasypros_player_identity(source_row)
        candidates = by_identity.get((name, position), []) if name and position else []
        independent_candidates = [
            candidate
            for candidate in candidates
            if candidate.get("sleeper_player_id") or candidate.get("gsis_id")
        ]
        player = independent_candidates[0] if len(independent_candidates) == 1 else by_fp.get(fp_id or -1)
        match_method = "canonical_independent_identity" if len(independent_candidates) == 1 else "fantasypros_player_id"
        if player is None and candidates:
            if team:
                same_team = [candidate for candidate in candidates if candidate.get("team_abbrev") == team]
                if len(same_team) == 1:
                    candidates = same_team
            if len(candidates) == 1:
                player = candidates[0]
                match_method = "canonical_identity"
        if player is None:
            unmatched_players += 1
            continue

        matched_players += 1
        stats = projection_stats(source_row.get("stats"))
        for scoring in SCORING_TYPES:
            points = source_points(stats, scoring)
            db.execute(
                """INSERT INTO ff_player_source_projections
                   (source_snapshot_id,player_id,source,season,scoring,
                    projected_points,projected_stats,match_method)
                   VALUES (%s,%s,'fantasypros',%s,%s,%s,%s,%s)
                   ON CONFLICT (source_snapshot_id,player_id,scoring) DO NOTHING""",
                (
                    snapshot_id,
                    player["id"],
                    season,
                    scoring,
                    points,
                    Json(stats),
                    match_method,
                ),
            )
            values_written += 1
            if points is not None:
                score_counts[scoring] += 1
    return {
        "matched_players": matched_players,
        "unmatched_players": unmatched_players,
        "values_written": values_written,
        **{f"{scoring.lower()}_scores": count for scoring, count in score_counts.items()},
    }


def snapshot_fantasypros_contracts(
    db: DatabaseManager | RefreshDatabase,
    client: FantasyProsClient,
    season: int,
) -> dict[str, Any]:
    """Persist distinct immutable metadata for each verified vendor dataset."""
    payloads: list[tuple[FantasyProsEndpointContract, dict[str, Any]]] = []
    for contract in fantasypros_endpoint_contracts(season):
        try:
            payload = client.get(contract.path, contract.params)
        except requests.RequestException:
            if contract.required:
                raise
            continue
        rows = payload.get(contract.row_key)
        row_count = len(rows) if isinstance(rows, list) else 0
        if contract.required and (not isinstance(rows, list) or row_count < contract.minimum_rows):
            raise RuntimeError(
                f"FantasyPros {contract.dataset} contract failed: "
                f"received {row_count} rows; require at least {contract.minimum_rows}"
            )
        payloads.append((contract, payload))

    player_payload = next(payload for contract, payload in payloads if contract.dataset == "players")
    identity = link_fantasypros_players(db, season, player_payload)
    saved: list[dict[str, Any]] = []
    for contract, payload in payloads:
        rows = payload.get(contract.row_key)
        row_dicts = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        matched_count = count_fantasypros_payload_matches(db, season, row_dicts)
        snapshot_id = snapshot(
            db,
            dataset=contract.dataset,
            season=season,
            payload=payload,
            params=contract.params,
            scoring=str(contract.params.get("scoring")) if contract.params.get("scoring") else None,
            ranking_type=str(contract.params.get("type")) if contract.params.get("type") else None,
        )
        db.execute(
            "UPDATE ff_source_snapshots SET matched_count=%s,unmatched_count=%s WHERE id=%s",
            (matched_count, max(0, len(row_dicts) - matched_count), snapshot_id),
        )
        saved_row: dict[str, Any] = {
            "dataset": contract.dataset,
            "snapshot_id": snapshot_id,
            "row_count": len(rows) if isinstance(rows, list) else 0,
            "matched_count": matched_count,
            "unmatched_count": max(0, len(row_dicts) - matched_count),
            "response_hash": response_hash(payload),
        }
        if contract.dataset == "projections":
            saved_row["player_values"] = persist_fantasypros_projections(
                db,
                season,
                snapshot_id,
                payload,
            )
        saved.append(saved_row)
    return {"season": season, "identity": identity, "snapshots": saved}


class RefreshDatabase:
    """One connection/transaction for an entire refresh.

    DatabaseManager deliberately opens a connection per helper call, which is
    convenient for ordinary jobs but far too slow for hundreds of player
    upserts against Neon. This scoped adapter preserves the same tiny interface.
    """

    def __init__(self, database_url: str) -> None:
        import psycopg2

        self.conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)

    def execute(self, statement: str, params: Any = None) -> list[dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(statement, params or ())
        try:
            return list(cursor.fetchall())
        except Exception:
            return []

    def execute_one(self, statement: str, params: Any = None) -> dict[str, Any] | None:
        cursor = self.conn.cursor()
        cursor.execute(statement, params or ())
        return cursor.fetchone()

    def close(self, error: bool = False) -> None:
        try:
            self.conn.rollback() if error else self.conn.commit()
        finally:
            self.conn.close()


def response_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def snapshot(
    db: DatabaseManager | RefreshDatabase,
    *,
    dataset: str,
    season: int,
    payload: dict[str, Any],
    params: dict[str, Any],
    scoring: str | None = None,
    ranking_type: str | None = None,
) -> int:
    rows = payload.get("players") or payload.get("injuries") or payload.get("items") or []
    digest = response_hash(payload)
    result = db.execute_one(
        """INSERT INTO ff_source_snapshots
           (source, dataset, season, scoring, ranking_type, request_params,
            source_updated_at, response_hash, row_count, status)
           VALUES ('fantasypros', %s, %s, %s, %s, %s, %s, %s, %s, 'success')
           ON CONFLICT (source, dataset, response_hash)
           DO UPDATE SET fetched_at = NOW()
           RETURNING id""",
        (
            dataset,
            season,
            scoring,
            ranking_type,
            Json(params),
            datetime.fromtimestamp(payload["last_updated_ts"], tz=timezone.utc)
            if as_int(payload.get("last_updated_ts"))
            else None,
            digest,
            len(rows) if isinstance(rows, list) else 0,
        ),
    )
    return int(result["id"])


def upsert_players(db: DatabaseManager | RefreshDatabase, season: int, payload: dict[str, Any]) -> dict[int, int]:
    result: dict[int, int] = {}
    for row in payload.get("players", []):
        fp_id = as_int(row.get("player_id") or row.get("fpid"))
        position = str(row.get("position_id") or row.get("player_position_id") or row.get("player_positions") or "").split(",")[0]
        if not fp_id or position not in POSITIONS:
            continue
        team = str(row.get("team_id") or row.get("player_team_id") or "") or None
        team_row = db.execute_one("SELECT team_id FROM nfl_teams WHERE abbreviation = %s", (team,)) if team else None
        external = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
        saved = db.execute_one(
            """INSERT INTO ff_players
               (season, canonical_name, normalized_name, position, nfl_team_id,
                team_abbrev, fantasypros_player_id, gsis_id, espn_id, yahoo_id,
                mfl_id, draftkings_id, rookie, bye_week, metadata, fetched_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
               ON CONFLICT (season, fantasypros_player_id) DO UPDATE SET
                 canonical_name=EXCLUDED.canonical_name, normalized_name=EXCLUDED.normalized_name,
                 position=EXCLUDED.position, nfl_team_id=EXCLUDED.nfl_team_id,
                 team_abbrev=EXCLUDED.team_abbrev, gsis_id=COALESCE(EXCLUDED.gsis_id, ff_players.gsis_id),
                 espn_id=COALESCE(EXCLUDED.espn_id, ff_players.espn_id),
                 yahoo_id=COALESCE(EXCLUDED.yahoo_id, ff_players.yahoo_id),
                 mfl_id=COALESCE(EXCLUDED.mfl_id, ff_players.mfl_id),
                 draftkings_id=COALESCE(EXCLUDED.draftkings_id, ff_players.draftkings_id),
                 rookie=EXCLUDED.rookie, bye_week=COALESCE(EXCLUDED.bye_week, ff_players.bye_week),
                 metadata=EXCLUDED.metadata, fetched_at=NOW()
               RETURNING id""",
            (
                season,
                row.get("player_name") or row.get("name"),
                normalize_name(row.get("player_name") or row.get("name") or ""),
                position,
                team_row["team_id"] if team_row else None,
                team,
                fp_id,
                external.get("nfl") or row.get("nfl_id"),
                external.get("espn") or row.get("espn_id"),
                external.get("yahoo") or row.get("yahoo_id"),
                external.get("mfl") or row.get("mfl_id"),
                external.get("draftkings") or row.get("draftkings_id"),
                str(row.get("rookie") or "").upper() in {"Y", "1", "TRUE"},
                as_int(row.get("bye_week") or row.get("player_bye_week")),
                Json(row),
            ),
        )
        result[fp_id] = int(saved["id"])
    return result


def ingest_nflverse_history(db: DatabaseManager | RefreshDatabase, season: int) -> dict[int, dict[str, Any]]:
    prior = season - 1
    url = f"https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_reg_{prior}.csv"
    frame = pd.read_csv(url)
    frame = frame[frame["position"].isin(POSITIONS)].copy()
    for numeric in ("targets", "carries", "rushing_tds"):
        if numeric not in frame:
            frame[numeric] = 0
    frame["team_target_rank"] = frame.groupby("recent_team")["targets"].rank(method="min", ascending=False)
    frame["team_rush_rank"] = frame.groupby("recent_team")["carries"].rank(method="min", ascending=False)
    frame["nfl_target_rank"] = frame["targets"].rank(method="min", ascending=False)
    frame["nfl_rush_td_rank"] = frame["rushing_tds"].rank(method="min", ascending=False)
    team_targets = frame.groupby("recent_team")["targets"].transform("sum").replace(0, pd.NA)
    team_carries = frame.groupby("recent_team")["carries"].transform("sum").replace(0, pd.NA)
    frame["target_share"] = frame["targets"] / team_targets
    frame["rush_share"] = frame["carries"] / team_carries

    players = db.execute("SELECT id, normalized_name, position, team_abbrev FROM ff_players WHERE season=%s", (season,))
    by_key = {(p["normalized_name"], p["position"]): p for p in players}
    history: dict[int, dict[str, Any]] = {}
    for _, series in frame.iterrows():
        row = series.where(pd.notna(series), None).to_dict()
        player = by_key.get((normalize_name(str(row.get("player_display_name") or row.get("player_name") or "")), row.get("position")))
        if not player:
            continue
        values = {
            "games": as_int(row.get("games")),
            "fantasy_points_std": as_float(row.get("fantasy_points")),
            "fantasy_points_ppr": as_float(row.get("fantasy_points_ppr")),
            "targets": as_float(row.get("targets")),
            "receptions": as_float(row.get("receptions")),
            "receiving_yards": as_float(row.get("receiving_yards")),
            "receiving_tds": as_float(row.get("receiving_tds")),
            "carries": as_float(row.get("carries")),
            "rushing_yards": as_float(row.get("rushing_yards")),
            "rushing_tds": as_float(row.get("rushing_tds")),
            "target_share": as_float(row.get("target_share")),
            "rush_share": as_float(row.get("rush_share")),
            "team_target_rank": as_int(row.get("team_target_rank")),
            "team_rush_rank": as_int(row.get("team_rush_rank")),
            "nfl_target_rank": as_int(row.get("nfl_target_rank")),
            "nfl_rush_td_rank": as_int(row.get("nfl_rush_td_rank")),
            "prior_team": row.get("recent_team"),
        }
        db.execute(
            """INSERT INTO ff_player_season_features
               (player_id,season,source,games,fantasy_points_std,fantasy_points_ppr,
                targets,receptions,receiving_yards,receiving_tds,carries,rushing_yards,
                rushing_tds,target_share,rush_share,team_target_rank,team_rush_rank,
                nfl_target_rank,nfl_rush_td_rank,source_row,fetched_at)
               VALUES (%s,%s,'nflverse',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
               ON CONFLICT(player_id,season,source) DO UPDATE SET
                 games=EXCLUDED.games,fantasy_points_std=EXCLUDED.fantasy_points_std,
                 fantasy_points_ppr=EXCLUDED.fantasy_points_ppr,targets=EXCLUDED.targets,
                 receptions=EXCLUDED.receptions,receiving_yards=EXCLUDED.receiving_yards,
                 receiving_tds=EXCLUDED.receiving_tds,carries=EXCLUDED.carries,
                 rushing_yards=EXCLUDED.rushing_yards,rushing_tds=EXCLUDED.rushing_tds,
                 target_share=EXCLUDED.target_share,rush_share=EXCLUDED.rush_share,
                 team_target_rank=EXCLUDED.team_target_rank,team_rush_rank=EXCLUDED.team_rush_rank,
                 nfl_target_rank=EXCLUDED.nfl_target_rank,nfl_rush_td_rank=EXCLUDED.nfl_rush_td_rank,
                 source_row=EXCLUDED.source_row,fetched_at=NOW()""",
            (player["id"], prior, *[values[k] for k in (
                "games", "fantasy_points_std", "fantasy_points_ppr", "targets", "receptions",
                "receiving_yards", "receiving_tds", "carries", "rushing_yards", "rushing_tds",
                "target_share", "rush_share", "team_target_rank", "team_rush_rank",
                "nfl_target_rank", "nfl_rush_td_rank")], Json(values)),
        )
        history[int(player["id"])] = values
    return history


def save_injuries(db: DatabaseManager | RefreshDatabase, season: int, payload: dict[str, Any], fp_map: dict[int, int]) -> set[int]:
    injured: set[int] = set()
    for row in payload.get("injuries", []):
        player_id = fp_map.get(as_int(row.get("player_id")) or -1)
        if not player_id:
            continue
        status = row.get("status") or row.get("status_short")
        if status:
            injured.add(player_id)
            db.execute("UPDATE ff_players SET injury_status=%s, metadata=metadata || %s::jsonb WHERE id=%s", (status, json.dumps({"injury": row}), player_id))
    return injured


def create_indicators(
    db: DatabaseManager | RefreshDatabase,
    ranking_set_id: int,
    season: int,
    rows: list[dict[str, Any]],
    history: dict[int, dict[str, Any]],
    scoring: str = "PPR",
) -> None:
    def current_team(row: dict[str, Any]) -> str:
        return str(row.get("team_abbrev") or row.get("team") or "")

    team_roles: dict[tuple[str, str, int], int] = {}
    for team in {current_team(r) for r in rows}:
        for position in ("WR", "RB", "TE", "QB"):
            candidates = sorted(
                [r for r in rows if current_team(r) == team and r["position"] == position],
                key=lambda r: r.get("our_rank") or r.get("overall_rank") or 9999,
            )
            for role_rank, candidate in enumerate(candidates, start=1):
                team_roles[(team, position, candidate["player_id"])] = role_rank

    def history_points(hist: dict[str, Any]) -> float:
        std = float(hist.get("fantasy_points_std") or 0.0)
        ppr = float(hist.get("fantasy_points_ppr") or 0.0)
        return std if scoring == "STD" else ppr if scoring == "PPR" else (std + ppr) / 2.0

    point_leaders: dict[tuple[str, int], tuple[int, float]] = {}
    for position in ("QB", "RB", "WR", "TE", "K", "DST"):
        candidates = sorted(
            (
                (row["player_id"], history_points(history[row["player_id"]]))
                for row in rows
                if row["position"] == position
                and row["player_id"] in history
                and (history[row["player_id"]].get("games") or 0) > 0
            ),
            key=lambda item: (-item[1], item[0]),
        )
        for points_rank, (player_id, points) in enumerate(candidates[:3], start=1):
            point_leaders[(position, player_id)] = (points_rank, points)

    for row in rows:
        codes: list[tuple[str, str, str, float | None, dict[str, Any]]] = []
        hist = history.get(row["player_id"], {})
        team = current_team(row)
        if row.get("rookie"):
            codes.append(("ROOKIE", "fact", "ROOKIE", None, {"source": "player metadata"}))
        if row.get("injury_status"):
            codes.append(("INJURY", "risk", str(row["injury_status"]).upper(), None, {"status": row["injury_status"]}))
        points_leader = point_leaders.get((row["position"], row["player_id"]))
        if points_leader:
            points_rank, points = points_leader
            codes.append((
                "TOP_3_POSITION_POINTS",
                "fact",
                f"{season-1} {row['position']} FPTS #{points_rank}",
                points,
                {"season": season - 1, "scoring": scoring, "position": row["position"], "rank": points_rank, "points": points},
            ))
        role_rank = team_roles.get((team, row["position"], row["player_id"]))
        if row["position"] in {"WR", "RB"} and role_rank and role_rank <= 2:
            label = f"TEAM {row['position']}{role_rank}"
            codes.append((label.replace(" ", "_"), "role", label, None, {"basis": "projected positional order"}))
        if row["position"] == "RB" and role_rank == 2:
            codes.append(("HANDCUFF_CANDIDATE", "role", "HANDCUFF CANDIDATE", None, {"requires_depth_chart_confirmation": True}))
        if hist.get("team_target_rank") == 1 and (hist.get("targets") or 0) >= 40:
            codes.append(("TEAM_TARGET_LEADER", "fact", f"{hist.get('prior_team') or 'TEAM'} TARGET LEADER {season-1}", hist.get("targets"), hist))
        if (hist.get("nfl_target_rank") or 999) <= 10:
            codes.append(("NFL_TOP_10_TARGETS", "fact", f"NFL TOP-10 TARGETS {season-1}", hist.get("targets"), hist))
        if (hist.get("nfl_rush_td_rank") or 999) <= 10:
            codes.append(("NFL_TOP_10_RUSH_TDS", "fact", f"NFL TOP-10 RUSH TDS {season-1}", hist.get("rushing_tds"), hist))
        if hist.get("prior_team") and team and hist["prior_team"] != team:
            codes.append((
                "NEW_TEAM",
                "fact",
                f"NEW TEAM: {hist['prior_team']} → {team}",
                None,
                {"from": hist["prior_team"], "to": team},
            ))
        if row.get("adp") is not None:
            delta = float(row["adp"]) - float(row.get("our_rank") or row.get("overall_rank") or 0)
            if delta >= 12:
                codes.append(("OUR_BUY", "model", "OUR BUY", delta, {"adp_delta": delta}))
            elif delta <= -12:
                codes.append(("OUR_FADE", "model", "OUR FADE", delta, {"adp_delta": delta}))
        for code, klass, label, value, evidence in codes:
            db.execute(
                """INSERT INTO ff_player_indicators
                   (ranking_set_id,player_id,indicator_code,indicator_class,label,metric_value,confidence,season,evidence)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(ranking_set_id,player_id,indicator_code) DO UPDATE SET
                     label=EXCLUDED.label,metric_value=EXCLUDED.metric_value,
                     confidence=EXCLUDED.confidence,evidence=EXCLUDED.evidence""",
                (ranking_set_id, row["player_id"], code, klass, label, value, row.get("confidence"), season, Json(evidence)),
            )


def assign_our_ranks(db: DatabaseManager | RefreshDatabase, ranking_set_id: int, rows: list[dict[str, Any]]) -> None:
    """Rank league-specific value above a default 12-team replacement baseline."""
    demand = {"QB": 12, "RB": 36, "WR": 48, "TE": 12, "K": 12, "DST": 12}
    replacements: dict[str, float] = {}
    for position, required in demand.items():
        points = sorted(
            [float(r["our_projected_points"]) for r in rows if r["position"] == position and r.get("our_projected_points") is not None],
            reverse=True,
        )
        replacements[position] = points[min(required, len(points)) - 1] if points else 0.0
    for row in rows:
        points = row.get("our_projected_points")
        row["vor"] = float(points) - replacements.get(row["position"], 0.0) if points is not None else -999.0
    ordered = sorted(rows, key=lambda r: (-r["vor"], r.get("overall_rank") or 9999))
    for rank, row in enumerate(ordered, start=1):
        row["our_rank"] = rank
        db.execute(
            "UPDATE ff_player_rankings SET our_rank=%s,notes=COALESCE(notes,'') || %s WHERE ranking_set_id=%s AND player_id=%s",
            (rank, f"; VOR={row['vor']:.2f}", ranking_set_id, row["player_id"]),
        )


def _run_ingestion(season: int, db: RefreshDatabase, client: FantasyProsClient) -> dict[str, Any]:
    players_params = {"ecr": "included", "show": "pos_rank", "external_ids": "yahoo:espn:cbs:nfl:mfl:draftkings"}
    players_payload = client.get("nfl/players", players_params)
    snapshot(db, dataset="players", season=season, payload=players_payload, params=players_params)
    fp_map = upsert_players(db, season, players_payload)

    projection_params = {"week": 0, "positions": "QB:RB:WR:TE:K:DST"}
    projections_payload = client.get(f"nfl/{season}/projections", projection_params)
    snapshot(db, dataset="projections", season=season, payload=projections_payload, params=projection_params)
    fp_map.update(upsert_players(db, season, projections_payload))
    projections = {as_int(row.get("fpid")): projection_stats(row.get("stats")) for row in projections_payload.get("players", [])}

    injury_params = {"year": season, "week": 0, "include_probabilities": "true"}
    try:
        injuries_payload = client.get("nfl/injuries", injury_params)
        snapshot(db, dataset="injuries", season=season, payload=injuries_payload, params=injury_params)
    except requests.RequestException as exc:
        print(f"FantasyPros injuries unavailable; continuing without injury enrichment ({type(exc).__name__})")
        injuries_payload = {"injuries": []}
    injured = save_injuries(db, season, injuries_payload, fp_map)
    history = ingest_nflverse_history(db, season)

    created_sets: list[int] = []
    for scoring in SCORING_TYPES:
        ecr_params = {"position": "ALL", "type": "DRAFT", "scoring": scoring, "week": 0}
        adp_params = {"position": "ALL", "type": "ADP", "scoring": scoring, "week": 0}
        ecr_payload = client.get(f"nfl/{season}/consensus-rankings", ecr_params)
        adp_payload = client.get(f"nfl/{season}/consensus-rankings", adp_params)
        fp_map.update(upsert_players(db, season, ecr_payload))
        fp_map.update(upsert_players(db, season, adp_payload))
        history.update(ingest_nflverse_history(db, season))
        snap_id = snapshot(db, dataset="consensus-rankings", season=season, payload=ecr_payload, params=ecr_params, scoring=scoring, ranking_type="DRAFT")
        snapshot(db, dataset="consensus-rankings", season=season, payload=adp_payload, params=adp_params, scoring=scoring, ranking_type="ADP")
        existing = db.execute_one(
            """SELECT rs.id,COUNT(pr.id)::int AS player_count FROM ff_ranking_sets rs
               LEFT JOIN ff_player_rankings pr ON pr.ranking_set_id=rs.id
               WHERE rs.source_snapshot_id=%s AND rs.ranking_type='DRAFT' GROUP BY rs.id""",
            (snap_id,),
        )
        if existing and existing["player_count"] > 0:
            created_sets.append(int(existing["id"]))
            continue
        if existing:
            ranking_set_id = int(existing["id"])
        else:
            set_row = db.execute_one(
                """INSERT INTO ff_ranking_sets
                   (season,name,source,source_snapshot_id,source_date,scoring_profile,ranking_type,is_baseline,import_summary)
                   VALUES (%s,%s,'fantasypros+nflverse',%s,CURRENT_DATE,%s,'DRAFT',TRUE,%s) RETURNING id""",
                (season, f"{season} FantasyPros + Our Model ({scoring})", snap_id, Json({"preset": scoring}), Json({"model_version": "ff-v1-market-history-blend"})),
            )
            ranking_set_id = int(set_row["id"])
        adp = {as_int(row.get("player_id")): as_float(row.get("rank_ecr") or row.get("adp")) for row in adp_payload.get("players", [])}
        model_rows: list[dict[str, Any]] = []
        for raw in ecr_payload.get("players", []):
            fp_id = as_int(raw.get("player_id"))
            player_id = fp_map.get(fp_id or -1)
            if not player_id:
                continue
            player = db.execute_one("SELECT position,rookie,injury_status,team_abbrev FROM ff_players WHERE id=%s", (player_id,))
            stats = projections.get(fp_id, {})
            fp_points = source_points(stats, scoring)
            model = build_model_projection(fp_points, history.get(player_id), scoring=scoring, rookie=bool(player["rookie"]), injured=player_id in injured)
            overall = as_int(raw.get("rank_ecr"))
            db.execute(
                """INSERT INTO ff_player_rankings
                   (ranking_set_id,player_id,overall_rank,position_rank,tier,adp,
                    projected_points,projected_stats,rank_min,rank_max,rank_std,
                    our_rank,our_projected_points,expected_games,confidence,source_row,notes)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (ranking_set_id, player_id, overall, position_rank(raw.get("pos_rank")), as_int(raw.get("tier")), adp.get(fp_id), fp_points, Json(stats), as_float(raw.get("rank_min")), as_float(raw.get("rank_max")), as_float(raw.get("rank_std")), overall, model.points, model.expected_games, model.confidence, Json(raw), json.dumps(model.explanation)),
            )
            model_rows.append({"player_id": player_id, "position": player["position"], "rookie": player["rookie"], "injury_status": player["injury_status"], "team_abbrev": player["team_abbrev"], "overall_rank": overall, "our_rank": overall, "position_rank": position_rank(raw.get("pos_rank")), "adp": adp.get(fp_id), "confidence": model.confidence, "our_projected_points": model.points})
        assign_our_ranks(db, ranking_set_id, model_rows)
        create_indicators(db, ranking_set_id, season, model_rows, history, scoring)
        created_sets.append(ranking_set_id)

    return {"season": season, "players": len(fp_map), "ranking_sets": created_sets, "history_matches": len(history)}


def run(season: int) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)  # schema initialization and migrations
    db = RefreshDatabase(config.database_url)
    try:
        result = _run_ingestion(season, db, FantasyProsClient(os.environ.get("FANTASYPROS_API_KEY", "")))
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


def run_contract_snapshot(season: int) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    try:
        result = snapshot_fantasypros_contracts(
            db,
            FantasyProsClient(os.environ.get("FANTASYPROS_API_KEY", "")),
            season,
        )
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Verify endpoint entitlement and payload contracts without writing player data",
    )
    parser.add_argument(
        "--audit-output",
        help="Optional path for the sanitized JSON endpoint-audit report",
    )
    parser.add_argument(
        "--audit-seasons",
        help="Audit historical seasons as one season, comma list, or inclusive range (for example 2020:2025)",
    )
    parser.add_argument(
        "--snapshot-contracts",
        action="store_true",
        help="Persist verified FantasyPros source snapshots without creating a vendor-controlled board",
    )
    parser.add_argument(
        "--require-contracts",
        action="store_true",
        help="Exit non-zero unless every required endpoint returns a full-size payload",
    )
    args = parser.parse_args()
    if args.audit_only:
        client = FantasyProsClient(os.environ.get("FANTASYPROS_API_KEY", ""))
        if args.audit_seasons:
            seasons = parse_season_range(args.audit_seasons)
            schedule = pd.read_csv(NFLVERSE_SCHEDULE_URL)
            audit = audit_fantasypros_history(client, seasons, schedule)
        else:
            audit = audit_fantasypros_endpoints(client, args.season)
        write_audit_report(audit, args.audit_output)
        if args.require_contracts and (
            not audit["all_required_contracts_pass"]
            or not audit.get("all_ppr_adp_cutoffs_eligible", True)
        ):
            sys.exit(2)
    elif args.snapshot_contracts:
        print(json.dumps(run_contract_snapshot(args.season), indent=2))
    else:
        print(json.dumps(run(args.season), indent=2))
