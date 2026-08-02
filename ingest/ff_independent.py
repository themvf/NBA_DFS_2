"""Build a complete fantasy-football draft board without a market-data feed.

The current nflverse roster is the canonical player universe. Sleeper enriches
that universe with fantasy metadata, depth order and injury status. Three years
of nflverse player statistics power transparent STD/HALF/PPR projections.

Usage:
    python -m ingest.ff_independent --season 2026
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import requests
from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase, as_float, as_int, create_indicators, normalize_name


MODEL_VERSION = "ff-independent-v1.1"
SCORING_TYPES = ("STD", "HALF", "PPR")
POSITIONS = {"QB", "RB", "WR", "TE", "K", "DST"}
OFFENSIVE_POSITIONS = {"QB", "RB", "WR", "TE", "K"}
BOARD_SIZE = 400
SLEEPER_URL = "https://api.sleeper.app/v1/players/nfl?active=true"
NFLVERSE_ROSTER_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/"
    "roster_weekly_{season}.csv"
)
NFLVERSE_STATS_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/stats_player/"
    "stats_player_reg_{season}.csv"
)

SEASON_WEIGHTS = (0.15, 0.30, 0.55)
POSITION_PRIOR_PPG = {
    "STD": {"QB": 14.0, "RB": 5.5, "WR": 5.5, "TE": 3.5, "K": 7.0, "DST": 6.2},
    "HALF": {"QB": 14.0, "RB": 6.8, "WR": 7.0, "TE": 4.7, "K": 7.0, "DST": 6.2},
    "PPR": {"QB": 14.0, "RB": 8.0, "WR": 8.5, "TE": 6.0, "K": 7.0, "DST": 6.2},
}
DEPTH_FACTORS = {
    "QB": {1: 1.00, 2: 0.18, 3: 0.07},
    "RB": {1: 1.00, 2: 0.78, 3: 0.55, 4: 0.34, 5: 0.20},
    "WR": {1: 1.00, 2: 0.95, 3: 0.84, 4: 0.65, 5: 0.46, 6: 0.30},
    "TE": {1: 1.00, 2: 0.58, 3: 0.34, 4: 0.20},
    "K": {1: 1.00, 2: 0.25},
    "DST": {1: 1.00},
}
REPLACEMENT_DEMAND = {"QB": 12, "RB": 36, "WR": 48, "TE": 12, "K": 12, "DST": 12}
TEAM_ABBREV_OVERRIDES = {"LA": "LAR", "WAS": "WSH"}


@dataclass(frozen=True)
class IndependentProjection:
    points: float
    expected_games: float
    confidence: float
    low: float
    high: float
    explanation: dict[str, Any]


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _response_hash(value: Any) -> str:
    raw = json.dumps(_clean(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _fetch_json(url: str) -> tuple[dict[str, Any], str]:
    response = requests.get(url, timeout=45, headers={"User-Agent": "DFS-Vegas/1.0"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected an object response from {url}")
    return payload, hashlib.sha256(response.content).hexdigest()


def _fetch_csv(url: str) -> tuple[pd.DataFrame, str]:
    response = requests.get(url, timeout=60, headers={"User-Agent": "DFS-Vegas/1.0"})
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content)), hashlib.sha256(response.content).hexdigest()


def _snapshot(
    db: RefreshDatabase,
    *,
    source: str,
    dataset: str,
    season: int,
    digest: str,
    row_count: int,
    params: dict[str, Any],
) -> int:
    row = db.execute_one(
        """INSERT INTO ff_source_snapshots
           (source,dataset,season,request_params,response_hash,row_count,status)
           VALUES (%s,%s,%s,%s,%s,%s,'success')
           ON CONFLICT(source,dataset,response_hash) DO UPDATE SET
             fetched_at=NOW(),row_count=EXCLUDED.row_count,status='success',error_summary=NULL
           RETURNING id""",
        (source, dataset, season, Json(params), digest, row_count),
    )
    return int(row["id"])


def _sleeper_indexes(payload: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_gsis: dict[str, dict[str, Any]] = {}
    by_name_team: dict[tuple[str, str], dict[str, Any]] = {}
    for sleeper_id, raw in payload.items():
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["sleeper_id"] = str(sleeper_id)
        by_id[str(sleeper_id)] = row
        gsis = str(row.get("gsis_id") or "").strip()
        if gsis:
            by_gsis[gsis] = row
        name = normalize_name(str(row.get("full_name") or ""))
        team = str(row.get("team") or "")
        if name and team:
            by_name_team[(name, team)] = row
    return by_id, by_gsis, by_name_team


def _normalized_external_id(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text[:-2] if text.endswith(".0") else text or None


def normalize_team(value: Any) -> str:
    team = str(value or "").strip().upper()
    return TEAM_ABBREV_OVERRIDES.get(team, team)


def _upsert_player(db: RefreshDatabase, season: int, row: dict[str, Any], team_ids: dict[str, int]) -> int:
    sleeper_id = _normalized_external_id(row.get("sleeper_id"))
    gsis_id = _normalized_external_id(row.get("gsis_id"))
    name = str(row["name"])
    normalized = normalize_name(name)
    position = str(row["position"])
    team = str(row.get("team") or "") or None
    existing = None
    if sleeper_id:
        existing = db.execute_one(
            "SELECT id FROM ff_players WHERE season=%s AND sleeper_player_id=%s ORDER BY id LIMIT 1",
            (season, sleeper_id),
        )
    if not existing and gsis_id:
        existing = db.execute_one(
            "SELECT id FROM ff_players WHERE season=%s AND gsis_id=%s ORDER BY id LIMIT 1",
            (season, gsis_id),
        )
    if not existing:
        existing = db.execute_one(
            """SELECT id FROM ff_players WHERE season=%s AND normalized_name=%s
               AND position=%s AND COALESCE(team_abbrev,'')=COALESCE(%s,'')
               ORDER BY id LIMIT 1""",
            (season, normalized, position, team),
        )
    metadata = Json(_clean(row.get("metadata") or {}))
    params = (
        name,
        normalized,
        position,
        team_ids.get(team or ""),
        team,
        sleeper_id,
        gsis_id,
        _normalized_external_id(row.get("espn_id")),
        _normalized_external_id(row.get("yahoo_id")),
        bool(row.get("rookie")),
        row.get("injury_status"),
        metadata,
    )
    if existing:
        db.execute(
            """UPDATE ff_players SET canonical_name=%s,normalized_name=%s,position=%s,
                 nfl_team_id=%s,team_abbrev=%s,sleeper_player_id=COALESCE(%s,sleeper_player_id),
                 gsis_id=COALESCE(%s,gsis_id),espn_id=COALESCE(%s,espn_id),
                 yahoo_id=COALESCE(%s,yahoo_id),active=TRUE,rookie=%s,
                 injury_status=%s,metadata=metadata || %s::jsonb,fetched_at=NOW()
               WHERE id=%s""",
            (*params, int(existing["id"])),
        )
        return int(existing["id"])
    saved = db.execute_one(
        """INSERT INTO ff_players
           (season,canonical_name,normalized_name,position,nfl_team_id,team_abbrev,
            sleeper_player_id,gsis_id,espn_id,yahoo_id,active,rookie,injury_status,metadata,fetched_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,%s,%s,NOW()) RETURNING id""",
        (season, *params),
    )
    return int(saved["id"])


def build_player_universe(
    db: RefreshDatabase,
    season: int,
    roster: pd.DataFrame,
    sleeper_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    by_id, by_gsis, by_name_team = _sleeper_indexes(sleeper_payload)
    teams = db.execute("SELECT team_id,abbreviation,name FROM nfl_teams WHERE active")
    team_ids = {str(row["abbreviation"]): int(row["team_id"]) for row in teams}
    team_names = {str(row["abbreviation"]): str(row["name"]) for row in teams}

    current = roster[roster["position"].isin(OFFENSIVE_POSITIONS)].copy()
    current = current.drop_duplicates(subset=["team", "full_name", "position"], keep="last")
    universe: list[dict[str, Any]] = []
    for _, series in current.iterrows():
        raw = _clean(series.to_dict())
        team = normalize_team(raw.get("team"))
        gsis = _normalized_external_id(raw.get("gsis_id"))
        sleeper_id = _normalized_external_id(raw.get("sleeper_id"))
        sleeper = by_id.get(sleeper_id or "") or by_gsis.get(gsis or "") or by_name_team.get(
            (normalize_name(str(raw.get("full_name") or "")), team)
        ) or {}
        position = str(raw.get("position"))
        draft_number = as_int(raw.get("draft_number"))
        rookie_year = as_int(raw.get("rookie_year") or (sleeper.get("metadata") or {}).get("rookie_year"))
        player = {
            "name": raw.get("full_name"),
            "position": position,
            "team": team,
            "sleeper_id": sleeper.get("sleeper_id") or sleeper_id,
            "gsis_id": gsis or sleeper.get("gsis_id"),
            "espn_id": raw.get("espn_id") or sleeper.get("espn_id"),
            "yahoo_id": raw.get("yahoo_id") or sleeper.get("yahoo_id"),
            "rookie": rookie_year == season or as_int(raw.get("years_exp")) == 0,
            "draft_number": draft_number,
            "depth_order": as_int(sleeper.get("depth_chart_order")),
            "injury_status": sleeper.get("injury_status"),
            "metadata": {"nflverse_roster": raw, "sleeper": sleeper, "source": "nflverse+sleeper"},
        }
        player["player_id"] = _upsert_player(db, season, player, team_ids)
        universe.append(player)

    sleeper_defenses = {
        str(row.get("team")): row
        for row in sleeper_payload.values()
        if isinstance(row, dict) and row.get("position") == "DEF" and row.get("team")
    }
    for team, team_id in team_ids.items():
        sleeper = sleeper_defenses.get(team, {})
        defense = {
            "name": team_names[team],
            "position": "DST",
            "team": team,
            "sleeper_id": sleeper.get("player_id") or team,
            "gsis_id": None,
            "espn_id": sleeper.get("espn_id"),
            "yahoo_id": sleeper.get("yahoo_id"),
            "rookie": False,
            "draft_number": None,
            "depth_order": 1,
            "injury_status": None,
            "metadata": {"sleeper": sleeper, "source": "nflverse+sleeper", "team_defense": True},
        }
        defense["player_id"] = _upsert_player(db, season, defense, team_ids)
        universe.append(defense)
    return universe


def _history_values(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame[frame["position"].isin(OFFENSIVE_POSITIONS)].copy()
    for column in ("targets", "carries", "rushing_tds"):
        if column not in work:
            work[column] = 0.0
    work["team_target_rank"] = work.groupby("recent_team")["targets"].rank(method="min", ascending=False)
    work["team_rush_rank"] = work.groupby("recent_team")["carries"].rank(method="min", ascending=False)
    work["nfl_target_rank"] = work["targets"].rank(method="min", ascending=False)
    work["nfl_rush_td_rank"] = work["rushing_tds"].rank(method="min", ascending=False)
    team_targets = work.groupby("recent_team")["targets"].transform("sum").replace(0, pd.NA)
    team_carries = work.groupby("recent_team")["carries"].transform("sum").replace(0, pd.NA)
    work["derived_target_share"] = work["targets"] / team_targets
    work["derived_rush_share"] = work["carries"] / team_carries
    return work


def save_history(
    db: RefreshDatabase,
    season: int,
    universe: list[dict[str, Any]],
    history_frames: dict[int, pd.DataFrame],
) -> dict[int, list[dict[str, Any]]]:
    by_gsis = {str(row.get("gsis_id") or "").strip(): row for row in universe if str(row.get("gsis_id") or "").strip()}
    by_name_position = {(normalize_name(str(row["name"])), row["position"]): row for row in universe}
    result: dict[int, list[dict[str, Any]]] = {}
    for history_season, frame in history_frames.items():
        for _, series in _history_values(frame).iterrows():
            raw = _clean(series.to_dict())
            gsis = str(raw.get("player_id") or "").strip()
            player = by_gsis.get(gsis) or by_name_position.get(
                (normalize_name(str(raw.get("player_display_name") or raw.get("player_name") or "")), raw.get("position"))
            )
            if not player:
                continue
            values = {
                "season": history_season,
                "games": as_int(raw.get("games")) or 0,
                "fantasy_points_std": as_float(raw.get("fantasy_points")) or 0.0,
                "fantasy_points_ppr": as_float(raw.get("fantasy_points_ppr")) or 0.0,
                "targets": as_float(raw.get("targets")) or 0.0,
                "receptions": as_float(raw.get("receptions")) or 0.0,
                "receiving_yards": as_float(raw.get("receiving_yards")) or 0.0,
                "receiving_tds": as_float(raw.get("receiving_tds")) or 0.0,
                "carries": as_float(raw.get("carries")) or 0.0,
                "rushing_yards": as_float(raw.get("rushing_yards")) or 0.0,
                "rushing_tds": as_float(raw.get("rushing_tds")) or 0.0,
                "target_share": as_float(raw.get("target_share") or raw.get("derived_target_share")),
                "rush_share": as_float(raw.get("derived_rush_share")),
                "team_target_rank": as_int(raw.get("team_target_rank")),
                "team_rush_rank": as_int(raw.get("team_rush_rank")),
                "nfl_target_rank": as_int(raw.get("nfl_target_rank")),
                "nfl_rush_td_rank": as_int(raw.get("nfl_rush_td_rank")),
                "prior_team": normalize_team(raw.get("recent_team")),
                "fg_made": as_float(raw.get("fg_made")) or 0.0,
                "pat_made": as_float(raw.get("pat_made")) or 0.0,
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
                (
                    player["player_id"], history_season, values["games"], values["fantasy_points_std"],
                    values["fantasy_points_ppr"], values["targets"], values["receptions"],
                    values["receiving_yards"], values["receiving_tds"], values["carries"],
                    values["rushing_yards"], values["rushing_tds"], values["target_share"],
                    values["rush_share"], values["team_target_rank"], values["team_rush_rank"],
                    values["nfl_target_rank"], values["nfl_rush_td_rank"], Json(values),
                ),
            )
            result.setdefault(int(player["player_id"]), []).append(values)
    return result


def _depth_factor(position: str, depth_order: int | None) -> float:
    if not depth_order:
        return 1.0
    mapping = DEPTH_FACTORS[position]
    return mapping.get(depth_order, max(0.08, min(mapping.values()) * 0.65 ** (depth_order - max(mapping))))


def _rookie_points(position: str, draft_number: int | None, scoring: str) -> float:
    pick = draft_number or 999
    if position == "QB":
        std = 255.0 if pick <= 12 else 205.0 if pick <= 50 else 115.0 if pick <= 140 else 42.0
    elif position == "RB":
        std = 165.0 if pick <= 50 else 115.0 if pick <= 120 else 72.0 if pick <= 210 else 36.0
    elif position == "WR":
        std = 142.0 if pick <= 50 else 105.0 if pick <= 120 else 65.0 if pick <= 210 else 32.0
    elif position == "TE":
        std = 92.0 if pick <= 50 else 65.0 if pick <= 120 else 38.0 if pick <= 210 else 22.0
    elif position == "K":
        std = 105.0
    else:
        std = 105.0
    reception_bonus = {"QB": 0.0, "RB": 32.0, "WR": 52.0, "TE": 42.0, "K": 0.0, "DST": 0.0}[position]
    return std if scoring == "STD" else std + reception_bonus * (0.5 if scoring == "HALF" else 1.0)


def _season_points(history: dict[str, Any], position: str, scoring: str) -> float:
    if position == "K":
        return float(history.get("fg_made") or 0.0) * 3.0 + float(history.get("pat_made") or 0.0)
    std = float(history.get("fantasy_points_std") or 0.0)
    ppr = float(history.get("fantasy_points_ppr") or 0.0)
    return std if scoring == "STD" else ppr if scoring == "PPR" else (std + ppr) / 2.0


def project_player(player: dict[str, Any], histories: list[dict[str, Any]], scoring: str, target_season: int) -> IndependentProjection:
    position = str(player["position"])
    injury = str(player.get("injury_status") or "").upper()
    depth = as_int(player.get("depth_order"))
    eligible_history = [row for row in histories if target_season - 3 <= int(row["season"]) < target_season and int(row.get("games") or 0) > 0]
    eligible_history.sort(key=lambda row: int(row["season"]))

    if position == "DST":
        base_points = 105.0
        expected_games = 17.0
        confidence = 0.35
        history_games = 0
        role_factor = 1.0
    elif eligible_history:
        weights_by_season = {
            target_season - 3: SEASON_WEIGHTS[0],
            target_season - 2: SEASON_WEIGHTS[1],
            target_season - 1: SEASON_WEIGHTS[2],
        }
        weighted_ppg = 0.0
        weight_total = 0.0
        weighted_games = 0.0
        weighted_availability = 0.0
        history_games = 0
        for history in eligible_history:
            games = int(history.get("games") or 0)
            weight = weights_by_season.get(int(history["season"]), 0.0)
            if not games or not weight:
                continue
            weighted_ppg += (_season_points(history, position, scoring) / games) * weight
            weighted_games += min(games, 17) * weight
            weighted_availability += min(games / 17.0, 1.0) * weight
            weight_total += weight
            history_games += games
        raw_ppg = weighted_ppg / weight_total if weight_total else 0.0
        effective_games = weighted_games / weight_total if weight_total else 0.0
        prior_ppg = POSITION_PRIOR_PPG[scoring][position]
        regressed_ppg = (raw_ppg * effective_games + prior_ppg * 4.0) / (effective_games + 4.0)
        availability = weighted_availability / weight_total if weight_total else 0.75
        expected_games = 13.5 + 2.5 * availability
        raw_role = _depth_factor(position, depth)
        role_factor = 0.75 + raw_role * 0.25
        base_points = regressed_ppg * expected_games * role_factor
        confidence = min(0.86, 0.40 + min(history_games, 40) / 100.0 + (0.06 if depth else 0.0))
    else:
        base_points = _rookie_points(position, as_int(player.get("draft_number")), scoring)
        expected_games = 15.0 if player.get("rookie") else 13.5
        role_factor = _depth_factor(position, depth)
        base_points *= role_factor
        confidence = 0.38 if player.get("rookie") else 0.24
        history_games = 0

    injury_factor = 1.0
    if any(token in injury for token in ("IR", "PUP", "OUT")):
        injury_factor = 0.80
        expected_games = max(8.0, expected_games - 3.0)
        confidence -= 0.10
    elif injury:
        injury_factor = 0.96
        expected_games = max(10.0, expected_games - 0.5)
        confidence -= 0.04
    points = max(0.0, base_points * injury_factor)
    confidence = max(0.15, min(0.90, confidence))
    return IndependentProjection(
        points=round(points, 2),
        expected_games=round(expected_games, 1),
        confidence=round(confidence, 3),
        low=round(points * (0.72 if history_games else 0.62), 2),
        high=round(points * (1.28 if history_games else 1.42), 2),
        explanation={
            "model": MODEL_VERSION,
            "sources": ["nflverse", "Sleeper"],
            "history_seasons": [int(row["season"]) for row in eligible_history],
            "history_games": history_games,
            "depth_order": depth,
            "role_factor": round(role_factor, 3),
            "injury_factor": injury_factor,
            "market_data_used": False,
        },
    )


def rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    replacements: dict[str, float] = {}
    for position, demand in REPLACEMENT_DEMAND.items():
        values = sorted((float(row["our_projected_points"]) for row in rows if row["position"] == position), reverse=True)
        replacements[position] = values[min(demand, len(values)) - 1] if values else 0.0
    for position in POSITIONS:
        position_rows = sorted(
            (row for row in rows if row["position"] == position),
            key=lambda row: (-float(row["our_projected_points"]), str(row["name"])),
        )
        tier = 1
        tier_anchor = float(position_rows[0]["our_projected_points"]) if position_rows else 0.0
        for position_rank, row in enumerate(position_rows, start=1):
            points = float(row["our_projected_points"])
            if tier_anchor and points < tier_anchor * 0.88:
                tier += 1
                tier_anchor = points
            row["position_rank"] = position_rank
            row["tier"] = tier
            row["vor"] = points - replacements[position]
    ordered = sorted(rows, key=lambda row: (-float(row["vor"]), -float(row["our_projected_points"]), str(row["name"])))
    for rank, row in enumerate(ordered, start=1):
        row["our_rank"] = rank
    return ordered


def create_ranking_set(
    db: RefreshDatabase,
    *,
    season: int,
    scoring: str,
    source_snapshot_id: int,
    universe: list[dict[str, Any]],
    histories: dict[int, list[dict[str, Any]]],
) -> int:
    existing = db.execute_one(
        """SELECT rs.id,COUNT(pr.id)::int AS player_count FROM ff_ranking_sets rs
           LEFT JOIN ff_player_rankings pr ON pr.ranking_set_id=rs.id
           WHERE rs.source_snapshot_id=%s AND rs.ranking_type='DRAFT'
             AND rs.scoring_profile->>'preset'=%s
           GROUP BY rs.id""",
        (source_snapshot_id, scoring),
    )
    if existing and int(existing["player_count"]) >= 100:
        return int(existing["id"])
    if existing:
        ranking_set_id = int(existing["id"])
        db.execute("DELETE FROM ff_player_rankings WHERE ranking_set_id=%s", (ranking_set_id,))
    else:
        saved = db.execute_one(
            """INSERT INTO ff_ranking_sets
               (season,name,source,source_snapshot_id,source_date,scoring_profile,
                ranking_type,is_baseline,import_summary)
               VALUES (%s,%s,'nflverse+sleeper',%s,%s,%s,'DRAFT',TRUE,%s) RETURNING id""",
            (
                season,
                f"{season} Independent Model ({scoring})",
                source_snapshot_id,
                date.today(),
                Json({"preset": scoring}),
                Json({"model_version": MODEL_VERSION, "market_data_used": False, "board_size": BOARD_SIZE}),
            ),
        )
        ranking_set_id = int(saved["id"])

    model_rows: list[dict[str, Any]] = []
    for player in universe:
        projection = project_player(player, histories.get(int(player["player_id"]), []), scoring, season)
        model_rows.append({
            **player,
            "our_projected_points": projection.points,
            "expected_games": projection.expected_games,
            "confidence": projection.confidence,
            "projection_low": projection.low,
            "projection_high": projection.high,
            "explanation": projection.explanation,
            "overall_rank": None,
            "adp": None,
        })
    board = rank_rows(model_rows)[:BOARD_SIZE]
    for row in board:
        db.execute(
            """INSERT INTO ff_player_rankings
               (ranking_set_id,player_id,overall_rank,position_rank,tier,adp,
                projected_points,projection_low,projection_high,projected_stats,
                our_rank,our_projected_points,expected_games,confidence,source_row,notes)
               VALUES (%s,%s,NULL,%s,%s,NULL,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(ranking_set_id,player_id) DO UPDATE SET
                position_rank=EXCLUDED.position_rank,tier=EXCLUDED.tier,
                projection_low=EXCLUDED.projection_low,projection_high=EXCLUDED.projection_high,
                projected_stats=EXCLUDED.projected_stats,our_rank=EXCLUDED.our_rank,
                our_projected_points=EXCLUDED.our_projected_points,
                expected_games=EXCLUDED.expected_games,confidence=EXCLUDED.confidence,
                source_row=EXCLUDED.source_row,notes=EXCLUDED.notes""",
            (
                ranking_set_id, row["player_id"], row["position_rank"], row["tier"],
                row["projection_low"], row["projection_high"], Json(row["explanation"]),
                row["our_rank"], row["our_projected_points"], row["expected_games"],
                row["confidence"], Json(_clean(row.get("metadata") or {})),
                f"VOR={row['vor']:.2f}; model={MODEL_VERSION}; market_data_used=false",
            ),
        )
    latest_history = {
        player_id: max(player_history, key=lambda row: int(row["season"]))
        for player_id, player_history in histories.items() if player_history
    }
    create_indicators(db, ranking_set_id, season, board, latest_history)
    return ranking_set_id


def _run(season: int, db: RefreshDatabase) -> dict[str, Any]:
    sleeper_payload, sleeper_digest = _fetch_json(SLEEPER_URL)
    roster, roster_digest = _fetch_csv(NFLVERSE_ROSTER_URL.format(season=season))
    if len(sleeper_payload) < 1000 or len(roster) < 500:
        raise RuntimeError("Independent player-universe source returned suspiciously few rows")
    _snapshot(
        db, source="sleeper", dataset="players", season=season, digest=sleeper_digest,
        row_count=len(sleeper_payload), params={"url": SLEEPER_URL, "canonical": False},
    )
    _snapshot(
        db, source="nflverse", dataset="weekly-roster", season=season, digest=roster_digest,
        row_count=len(roster), params={"url": NFLVERSE_ROSTER_URL.format(season=season), "canonical": True},
    )
    universe = build_player_universe(db, season, roster, sleeper_payload)

    history_frames: dict[int, pd.DataFrame] = {}
    source_digests = [sleeper_digest, roster_digest]
    for history_season in range(season - 3, season):
        url = NFLVERSE_STATS_URL.format(season=history_season)
        frame, digest = _fetch_csv(url)
        if len(frame) < 500:
            raise RuntimeError(f"nflverse {history_season} stats returned suspiciously few rows")
        history_frames[history_season] = frame
        source_digests.append(digest)
        _snapshot(
            db, source="nflverse", dataset="player-stats", season=history_season,
            digest=digest, row_count=len(frame), params={"url": url, "season_type": "REG"},
        )
    histories = save_history(db, season, universe, history_frames)
    board_digest = _response_hash({
        "model_version": MODEL_VERSION,
        "season": season,
        "source_digests": source_digests,
        "universe_rows": len(universe),
    })
    board_snapshot_id = _snapshot(
        db, source="independent-model", dataset="draft-board-inputs", season=season,
        digest=board_digest, row_count=len(universe),
        params={"model_version": MODEL_VERSION, "history_seasons": list(history_frames)},
    )
    ranking_sets = [
        create_ranking_set(
            db, season=season, scoring=scoring, source_snapshot_id=board_snapshot_id,
            universe=universe, histories=histories,
        )
        for scoring in SCORING_TYPES
    ]
    return {
        "season": season,
        "model_version": MODEL_VERSION,
        "universe_players": len(universe),
        "history_matches": len(histories),
        "ranking_sets": ranking_sets,
        "players_per_board": BOARD_SIZE,
        "market_data_used": False,
    }


def run(season: int) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)
    db = RefreshDatabase(config.database_url)
    try:
        result = _run(season, db)
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()
    print(json.dumps(run(args.season), indent=2))
