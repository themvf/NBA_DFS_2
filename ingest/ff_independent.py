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
import warnings
import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import requests
from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from ingest.ff_playoff_sos import compute_playoff_sos
from ingest.ff_fantasypros import RefreshDatabase, as_float, as_int, create_indicators, normalize_name
from ingest.ff_injuries import persist_sleeper_injury_observations


# v1.12 (2026-08-16): board no longer silently drops a team's presumptive
# starter or curated RB handcuff when they miss the top-BOARD_SIZE VOR cut
# (see _must_include_ids/RB_HANDCUFFS below) -- all 4 Browns QBs and 10 other
# real depth-chart players across the league were previously invisible
# everywhere in the app (rankings table, Best Ball board, draft room). Also
# fixes the "AZ"/"JAC" TEAM_ABBREV_OVERRIDES gap (nfl_team_id/bye_week were
# silently NULL for the entire Cardinals offense).
# v1.13 (2026-08-16): v1.12 guaranteed only the #1 QB per team by our own
# projection, which turned out to still exclude Shedeur Sanders on Cleveland
# -- our model (and Sleeper's own depth_chart_order) has Watson above him,
# while the public ECR depth chart that prompted this fix has it reversed.
# QB now guarantees the top 2 per team instead of adjudicating the
# competition (GUARANTEE_COUNT). Bumped because board membership changed --
# board_digest hashes MODEL_VERSION, forcing a fresh build.
MODEL_VERSION = "ff-independent-v1.14"
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
NFLVERSE_SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"
NFLVERSE_TEAM_STATS_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/stats_team/"
    "stats_team_reg_{season}.csv"
)
FFC_ADP_URL = "https://fantasyfootballcalculator.com/api/v1/adp/{format}?teams=12&year={season}"
FFC_FORMATS = {"STD": "standard", "HALF": "half-ppr", "PPR": "ppr"}

# Yahoo standard Team Defense/Special Teams scoring, verified 2026-08-07
# against Yahoo's own live express-settings default page (not a secondary
# source and not assumed -- this project's standing rule for any scoring
# formula, see the DK soccer scoring note elsewhere in this codebase).
# Scoring is identical across STD/HALF/PPR (DST has no reception bonus).
YAHOO_DST_SACK_PTS = 1.0
YAHOO_DST_INT_PTS = 2.0
YAHOO_DST_FUMBLE_REC_PTS = 2.0
YAHOO_DST_SAFETY_PTS = 2.0
YAHOO_DST_TD_PTS = 6.0  # both defensive TDs and kickoff/punt return TDs
# (max_points_in_tier, fantasy_points) -- checked in ascending order.
YAHOO_DST_POINTS_ALLOWED_TIERS = (
    (0, 10.0), (6, 7.0), (13, 4.0), (20, 1.0), (27, 0.0), (34, -1.0), (10_000, -4.0),
)
# Yahoo standard Kicker scoring, verified 2026-08-07 against the same live
# express-settings default page. Field goals are DISTANCE-TIERED, not a flat 3:
# a 50-yarder is worth 5. Scoring is identical across STD/HALF/PPR.
YAHOO_K_FG_0_39_PTS = 3.0
YAHOO_K_FG_40_49_PTS = 4.0
YAHOO_K_FG_50_PLUS_PTS = 5.0
YAHOO_K_PAT_PTS = 1.0

# Blocked kicks (+2) are deliberately NOT modeled: nflverse's team-stats
# release only records kicks THIS team's OWN unit had blocked against it
# (fg_blocked/pat_blocked/pt_blocked), not blocks credited to this team's
# defense against an opponent -- crediting that correctly requires per-game
# opponent cross-referencing this season-aggregate dataset doesn't provide.
# Rare, low-magnitude category (~1-2 occurrences/team/season); same "too
# rare to model, omit" judgment already applied elsewhere in this project
# (DK soccer CG/CGSO/NH scoring). Documented in source_row, not hidden.

SEASON_WEIGHTS = (0.05, 0.20, 0.75)
# Per-position overrides fitted walk-forward in model/ff_skill_constant_screen.py
# (tuned on 2022-24 targets, graded once on a held-out 2025, PPR points per game,
# role_factor pinned to 1.0 so only the history blend is under test).
#
# RB and TE both want a FLATTER blend than the global default -- more weight on
# two and three seasons back, less on last season alone. Held out on 2025 that
# improves MAE 2.900 -> 2.762 for RB (95% CI [-0.261, -0.014]) and
# 2.039 -> 1.947 for TE (CI [-0.173, -0.006]); rank correlation improves too
# (RB 0.795 -> 0.804, TE 0.744 -> 0.754).
#
# WR is NOT here on purpose: the fitted weights came back identical to the
# global default, so only its shrinkage changed (below).
#
# QB is NOT here either, and that one is a near miss worth recording. Its fitted
# weights (0.20/0.30/0.50, prior_games=12) had the LARGEST apparent gain of any
# position, MAE 3.095 -> 2.826 -- but on only 36 held-out quarterbacks, and the
# bootstrap CI on that delta is [-0.637, +0.105], which spans zero. A 90-point
# grid searched against 36 rows can find that much by luck. Directionally it
# suggests QBs want more history and heavier shrinkage than the default, which
# is plausible (QB is the most stable position), but "plausible and not
# significant" is how v1.7's DST regression shipped. Re-run the screen once more
# seasons exist rather than shipping this on the point estimate.
POSITION_SEASON_WEIGHTS = {
    "RB": (0.15, 0.25, 0.60),
    "TE": (0.15, 0.25, 0.60),
}
BASELINE_GAMES = 17.0
REGRESSION_PRIOR_GAMES = 4.0
# How hard each position's weighted history is pulled toward the position prior,
# expressed as "equivalent prior games". Higher = more shrinkage.
#
# RB/WR/TE fitted walk-forward in model/ff_skill_constant_screen.py. All three
# came back at 2.0, i.e. LESS shrinkage than the 4.0 default: a skill player with
# real snap history is a better guide to himself than the position average is.
#
# K is historical and no longer live. It was the shrinkage for kickers' old
# 3-year weighted-blend path (37 prior games ~ effective weight 0.58 at a
# 51-game sample, vs the 4-game default's 0.93). model/ff_kicker_projection_
# backtest.py showed that blend was MORE ACCURATE than prior-season-only
# (held-out MAE 22.1 vs 23.1) -- but v1.11 switched kickers to prior-season
# carry-forward anyway, on explicit user instruction, to match DST's "rank on
# last season" pattern. Kept as that backtest's comparison baseline; K no longer
# routes through the eligible_history branch that reads this map, and DST never
# did. (Before v1.14 no live position read this map at all; RB/WR/TE now do.)
POSITION_REGRESSION_PRIOR_GAMES = {"K": 37.0, "RB": 2.0, "WR": 2.0, "TE": 2.0}
# How much of a defense's / kicker's prior-season result carries into its
# projection. DST_CARRY_FORWARD_WEIGHT fitted walk-forward (tuned on 2023-24,
# held out on 2025) in model/ff_dst_projection_backtest.py.
# K_CARRY_FORWARD_WEIGHT is the "prior season only" row fitted the same way in
# model/ff_kicker_projection_backtest.py (0.54, held-out MAE 23.1, rank rho
# 0.17) -- shipped even though that same backtest shows the 3-year weighted
# blend is MORE ACCURATE (MAE 22.1, rho 0.18). This is a knowing, informed
# tradeoff: the user explicitly asked for kickers to rank on 2025 alone, the
# same way DST does, after being shown the ~1-point MAE cost. Shrinkage is
# monotonic, so both knobs change the displayed SPREAD but never the draft
# ORDER -- raising either only trades calibration for visual separation.
# Re-run the relevant backtest after changing either constant.
DST_CARRY_FORWARD_WEIGHT = 0.05
K_CARRY_FORWARD_WEIGHT = 0.54
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
# "AZ"/"JAC" added 2026-08-16: nflverse's weekly roster feed codes Arizona as
# "AZ" (not "ARI", nfl_teams' canonical abbreviation) for every Cardinals
# offensive player, and a legacy ingestion run left one Jaguars row on "JAC".
# Without the override, normalize_team() passed those strings through
# unchanged, team_ids.get(team) (keyed by nfl_teams.abbreviation) missed, and
# every affected player silently got nfl_team_id=NULL and bye_week=NULL
# (bye_weeks is also keyed by the normalized abbreviation) -- found while
# building RB_HANDCUFFS below, which needs a consistent canonical abbreviation
# to key off. Live rows already written under "AZ"/"JAC" were repaired
# out-of-band (one-time UPDATE); this override prevents recurrence.
TEAM_ABBREV_OVERRIDES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}

# Real depth-chart RB handcuffs (starter's presumed backup, i.e. who inherits
# the bell-cow role on injury) for all 32 teams, captured from public
# consensus ECR/ADP 2026-08-16. Keyed by nfl_teams.abbreviation. This exists
# because the fallback heuristic (role_rank==2 -- just the 2nd-highest-VOR RB
# on the roster by our own model) can pick a passing-down complement back
# instead of the real bell-cow backup; every name below was cross-checked
# against the live 2026 roster (ff_players) before being hardcoded. Names use
# our DB's canonical_name where it differs from common usage (e.g.
# "Kenneth Gainwell", not "Kenny") so normalize_name() matching in
# create_indicators() resolves on the first try.
RB_HANDCUFFS: dict[str, str] = {
    "ARI": "Tyler Allgeier",
    "ATL": "Brian Robinson Jr.",
    "BAL": "Justice Hill",
    "BUF": "Ray Davis",
    "CAR": "Jonathon Brooks",
    "CHI": "Kyle Monangai",
    "CIN": "Samaje Perine",
    "CLE": "Dylan Sampson",
    "DAL": "Jaydon Blue",
    "DEN": "RJ Harvey",
    "DET": "Isiah Pacheco",
    "GB": "MarShawn Lloyd",
    "HOU": "Woody Marks",
    "IND": "Seth McGowan",
    "JAX": "Chris Rodriguez Jr.",
    "KC": "Emmett Johnson",
    "LAC": "Kimani Vidal",
    "LAR": "Blake Corum",
    "LV": "Mike Washington Jr.",
    "MIA": "Ollie Gordon II",
    "MIN": "Aaron Jones Sr.",
    "NE": "Rhamondre Stevenson",
    "NO": "Alvin Kamara",
    "NYG": "Tyrone Tracy Jr.",
    "NYJ": "Braelon Allen",
    "PHI": "Tank Bigsby",
    "PIT": "Jaylen Warren",
    "SEA": "Emanuel Wilson",
    "SF": "Kaelon Black",
    "TB": "Kenneth Gainwell",
    "TEN": "Tyjae Spears",
    "WSH": "Jacory Croskey-Merritt",
}
STARTER_POSITIONS = ("QB", "RB", "WR", "TE")

# Fitted from 195 true rookies across the 2023-2025 draft classes (nflverse
# roster_weekly rookie_year==season, draft_number joined by gsis_id, actual
# outcome from ff_player_season_features). Walk-forward validated (leave-one-
# class-out): beats the flat pick-bucket table in _rookie_points() on every
# RB/WR/TE fold (RB -26% MAE, WR -42% MAE, TE -32% MAE, n=53/84/37). QB showed
# no improvement over the bucket table (matches the independent PFF/4for4
# "no rookie model beats the draft for QB" finding) and stays on _rookie_points().
# See model/ff_rookie_draft_curve.py for the fitting/validation script.
ROOKIE_CURVE_POSITIONS = {"RB", "WR", "TE"}
ROOKIE_DRAFT_CURVE = {
    "RB": {"STD": {"floor": 39.44, "peak": 214.99, "decay": 42.56}, "PPR": {"floor": 48.60, "peak": 276.50, "decay": 42.58}},
    "WR": {"STD": {"floor": 24.91, "peak": 158.57, "decay": 44.50}, "PPR": {"floor": 39.34, "peak": 245.14, "decay": 43.71}},
    "TE": {"STD": {"floor": 19.84, "peak": 180.22, "decay": 27.92}, "PPR": {"floor": 33.57, "peak": 300.51, "decay": 28.01}},
}
# Empirical actual/predicted ratio at the 15th/85th percentile of the same
# fitted sample -- replaces the flat 0.62x-1.42x rookie range with a
# position-derived one. Genuinely this wide: rookie outcomes are bimodal
# (role/competition resolution), not a tight band around the point estimate.
ROOKIE_RANGE_RATIO = {
    "RB": (0.11, 1.94),
    "WR": (0.17, 2.02),
    "TE": (0.19, 1.79),
}


def _rookie_curve_points(position: str, draft_number: int | None, scoring: str) -> float:
    pick = float(draft_number or 999)
    curve = ROOKIE_DRAFT_CURVE[position]

    def _value(key: str) -> float:
        params = curve[key]
        return params["floor"] + (params["peak"] - params["floor"]) * math.exp(-pick / params["decay"])

    std_value = _value("STD")
    ppr_value = _value("PPR")
    if scoring == "STD":
        return std_value
    if scoring == "PPR":
        return ppr_value
    return (std_value + ppr_value) / 2.0


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
    scoring: str | None = None,
    ranking_type: str | None = None,
) -> int:
    row = db.execute_one(
        """INSERT INTO ff_source_snapshots
           (source,dataset,season,scoring,ranking_type,request_params,response_hash,row_count,status)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'success')
           ON CONFLICT(source,dataset,response_hash) DO UPDATE SET
             fetched_at=NOW(),row_count=EXCLUDED.row_count,status='success',error_summary=NULL
           RETURNING id""",
        (source, dataset, season, scoring, ranking_type, Json(params), digest, row_count),
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


def compute_bye_weeks(schedule: pd.DataFrame, season: int) -> dict[str, int]:
    regular = schedule[(schedule["season"] == season) & (schedule["game_type"] == "REG")].copy()
    regular["home_team"] = regular["home_team"].map(normalize_team)
    regular["away_team"] = regular["away_team"].map(normalize_team)
    teams = {
        normalize_team(team)
        for team in pd.concat([regular["home_team"], regular["away_team"]]).dropna().unique()
    }
    bye_weeks: dict[str, int] = {}
    for team in teams:
        played = set(
            regular.loc[
                (regular["home_team"] == team) | (regular["away_team"] == team),
                "week",
            ].astype(int)
        )
        missing = sorted(set(range(1, 19)) - played)
        if len(missing) == 1:
            bye_weeks[team] = missing[0]
    return bye_weeks


def build_adp_lookup(payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in payload.get("players", []):
        if not isinstance(raw, dict):
            continue
        source_position = str(raw.get("position") or "").upper()
        position = "DST" if source_position in {"DEF", "DST"} else "K" if source_position in {"K", "PK"} else source_position
        if position not in POSITIONS:
            continue
        team = normalize_team(raw.get("team"))
        key = (team, "DST") if position == "DST" else (normalize_name(str(raw.get("name") or "")), position)
        if key[0]:
            lookup[key] = raw
    return lookup


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
        as_int(row.get("bye_week")),
        row.get("injury_status"),
        metadata,
    )
    if existing:
        db.execute(
            """UPDATE ff_players SET canonical_name=%s,normalized_name=%s,position=%s,
                 nfl_team_id=%s,team_abbrev=%s,sleeper_player_id=COALESCE(%s,sleeper_player_id),
                 gsis_id=COALESCE(%s,gsis_id),espn_id=COALESCE(%s,espn_id),
                 yahoo_id=COALESCE(%s,yahoo_id),active=TRUE,rookie=%s,
                 bye_week=%s,injury_status=%s,metadata=metadata || %s::jsonb,fetched_at=NOW()
               WHERE id=%s""",
            (*params, int(existing["id"])),
        )
        return int(existing["id"])
    saved = db.execute_one(
        """INSERT INTO ff_players
           (season,canonical_name,normalized_name,position,nfl_team_id,team_abbrev,
            sleeper_player_id,gsis_id,espn_id,yahoo_id,active,rookie,bye_week,injury_status,metadata,fetched_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,%s,%s,%s,NOW()) RETURNING id""",
        (season, *params),
    )
    return int(saved["id"])


def build_player_universe(
    db: RefreshDatabase,
    season: int,
    roster: pd.DataFrame,
    sleeper_payload: dict[str, Any],
    bye_weeks: dict[str, int],
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
            "bye_week": bye_weeks.get(team),
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
            "bye_week": bye_weeks.get(team),
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
                # Per-distance buckets drive Yahoo's tiered kicker scoring. The
                # flat fg_made above is kept only as a degraded fallback.
                **{
                    bucket: as_float(raw.get(bucket)) or 0.0
                    for bucket in ("fg_made_0_19", "fg_made_20_29", "fg_made_30_39",
                                   "fg_made_40_49", "fg_made_50_59", "fg_made_60_")
                },
            }
            if player["position"] == "K":
                kicker_points = yahoo_kicker_points(values)
                values["fantasy_points_std"] = kicker_points
                values["fantasy_points_ppr"] = kicker_points
            # source_row stores the FULL raw nflverse row (raw), not just the
            # curated `values` subset -- nflverse's stats_player_reg CSV already
            # includes advanced columns (rushing_epa, receiving_epa,
            # receiving_air_yards, air_yards_share, wopr, racr, passing_epa,
            # passing_cpoe, ...) that were previously fetched and then discarded.
            # Nothing reads source_row from this table yet, so this is a pure
            # superset with no migration -- percentile-profile queries pull
            # these via source_row->>'field'; the dedicated typed columns above
            # remain the authoritative source for the stats they cover.
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
                    values["nfl_target_rank"], values["nfl_rush_td_rank"], Json(raw),
                ),
            )
            result.setdefault(int(player["player_id"]), []).append(values)
    return result


def _points_allowed_fpts(points_allowed: int) -> float:
    """Yahoo's DST points-allowed tier, applied PER GAME (not season total)."""
    for max_points, fpts in YAHOO_DST_POINTS_ALLOWED_TIERS:
        if points_allowed <= max_points:
            return fpts
    return YAHOO_DST_POINTS_ALLOWED_TIERS[-1][1]


def _team_points_allowed_fpts_by_season(schedule: pd.DataFrame, season: int) -> dict[str, float]:
    """Sum each team's per-game points-allowed fantasy points for one season.

    Reuses the same schedule frame already fetched for bye-week computation
    (games.csv) -- no separate request needed. Games without a final score
    yet (future/unplayed) are skipped rather than treated as 0 allowed.
    """
    regular = schedule[(schedule["season"] == season) & (schedule["game_type"] == "REG")]
    allowed: dict[str, float] = {}
    for _, game in regular.iterrows():
        home_score = as_int(game.get("home_score"))
        away_score = as_int(game.get("away_score"))
        if home_score is None or away_score is None:
            continue
        home = normalize_team(game.get("home_team"))
        away = normalize_team(game.get("away_team"))
        allowed[home] = allowed.get(home, 0.0) + _points_allowed_fpts(away_score)
        allowed[away] = allowed.get(away, 0.0) + _points_allowed_fpts(home_score)
    return allowed


def save_dst_history(
    db: RefreshDatabase,
    universe: list[dict[str, Any]],
    dst_history_frames: dict[int, pd.DataFrame],
    schedule: pd.DataFrame,
) -> dict[int, list[dict[str, Any]]]:
    """Real per-team-season DST history under Yahoo standard scoring.

    Mirrors save_history()'s shape/conventions exactly (same
    ff_player_season_features table, same fantasy_points_std/ppr columns --
    identical value in both, since DST scoring has no reception bonus) so
    project_player()'s existing history-regression path (weighted 3-year
    blend, shrinkage toward the position prior, confidence-from-sample-depth)
    applies to DST with no special-casing needed.
    """
    by_team = {row["team"]: row for row in universe if row["position"] == "DST"}
    result: dict[int, list[dict[str, Any]]] = {}
    for history_season, frame in dst_history_frames.items():
        points_allowed = _team_points_allowed_fpts_by_season(schedule, history_season)
        for _, series in frame.iterrows():
            raw = _clean(series.to_dict())
            team = normalize_team(raw.get("team"))
            player = by_team.get(team)
            if not player:
                continue
            sacks = as_float(raw.get("def_sacks")) or 0.0
            interceptions = as_float(raw.get("def_interceptions")) or 0.0
            fumble_recoveries = as_float(raw.get("fumble_recovery_opp")) or 0.0
            safeties = as_float(raw.get("def_safeties")) or 0.0
            defensive_tds = as_float(raw.get("def_tds")) or 0.0
            return_tds = as_float(raw.get("special_teams_tds")) or 0.0
            points_allowed_fpts = points_allowed.get(team, 0.0)
            games = as_int(raw.get("games")) or 0
            fpts = round(
                sacks * YAHOO_DST_SACK_PTS
                + interceptions * YAHOO_DST_INT_PTS
                + fumble_recoveries * YAHOO_DST_FUMBLE_REC_PTS
                + safeties * YAHOO_DST_SAFETY_PTS
                + (defensive_tds + return_tds) * YAHOO_DST_TD_PTS
                + points_allowed_fpts,
                2,
            )
            values = {"season": history_season, "games": games, "fantasy_points_std": fpts, "fantasy_points_ppr": fpts}
            db.execute(
                """INSERT INTO ff_player_season_features
                   (player_id,season,source,games,fantasy_points_std,fantasy_points_ppr,source_row,fetched_at)
                   VALUES (%s,%s,'nflverse',%s,%s,%s,%s,NOW())
                   ON CONFLICT(player_id,season,source) DO UPDATE SET
                     games=EXCLUDED.games,fantasy_points_std=EXCLUDED.fantasy_points_std,
                     fantasy_points_ppr=EXCLUDED.fantasy_points_ppr,source_row=EXCLUDED.source_row,fetched_at=NOW()""",
                (
                    player["player_id"], history_season, games, fpts, fpts,
                    Json({
                        "model": "yahoo-dst-scoring-v1",
                        "sacks": sacks,
                        "interceptions": interceptions,
                        "fumble_recoveries_opp": fumble_recoveries,
                        "safeties": safeties,
                        "defensive_tds": defensive_tds,
                        "special_teams_return_tds": return_tds,
                        "points_allowed_fpts": round(points_allowed_fpts, 2),
                        "blocked_kicks_modeled": False,
                        "raw_team_stats": raw,
                    }),
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


def yahoo_kicker_points(row: dict[str, Any]) -> float:
    """Yahoo standard kicker scoring: 0-39 = 3, 40-49 = 4, 50+ = 5, PAT = 1.

    Single source of truth for the formula, used by both the history ingest and
    the projection path. Falls back to a flat 3 per field goal ONLY when the
    per-distance buckets are absent (older rows written before this shipped),
    so a stale row degrades instead of silently scoring zero.
    """
    value = lambda key: float(row.get(key) or 0.0)  # noqa: E731
    buckets = ("fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49", "fg_made_50_59", "fg_made_60_")
    if not any(key in row for key in buckets):
        return value("fg_made") * YAHOO_K_FG_0_39_PTS + value("pat_made") * YAHOO_K_PAT_PTS
    short = value("fg_made_0_19") + value("fg_made_20_29") + value("fg_made_30_39")
    long_range = value("fg_made_50_59") + value("fg_made_60_")
    return (
        short * YAHOO_K_FG_0_39_PTS
        + value("fg_made_40_49") * YAHOO_K_FG_40_49_PTS
        + long_range * YAHOO_K_FG_50_PLUS_PTS
        + value("pat_made") * YAHOO_K_PAT_PTS
    )


def _season_points(history: dict[str, Any], position: str, scoring: str) -> float:
    if position == "K":
        return yahoo_kicker_points(history)
    std = float(history.get("fantasy_points_std") or 0.0)
    ppr = float(history.get("fantasy_points_ppr") or 0.0)
    return std if scoring == "STD" else ppr if scoring == "PPR" else (std + ppr) / 2.0


def project_player(player: dict[str, Any], histories: list[dict[str, Any]], scoring: str, target_season: int) -> IndependentProjection:
    position = str(player["position"])
    injury = str(player.get("injury_status") or "").upper()
    depth = as_int(player.get("depth_order"))
    eligible_history = [row for row in histories if target_season - 3 <= int(row["season"]) < target_season and int(row.get("games") or 0) > 0]
    eligible_history.sort(key=lambda row: int(row["season"]))
    method = "history_regression"
    season_inputs: list[dict[str, Any]] = []
    weighted_history_ppg: float | None = None
    position_prior_ppg: float | None = None
    regressed_ppg: float | None = None
    rookie_prior_points: float | None = None
    role_floor_points: float | None = None

    if position in ("DST", "K"):
        # Ordering comes from the MOST RECENT completed season's actual
        # Yahoo-scored points; the displayed value is that number shrunk hard
        # toward the league prior. Both halves are deliberate:
        #
        #   Ordering  -- for DST, prior-season carry-forward is the best
        #     RANKABLE signal tested (Spearman 0.18, top-12 hit rate 42% vs
        #     38% random) -- better than the 3-year weighted blend v1.7 used
        #     (0.15), and a flat constant ranks nothing at all.
        #   Magnitude -- shrinkage is a monotonic transform, so it leaves that
        #     ordering EXACTLY unchanged while fixing calibration. For DST,
        #     held out on 2025 (tuned on 2023-24, n=96 walk-forward): raw
        #     carry-forward MAE 28.0, shrunk 24.3, flat constant 24.8. The
        #     shrunk version beats both -- it is not a compromise, it is the
        #     best of the three.
        #
        # Why DST's weight is so small: every Yahoo DST scoring component is
        # near-noise year over year (sacks r=0.20, INTs r=0.11, fumble
        # recoveries r=0.06, defensive TDs r=-0.03). The only component with
        # real persistence is points allowed (r=0.31), and Yahoo's tiers make
        # it just ~8% of total scoring. So the honest projection spread is
        # narrow by design -- these defenses genuinely are hard to tell apart,
        # and a wide confident-looking spread would be fiction. The board's
        # prior-season FPTS column shows the real uncompressed number
        # alongside, so the underlying evidence stays visible.
        #
        # Kickers (v1.11): unlike DST, model/ff_kicker_projection_backtest.py
        # shows kicker history IS genuinely predictive and the 3-year
        # weighted blend beats prior-season-only (held-out MAE 22.1 vs 23.1,
        # rank rho 0.18 vs 0.17) -- the accuracy-maximizing choice would have
        # kept kickers on the multi-year regression path below. Kickers were
        # moved onto this carry-forward branch anyway on explicit, informed
        # user instruction ("I want 2025 for both kickers and defense"),
        # after that tradeoff was shown. K_CARRY_FORWARD_WEIGHT=0.54 is the
        # backtest-fitted shrinkage for the "prior season only" predictor
        # specifically -- it is not the same number as the abandoned 3-year
        # blend's effective weight (0.58, via POSITION_REGRESSION_PRIOR_GAMES).
        #
        # *_CARRY_FORWARD_WEIGHT is the intended tuning knob for both
        # positions. Raising it widens the displayed spread WITHOUT changing
        # the draft order at all; it only trades calibration for visual
        # separation. Re-run the matching backtest after any change.
        weight = DST_CARRY_FORWARD_WEIGHT if position == "DST" else K_CARRY_FORWARD_WEIGHT
        prior_points = POSITION_PRIOR_PPG[scoring][position] * BASELINE_GAMES
        recent = max(eligible_history, key=lambda row: int(row["season"])) if eligible_history else None
        if recent is not None:
            method = "dst_prior_season_carry_forward" if position == "DST" else "k_prior_season_carry_forward"
            carry_points = _season_points(recent, position, scoring)
            base_points = prior_points + weight * (carry_points - prior_points)
            history_games = int(recent.get("games") or 0)
            season_inputs.append({
                "season": int(recent["season"]),
                "games": history_games,
                "points": round(carry_points, 2),
                "weight": 1.0,
            })
            confidence = 0.35
        else:
            method = "position_baseline_no_history"
            base_points = prior_points
            history_games = 0
            confidence = 0.30
        position_prior_ppg = POSITION_PRIOR_PPG[scoring][position]
        expected_games = BASELINE_GAMES
        role_factor = 1.0
    elif eligible_history:
        season_weights = POSITION_SEASON_WEIGHTS.get(position, SEASON_WEIGHTS)
        weights_by_season = {
            target_season - 3: season_weights[0],
            target_season - 2: season_weights[1],
            target_season - 1: season_weights[2],
        }
        weighted_ppg = 0.0
        weight_total = 0.0
        weighted_availability = 0.0
        history_games = 0
        for history in eligible_history:
            games = int(history.get("games") or 0)
            weight = weights_by_season.get(int(history["season"]), 0.0)
            if not games or not weight:
                continue
            season_points = _season_points(history, position, scoring)
            season_ppg = season_points / games
            weighted_ppg += season_ppg * weight
            weighted_availability += min(games / BASELINE_GAMES, 1.0) * weight
            weight_total += weight
            history_games += games
            season_inputs.append({
                "season": int(history["season"]),
                "games": games,
                "points": round(season_points, 2),
                "ppg": round(season_ppg, 3),
                "weight": weight,
            })
        raw_ppg = weighted_ppg / weight_total if weight_total else 0.0
        weighted_history_ppg = raw_ppg
        prior_ppg = POSITION_PRIOR_PPG[scoring][position]
        position_prior_ppg = prior_ppg
        regression_prior_games = POSITION_REGRESSION_PRIOR_GAMES.get(position, REGRESSION_PRIOR_GAMES)
        regressed_ppg = (
            raw_ppg * history_games + prior_ppg * regression_prior_games
        ) / (history_games + regression_prior_games)
        availability = weighted_availability / weight_total if weight_total else 0.75
        expected_games = 13.5 + 2.5 * availability
        raw_role = _depth_factor(position, depth)
        role_factor = 0.75 + raw_role * 0.25
        base_points = regressed_ppg * BASELINE_GAMES * role_factor
        confidence = min(0.86, 0.40 + min(history_games, 40) / 100.0 + (0.06 if depth else 0.0))
    else:
        is_rookie = bool(player.get("rookie"))
        role_factor = _depth_factor(position, depth)
        role_floor_points = None
        if is_rookie and position in ROOKIE_CURVE_POSITIONS:
            method = "rookie_draft_curve"
            rookie_prior_points = _rookie_curve_points(position, as_int(player.get("draft_number")), scoring)
            capital_component = rookie_prior_points * role_factor
            # Draft capital alone can crush a confirmed starter to near-zero at a
            # late pick (e.g. pick #199 TE1 -> 34 pts, below an average TE1's 102).
            # Unvalidated against real outcomes (Sleeper has no historical depth-
            # chart snapshot to backtest against -- see model/ff_rookie_draft_curve.py
            # notes) but structurally necessary: a CONFIRMED depth-chart role floors
            # the projection at the position-average prior for that role, so role
            # can lift a late pick but never drags an already-strong pick down.
            # Requires depth to be actually known -- _depth_factor(pos, None) == 1.0
            # (its "no discount" default for a discount-only design) must NOT also
            # hand an unknown-role UDFA the full confirmed-starter floor.
            if depth is not None:
                role_floor_points = POSITION_PRIOR_PPG[scoring][position] * BASELINE_GAMES * role_factor
                base_points = max(capital_component, role_floor_points)
            else:
                base_points = capital_component
        else:
            method = "rookie_prior" if is_rookie else "position_prior"
            rookie_prior_points = _rookie_points(position, as_int(player.get("draft_number")), scoring)
            base_points = rookie_prior_points * role_factor
        expected_games = BASELINE_GAMES
        confidence = 0.38 if player.get("rookie") else 0.24
        history_games = 0

    expected_games_before_injury = expected_games
    base_points_before_injury = base_points
    injury_availability_factor = 1.0
    if any(token in injury for token in ("IR", "PUP", "OUT")):
        injury_availability_factor = 0.80
        expected_games = max(8.0, expected_games - 3.0)
        confidence -= 0.10
    elif injury:
        injury_availability_factor = 0.96
        expected_games = max(10.0, expected_games - 0.5)
        confidence -= 0.04
    # This is an active-performance season baseline. Availability belongs in
    # the weekly simulation and must not silently reduce the displayed score.
    points = max(0.0, base_points)
    confidence = max(0.15, min(0.90, confidence))
    if method == "rookie_draft_curve":
        low_ratio, high_ratio = ROOKIE_RANGE_RATIO[position]
    elif history_games:
        low_ratio, high_ratio = 0.72, 1.28
    else:
        low_ratio, high_ratio = 0.62, 1.42
    return IndependentProjection(
        points=round(points, 2),
        expected_games=round(expected_games, 1),
        confidence=round(confidence, 3),
        low=round(points * low_ratio, 2),
        high=round(points * high_ratio, 2),
        explanation={
            "model": MODEL_VERSION,
            "sources": ["nflverse", "Sleeper"],
            "history_seasons": [int(row["season"]) for row in eligible_history],
            "history_games": history_games,
            "method": method,
            "scoring": scoring,
            "season_inputs": season_inputs,
            "weighted_history_ppg": round(weighted_history_ppg, 3) if weighted_history_ppg is not None else None,
            "position_prior_ppg": position_prior_ppg,
            "regression_prior_games": (
                POSITION_REGRESSION_PRIOR_GAMES.get(position, REGRESSION_PRIOR_GAMES)
                if regressed_ppg is not None else None
            ),
            "dst_carry_forward_weight": DST_CARRY_FORWARD_WEIGHT if method == "dst_prior_season_carry_forward" else None,
            "k_carry_forward_weight": K_CARRY_FORWARD_WEIGHT if method == "k_prior_season_carry_forward" else None,
            "regression_sample_games": history_games if regressed_ppg is not None else None,
            "regressed_ppg": round(regressed_ppg, 3) if regressed_ppg is not None else None,
            "rookie_prior_points": round(rookie_prior_points, 2) if rookie_prior_points is not None else None,
            "rookie_curve_params": ROOKIE_DRAFT_CURVE.get(position) if method == "rookie_draft_curve" else None,
            "rookie_range_ratio": list(ROOKIE_RANGE_RATIO[position]) if method == "rookie_draft_curve" else None,
            "rookie_role_floor_points": round(role_floor_points, 2) if role_floor_points is not None else None,
            "rookie_role_floor_applied": bool(role_floor_points is not None and role_floor_points > rookie_prior_points * role_factor),
            "draft_number": as_int(player.get("draft_number")),
            "baseline_games": BASELINE_GAMES,
            "expected_games_before_injury": round(expected_games_before_injury, 2),
            "expected_games_after_injury": round(expected_games, 2),
            "base_points_before_injury": round(base_points_before_injury, 2),
            "final_points": round(points, 2),
            "depth_order": depth,
            "role_factor": round(role_factor, 3),
            "injury_factor": 1.0,
            "injury_availability_factor": injury_availability_factor,
            "availability_adjustment_applied_to_baseline": False,
            "not_modeled": ["current teammates", "offensive line", "coaching/play-caller", "future schedule"],
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


# How many of a team's top-our_projected_points players at each position are
# force-included regardless of the VOR cut. QB gets 2, not 1: a live,
# genuinely disputed QB competition (verified 2026-08-16 on Cleveland --
# Sleeper's own depth_chart_order has Watson QB1/Sanders QB2, while public
# ECR-based depth charts had it reversed) means picking only our model's #1
# QB can still exclude the real starter if our source disagrees with the
# depth chart a user is looking at. Guaranteeing both sidesteps having to
# adjudicate which source is right. RB/WR/TE keep 1 -- true committees are
# rarer there, and RB_HANDCUFFS below already covers the "real backup, not
# whichever RB our model likes 2nd-best" case explicitly.
GUARANTEE_COUNT = {"QB": 2, "RB": 1, "WR": 1, "TE": 1}


def _must_include_ids(rows: list[dict[str, Any]]) -> set[int]:
    """Players who must always get an ff_player_rankings row regardless of the
    top-BOARD_SIZE VOR cut: each team's top GUARANTEE_COUNT[position] players
    by our_projected_points at QB/RB/WR/TE (the presumptive starter(s) a
    drafter would look for), plus each team's curated RB_HANDCUFFS entry. All
    of these can rank well outside the top 400 (a backup QB, a zero-touch
    handcuff) and were previously invisible on the rankings table, the Best
    Ball board, and the draft room -- all three only ever query
    ff_player_rankings, so a player who missed the VOR cut simply didn't
    exist there even though they're on the roster. `rows` must already have
    position_rank/our_rank/vor assigned (i.e. be rank_rows()'s output).
    """
    must_include: set[int] = set()
    by_team_position: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        team = str(row.get("team_abbrev") or row.get("team") or "")
        if not team or row["position"] not in STARTER_POSITIONS:
            continue
        by_team_position.setdefault((team, row["position"]), []).append(row)

    for (team, position), team_rows in by_team_position.items():
        ranked = sorted(team_rows, key=lambda r: -float(r["our_projected_points"]))
        for candidate in ranked[: GUARANTEE_COUNT[position]]:
            must_include.add(int(candidate["player_id"]))
        if position == "RB":
            handcuff_name = RB_HANDCUFFS.get(team)
            if handcuff_name:
                target = normalize_name(handcuff_name)
                match = next(
                    (r for r in team_rows if normalize_name(str(r["name"])) == target),
                    None,
                )
                if match:
                    must_include.add(int(match["player_id"]))
    return must_include


def create_ranking_set(
    db: RefreshDatabase,
    *,
    season: int,
    scoring: str,
    source_snapshot_id: int,
    universe: list[dict[str, Any]],
    histories: dict[int, list[dict[str, Any]]],
    adp_lookup: dict[tuple[str, str], dict[str, Any]],
    playoff_sos: dict[tuple[str, str], dict[str, Any]] | None = None,
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
                Json({
                    "model_version": MODEL_VERSION,
                    "adp_source": "Fantasy Football Calculator",
                    "adp_used_for_projection": False,
                    "board_size": BOARD_SIZE,
                    "must_include_positions": list(STARTER_POSITIONS),
                    "rb_handcuffs_captured": "2026-08-16",
                }),
            ),
        )
        ranking_set_id = int(saved["id"])

    model_rows: list[dict[str, Any]] = []
    for player in universe:
        projection = project_player(player, histories.get(int(player["player_id"]), []), scoring, season)
        adp_key = (
            (str(player.get("team") or ""), "DST")
            if player["position"] == "DST"
            else (normalize_name(str(player["name"])), str(player["position"]))
        )
        adp_row = adp_lookup.get(adp_key)
        model_rows.append({
            **player,
            "our_projected_points": projection.points,
            "expected_games": projection.expected_games,
            "confidence": projection.confidence,
            "projection_low": projection.low,
            "projection_high": projection.high,
            "explanation": projection.explanation,
            "overall_rank": None,
            "adp": as_float(adp_row.get("adp")) if adp_row else None,
            "adp_source_row": adp_row,
        })
    ranked_all = rank_rows(model_rows)
    board = ranked_all[:BOARD_SIZE]
    board_ids = {int(row["player_id"]) for row in board}
    must_include = _must_include_ids(ranked_all)
    extra_rows = [
        row for row in ranked_all
        if int(row["player_id"]) in must_include and int(row["player_id"]) not in board_ids
    ]
    board = board + extra_rows
    for row in board:
        db.execute(
            """INSERT INTO ff_player_rankings
               (ranking_set_id,player_id,overall_rank,position_rank,tier,adp,
                projected_points,projection_low,projection_high,projected_stats,
                our_rank,our_projected_points,expected_games,confidence,source_row,notes)
               VALUES (%s,%s,NULL,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(ranking_set_id,player_id) DO UPDATE SET
                position_rank=EXCLUDED.position_rank,tier=EXCLUDED.tier,adp=EXCLUDED.adp,
                projection_low=EXCLUDED.projection_low,projection_high=EXCLUDED.projection_high,
                projected_stats=EXCLUDED.projected_stats,our_rank=EXCLUDED.our_rank,
                our_projected_points=EXCLUDED.our_projected_points,
                expected_games=EXCLUDED.expected_games,confidence=EXCLUDED.confidence,
                source_row=EXCLUDED.source_row,notes=EXCLUDED.notes""",
            (
                ranking_set_id, row["player_id"], row["position_rank"], row["tier"],
                row["adp"], row["projection_low"], row["projection_high"], Json(row["explanation"]),
                row["our_rank"], row["our_projected_points"], row["expected_games"],
                row["confidence"], Json({
                    "player": _clean(row.get("metadata") or {}),
                    "adp": _clean(row.get("adp_source_row")),
                }),
                f"VOR={row['vor']:.2f}; model={MODEL_VERSION}; adp_used_for_projection=false",
            ),
        )
    latest_history = {
        player_id: max(player_history, key=lambda row: int(row["season"]))
        for player_id, player_history in histories.items() if player_history
    }
    create_indicators(db, ranking_set_id, season, board, latest_history, scoring,
                      rb_handcuffs=RB_HANDCUFFS, playoff_sos=playoff_sos or {})
    return ranking_set_id


def _run(season: int, db: RefreshDatabase) -> dict[str, Any]:
    sleeper_payload, sleeper_digest = _fetch_json(SLEEPER_URL)
    roster, roster_digest = _fetch_csv(NFLVERSE_ROSTER_URL.format(season=season))
    schedule, schedule_digest = _fetch_csv(NFLVERSE_SCHEDULE_URL)
    if len(sleeper_payload) < 1000 or len(roster) < 500:
        raise RuntimeError("Independent player-universe source returned suspiciously few rows")
    bye_weeks = compute_bye_weeks(schedule, season)
    if len(bye_weeks) != 32:
        raise RuntimeError(f"Expected 32 schedule-derived bye weeks for {season}; found {len(bye_weeks)}")
    sleeper_snapshot_id = _snapshot(
        db, source="sleeper", dataset="players", season=season, digest=sleeper_digest,
        row_count=len(sleeper_payload), params={"url": SLEEPER_URL, "canonical": False},
    )
    _snapshot(
        db, source="nflverse", dataset="weekly-roster", season=season, digest=roster_digest,
        row_count=len(roster), params={"url": NFLVERSE_ROSTER_URL.format(season=season), "canonical": True},
    )
    _snapshot(
        db, source="nflverse", dataset="schedule", season=season, digest=schedule_digest,
        row_count=len(schedule), params={"url": NFLVERSE_SCHEDULE_URL, "use": "schedule-derived bye weeks"},
    )
    universe = build_player_universe(db, season, roster, sleeper_payload, bye_weeks)
    injury_ingestion = persist_sleeper_injury_observations(
        db,
        season=season,
        source_snapshot_id=sleeper_snapshot_id,
        players=universe,
    )

    history_frames: dict[int, pd.DataFrame] = {}
    source_digests = [sleeper_digest, roster_digest, schedule_digest]
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

    dst_history_frames: dict[int, pd.DataFrame] = {}
    for history_season in range(season - 3, season):
        url = NFLVERSE_TEAM_STATS_URL.format(season=history_season)
        frame, digest = _fetch_csv(url)
        if len(frame) != 32:
            raise RuntimeError(f"nflverse {history_season} team stats returned {len(frame)} rows; expected 32 teams")
        dst_history_frames[history_season] = frame
        source_digests.append(digest)
        _snapshot(
            db, source="nflverse", dataset="team-stats", season=history_season,
            digest=digest, row_count=len(frame), params={"url": url, "season_type": "REG"},
        )
    # Reuses `schedule` (games.csv), already fetched above for bye weeks --
    # points-allowed is derived from the same per-game home/away scores.
    dst_histories = save_dst_history(db, universe, dst_history_frames, schedule)
    for player_id, rows in dst_histories.items():
        histories.setdefault(player_id, []).extend(rows)

    adp_lookups: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
    adp_snapshot_ids: dict[str, int] = {}
    for scoring, source_format in FFC_FORMATS.items():
        url = FFC_ADP_URL.format(format=source_format, season=season)
        payload, digest = _fetch_json(url)
        player_rows = payload.get("players", [])
        if not isinstance(player_rows, list) or len(player_rows) < 100:
            raise RuntimeError(f"Fantasy Football Calculator {scoring} ADP returned suspiciously few rows")
        lookup = build_adp_lookup(payload)
        universe_keys = {
            ((str(player.get("team") or ""), "DST") if player["position"] == "DST"
             else (normalize_name(str(player["name"])), str(player["position"])))
            for player in universe
        }
        matched = sum(1 for key in lookup if key in universe_keys)
        snapshot_id = _snapshot(
            db, source="fantasy-football-calculator", dataset="adp", season=season,
            digest=digest, row_count=len(player_rows), scoring=scoring, ranking_type="ADP",
            params={"url": url, "teams": 12, "format": source_format, "projection_input": False},
        )
        db.execute(
            "UPDATE ff_source_snapshots SET matched_count=%s,unmatched_count=%s WHERE id=%s",
            (matched, len(player_rows) - matched, snapshot_id),
        )
        adp_lookups[scoring] = lookup
        adp_snapshot_ids[scoring] = snapshot_id
        source_digests.append(digest)

    board_digest = _response_hash({
        "model_version": MODEL_VERSION,
        "season": season,
        "source_digests": source_digests,
        "universe_rows": len(universe),
    })
    board_snapshot_id = _snapshot(
        db, source="independent-model", dataset="draft-board-inputs", season=season,
        digest=board_digest, row_count=len(universe),
        params={
            "model_version": MODEL_VERSION,
            "history_seasons": list(history_frames),
            "bye_source": "nflverse schedule",
            "adp_snapshot_ids": adp_snapshot_ids,
            "adp_used_for_projection": False,
        },
    )
    # Fantasy-playoff (weeks 15-17) slate difficulty, from the prior season's
    # defenses against this season's schedule. Computed once and reused across
    # all three scoring boards -- it depends only on schedule and defense, not
    # on scoring. Surfaced as an indicator, never as a projection input.
    #
    # Fails soft on purpose: this is a tie-breaker signal, and a missing or
    # renamed nflverse team-week file must not take down an entire board
    # refresh for it.
    try:
        playoff_sos = compute_playoff_sos(schedule, season)
    except Exception as exc:  # noqa: BLE001 - advisory signal, never fatal
        warnings.warn(f"playoff SOS unavailable, continuing without it: {exc}", stacklevel=2)
        playoff_sos = {}
    ranking_sets = [
        create_ranking_set(
            db, season=season, scoring=scoring, source_snapshot_id=board_snapshot_id,
            universe=universe, histories=histories, adp_lookup=adp_lookups[scoring],
            playoff_sos=playoff_sos,
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
        "bye_weeks": len(bye_weeks),
        "adp_coverage": {scoring: len(lookup) for scoring, lookup in adp_lookups.items()},
        "adp_used_for_projection": False,
        "injury_ingestion": injury_ingestion,
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
