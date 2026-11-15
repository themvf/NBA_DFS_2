"""Fetch NBA team stats plus raw player/team game logs from stats.nba.com.

Uses the nba_api Python package which wraps stats.nba.com endpoints.
No API key required - nba_api sets a browser-like User-Agent automatically.

Data fetched:
  - Team stats: pace, OffRtg, DefRtg via LeagueDashTeamStats (Advanced)
  - Raw player game logs: season-long per-game rows via PlayerGameLogs
  - Raw team game logs: season-long per-game rows via TeamGameLogs
  - Player stats: 10-game rolling averages derived from stored raw logs

Usage:
    python -m ingest.nba_stats
    python -m ingest.nba_stats                     # current season (from today's date)
    python -m ingest.nba_stats --season 2025-26 --season-type All
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import logging
import sys
import time
from typing import Callable, TypeVar

import pandas as pd

from config import load_config, nba_season_for
from db.database import DatabaseManager
from db.queries import (
    build_team_abbrev_cache,
    upsert_nba_player_game_logs,
    upsert_nba_player_stats,
    upsert_nba_team_game_logs,
    upsert_nba_team_stats,
)
from ingest.nba_teams import NBA_ID_TO_ABBREV

logger = logging.getLogger(__name__)

SLEEP_SECONDS = 1.0
_MAX_RETRIES = 3
_RETRY_BASE = 10.0
DEFAULT_SEASON_TYPE = "Regular Season"
DEFAULT_SEASON_TYPES = ("Regular Season", "Playoffs")
NBA_API_TIMEOUT_SECONDS = 90

T = TypeVar("T")


def _call_with_retry(fn: Callable[[], T], label: str) -> T:
    """Call an nba_api endpoint with exponential backoff."""
    import requests

    delay = _RETRY_BASE
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return fn()
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as exc:
            if attempt == _MAX_RETRIES:
                raise
            logger.warning(
                "%s: network error on attempt %d/%d (%s). Retrying in %.0fs...",
                label,
                attempt,
                _MAX_RETRIES,
                exc,
                delay,
            )
            time.sleep(delay)
            delay *= 2
        except requests.exceptions.HTTPError as exc:
            if attempt == _MAX_RETRIES:
                raise
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning(
                "%s: HTTP %s on attempt %d/%d. Retrying in %.0fs...",
                label,
                status,
                attempt,
                _MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            delay *= 2
        except Exception as exc:  # noqa: BLE001
            if attempt == _MAX_RETRIES:
                raise
            logger.warning(
                "%s: unexpected error on attempt %d/%d (%s). Retrying in %.0fs...",
                label,
                attempt,
                _MAX_RETRIES,
                type(exc).__name__,
                delay,
            )
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"{label}: all {_MAX_RETRIES} attempts failed")


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _parse_minutes(val) -> float:
    """Parse NBA minutes string MM:SS or plain float to decimal minutes."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if ":" in s:
        parts = s.split(":")
        try:
            return int(parts[0]) + int(parts[1]) / 60
        except (ValueError, IndexError):
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_game_date(val):
    if val is None:
        return None
    try:
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:  # noqa: BLE001
        return None


def _parse_opponent_abbreviation(matchup: str | None) -> str | None:
    if not matchup:
        return None
    matchup = matchup.strip()
    if " @ " in matchup:
        return matchup.split(" @ ")[-1].strip().upper()
    if " vs. " in matchup:
        return matchup.split(" vs. ")[-1].strip().upper()
    return None


def _parse_is_home(matchup: str | None) -> bool | None:
    if not matchup:
        return None
    if " vs. " in matchup:
        return True
    if " @ " in matchup:
        return False
    return None


def _map_team_id(team_abbrev: str | None, nba_team_id, abbrev_cache: dict[str, int]) -> int | None:
    if team_abbrev:
        mapped = abbrev_cache.get(team_abbrev)
        if mapped:
            return mapped
    nba_id = _safe_int(nba_team_id)
    if nba_id is None:
        return None
    fallback_abbrev = NBA_ID_TO_ABBREV.get(nba_id, "").upper()
    return abbrev_cache.get(fallback_abbrev)


def fetch_team_stats(db: DatabaseManager, season: str) -> int:
    """Fetch pace, OffRtg, and DefRtg for all teams."""
    from nba_api.stats.endpoints import LeagueDashTeamStats

    logger.info("Fetching team stats for season %s ...", season)
    time.sleep(SLEEP_SECONDS)

    def _fetch() -> pd.DataFrame:
        endpoint = LeagueDashTeamStats(
            season=season,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Advanced",
            timeout=NBA_API_TIMEOUT_SECONDS,
        )
        return endpoint.get_data_frames()[0]

    df: pd.DataFrame = _call_with_retry(_fetch, "LeagueDashTeamStats")
    if df.empty:
        logger.warning("LeagueDashTeamStats returned empty DataFrame for season %s", season)
        return 0

    abbrev_cache = build_team_abbrev_cache(db)
    updated = 0
    for _, row in df.iterrows():
        nba_id = int(row["TEAM_ID"])
        abbrev = NBA_ID_TO_ABBREV.get(nba_id, "").upper()
        team_id = abbrev_cache.get(abbrev)
        if not team_id:
            logger.warning("No DB team_id for NBA team ID %s (%s)", nba_id, row.get("TEAM_NAME"))
            continue

        upsert_nba_team_stats(
            db,
            team_id=team_id,
            season=season,
            pace=_safe_float(row.get("PACE")),
            off_rtg=_safe_float(row.get("OFF_RATING")),
            def_rtg=_safe_float(row.get("DEF_RATING")),
            source=NBA_API_SOURCE,
            stats_through=datetime.now(timezone.utc),
        )
        updated += 1

    print(f"Team stats: {updated}/30 teams updated for {season}")
    return updated


def fetch_player_game_logs(
    db: DatabaseManager,
    season: str,
    season_type: str = DEFAULT_SEASON_TYPE,
) -> int:
    """Fetch season-long player game logs and persist them into Neon."""
    from nba_api.stats.endpoints import playergamelogs

    logger.info("Fetching raw player game logs for season %s (%s) ...", season, season_type)
    time.sleep(SLEEP_SECONDS)

    def _fetch() -> pd.DataFrame:
        endpoint = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
            player_id_nullable=None,
            timeout=NBA_API_TIMEOUT_SECONDS,
        )
        return endpoint.get_data_frames()[0]

    df: pd.DataFrame = _call_with_retry(_fetch, "PlayerGameLogs")
    if df.empty:
        logger.warning("PlayerGameLogs returned empty DataFrame")
        return 0

    df.columns = [c.upper() for c in df.columns]
    abbrev_cache = build_team_abbrev_cache(db)
    rows: list[dict] = []

    for record in df.to_dict(orient="records"):
        player_id = _safe_int(record.get("PLAYER_ID") or record.get("PLAYERID"))
        game_id = record.get("GAME_ID")
        if player_id is None or not game_id:
            continue

        matchup = record.get("MATCHUP")
        team_abbrev = str(record.get("TEAM_ABBREVIATION") or "").upper() or None
        opp_abbrev = _parse_opponent_abbreviation(matchup)

        team_id = _map_team_id(team_abbrev, record.get("TEAM_ID"), abbrev_cache)
        rows.append(
            {
                "season": season,
                "season_type": season_type,
                "player_id": player_id,
                "name": str(record.get("PLAYER_NAME") or "Unknown Player"),
                "team_id": team_id,
                "opponent_team_id": abbrev_cache.get(opp_abbrev) if opp_abbrev else None,
                "game_id": str(game_id),
                "game_date": _parse_game_date(record.get("GAME_DATE")),
                "matchup": matchup,
                "team_abbreviation": team_abbrev,
                "opponent_abbreviation": opp_abbrev,
                "is_home": _parse_is_home(matchup),
                "win_loss": record.get("WL"),
                "minutes": _parse_minutes(record.get("MIN")),
                "points": _safe_float(record.get("PTS")),
                "rebounds": _safe_float(record.get("REB")),
                "assists": _safe_float(record.get("AST")),
                "steals": _safe_float(record.get("STL")),
                "blocks": _safe_float(record.get("BLK")),
                "turnovers": _safe_float(record.get("TOV")),
                "fgm": _safe_float(record.get("FGM")),
                "fga": _safe_float(record.get("FGA")),
                "fg3m": _safe_float(record.get("FG3M")),
                "fg3a": _safe_float(record.get("FG3A")),
                "ftm": _safe_float(record.get("FTM")),
                "fta": _safe_float(record.get("FTA")),
                "plus_minus": _safe_float(record.get("PLUS_MINUS")),
            }
        )

    inserted = upsert_nba_player_game_logs(db, rows)
    print(f"Player game logs: {inserted} rows upserted for {season} ({season_type})")
    return inserted


def fetch_team_game_logs(
    db: DatabaseManager,
    season: str,
    season_type: str = DEFAULT_SEASON_TYPE,
) -> int:
    """Fetch season-long team game logs and persist them into Neon."""
    from nba_api.stats.endpoints import teamgamelogs

    logger.info("Fetching raw team game logs for season %s (%s) ...", season, season_type)
    time.sleep(SLEEP_SECONDS)

    def _fetch() -> pd.DataFrame:
        endpoint = teamgamelogs.TeamGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
            timeout=NBA_API_TIMEOUT_SECONDS,
        )
        return endpoint.get_data_frames()[0]

    df: pd.DataFrame = _call_with_retry(_fetch, "TeamGameLogs")
    if df.empty:
        logger.warning("TeamGameLogs returned empty DataFrame")
        return 0

    df.columns = [c.upper() for c in df.columns]
    abbrev_cache = build_team_abbrev_cache(db)
    rows: list[dict] = []

    for record in df.to_dict(orient="records"):
        game_id = record.get("GAME_ID")
        if not game_id:
            continue

        matchup = record.get("MATCHUP")
        opp_abbrev = _parse_opponent_abbreviation(matchup)
        pts = _safe_float(record.get("PTS"))
        plus_minus = _safe_float(record.get("PLUS_MINUS"))
        opp_pts = pts - plus_minus if pts is not None and plus_minus is not None else None

        team_abbrev = str(record.get("TEAM_ABBREVIATION") or "").upper() or None
        team_db_id = _map_team_id(team_abbrev, record.get("TEAM_ID"), abbrev_cache)
        if team_db_id is None:
            continue
        rows.append(
            {
                "season": season,
                "season_type": season_type,
                "team_id": team_db_id,
                "opponent_team_id": abbrev_cache.get(opp_abbrev) if opp_abbrev else None,
                "team_name": str(record.get("TEAM_NAME") or "Unknown Team"),
                "team_abbreviation": team_abbrev,
                "opponent_abbreviation": opp_abbrev,
                "game_id": str(game_id),
                "game_date": _parse_game_date(record.get("GAME_DATE")),
                "matchup": matchup,
                "is_home": _parse_is_home(matchup),
                "win_loss": record.get("WL"),
                "fg3m": _safe_float(record.get("FG3M")),
                "fg3a": _safe_float(record.get("FG3A")),
                "opp_fg3m": _safe_float(record.get("OPP_FG3M")),
                "opp_fg3a": _safe_float(record.get("OPP_FG3A")),
                "pts": pts,
                "opp_pts": opp_pts,
                "ast": _safe_float(record.get("AST")),
                "reb": _safe_float(record.get("REB")),
                "opp_ast": _safe_float(record.get("OPP_AST")),
                "opp_reb": _safe_float(record.get("OPP_REB")),
                "fga": _safe_float(record.get("FGA")),
                "fta": _safe_float(record.get("FTA")),
                "oreb": _safe_float(record.get("OREB")),
                "tov": _safe_float(record.get("TOV")),
                "opp_fga": _safe_float(record.get("OPP_FGA")),
                "opp_fta": _safe_float(record.get("OPP_FTA")),
                "opp_oreb": _safe_float(record.get("OPP_OREB")),
                "opp_tov": _safe_float(record.get("OPP_TOV")),
                "plus_minus": plus_minus,
            }
        )

    inserted = upsert_nba_team_game_logs(db, rows)
    print(f"Team game logs: {inserted} rows upserted for {season} ({season_type})")
    return inserted


EWMA_ALPHA = 0.25


def _ewma(series: pd.Series) -> float | None:
    """EWMA of a newest-first series, oldest game weighted least."""
    vals = series.dropna()
    if vals.empty:
        return None
    chronological = vals.iloc[::-1].reset_index(drop=True)
    smoothed = chronological.ewm(alpha=EWMA_ALPHA, adjust=False).mean()
    return float(smoothed.iloc[-1])


def _rolling_values(group: pd.DataFrame) -> dict:
    """Rolling per-game values for one player's games, newest first.

    Shared by the stats.nba.com game-log path and the ESPN box-score path so
    both sources produce the same numbers from the same games. Expects columns
    minutes, points, rebounds, assists, steals, blocks, turnovers, fga, fta, fg3m.
    """
    avg_minutes = _ewma(group["minutes"].fillna(0))
    ppg = _ewma(group["points"])
    rpg = _ewma(group["rebounds"])
    apg = _ewma(group["assists"])
    spg = _ewma(group["steals"])
    bpg = _ewma(group["blocks"])
    tovpg = _ewma(group["turnovers"])
    threefgm_pg = _ewma(group["fg3m"])

    fga_pg = _ewma(group["fga"]) or 0.0
    fta_pg = _ewma(group["fta"]) or 0.0
    tov_pg = tovpg or 0.0
    min_pg = avg_minutes or 1.0
    usage_rate = ((fga_pg + 0.44 * fta_pg + tov_pg) / max(min_pg / 48.0 * 100, 1)) * 100

    def _has_dd(row: pd.Series) -> bool:
        cats = [
            row.get("points", 0) or 0,
            row.get("rebounds", 0) or 0,
            row.get("assists", 0) or 0,
            row.get("steals", 0) or 0,
            row.get("blocks", 0) or 0,
        ]
        return sum(c >= 10 for c in cats) >= 2

    dd_binary = group.apply(_has_dd, axis=1).astype(float)
    dd_rate = _ewma(dd_binary) or 0.0

    fpts_series = (
        group["points"].fillna(0) * 1.0
        + group["rebounds"].fillna(0) * 1.25
        + group["assists"].fillna(0) * 1.5
        + group["steals"].fillna(0) * 2.0
        + group["blocks"].fillna(0) * 2.0
        - group["turnovers"].fillna(0) * 0.5
        + group["fg3m"].fillna(0) * 0.5
        + dd_binary * 1.5
    )
    fpts_std = _safe_float(fpts_series.std()) if len(fpts_series) > 1 else None

    return {
        "avg_minutes": avg_minutes or 0.0,
        "ppg": ppg or 0.0,
        "rpg": rpg or 0.0,
        "apg": apg or 0.0,
        "spg": spg or 0.0,
        "bpg": bpg or 0.0,
        "tovpg": tovpg or 0.0,
        "threefgm_pg": threefgm_pg or 0.0,
        "usage_rate": round(usage_rate, 1),
        "dd_rate": round(float(dd_rate), 3),
        "fpts_std": round(fpts_std, 3) if fpts_std is not None else None,
    }


def fetch_player_rolling_stats(
    db: DatabaseManager,
    season: str,
    season_type: str | None = None,
    season_types: list[str] | None = None,
    n_games: int = 10,
) -> int:
    """Compute rolling n-game averages per player from stored raw game logs."""
    selected_season_types = season_types or ([season_type] if season_type else [DEFAULT_SEASON_TYPE])
    logger.info(
        "Computing rolling player stats from raw logs for %s (%s) ...",
        season,
        ", ".join(selected_season_types),
    )

    rows = db.execute(
        f"""
        SELECT
            player_id,
            name,
            team_id,
            team_abbreviation,
            game_id,
            game_date,
            minutes,
            points,
            rebounds,
            assists,
            steals,
            blocks,
            turnovers,
            fga,
            fta,
            fg3m
        FROM nba_player_game_logs
        WHERE season = %s AND season_type IN ({", ".join(["%s"] * len(selected_season_types))})
        ORDER BY player_id, game_date DESC NULLS LAST, game_id DESC
        """,
        (season, *selected_season_types),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("No raw player game logs found for %s (%s)", season, ", ".join(selected_season_types))
        return 0

    abbrev_cache = build_team_abbrev_cache(db)
    stats_through = _as_utc_midnight(df["game_date"].max())

    updated = 0
    for player_id, group in df.groupby("player_id"):
        group = group.sort_values(["game_date", "game_id"], ascending=[False, False], na_position="last")
        group = group.head(n_games)
        games = len(group)
        if games == 0:
            continue

        latest = group.iloc[0]
        name = str(latest["name"])
        team_abbrev = str(latest["team_abbreviation"]).upper() if latest["team_abbreviation"] else ""
        team_id = abbrev_cache.get(team_abbrev) or _safe_int(latest["team_id"])

        upsert_nba_player_stats(
            db,
            player_id=int(player_id),
            season=season,
            team_id=team_id,
            name=name,
            position=None,
            games=games,
            source=NBA_API_SOURCE,
            stats_through=stats_through,
            **_rolling_values(group),
        )
        updated += 1

    if updated:
        _drop_other_source_rows(db, "nba_player_stats", season, NBA_API_SOURCE)
    print(f"Player stats: {updated} players updated for {season} (last {n_games} games)")
    return updated


# ── Current-season stats from ESPN box scores ────────────────────────────────
#
# stats.nba.com has timed out from GitHub runners since about 2026-07-01 and
# from home since 2026-11-01, so it cannot be the source for a live season.
# ESPN box scores (ingest.nba_box_scores, nightly, cloud-reachable) carry every
# input the rolling stats need. Rows built from them are marked source='espn'
# and carry player_id = -espn_athlete_id: a negative id cannot collide with an
# NBA person id, so a stats.nba.com upsert can never overwrite a different
# player's ESPN row. Consumers match on team + name, never on player_id.
# One source per season: whichever writer runs last deletes the other's rows
# for that season, so a player never has two rows a consumer must choose from.

ESPN_SOURCE = "espn"
NBA_API_SOURCE = "nba_api"

# Possessions from box-score columns without offensive rebounds (ESPN player
# rows carry total rebounds only): OREB ~ this share of missed field goals.
# Measured 0.2506 on 2026-27 ESPN team boxes (2026-11-15). Pace and ratings are
# then rescaled to the model's league constants, so the level cancels and only
# the team-to-team differences remain (pace r=0.92, ratings r=0.98 vs exact).
OREB_PER_MISSED_FG = 0.25
# Early-season shrinkage of team pace/ratings toward the league constant:
# weight n / (n + TEAM_SHRINK_GAMES). Stated prior, not fitted; with ~12 games
# a team's own value counts half. Out-of-sample on 2026-27, a team's first six
# games predicted its next six worse than the league constant did unshrunk.
TEAM_SHRINK_GAMES = 12.0


def season_window(season: str) -> tuple[date, date]:
    """[Aug 1 of the start year, Aug 1 of the next year): config.nba_season_for's rule."""
    start = int(season[:4])
    return date(start, 8, 1), date(start + 1, 8, 1)


def _as_utc_midnight(value) -> datetime | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


# ESPN event season types that count: regular season, playoffs, play-in.
# Preseason (1) is left out, like the stats.nba.com path's season types.
ESPN_COUNTED_SEASON_TYPES = (2, 3, 5)

# A real NBA game: a counted season type and two franchise teams. ESPN labels
# the All-Star and Rising Stars games regular season (type 2) under names such
# as 'Team Stars' and 'World', and preseason opponents can be foreign clubs.
_REAL_GAME_SQL = """
    g.completed AND g.season_type IN %s
    AND EXISTS (SELECT 1 FROM teams th WHERE th.name = g.home_team)
    AND EXISTS (SELECT 1 FROM teams ta WHERE ta.name = g.away_team)
"""


def _espn_player_rows(db: DatabaseManager, season: str) -> pd.DataFrame:
    """Every appearance with minutes > 0 in the season's real games, newest first.

    stats.nba.com logs hold no DNP rows, so ``played AND minutes > 0`` keeps
    the two sources comparable (11 garbage-time rows are played with 0 minutes).
    Team id comes from the ESPN team display name, which matches teams.name for
    all 30 teams; ESPN's abbreviations (GS, NY, UTAH, WSH...) do not.
    """
    lo, hi = season_window(season)
    rows = db.execute(
        f"""
        SELECT p.espn_athlete_id, p.player AS name, t.team_id, p.team,
               p.espn_event_id AS game_id, g.game_date, g.start_time,
               p.minutes, p.points, p.rebounds, p.assists, p.steals, p.blocks,
               p.turnovers, p.fga, p.fta, p.fg3m, p.fgm
        FROM nba_box_score_players p
        JOIN nba_box_score_games g ON g.espn_event_id = p.espn_event_id
        LEFT JOIN teams t ON t.name = p.team
        WHERE {_REAL_GAME_SQL} AND p.played AND p.minutes > 0
          AND g.game_date >= %s AND g.game_date < %s
        ORDER BY p.espn_athlete_id, g.start_time DESC, p.espn_event_id DESC
        """,
        (ESPN_COUNTED_SEASON_TYPES, lo, hi),
    )
    return pd.DataFrame(rows)


def build_player_stats_from_espn(db: DatabaseManager, season: str, n_games: int = 10) -> int:
    """Rolling n-game player stats for ``season`` from ESPN box scores."""
    df = _espn_player_rows(db, season)
    if df.empty:
        logger.warning("No ESPN box scores for %s", season)
        return 0
    unmapped = sorted(df.loc[df["team_id"].isna(), "team"].dropna().unique())
    if unmapped:
        raise RuntimeError(f"ESPN team names with no teams row: {unmapped}")
    stats_through = _as_utc_midnight(df["start_time"].max())

    updated = 0
    for athlete_id, group in df.groupby("espn_athlete_id"):
        # Newest first by tip time: two rows can share an Eastern date (a
        # traded player listed by both teams), so game_date cannot order them.
        group = group.sort_values(["start_time", "game_id"], ascending=[False, False]).head(n_games)
        latest = group.iloc[0]
        upsert_nba_player_stats(
            db,
            player_id=-int(athlete_id),
            season=season,
            team_id=int(latest["team_id"]),
            name=str(latest["name"]),   # ESPN display name: consumers match raw names
            position=None,
            games=len(group),
            source=ESPN_SOURCE,
            stats_through=stats_through,
            **_rolling_values(group),
        )
        updated += 1

    removed = _drop_other_source_rows(db, "nba_player_stats", season, ESPN_SOURCE)
    print(f"Player stats (ESPN): {updated} players for {season} through {stats_through:%Y-%m-%d %H:%M} UTC"
          + (f"; removed {removed} rows from another source" if removed else ""))
    return updated


def _team_game_frame(df: pd.DataFrame) -> pd.DataFrame:
    """One row per team-game: possessions, points and minutes for and against."""
    df = df.assign(fgm=df["fgm"].fillna(0), fga=df["fga"].fillna(0), fta=df["fta"].fillna(0),
                   turnovers=df["turnovers"].fillna(0), points=df["points"].fillna(0),
                   minutes=df["minutes"].fillna(0))
    team = (df.groupby(["game_id", "team_id"], as_index=False)
              [["fga", "fgm", "fta", "turnovers", "points", "minutes"]].sum())
    team["poss"] = (team.fga + 0.44 * team.fta + team.turnovers
                    - OREB_PER_MISSED_FG * (team.fga - team.fgm))
    games = team.groupby("game_id").agg(n=("team_id", "size"), game_poss=("poss", "mean"),
                                        game_minutes=("minutes", "sum"))
    team = team.merge(games, on="game_id")
    team = team[team.n == 2].copy()          # both sides present
    opp = team[["game_id", "team_id", "points"]].rename(columns={"team_id": "opp_id", "points": "opp_points"})
    team = team.merge(opp, on="game_id")
    team = team[team.team_id != team.opp_id]
    # Game length in minutes: 10 player-minutes per game-minute (5 a side).
    team["game_len"] = team.game_minutes / 10.0
    return team


def build_team_stats_from_espn(db: DatabaseManager, season: str) -> int:
    """Pace / OffRtg / DefRtg for ``season`` from ESPN box scores.

    Possessions are estimated (no OREB column), averaged over both teams of a
    game, then aggregated over the season. Values are rescaled so the league
    mean equals the model constants (LEAGUE_AVG_PACE, LEAGUE_AVG_OFF/DEF_RTG)
    and shrunk toward them by n / (n + TEAM_SHRINK_GAMES).
    """
    from model.dfs_projections import LEAGUE_AVG_DEF_RTG, LEAGUE_AVG_OFF_RTG, LEAGUE_AVG_PACE

    df = _espn_player_rows(db, season)
    if df.empty:
        logger.warning("No ESPN box scores for %s", season)
        return 0
    tg = _team_game_frame(df.dropna(subset=["team_id"]))
    if tg.empty:
        return 0
    stats_through = _as_utc_midnight(df["start_time"].max())
    agg = tg.groupby("team_id").agg(games=("game_id", "nunique"), poss=("game_poss", "sum"),
                                    length=("game_len", "sum"), pts=("points", "sum"),
                                    opp_pts=("opp_points", "sum"))
    agg["pace"] = 48.0 * agg.poss / agg.length
    agg["off"] = 100.0 * agg.pts / agg.poss
    agg["def"] = 100.0 * agg.opp_pts / agg.poss
    league_pace = 48.0 * agg.poss.sum() / agg.length.sum()
    league_rtg = 100.0 * agg.pts.sum() / agg.poss.sum()   # = league DefRtg by construction
    weight = agg.games / (agg.games + TEAM_SHRINK_GAMES)

    def scaled(raw: pd.Series, league_raw: float, constant: float) -> pd.Series:
        return constant + (raw * constant / league_raw - constant) * weight

    agg["pace_s"] = scaled(agg.pace, league_pace, LEAGUE_AVG_PACE)
    agg["off_s"] = scaled(agg["off"], league_rtg, LEAGUE_AVG_OFF_RTG)
    agg["def_s"] = scaled(agg["def"], league_rtg, LEAGUE_AVG_DEF_RTG)
    for team_id, row in agg.iterrows():
        upsert_nba_team_stats(
            db, team_id=int(team_id), season=season,
            pace=round(float(row.pace_s), 2), off_rtg=round(float(row.off_s), 2),
            def_rtg=round(float(row.def_s), 2), source=ESPN_SOURCE, stats_through=stats_through,
        )
    print(f"Team stats (ESPN): {len(agg)}/30 teams for {season} (league raw pace {league_pace:.1f}, "
          f"rating {league_rtg:.1f}; rescaled to {LEAGUE_AVG_PACE:g}/{LEAGUE_AVG_OFF_RTG:g}, "
          f"shrunk n/(n+{TEAM_SHRINK_GAMES:g}))")
    return len(agg)


def _drop_other_source_rows(db: DatabaseManager, table: str, season: str, keep_source: str) -> int:
    """Delete ``table`` rows for ``season`` written by a different source."""
    assert table in {"nba_player_stats", "nba_team_stats"}
    rows = db.execute(
        f"DELETE FROM {table} WHERE season = %s AND COALESCE(source, %s) <> %s RETURNING id",
        (season, NBA_API_SOURCE, keep_source),
    )
    return len(rows or [])


def espn_has_season(db: DatabaseManager, season: str) -> bool:
    lo, hi = season_window(season)
    row = db.execute_one(
        f"SELECT COUNT(*) AS n FROM nba_box_score_games g WHERE {_REAL_GAME_SQL} AND game_date >= %s AND game_date < %s",
        (ESPN_COUNTED_SEASON_TYPES, lo, hi),
    )
    return bool(row and int(row["n"]) > 0)


def _run_refresh_stage(label: str, fn: Callable[[], int]) -> tuple[bool, int | None]:
    try:
        count = fn()
        logger.info("%s completed (%s)", label, count)
        return True, count
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s failed: %s", label, exc)
        print(f"{label}: FAILED ({type(exc).__name__}: {exc})")
        return False, None


def _normalize_refresh_season_types(season_type: str | None) -> list[str]:
    if season_type is None:
        return list(DEFAULT_SEASON_TYPES)
    normalized = season_type.strip().lower()
    if normalized in {"all", "both", "regular+playoffs", "regular season + playoffs"}:
        return list(DEFAULT_SEASON_TYPES)
    return [season_type]


def run_refresh(db: DatabaseManager, season: str, season_type: str | None, n_games: int) -> int:
    """Refresh one season's team and player stats; exit code 1 when either was not written.

    A season ESPN covers (2026-27 on) is built from ESPN box scores; stats.nba.com
    is not tried, because it has timed out from every runner since mid-2026 and a
    stage that always fails would hide a real failure. Today's season before its
    first real game (Aug 1 to opening night) has nothing to build and exits 0
    with a notice. Older seasons still use stats.nba.com game logs, and there a
    recompute of stored logs is not a refresh: the run fails if any fetch stage
    failed, or if nothing was written for the season.
    """
    selected_season_types = _normalize_refresh_season_types(season_type)
    stages: list[tuple[str, bool, int | None]] = []

    def stage(name: str, fn: Callable[[], int]) -> int | None:
        ok, count = _run_refresh_stage(name, fn)
        stages.append((name, ok, count))
        return count if ok else None

    if espn_has_season(db, season):
        team_rows = stage("team_stats:espn", lambda: build_team_stats_from_espn(db, season))
        player_rows = stage("player_stats:espn", lambda: build_player_stats_from_espn(db, season, n_games))
        fetch_failed: list[str] = []
    elif season == nba_season_for():
        print(f"NBA refresh: no {season} games yet (ESPN box scores start with the regular season); "
              "nothing to build until opening night.")
        return 0
    else:
        team_rows = stage("team_stats", lambda: fetch_team_stats(db, season))
        for selected_season_type in selected_season_types:
            stage(f"team_game_logs:{selected_season_type}",
                  lambda s=selected_season_type: fetch_team_game_logs(db, season, season_type=s))
            stage(f"player_game_logs:{selected_season_type}",
                  lambda s=selected_season_type: fetch_player_game_logs(db, season, season_type=s))
        fetch_failed = [n for n, ok, _ in stages if not ok]
        player_rows = stage(
            "player_rolling_stats",
            lambda: fetch_player_rolling_stats(db, season, season_types=selected_season_types, n_games=n_games),
        )

    print(
        "NBA refresh summary: "
        + ", ".join(
            f"{name}={'ok' if ok else 'failed'}{f' ({count})' if count is not None else ''}"
            for name, ok, count in stages
        )
    )
    problems: list[str] = []
    if fetch_failed:
        problems.append(f"stats.nba.com stages failed ({', '.join(fetch_failed)}); "
                        "player rows were only re-derived from stored logs")
    if not team_rows:
        problems.append(f"no team stats written for {season}")
    if not player_rows:
        problems.append(f"no player stats written for {season}")
    if problems:
        logger.error("NBA refresh FAILED: %s", "; ".join(problems))
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build NBA team and player rolling stats (ESPN box scores; stats.nba.com for older seasons)")
    parser.add_argument("--season", default=None, help="Season string e.g. 2026-27 (default: today's season)")
    parser.add_argument(
        "--season-type",
        default="All",
        help='Season type: "Regular Season", "Playoffs", or "All" (default)',
    )
    parser.add_argument("--games", type=int, default=10, help="Rolling game window (default 10)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    season = args.season or config.nba_api.season

    sys.exit(run_refresh(db, season, args.season_type, args.games))
