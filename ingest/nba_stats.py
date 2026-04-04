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
    python -m ingest.nba_stats --season 2025-26 --season-type "Regular Season"
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Callable, TypeVar

import pandas as pd

from config import load_config
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


def fetch_player_rolling_stats(
    db: DatabaseManager,
    season: str,
    season_type: str = DEFAULT_SEASON_TYPE,
    n_games: int = 10,
) -> int:
    """Compute rolling n-game averages per player from stored raw game logs."""
    logger.info("Computing rolling player stats from raw logs for %s (%s) ...", season, season_type)

    rows = db.execute(
        """
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
        WHERE season = %s AND season_type = %s
        ORDER BY player_id, game_date DESC NULLS LAST, game_id DESC
        """,
        (season, season_type),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("No raw player game logs found for %s (%s)", season, season_type)
        return 0

    abbrev_cache = build_team_abbrev_cache(db)
    ewma_alpha = 0.25

    def _ewma(series: pd.Series) -> float | None:
        vals = series.dropna()
        if vals.empty:
            return None
        chronological = vals.iloc[::-1].reset_index(drop=True)
        smoothed = chronological.ewm(alpha=ewma_alpha, adjust=False).mean()
        return float(smoothed.iloc[-1])

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

        upsert_nba_player_stats(
            db,
            player_id=int(player_id),
            season=season,
            team_id=team_id,
            name=name,
            position=None,
            games=games,
            avg_minutes=avg_minutes or 0.0,
            ppg=ppg or 0.0,
            rpg=rpg or 0.0,
            apg=apg or 0.0,
            spg=spg or 0.0,
            bpg=bpg or 0.0,
            tovpg=tovpg or 0.0,
            threefgm_pg=threefgm_pg or 0.0,
            usage_rate=round(usage_rate, 1),
            dd_rate=round(float(dd_rate), 3),
            fpts_std=round(fpts_std, 3) if fpts_std is not None else None,
        )
        updated += 1

    print(f"Player stats: {updated} players updated for {season} (last {n_games} games)")
    return updated


def _run_refresh_stage(label: str, fn: Callable[[], int]) -> tuple[bool, int | None]:
    try:
        count = fn()
        logger.info("%s completed (%s)", label, count)
        return True, count
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s failed: %s", label, exc)
        print(f"{label}: FAILED ({type(exc).__name__}: {exc})")
        return False, None


def run_refresh(db: DatabaseManager, season: str, season_type: str, n_games: int) -> int:
    stages: list[tuple[str, bool, int | None]] = []

    ok, count = _run_refresh_stage("team_stats", lambda: fetch_team_stats(db, season))
    stages.append(("team_stats", ok, count))

    ok, count = _run_refresh_stage(
        "team_game_logs",
        lambda: fetch_team_game_logs(db, season, season_type=season_type),
    )
    stages.append(("team_game_logs", ok, count))

    ok, count = _run_refresh_stage(
        "player_game_logs",
        lambda: fetch_player_game_logs(db, season, season_type=season_type),
    )
    stages.append(("player_game_logs", ok, count))

    ok, count = _run_refresh_stage(
        "player_rolling_stats",
        lambda: fetch_player_rolling_stats(db, season, season_type=season_type, n_games=n_games),
    )
    stages.append(("player_rolling_stats", ok, count))

    succeeded = [name for name, ok, _ in stages if ok]
    failed = [name for name, ok, _ in stages if not ok]
    print(
        "NBA refresh summary: "
        + ", ".join(
            f"{name}={'ok' if ok else 'failed'}{f' ({count})' if count is not None else ''}"
            for name, ok, count in stages
        )
    )

    usable_outputs = {"team_stats", "player_rolling_stats"}
    if any(name in usable_outputs for name in succeeded):
        if failed:
            logger.warning("NBA refresh partially succeeded; failed stages: %s", ", ".join(failed))
        return 0

    logger.error("NBA refresh produced no usable outputs. Failed stages: %s", ", ".join(failed) or "all")
    return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch NBA stats from stats.nba.com")
    parser.add_argument("--season", default=None, help="Season string e.g. 2025-26")
    parser.add_argument("--season-type", default=DEFAULT_SEASON_TYPE, help="Season type, e.g. Regular Season")
    parser.add_argument("--games", type=int, default=10, help="Rolling game window (default 10)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    season = args.season or config.nba_api.season

    sys.exit(run_refresh(db, season, args.season_type, args.games))
