"""Fetch MLB batter and pitcher rolling stats via pybaseball (FanGraphs).

Uses pybaseball which wraps the FanGraphs API. No authentication required.
pybaseball caches responses in ~/.pybaseball by default — re-runs in the same
session skip redundant HTTP calls.

Player ID note:
  player_id stored in mlb_batter_stats / mlb_pitcher_stats is the FanGraphs
  integer playerid.  This is NOT the same as the MLBAM (MLB Stats API) ID
  used in mlb_matchups.home_sp_id / away_sp_id.  Pitcher→matchup linkage at
  projection time (Phase 5) uses name-based matching — standard in DFS tools.

Rolling window:
  Default: last 45 days ≈ 30-40 team games played.
  Per-game rates are derived from period totals / G (not true EWMA, which
  would require per-game MLB Stats API calls).  If the window is in the
  off-season and returns no data, the function falls back to full-season
  aggregates for the configured season.
  fpts_std is estimated at 0.70× avg_fpts (batters) / 0.55× (pitchers) —
  a practical heuristic; Phase 5 can refine with game-log variance.

DK MLB scoring (batters):
  1B×3  |  2B×5  |  3B×8  |  HR×10  |  RBI×2  |  R×2  |  BB×2  |  HBP×2  |  SB×5

DK MLB scoring (pitchers):
  IP×2.25  |  K×2  |  W+4  |  ER-2  |  H-0.6  |  BB-0.6

Usage:
    python -m ingest.mlb_stats                    # 2025 season, 45-day window
    python -m ingest.mlb_stats --season 2025      # explicit season
    python -m ingest.mlb_stats --season 2025 --full-season
    python -m ingest.mlb_stats --days 30          # tighter rolling window
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

import pandas as pd
import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_mlb_team_abbrev_cache,
    upsert_mlb_batter_stats,
    upsert_mlb_pitcher_stats,
    upsert_mlb_team_stats,
)
from model.mlb_projections import dk_batter_fpts, dk_pitcher_fpts

logger = logging.getLogger(__name__)

# ── FanGraphs → our mlb_teams.abbreviation mapping ───────────────────────────
# Only entries that differ from the standard 2-3 letter code are listed.
# All others (NYY, BOS, TB, BAL, CWS, CLE, MIN, KC, DET, HOU, LAA, SEA,
# TEX, NYM, ATL, PHI, MIA, CHC, STL, MIL, CIN, PIT, LAD, COL, ARI, SF,
# SD, TOR) pass through unchanged.
_FG_TEAM_MAP: dict[str, str] = {
    "WAS": "WSH",   # FanGraphs legacy code
    "WSN": "WSH",   # FanGraphs current Nationals code
    "ATH": "OAK",   # Athletics may appear as "ATH" on FanGraphs in 2025+
    "OAK": "OAK",   # Athletics legacy code still accepted
    "CHW": "CWS",
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
}

# Keep early-season rows so projection blending can shrink them instead of
# dropping coverage entirely. The model will still lean on the prior season
# until current-season samples earn weight.
_MIN_PA  = 1
_MIN_IP  = 1.0


# ── Public fetch functions ────────────────────────────────────────────────────

def fetch_batter_stats(db: DatabaseManager, season: str, days: int = 45, full_season: bool = False) -> int:
    """Fetch FanGraphs batter stats and upsert to mlb_batter_stats.

    Tries the rolling date-range window first; falls back to full-season
    aggregates if the window returns insufficient data (e.g., preseason).

    Returns number of batter rows upserted.
    """
    from pybaseball import batting_stats_range, batting_stats  # noqa: PLC0415

    start_dt = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    end_dt   = date.today().strftime("%Y-%m-%d")
    logger.info(
        "Fetching FanGraphs batter stats %s ...",
        f"full season {season}" if full_season else f"{start_dt} to {end_dt}",
    )

    df, source = _fetch_batting(batting_stats_range, batting_stats, start_dt, end_dt, season, full_season=full_season)
    if df is None or df.empty:
        logger.warning("No batter stats available (season=%s, days=%d)", season, days)
        return 0

    # Apply minimum PA filter
    df = df[df["PA"].fillna(0).astype(float) >= _MIN_PA].copy()
    if df.empty:
        logger.warning("All batters filtered out (PA < %d)", _MIN_PA)
        return 0

    abbrev_cache = build_mlb_team_abbrev_cache(db)
    updated = 0

    for _, row in df.iterrows():
        games = int(_safe_float(row.get("G")) or 0)
        if games == 0:
            continue

        player_id = _get_player_id(row)
        if player_id is None:
            continue

        name = str(row.get("Name", "")).strip()
        if not name:
            continue

        team_id = _resolve_team(row, abbrev_cache)

        # Singles: prefer explicit column; fall back to H - 2B - 3B - HR
        singles_raw = row.get("1B")
        if singles_raw is None or (isinstance(singles_raw, float) and pd.isna(singles_raw)):
            h  = _safe_float(row.get("H"))  or 0
            d  = _safe_float(row.get("2B")) or 0
            t  = _safe_float(row.get("3B")) or 0
            hr = _safe_float(row.get("HR")) or 0
            singles_raw = max(0.0, h - d - t - hr)

        # Per-game counting rates
        def _pg(col: str) -> float:
            return (_safe_float(row.get(col)) or 0.0) / games

        singles_pg  = (_safe_float(singles_raw) or 0.0) / games
        doubles_pg  = _pg("2B")
        triples_pg  = _pg("3B")
        hr_pg       = _pg("HR")
        rbi_pg      = _pg("RBI")
        runs_pg     = _pg("R")
        bb_pg       = _pg("BB")
        sb_pg       = _pg("SB")
        hbp_pg      = _pg("HBP")
        pa_pg       = _pg("PA")

        avg_fpts_pg = dk_batter_fpts(
            singles=singles_pg, doubles=doubles_pg, triples=triples_pg,
            hr=hr_pg, rbi=rbi_pg, runs=runs_pg, bb=bb_pg,
            hbp=hbp_pg, sb=sb_pg,
        )
        fpts_std = round(avg_fpts_pg * 0.70, 3) if avg_fpts_pg > 0 else None

        upsert_mlb_batter_stats(
            db,
            player_id=player_id,
            season=season,
            team_id=team_id,
            name=name,
            games=games,
            pa_pg=round(pa_pg, 3),
            avg=_safe_float(row.get("AVG")),
            obp=_safe_float(row.get("OBP")),
            slg=_safe_float(row.get("SLG")),
            iso=_safe_float(row.get("ISO")),
            babip=_safe_float(row.get("BABIP")),
            wrc_plus=_safe_float(row.get("wRC+")),
            k_pct=_safe_float(row.get("K%")),
            bb_pct=_safe_float(row.get("BB%")),
            hr_pg=round(hr_pg, 4),
            singles_pg=round(singles_pg, 4),
            doubles_pg=round(doubles_pg, 4),
            triples_pg=round(triples_pg, 4),
            rbi_pg=round(rbi_pg, 4),
            runs_pg=round(runs_pg, 4),
            sb_pg=round(sb_pg, 4),
            hbp_pg=round(hbp_pg, 4),
            avg_fpts_pg=round(avg_fpts_pg, 3),
            fpts_std=fpts_std,
        )
        updated += 1

    print(f"Batter stats: {updated} players upserted for {season} ({source})")
    return updated


def fetch_pitcher_stats(db: DatabaseManager, season: str, days: int = 45, full_season: bool = False) -> int:
    """Fetch FanGraphs pitcher stats and upsert to mlb_pitcher_stats.

    Returns number of pitcher rows upserted.
    """
    from pybaseball import pitching_stats_range, pitching_stats  # noqa: PLC0415

    start_dt = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    end_dt   = date.today().strftime("%Y-%m-%d")
    logger.info(
        "Fetching FanGraphs pitcher stats %s ...",
        f"full season {season}" if full_season else f"{start_dt} to {end_dt}",
    )

    df, source = _fetch_pitching(pitching_stats_range, pitching_stats, start_dt, end_dt, season, full_season=full_season)
    if df is None or df.empty:
        logger.warning("No pitcher stats available (season=%s, days=%d)", season, days)
        return 0

    # Compute decimal IP and apply minimum filter
    df["_ip"] = df["IP"].apply(_parse_ip)
    df = df[df["_ip"] >= _MIN_IP].copy()
    if df.empty:
        logger.warning("All pitchers filtered out (IP < %.1f)", _MIN_IP)
        return 0

    abbrev_cache = build_mlb_team_abbrev_cache(db)
    updated = 0

    for _, row in df.iterrows():
        games = int(_safe_float(row.get("G")) or 0)
        if games == 0:
            continue

        player_id = _get_player_id(row)
        if player_id is None:
            continue

        name = str(row.get("Name", "")).strip()
        if not name:
            continue

        team_id = _resolve_team(row, abbrev_cache)

        ip_total = _parse_ip(row.get("IP"))
        ip_pg    = ip_total / games if games > 0 else 0.0

        # K% and BB%: prefer FG columns; derive from SO/TBF if absent
        k_pct  = _safe_float(row.get("K%"))
        bb_pct = _safe_float(row.get("BB%"))
        if k_pct is None:
            tbf = _safe_float(row.get("TBF")) or 0.0
            so  = _safe_float(row.get("SO"))  or 0.0
            bb  = _safe_float(row.get("BB"))  or 0.0
            k_pct  = round(so / tbf, 3) if tbf > 0 else None
            bb_pct = round(bb / tbf, 3) if tbf > 0 else None

        # Win% only meaningful for starters
        gs    = _safe_float(row.get("GS")) or 0.0
        wins  = _safe_float(row.get("W"))  or 0.0
        win_pct = round(wins / gs, 3) if gs > 0 else None

        # Quality start estimate from average IP/G and ERA
        era  = _safe_float(row.get("ERA")) or 99.0
        qs_pct = _estimate_qs_pct(ip_pg, era) if gs > 0 else None

        # Per-game rates for DK FPTS
        def _pg(col: str) -> float:
            return (_safe_float(row.get(col)) or 0.0) / games

        k_pg   = _pg("SO")
        er_pg  = _pg("ER")
        h_pg   = _pg("H")
        bb_pg  = _pg("BB")

        avg_fpts_pg = dk_pitcher_fpts(
            ip=ip_pg, k=k_pg, er=er_pg, h=h_pg, bb=bb_pg,
            win_prob=win_pct or 0.0,
        )
        fpts_std = round(avg_fpts_pg * 0.55, 3) if avg_fpts_pg > 0 else None

        upsert_mlb_pitcher_stats(
            db,
            player_id=player_id,
            season=season,
            team_id=team_id,
            name=name,
            games=games,
            ip_pg=round(ip_pg, 3),
            era=_safe_float(row.get("ERA")),
            fip=_safe_float(row.get("FIP")),
            xfip=_safe_float(row.get("xFIP")),
            k_per_9=_safe_float(row.get("K/9")),
            bb_per_9=_safe_float(row.get("BB/9")),
            hr_per_9=_safe_float(row.get("HR/9")),
            k_pct=k_pct,
            bb_pct=bb_pct,
            hr_fb_pct=_safe_float(row.get("HR/FB")),
            whip=_safe_float(row.get("WHIP")),
            avg_fpts_pg=round(avg_fpts_pg, 3),
            fpts_std=fpts_std,
            win_pct=win_pct,
            qs_pct=qs_pct,
        )
        updated += 1

    print(f"Pitcher stats: {updated} pitchers upserted for {season} ({source})")
    return updated


def fetch_team_stats(db: DatabaseManager, season: str) -> int:
    """Fetch team batting + pitching environment stats from FanGraphs.

    team_wrc_plus / team_k_pct:  lineup quality for pitcher matchup scoring.
    bullpen_era / bullpen_fip:   used when starter exits early in projections.

    Returns count of teams updated.
    """
    from pybaseball import team_batting, team_pitching  # noqa: PLC0415

    season_int = int(season)
    logger.info("Fetching FanGraphs team stats for %s ...", season)

    try:
        bat_df: pd.DataFrame = team_batting(season_int)
        pit_df: pd.DataFrame = team_pitching(season_int)
    except Exception as exc:
        logger.warning("pybaseball team stats failed: %s", exc)
        return 0

    if bat_df is None or bat_df.empty:
        logger.warning("team_batting returned empty data for %s", season)
        return 0

    abbrev_cache = build_mlb_team_abbrev_cache(db)

    # Index pitching rows by FG team code for O(1) lookup
    pit_by_team: dict[str, dict] = {}
    if pit_df is not None and not pit_df.empty:
        for _, row in pit_df.iterrows():
            fg_team = _fg_team_code(row)
            pit_by_team[fg_team] = row.to_dict()

    updated = 0
    for _, row in bat_df.iterrows():
        fg_team = _fg_team_code(row)
        abbrev  = _FG_TEAM_MAP.get(fg_team, fg_team)
        team_id = abbrev_cache.get(abbrev)
        if not team_id:
            logger.debug("No team_id for FG team '%s' (mapped '%s')", fg_team, abbrev)
            continue

        pit = pit_by_team.get(fg_team, {})

        upsert_mlb_team_stats(
            db,
            team_id=team_id,
            season=season,
            team_wrc_plus=_safe_float(row.get("wRC+")),
            team_k_pct=_safe_float(row.get("K%")),
            team_bb_pct=_safe_float(row.get("BB%")),
            team_iso=_safe_float(row.get("ISO")),
            team_ops=_safe_float(row.get("OPS")),
            bullpen_era=_safe_float(pit.get("ERA")),
            bullpen_fip=_safe_float(pit.get("FIP")),
            staff_k_pct=_safe_float(pit.get("K%")),
            staff_bb_pct=_safe_float(pit.get("BB%")),
        )
        updated += 1

    print(f"Team stats: {updated}/30 teams updated for {season}")
    return updated


def fetch_batter_splits(db: DatabaseManager, season: str) -> int:
    """Fetch wRC+ vs LHP and vs RHP from FanGraphs and update mlb_batter_stats.

    Uses the FanGraphs internal leaders API (same endpoint pybaseball uses for
    batting_stats) with month=13 (vs LHP) and month=14 (vs RHP) split parameters.
    Falls back gracefully if the endpoint is unavailable — columns stay NULL
    and compute_batter_projection uses matchup_factor=1.0 (neutral).

    Returns count of players updated with split data.
    """
    year = int(season)
    _FG_URL = "https://www.fangraphs.com/api/leaders/major-league/data"
    _FG_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.fangraphs.com/",
    }
    _FG_PARAMS_BASE = {
        "pos": "all", "stats": "bat", "lg": "all",
        "qual": "1", "type": "8",
        "season": str(year), "season1": str(year),
        "ind": "0", "team": "0", "rost": "0", "age": "0",
        "filter": "", "players": "0",
        "startdate": "", "enddate": "",
        "pageitems": "2000000000", "pagenum": "1",
    }

    # Fetch both splits: month=13 = vs LHP, month=14 = vs RHP
    splits: dict[str, dict[int, float]] = {"L": {}, "R": {}}  # hand → playerid → wrc+
    for month, hand in [(13, "L"), (14, "R")]:
        params = {**_FG_PARAMS_BASE, "month": str(month)}
        try:
            resp = requests.get(_FG_URL, params=params, headers=_FG_HEADERS, timeout=30)
            resp.raise_for_status()
            raw = resp.json()
            rows = raw.get("data", raw) if isinstance(raw, dict) else raw
            for row in rows:
                pid_raw = row.get("playerid") or row.get("PlayerID")
                wrc_raw = row.get("wRC+")
                if pid_raw is None or wrc_raw is None:
                    continue
                try:
                    pid = int(float(pid_raw))
                    wrc = float(wrc_raw)
                    if not (0 < wrc < 400):
                        continue
                    splits[hand][pid] = wrc
                except (TypeError, ValueError):
                    continue
            logger.info("FanGraphs splits vs %sHP: %d players", hand, len(splits[hand]))
        except Exception as exc:
            logger.warning("FanGraphs split (vs %sHP) failed: %s", hand, exc)

    if not splits["L"] and not splits["R"]:
        print(f"Batter splits: no data returned for {season} — skipping L/R split update")
        return 0

    # Update players that have matching FG playerid in mlb_batter_stats
    updated = 0
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, player_id FROM mlb_batter_stats WHERE season = %s",
                    (season,)
                )
                rows = cur.fetchall()
                for row in rows:
                    row_id = row["id"] if isinstance(row, dict) else row[0]
                    player_id = row["player_id"] if isinstance(row, dict) else row[1]
                    wrc_l = splits["L"].get(player_id)
                    wrc_r = splits["R"].get(player_id)
                    if wrc_l is None and wrc_r is None:
                        continue
                    cur.execute(
                        """UPDATE mlb_batter_stats
                           SET wrc_plus_vs_l = %s, wrc_plus_vs_r = %s, fetched_at = NOW()
                           WHERE id = %s""",
                        (wrc_l, wrc_r, row_id)
                    )
                    updated += 1
    except Exception as exc:
        logger.warning("DB update for batter splits failed: %s", exc)

    print(f"Batter splits: {updated} players updated with L/R wRC+ for {season}")
    return updated


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_batting(
    range_fn, season_fn, start_dt: str, end_dt: str, season: str, full_season: bool = False,
) -> tuple[pd.DataFrame | None, str]:
    """Try rolling-window first; fall back to full-season aggregates."""
    if full_season:
        try:
            return season_fn(int(season), qual=1), f"{season} full season"
        except Exception as exc:
            logger.warning("batting_stats(%s) failed: %s", season, exc)
            return None, f"{season} full season"

    df: pd.DataFrame | None = None
    try:
        df = range_fn(start_dt, end_dt)
    except Exception as exc:
        logger.warning("batting_stats_range failed: %s", exc)

    # Determine if the window has enough qualified batters
    qualified_count = 0
    if df is not None and not df.empty:
        qualified_count = int((df["PA"].fillna(0).astype(float) >= _MIN_PA).sum())

    if qualified_count >= 50:   # healthy mid-season data
        return df, f"{start_dt} → {end_dt}"

    logger.info(
        "Rolling window sparse (%d qualified) — falling back to full season %s",
        qualified_count, season,
    )
    try:
        df = season_fn(int(season), qual=1)
        return df, f"{season} full season"
    except Exception as exc:
        logger.warning("batting_stats(%s) failed: %s", season, exc)
        return df, f"{start_dt} → {end_dt}"   # return sparse range data if all else fails


def _fetch_pitching(
    range_fn, season_fn, start_dt: str, end_dt: str, season: str, full_season: bool = False,
) -> tuple[pd.DataFrame | None, str]:
    """Try rolling-window first; fall back to full-season aggregates."""
    if full_season:
        try:
            return season_fn(int(season), qual=1), f"{season} full season"
        except Exception as exc:
            logger.warning("pitching_stats(%s) failed: %s", season, exc)
            return None, f"{season} full season"

    df: pd.DataFrame | None = None
    try:
        df = range_fn(start_dt, end_dt)
    except Exception as exc:
        logger.warning("pitching_stats_range failed: %s", exc)

    qualified_count = 0
    if df is not None and not df.empty:
        ip_series = df["IP"].apply(_parse_ip)
        qualified_count = int((ip_series >= _MIN_IP).sum())

    if qualified_count >= 30:   # healthy number of pitchers
        return df, f"{start_dt} → {end_dt}"

    logger.info(
        "Rolling window sparse (%d qualified pitchers) — falling back to full season %s",
        qualified_count, season,
    )
    try:
        df = season_fn(int(season), qual=1)
        return df, f"{season} full season"
    except Exception as exc:
        logger.warning("pitching_stats(%s) failed: %s", season, exc)
        return df, f"{start_dt} → {end_dt}"


def _get_player_id(row: pd.Series) -> int | None:
    """Extract FanGraphs playerid as int, or None if missing."""
    raw = row.get("playerid")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        raw = row.get("IDfg")
    if raw is None:
        return None
    try:
        f = float(raw)
        if pd.isna(f):
            return None
        return int(f)
    except (TypeError, ValueError):
        return None


def _resolve_team(row: pd.Series, abbrev_cache: dict[str, int]) -> int | None:
    """Map FanGraphs team code to our mlb_teams.team_id."""
    fg = _fg_team_code(row)
    abbrev = _FG_TEAM_MAP.get(fg, fg)
    return abbrev_cache.get(abbrev)


def _fg_team_code(row: pd.Series) -> str:
    """Extract normalized FanGraphs team code from a DataFrame row."""
    raw = row.get("Team") or row.get("team") or ""
    return str(raw).upper().strip()


def _parse_ip(val) -> float:
    """Convert baseball IP notation to decimal innings.

    FanGraphs stores IP as "6.2" meaning 6 full innings + 2 outs = 6⅔ innings.
    The fractional part is in units of outs (0-2), not tenths.

      _parse_ip("6.2")  → 6.667
      _parse_ip("5.1")  → 5.333
      _parse_ip("7.0")  → 7.0
      _parse_ip(None)   → 0.0
    """
    if val is None:
        return 0.0
    try:
        f = float(val)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(f):
        return 0.0
    whole = int(f)
    outs  = round((f - whole) * 10)   # ".2" → 2 outs; ".1" → 1 out
    return whole + outs / 3.0


def _estimate_qs_pct(ip_pg: float, era: float) -> float | None:
    """Estimate quality start probability from avg IP/G and ERA.

    A quality start requires 6+ IP and ≤3 earned runs in a start.
    Formula: base rate from IP/G scaled down by ERA penalty.
    """
    if ip_pg <= 0:
        return None
    # Base from average innings depth
    if ip_pg < 4.0:
        base = 0.05
    elif ip_pg < 5.0:
        base = 0.15
    elif ip_pg < 5.5:
        base = 0.30
    elif ip_pg < 6.0:
        base = 0.45
    else:
        base = min(0.75, 0.55 + (ip_pg - 6.0) * 0.05)

    # ERA penalty: each run above 4.0 ERA reduces QS% by ~10%
    era_factor = max(0.25, 1.0 - max(0.0, era - 4.0) * 0.10)
    return round(base * era_factor, 3)


def _safe_float(val) -> float | None:
    """Convert to float; return None if not numeric or NaN."""
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch MLB stats from FanGraphs via pybaseball")
    parser.add_argument("--season", default=None, help="Season year e.g. 2025")
    parser.add_argument("--days",   type=int, default=45, help="Rolling window days (default 45)")
    parser.add_argument("--full-season", action="store_true", help="Force FanGraphs full-season aggregates")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    season = args.season or config.mlb_api.season

    fetch_team_stats(db, season)
    fetch_batter_stats(db, season, days=args.days, full_season=args.full_season)
    fetch_pitcher_stats(db, season, days=args.days, full_season=args.full_season)
    fetch_batter_splits(db, season)
