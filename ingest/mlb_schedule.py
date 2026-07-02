"""Fetch today's MLB schedule and Vegas odds into mlb_matchups.

Two data sources combined:
  1. MLB Stats API (statsapi.mlb.com) — game IDs, home/away teams,
     ballpark, probable starting pitchers
  2. The Odds API (optional) — Vegas totals + moneylines + team-specific
     implied run totals, matched by full team name

No authentication or rate limiting on the MLB Stats API.

Usage:
    python -m ingest.mlb_schedule                    # today's games
    python -m ingest.mlb_schedule --date 2025-04-01  # specific date
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_mlb_team_abbrev_cache, insert_game_odds_history_rows, upsert_mlb_matchup
from ingest.mlb_teams import MLB_ID_TO_ABBREV
from model.dfs_projections import compute_team_implied_total
from model.soccer_bet_rating import american_to_prob, prob_to_american

logger = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
NON_PLAYED_STATES = {"Cancelled", "Postponed"}


def _build_mlb_team_context_cache(db: DatabaseManager) -> dict[int, dict[str, str | None]]:
    rows = db.execute("SELECT team_id, city, ballpark FROM mlb_teams")
    return {
        int(row["team_id"]): {
            "city": row.get("city"),
            "ballpark": row.get("ballpark"),
        }
        for row in rows
    }


def _to_compass(degrees: float | None) -> str | None:
    if degrees is None:
        return None
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = round((degrees % 360) / 45) % len(directions)
    return directions[idx]


def _nearest_hour_index(times: list[str], target_iso: str) -> int | None:
    if not times:
        return None
    try:
        target = datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
    except ValueError:
        return None

    best_idx: int | None = None
    best_delta: float | None = None
    for idx, ts in enumerate(times):
        try:
            point = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if point.tzinfo is None:
                point = point.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        delta = abs((point - target).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
    return best_idx


def _geocode_ballpark(
    query: str,
    *,
    timeout_seconds: int,
    cache: dict[str, tuple[float, float] | None],
) -> tuple[float, float] | None:
    if query in cache:
        return cache[query]
    try:
        resp = requests.get(
            NOMINATIM_SEARCH_URL,
            params={"q": query, "format": "jsonv2", "limit": 1},
            headers={"User-Agent": "NBADFS_v2/1.0"},
            timeout=timeout_seconds,
        )
        resp.raise_for_status()
        results = resp.json() or []
        if results:
            match = results[0]
            coords = (float(match["lat"]), float(match["lon"]))
            cache[query] = coords
            return coords
    except requests.RequestException as exc:
        logger.debug("Nominatim geocode failed for %s: %s", query, exc)
    cache[query] = None
    return None


def _fetch_weather_snapshot(
    *,
    latitude: float,
    longitude: float,
    game_start_iso: str,
    timeout_seconds: int,
) -> tuple[int | None, int | None, str | None]:
    try:
        game_start = datetime.fromisoformat(game_start_iso.replace("Z", "+00:00"))
    except ValueError:
        return (None, None, None)

    endpoint = OPEN_METEO_ARCHIVE_URL if game_start.date() < datetime.now(timezone.utc).date() else OPEN_METEO_FORECAST_URL
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "start_date": game_start.date().isoformat(),
        "end_date": game_start.date().isoformat(),
        "timezone": "UTC",
    }
    try:
        resp = requests.get(endpoint, params=params, timeout=timeout_seconds)
        resp.raise_for_status()
        payload = resp.json() or {}
    except requests.RequestException as exc:
        logger.debug("Open-Meteo weather fetch failed for %.4f, %.4f: %s", latitude, longitude, exc)
        return (None, None, None)

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    idx = _nearest_hour_index(times, game_start_iso)
    if idx is None:
        return (None, None, None)

    temps = hourly.get("temperature_2m") or []
    winds = hourly.get("wind_speed_10m") or []
    wind_dirs = hourly.get("wind_direction_10m") or []

    temp = round(float(temps[idx])) if idx < len(temps) and temps[idx] is not None else None
    wind_speed = round(float(winds[idx])) if idx < len(winds) and winds[idx] is not None else None
    wind_direction = _to_compass(float(wind_dirs[idx])) if idx < len(wind_dirs) and wind_dirs[idx] is not None else None
    return (temp, wind_speed, wind_direction)


def fetch_schedule(db: DatabaseManager, game_date: str | None = None) -> list[int]:
    """Fetch games for game_date (YYYY-MM-DD), upsert into mlb_matchups.

    Includes probable starting pitchers when posted by MLB. home_sp_id /
    away_sp_id store MLB Stats API player ids, and home_sp_name / away_sp_name
    store the probable starter names directly for later analytics joins.

    Returns list of mlb_matchup IDs upserted.
    """
    target_date = game_date or date.today().isoformat()
    logger.info("Fetching MLB schedule for %s ...", target_date)

    try:
        resp = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={
                "sportId": 1,
                "date": target_date,
                "hydrate": "probablePitcher",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("MLB Stats API request failed: %s", e)
        return []

    dates = data.get("dates", [])
    if not dates:
        print(f"No games found for {target_date}")
        return []

    abbrev_cache = build_mlb_team_abbrev_cache(db)
    team_context_cache = _build_mlb_team_context_cache(db)
    geocode_cache: dict[str, tuple[float, float] | None] = {}
    weather_timeout = load_config().mlb_api.timeout_seconds

    matchup_ids: list[int] = []
    skipped_non_played = 0
    skipped_unknown_team = 0
    for game in dates[0].get("games", []):
        detailed_state = game.get("status", {}).get("detailedState", "")
        if detailed_state in NON_PLAYED_STATES:
            skipped_non_played += 1
            continue

        game_id    = str(game.get("gamePk", ""))
        game_start = game.get("gameDate")
        home_info  = game.get("teams", {}).get("home", {})
        away_info  = game.get("teams", {}).get("away", {})

        home_mlb_id = home_info.get("team", {}).get("id")
        away_mlb_id = away_info.get("team", {}).get("id")

        home_abbrev = MLB_ID_TO_ABBREV.get(home_mlb_id)
        away_abbrev = MLB_ID_TO_ABBREV.get(away_mlb_id)

        home_team_id = abbrev_cache.get(home_abbrev) if home_abbrev else None
        away_team_id = abbrev_cache.get(away_abbrev) if away_abbrev else None

        if not home_team_id or not away_team_id:
            logger.warning(
                "Unknown team IDs for game %s: home_mlb_id=%s (%s) away_mlb_id=%s (%s)",
                game_id, home_mlb_id, home_abbrev, away_mlb_id, away_abbrev,
            )
            skipped_unknown_team += 1
            continue

        # Probable starters — store MLB player_id; NULL if not yet announced
        home_sp_id = home_info.get("probablePitcher", {}).get("id")
        home_sp_name = home_info.get("probablePitcher", {}).get("fullName")
        away_sp_id = away_info.get("probablePitcher", {}).get("id")
        away_sp_name = away_info.get("probablePitcher", {}).get("fullName")
        ballpark   = game.get("venue", {}).get("name")
        team_context = team_context_cache.get(home_team_id, {})
        query_ballpark = ballpark or team_context.get("ballpark")
        query_city = team_context.get("city")
        weather_temp: int | None = None
        wind_speed: int | None = None
        wind_direction: str | None = None

        if game_start and query_ballpark:
            geocode_queries = []
            if query_city:
                geocode_queries.append(f"{query_ballpark}, {query_city}")
                geocode_queries.append(query_city)
            geocode_queries.append(query_ballpark)

            coords = None
            for geocode_query in geocode_queries:
                coords = _geocode_ballpark(
                    geocode_query,
                    timeout_seconds=weather_timeout,
                    cache=geocode_cache,
                )
                if coords is not None:
                    break
            if coords is not None:
                weather_temp, wind_speed, wind_direction = _fetch_weather_snapshot(
                    latitude=coords[0],
                    longitude=coords[1],
                    game_start_iso=game_start,
                    timeout_seconds=weather_timeout,
                )

        mid = upsert_mlb_matchup(
            db,
            game_date=target_date,
            game_id=game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_sp_id=home_sp_id,
            home_sp_name=home_sp_name,
            away_sp_id=away_sp_id,
            away_sp_name=away_sp_name,
            ballpark=ballpark,
            weather_temp=weather_temp,
            wind_speed=wind_speed,
            wind_direction=wind_direction,
            commence_time=game_start,
        )
        if mid:
            matchup_ids.append(mid)

    msg = f"Schedule: {len(matchup_ids)} games upserted for {target_date}"
    skipped_parts = []
    if skipped_non_played:
        skipped_parts.append(f"{skipped_non_played} non-played")
    if skipped_unknown_team:
        skipped_parts.append(f"{skipped_unknown_team} unknown team IDs")
    if skipped_parts:
        msg += f" ({', '.join(skipped_parts)} skipped)"
    print(msg)
    return matchup_ids


def _consensus_american(prices: list[int]) -> int | None:
    """Consensus American odds by averaging in IMPLIED-PROBABILITY space.

    Arithmetic averaging of American odds is invalid: mixed-sign prices around
    even money (+102, −112, …) average into the impossible (−100, +100) zone —
    the ledger held prices like −74 and −42 — and converting that fiction to
    decimal inflated every moneyline EV, star rating, and backtest payout
    (discovered 2026-07-02; soccer fixed the same bug in June). Average the
    implied probabilities and convert back.
    """
    if not prices:
        return None
    avg_prob = sum(american_to_prob(p) for p in prices) / len(prices)
    return prob_to_american(avg_prob)


def fetch_odds(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    """Fetch Vegas totals + moneylines from The Odds API and update mlb_matchups.

    Computes consensus averages across ALL bookmakers (not just [0]) for
    stability.  Also derives team-specific implied run totals from moneylines
    using the same compute_team_implied_total() formula as NBA.

    Matches games by home team name against mlb_teams.name.
    Returns number of matchups updated.
    """
    if not api_key:
        logger.info("ODDS_API_KEY not set — skipping MLB odds fetch")
        return 0

    target_date = game_date or date.today().isoformat()

    try:
        resp = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={
                "apiKey": api_key,
                "regions": "us",
                "markets": "h2h,totals,spreads",
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=20,
        )
        resp.raise_for_status()
        games = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API request failed: %s", e)
        return 0

    # Build lookup: home team name → matchup row for today's MLB games
    rows = db.execute(
        """
        SELECT nm.id, t_home.name AS home_name, t_away.name AS away_name
        FROM mlb_matchups nm
        JOIN mlb_teams t_home ON t_home.team_id = nm.home_team_id
        JOIN mlb_teams t_away ON t_away.team_id = nm.away_team_id
        WHERE nm.game_date = %s
        """,
        (target_date,),
    )
    matchup_by_home: dict[str, dict] = {r["home_name"]: r for r in rows}
    # Ensure h2h + totals + spreads (run line) are all fetched
    markets_to_fetch = "h2h,totals,spreads"
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = captured_at.isoformat()

    # The Athletics changed name — try both variants
    _OAK_ALIASES = {"Oakland Athletics", "Athletics", "Sacramento Athletics"}

    updated = 0
    skipped_live = 0
    now = datetime.now(timezone.utc)
    history_rows: list[dict] = []
    for g in games:
        # In-play guard: after first pitch the odds feed serves LIVE prices.
        # Writing them replaces the pre-game closing line that predictions,
        # the bet ledger reference, and CLV history all assume (the 30-min
        # odds-capture cron polls straight through the game window). Same
        # freeze-at-start rule as soccer (2026-07-01).
        commence_iso = g.get("commence_time")
        if commence_iso:
            try:
                if datetime.fromisoformat(commence_iso.replace("Z", "+00:00")) <= now:
                    skipped_live += 1
                    continue
            except ValueError:
                pass

        home_name = g.get("home_team", "")
        matchup = matchup_by_home.get(home_name)

        # Handle Athletics name variants from the Odds API
        if not matchup and home_name in _OAK_ALIASES:
            for alias in _OAK_ALIASES:
                matchup = matchup_by_home.get(alias)
                if matchup:
                    break

        if not matchup:
            logger.debug("No matchup found for Odds API home team: %s", home_name)
            continue

        # Consensus across ALL bookmakers for h2h and totals
        away_name = g.get("away_team", "")
        home_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        home_spreads: list[float] = []
        bookmakers = g.get("bookmakers") or []
        for bm in bookmakers:
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        if o["name"] == home_name or o["name"] in _OAK_ALIASES and home_name in _OAK_ALIASES:
                            home_prices.append(o["price"])
                        elif o["name"] == away_name:
                            away_prices.append(o["price"])
                elif market["key"] == "totals":
                    over = next(
                        (o for o in market.get("outcomes", []) if o["name"] == "Over"),
                        None,
                    )
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))
                elif market["key"] == "spreads":
                    home_outcome = next(
                        (o for o in market.get("outcomes", []) if o["name"] == home_name),
                        None,
                    )
                    if home_outcome and home_outcome.get("point") is not None:
                        home_spreads.append(float(home_outcome["point"]))

        home_ml    = _consensus_american(home_prices)
        away_ml    = _consensus_american(away_prices)
        vegas_total = round(sum(total_points) / len(total_points) * 2) / 2 if total_points else None
        home_spread = round(sum(home_spreads) / len(home_spreads) * 2) / 2 if home_spreads else None
        vegas_prob_home = _ml_to_prob(home_ml, away_ml) if home_ml and away_ml else None

        # Team-specific implied run totals from moneylines
        # MLB avg is ~9 runs/game total, ~4.5 per team.
        # A -200 home favorite in a 9.5 O/U gets ~5.3 implied, not 4.75.
        home_implied = away_implied = None
        if vegas_total and home_ml and away_ml:
            home_implied = round(
                compute_team_implied_total(vegas_total, home_ml, away_ml, is_home=True), 3
            )
            away_implied = round(vegas_total - home_implied, 3)

        db.execute(
            """
            UPDATE mlb_matchups
            SET vegas_total     = %s,
                home_ml         = %s,
                away_ml         = %s,
                home_spread     = %s,
                vegas_prob_home = %s,
                home_implied    = %s,
                away_implied    = %s
            WHERE id = %s
            """,
            (vegas_total, home_ml, away_ml, home_spread, vegas_prob_home,
             home_implied, away_implied, matchup["id"]),
        )
        history_rows.append(
            {
                "sport": "mlb",
                "matchup_id": matchup["id"],
                "event_id": g.get("id"),
                "game_date": target_date,
                "home_team_name": home_name,
                "away_team_name": away_name,
                "bookmaker_count": len(bookmakers),
                "home_ml": home_ml,
                "away_ml": away_ml,
                "home_spread": home_spread,
                "vegas_total": vegas_total,
                "vegas_prob_home": vegas_prob_home,
                "home_implied": home_implied,
                "away_implied": away_implied,
                "capture_key": capture_key,
                "captured_at": captured_at,
            }
        )
        updated += 1

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)
    msg = f"Odds: {updated} matchups updated with Vegas lines for {target_date}"
    if skipped_live:
        msg += f" ({skipped_live} in-play games skipped — closing lines frozen)"
    print(msg)
    return updated


def fetch_scores(db: DatabaseManager, game_date: str | None = None) -> int:
    """Fetch final scores for completed MLB games and write to mlb_matchups.

    Uses the MLB Stats API schedule endpoint with linescore hydration.
    Only writes when game status is 'Final'.  Safe to call for past dates.
    Returns number of matchups updated.
    """
    target_date = game_date or date.today().isoformat()
    logger.info("Fetching MLB scores for %s ...", target_date)

    try:
        resp = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={
                "sportId": 1,
                "date": target_date,
                "hydrate": "linescore",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("MLB Stats API scores request failed: %s", e)
        return 0

    dates = data.get("dates", [])
    if not dates:
        return 0

    # 'Game Over' = game ended, stats being finalized — the score is safe to
    # settle on ('Final' can lag it by hours, leaving bets stuck pending).
    _FINAL_STATES = {"Final", "Game Over", "Completed Early"}
    # Postponed/cancelled games are made up on a LATER date under the SAME
    # gamePk, so without stamping the status the makeup game's score would
    # eventually land on the original date's row and grade bets on a game
    # played weeks later. Books void when the game doesn't play as scheduled;
    # settle() voids pending bets on these rows. (Suspended games resume and
    # finish under the same gamePk — those stay pending until final.)
    _VOID_STATES = {"Postponed", "Cancelled"}

    updated = 0
    for game in dates[0].get("games", []):
        detailed_state = game.get("status", {}).get("detailedState", "")
        game_id = str(game.get("gamePk", ""))
        if detailed_state in _VOID_STATES:
            db.execute(
                "UPDATE mlb_matchups SET game_status = %s WHERE game_id = %s",
                (detailed_state, game_id),
            )
            continue
        if detailed_state not in _FINAL_STATES:
            continue
        linescore = game.get("linescore", {})
        teams_ls = linescore.get("teams", {})
        home_runs = teams_ls.get("home", {}).get("runs")
        away_runs = teams_ls.get("away", {}).get("runs")

        if home_runs is None or away_runs is None:
            continue

        hs, as_ = int(home_runs), int(away_runs)
        prev = db.execute_one(
            "SELECT id, home_score, away_score FROM mlb_matchups WHERE game_id = %s", (game_id,)
        )
        if prev is None or (prev["home_score"], prev["away_score"]) == (hs, as_):
            continue  # unknown game or already correct
        # Write the final score, correcting any stale/wrong value that got frozen
        # earlier (the old NULL-only guard could never fix a wrong score).
        db.execute(
            "UPDATE mlb_matchups SET home_score = %s, away_score = %s WHERE id = %s",
            (hs, as_, prev["id"]),
        )
        updated += 1
        # If we corrected a previously-recorded score, reopen this game's settled
        # bets so the next settle pass re-grades against the truth.
        if prev["home_score"] is not None or prev["away_score"] is not None:
            db.execute(
                "UPDATE mlb_bets SET status = 'pending', settled_at = NULL, result_detail = NULL "
                "WHERE matchup_id = %s AND status IN ('won', 'lost', 'void')",
                (prev["id"],),
            )

    logger.info("MLB Scores: %d matchups updated for %s", updated, target_date)
    return updated


def _ml_to_prob(home_ml: int, away_ml: int) -> float:
    """Convert American moneylines to vig-removed home win probability."""
    def _raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    home_raw = _raw(home_ml)
    away_raw = _raw(away_ml)
    total    = home_raw + away_raw
    return round(home_raw / total, 4) if total > 0 else 0.5


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch MLB schedule + odds")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    fetch_schedule(db, args.date)
    fetch_odds(db, config.odds_api.api_key, args.date)
    fetch_scores(db, args.date)
