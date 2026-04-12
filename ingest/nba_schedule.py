"""Fetch today's NBA schedule and Vegas odds into nba_matchups.

Two data sources combined:
  1. ScoreboardV2 (stats.nba.com) — game IDs, home/away teams, tip-off times
  2. The Odds API (optional) — Vegas totals + moneylines, matched by team name

Usage:
    python -m ingest.nba_schedule                    # today's games
    python -m ingest.nba_schedule --date 2026-03-25  # specific date
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_team_abbrev_cache,
    insert_game_odds_history_rows,
    insert_player_prop_history_rows,
    upsert_nba_matchup,
)
from ingest.nba_teams import NBA_ID_TO_ABBREV

logger = logging.getLogger(__name__)

SLEEP_SECONDS = 0.6

# DK sometimes uses non-standard abbreviations — map to our canonical ones.
# NBA abbreviations are standardized enough that only a handful need overrides.
DK_ABBREV_OVERRIDES: dict[str, str] = {
    "GS":  "GSW",
    "SA":  "SAS",
    "NO":  "NOP",
    "NY":  "NYK",
    "PHO": "PHX",
    "OKL": "OKC",
    "UTH": "UTA",
}


def fetch_schedule(db: DatabaseManager, game_date: str | None = None) -> list[int]:
    """Fetch games for game_date (YYYY-MM-DD), upsert into nba_matchups.

    Returns list of nba_matchup IDs upserted.
    """
    from nba_api.stats.endpoints import ScoreboardV2

    from ingest.nba_stats import _call_with_retry

    target_date = game_date or date.today().isoformat()
    logger.info("Fetching NBA schedule for %s ...", target_date)
    time.sleep(SLEEP_SECONDS)

    def _fetch():
        return ScoreboardV2(game_date=target_date, timeout=60)

    scoreboard = _call_with_retry(_fetch, "ScoreboardV2")
    game_header = scoreboard.game_header.get_data_frame()

    if game_header.empty:
        print(f"No games found for {target_date}")
        return []

    abbrev_cache = build_team_abbrev_cache(db)

    matchup_ids = []
    for _, row in game_header.iterrows():
        nba_game_id  = str(row["GAME_ID"])
        home_nba_id  = int(row["HOME_TEAM_ID"])
        away_nba_id  = int(row["VISITOR_TEAM_ID"])

        home_abbrev = NBA_ID_TO_ABBREV.get(home_nba_id)
        away_abbrev = NBA_ID_TO_ABBREV.get(away_nba_id)

        home_team_id = abbrev_cache.get(home_abbrev) if home_abbrev else None
        away_team_id = abbrev_cache.get(away_abbrev) if away_abbrev else None

        mid = upsert_nba_matchup(
            db,
            game_date=target_date,
            game_id=nba_game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
        )
        if mid:
            matchup_ids.append(mid)

    print(f"Schedule: {len(matchup_ids)} games upserted for {target_date}")
    return matchup_ids


def fetch_odds(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    """Fetch Vegas totals + moneylines from The Odds API and update nba_matchups.

    Matches games by home team name → our teams.name column.
    Returns number of matchups updated with odds.
    """
    if not api_key:
        logger.info("ODDS_API_KEY not set — skipping Vegas odds fetch")
        return 0

    target_date = game_date or date.today().isoformat()
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        games = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API request failed: %s", e)
        return 0

    # Build a lookup: canonical team name → nba_matchup.game_id for today's games
    rows = db.execute(
        """
        SELECT nm.id, nm.game_id, t_home.name AS home_name, t_away.name AS away_name
        FROM nba_matchups nm
        JOIN teams t_home ON t_home.team_id = nm.home_team_id
        JOIN teams t_away ON t_away.team_id = nm.away_team_id
        WHERE nm.game_date = %s
        """,
        (target_date,),
    )
    matchup_by_home: dict[str, dict] = {r["home_name"]: r for r in rows}
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = captured_at.isoformat()

    updated = 0
    history_rows: list[dict] = []
    for g in games:
        # Odds API uses full team names e.g. "Los Angeles Lakers"
        home_name = g.get("home_team", "")
        matchup = matchup_by_home.get(home_name)
        if not matchup:
            logger.debug("No matchup found for Odds API home team: %s", home_name)
            continue

        # Consensus across ALL bookmakers for h2h (moneylines) and totals
        away_name = g.get("away_team", "")
        home_prices: list[int] = []
        away_prices: list[int] = []
        home_spreads: list[float] = []
        total_points: list[float] = []
        bookmakers = g.get("bookmakers") or []
        for bm in bookmakers:
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        if o["name"] == home_name:
                            home_prices.append(o["price"])
                        elif o["name"] == away_name:
                            away_prices.append(o["price"])
                elif market["key"] == "spreads":
                    home_outcome = next((o for o in market.get("outcomes", []) if o["name"] == home_name), None)
                    if home_outcome and home_outcome.get("point") is not None:
                        home_spreads.append(float(home_outcome["point"]))
                elif market["key"] == "totals":
                    over = next((o for o in market.get("outcomes", []) if o["name"] == "Over"), None)
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))

        home_ml = round(sum(home_prices) / len(home_prices)) if home_prices else None
        away_ml = round(sum(away_prices) / len(away_prices)) if away_prices else None
        home_spread = round(sum(home_spreads) / len(home_spreads) * 2) / 2 if home_spreads else None
        vegas_total = round(sum(total_points) / len(total_points) * 2) / 2 if total_points else None
        vegas_prob_home = _ml_to_prob(home_ml, away_ml) if home_ml and away_ml else None

        home_implied, away_implied = _compute_implied_totals(vegas_total, home_ml, away_ml)
        db.execute(
            """
            UPDATE nba_matchups
            SET vegas_total = %s, home_ml = %s, away_ml = %s, home_spread = %s,
                vegas_prob_home = %s, home_implied = %s, away_implied = %s
            WHERE id = %s
            """,
            (vegas_total, home_ml, away_ml, home_spread, vegas_prob_home,
             home_implied, away_implied, matchup["id"]),
        )
        history_rows.append(
            {
                "sport": "nba",
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
                "capture_key": capture_key,
                "captured_at": captured_at,
            }
        )
        updated += 1

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)
    print(f"Odds: {updated} matchups updated with Vegas lines for {target_date}")
    return updated


def fetch_player_props(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    """Fetch player pts/reb/ast over-under lines from The Odds API.

    Matches Odds API player names to dk_players for the current slate using
    exact then fuzzy (Levenshtein ≤ 3) matching.  Updates prop_pts, prop_reb,
    prop_ast columns.  Returns number of players updated.

    Requires the current slate to already be loaded into dk_players.
    """
    if not api_key:
        logger.info("ODDS_API_KEY not set — skipping player props fetch")
        return 0

    target_date = game_date or date.today().isoformat()

    # Step 1: Get event IDs for today's NBA games
    try:
        resp = requests.get(
            "https://api.the-odds-api.com/v4/sports/basketball_nba/events",
            params={"apiKey": api_key, "dateFormat": "iso"},
            timeout=20,
        )
        resp.raise_for_status()
        events = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API events request failed: %s", e)
        return 0

    # Filter to target date (games can tip off late ET = next day UTC, so use ±36h window)
    target_start = datetime.fromisoformat(target_date).replace(tzinfo=timezone.utc)
    target_end   = target_start + timedelta(hours=36)
    today_events = [
        e for e in events
        if target_start <= datetime.fromisoformat(
            e["commence_time"].replace("Z", "+00:00")
        ) < target_end
    ]
    if not today_events:
        logger.info("No events found for %s — skipping player props", target_date)
        return 0

    # Step 2: Get current slate players from DB
    slate_players = db.execute(
        """
        SELECT dp.id, dp.dk_player_id, dp.slate_id, dp.name, dp.team_id
        FROM dk_players dp
        INNER JOIN dk_slates ds ON ds.id = dp.slate_id
        WHERE ds.slate_date = %s
          AND ds.sport = 'nba'
        """,
        (target_date,),
    )
    if not slate_players:
        logger.warning("No dk_players for %s — load a DK slate first", target_date)
        return 0

    player_lookup = {row["name"].lower(): row for row in slate_players}

    # Step 3: Fetch props per event and accumulate by player name.
    # Average across ALL bookmakers — consensus is more stable and accurate
    # than whichever book happens to be index [0] on any given API call.
    MARKET_TO_KEY = {
        "player_points": "pts",
        "player_rebounds": "reb",
        "player_assists": "ast",
    }
    # Accumulators: {lower_name: {stat: [sum, count]}} for computing averages
    prop_accum: dict[str, dict[str, list]] = {}

    for event in today_events:
        try:
            r = requests.get(
                f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event['id']}/odds",
                params={
                    "apiKey": api_key,
                    "regions": "us",
                    "markets": "player_points,player_rebounds,player_assists",
                    "oddsFormat": "american",
                },
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.debug("Event %s props failed: %s", event["id"], e)
            continue

        # Iterate ALL bookmakers, not just [0]
        for bookmaker in data.get("bookmakers") or []:
            for market in bookmaker.get("markets", []):
                stat_key = MARKET_TO_KEY.get(market["key"])
                if not stat_key:
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome.get("description", "").lower() != "over":
                        continue
                    pname = outcome["name"].lower()
                    point = outcome.get("point")
                    if point is None:
                        continue
                    entry = prop_accum.setdefault(pname, {})
                    if stat_key not in entry:
                        entry[stat_key] = [0.0, 0]
                    entry[stat_key][0] += float(point)
                    entry[stat_key][1] += 1

    # Collapse accumulators → consensus averages (rounded to nearest 0.5)
    prop_data: dict[str, dict[str, dict[str, float | int]]] = {}
    for pname, stats in prop_accum.items():
        prop_data[pname] = {
            stat: {
                "line": round(total / count * 2) / 2,
                "book_count": count,
            }
            for stat, (total, count) in stats.items()
        }

    # Step 4: Match prop names to slate players and write to DB
    updated = 0
    capture_time = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = capture_time.isoformat()
    history_rows: list[dict] = []
    market_key_by_stat = {
        "pts": "player_points",
        "reb": "player_rebounds",
        "ast": "player_assists",
    }
    for prop_name, props in prop_data.items():
        row = player_lookup.get(prop_name)
        if not row:
            # Fuzzy match
            best_dist, best_row = 4, None
            for dk_name, dk_row in player_lookup.items():
                d = _levenshtein(prop_name, dk_name)
                if d < best_dist:
                    best_dist, best_row = d, dk_row
            row = best_row
        if not row:
            continue

        db.execute(
            """
            UPDATE dk_players
            SET prop_pts = COALESCE(%s, prop_pts),
                prop_reb = COALESCE(%s, prop_reb),
                prop_ast = COALESCE(%s, prop_ast)
            WHERE id = %s
            """,
            (
                props.get("pts", {}).get("line"),
                props.get("reb", {}).get("line"),
                props.get("ast", {}).get("line"),
                row["id"],
            ),
        )
        for stat, payload in props.items():
            history_rows.append(
                {
                    "sport": "nba",
                    "slate_id": row["slate_id"],
                    "dk_player_id": row["dk_player_id"],
                    "player_name": row["name"],
                    "team_id": row.get("team_id"),
                    "event_id": None,
                    "market_key": market_key_by_stat[stat],
                    "line": payload.get("line"),
                    "price": None,
                    "bookmaker_key": None,
                    "bookmaker_title": "Consensus",
                    "book_count": int(payload.get("book_count", 0)),
                    "capture_key": capture_key,
                    "captured_at": capture_time,
                }
            )
        updated += 1

    if history_rows:
        insert_player_prop_history_rows(db, history_rows)
    print(f"Player props: {updated} players updated for {target_date}")
    return updated


def _compute_implied_totals(
    vegas_total: float | None,
    home_ml: int | None,
    away_ml: int | None,
) -> tuple[float | None, float | None]:
    """Derive per-team implied point totals from moneylines + O/U.

    Uses vig-removed win probability to split the total, matching the
    TypeScript computeTeamImpliedTotal() in dfs-client.tsx.
    """
    if vegas_total is None:
        return None, None
    if home_ml is None or away_ml is None:
        return round(vegas_total / 2, 1), round(vegas_total / 2, 1)

    def ml_to_raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    home_raw = ml_to_raw(home_ml)
    away_raw = ml_to_raw(away_ml)
    vig = home_raw + away_raw
    home_prob_clean = home_raw / vig if vig > 0 else 0.5
    implied_spread = max(-15.0, min(15.0, (home_prob_clean - 0.5) / 0.025))
    home_implied = round(vegas_total / 2 + implied_spread / 2, 1)
    away_implied = round(vegas_total - home_implied, 1)
    return home_implied, away_implied


def fetch_scores(db: DatabaseManager, game_date: str | None = None) -> int:
    """Fetch final scores for completed NBA games and write to nba_matchups.

    Uses ScoreboardV2 line_score data.  Only writes when GAME_STATUS_ID == 3
    (Final).  Safe to call for dates in the past — skips games already scored.
    Returns number of matchups updated.
    """
    from nba_api.stats.endpoints import ScoreboardV2
    from ingest.nba_stats import _call_with_retry

    target_date = game_date or date.today().isoformat()
    logger.info("Fetching final scores for %s ...", target_date)
    time.sleep(SLEEP_SECONDS)

    def _fetch():
        return ScoreboardV2(game_date=target_date, timeout=60)

    try:
        scoreboard = _call_with_retry(_fetch, "ScoreboardV2-scores")
    except Exception as exc:
        logger.warning("Could not fetch scores for %s: %s", target_date, exc)
        return 0

    game_header = scoreboard.game_header.get_data_frame()
    line_score  = scoreboard.line_score.get_data_frame()

    if game_header.empty or line_score.empty:
        return 0

    # Only process final games
    final_games = game_header[game_header["GAME_STATUS_ID"] == 3]
    if final_games.empty:
        logger.info("No final games found for %s", target_date)
        return 0

    # Build score lookup: game_id → {home_team_id: score, away_team_id: score}
    abbrev_cache = build_team_abbrev_cache(db)
    from ingest.nba_teams import NBA_ID_TO_ABBREV

    updated = 0
    for _, hdr in final_games.iterrows():
        game_id     = str(hdr["GAME_ID"])
        home_nba_id = int(hdr["HOME_TEAM_ID"])
        away_nba_id = int(hdr["VISITOR_TEAM_ID"])

        # Get PTS from line_score for home and away
        home_row = line_score[
            (line_score["GAME_ID"] == game_id) &
            (line_score["TEAM_ID"] == home_nba_id)
        ]
        away_row = line_score[
            (line_score["GAME_ID"] == game_id) &
            (line_score["TEAM_ID"] == away_nba_id)
        ]
        if home_row.empty or away_row.empty:
            continue

        home_pts = home_row.iloc[0]["PTS"]
        away_pts = away_row.iloc[0]["PTS"]
        if home_pts is None or away_pts is None:
            continue

        result = db.execute_one(
            """
            UPDATE nba_matchups
            SET home_score = %s, away_score = %s
            WHERE game_id = %s
              AND (home_score IS NULL OR away_score IS NULL)
            RETURNING id
            """,
            (int(home_pts), int(away_pts), game_id),
        )
        if result:
            updated += 1

    logger.info("Scores: %d matchups updated for %s", updated, target_date)
    return updated


def _levenshtein(a: str, b: str) -> int:
    """Levenshtein edit distance for fuzzy player name matching."""
    m, n = len(a), len(b)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[:]
        dp[0] = i
        for j in range(1, n + 1):
            dp[j] = (prev[j - 1] if a[i - 1] == b[j - 1]
                     else 1 + min(prev[j], dp[j - 1], prev[j - 1]))
    return dp[n]


def _ml_to_prob(home_ml: int, away_ml: int) -> float:
    """Convert American moneylines to vig-removed home win probability."""
    def ml_to_raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    home_raw = ml_to_raw(home_ml)
    away_raw = ml_to_raw(away_ml)
    total = home_raw + away_raw
    return round(home_raw / total, 4) if total > 0 else 0.5


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch NBA schedule + odds")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    fetch_schedule(db, args.date)
    fetch_odds(db, config.odds_api.api_key, args.date)
    fetch_scores(db, args.date)
    fetch_player_props(db, config.odds_api.api_key, args.date)
