"""NFL events, game-line odds, and final-score ingestion.

The Odds API event ID is the canonical external identity. Odds captures are
append-only and are rejected at or after either the provider or stored kickoff.

Usage:
    python -m ingest.nfl_schedule
    python -m ingest.nfl_schedule --date 2026-09-13
    python -m ingest.nfl_schedule --date 2026-09-13 --require-fresh-upcoming-odds
    python -m ingest.nfl_schedule --scores-only --days-from 3
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_nfl_team_name_cache,
    insert_game_odds_history_rows,
    upsert_nfl_matchup,
)
from ingest.mlb_odds_policy import consensus_american, validate_event_prices
from ingest.nfl_teams import seed_teams
from model.soccer_bet_rating import american_to_prob

logger = logging.getLogger(__name__)

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
NFL_SPORT_KEYS = {
    "americanfootball_nfl": "regular",
    "americanfootball_nfl_preseason": "preseason",
}
# us_ex REMOVED 2026-08-24: billed as a region (cost = markets x regions, so
# it was a third of every NFL odds call) yet it returned Polymarket on 0 of
# 1,685 NFL captures in the preceding 30 days -- measured against the books
# JSONB, not assumed. This is why detector health reports
# nfl/pinnacle_polymarket_delta as DEAD (0 alerts ever). MLB deliberately
# KEEPS us_ex: there it lands on 89.4% of captures and its Pin/Poly detector
# does fire, so this is a per-sport fact, not a blanket judgement on the
# region.
NFL_ODDS_REGIONS = "us,eu"
EASTERN = ZoneInfo("America/New_York")


def _parse_iso(value: object) -> datetime:
    if not value:
        raise ValueError("missing commence_time")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _eastern_date(value: datetime) -> str:
    return value.astimezone(EASTERN).date().isoformat()


def _season_for_kickoff(value: datetime) -> int:
    eastern = value.astimezone(EASTERN)
    return eastern.year - 1 if eastern.month <= 3 else eastern.year


def _require_pregame_capture(
    *, event_commence: datetime, stored_commence: datetime, captured_at: datetime,
) -> None:
    stored = stored_commence if stored_commence.tzinfo else stored_commence.replace(tzinfo=timezone.utc)
    if captured_at >= event_commence.astimezone(timezone.utc):
        raise ValueError("capture is at or after provider kickoff")
    if captured_at >= stored.astimezone(timezone.utc):
        raise ValueError("capture is at or after stored kickoff")


def _log_quota(response: requests.Response, label: str) -> None:
    logger.info(
        "%s quota: remaining=%s used=%s last=%s",
        label,
        response.headers.get("x-requests-remaining", "?"),
        response.headers.get("x-requests-used", "?"),
        response.headers.get("x-requests-last", "?"),
    )


def _team_cache(db: DatabaseManager) -> dict[str, int]:
    cache = build_nfl_team_name_cache(db)
    if len(cache) < 32:
        seed_teams(db)
        cache = build_nfl_team_name_cache(db)
    return cache


def _resolve_team(cache: dict[str, int], name: object) -> int:
    value = str(name or "").strip()
    if value not in cache:
        raise ValueError(f"unmapped NFL provider team: {value or '<missing>'}")
    return cache[value]


def fetch_events(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for NFL event ingestion")
    cache = _team_cache(db)
    upserted = 0
    for sport_key, season_type in NFL_SPORT_KEYS.items():
        response = requests.get(
            f"{ODDS_API_BASE}/sports/{sport_key}/events",
            params={"apiKey": api_key, "dateFormat": "iso"},
            timeout=20,
        )
        response.raise_for_status()
        _log_quota(response, f"NFL {season_type} events")
        for event in response.json() or []:
            event_id = str(event.get("id") or "").strip()
            if not event_id:
                raise ValueError("NFL event missing provider event id")
            commence = _parse_iso(event.get("commence_time"))
            eastern_game_date = _eastern_date(commence)
            if game_date and eastern_game_date != game_date:
                continue
            home_id = _resolve_team(cache, event.get("home_team"))
            away_id = _resolve_team(cache, event.get("away_team"))
            if home_id == away_id:
                raise ValueError(f"NFL event {event_id} resolves both sides to one team")
            matchup_id = upsert_nfl_matchup(
                db,
                event_id=event_id,
                game_date=eastern_game_date,
                commence_time=commence,
                home_team_id=home_id,
                away_team_id=away_id,
                season=_season_for_kickoff(commence),
                season_type=season_type,
                game_status="Scheduled",
            )
            upserted += int(bool(matchup_id))
    print(f"NFL events: {upserted} matchups upserted" + (f" for {game_date}" if game_date else ""))
    return upserted


def _vig_free_home_probability(home_ml: int | None, away_ml: int | None) -> float | None:
    if home_ml is None or away_ml is None:
        return None
    home = american_to_prob(home_ml)
    away = american_to_prob(away_ml)
    return home / (home + away) if home + away > 0 else None


def _extract_markets(event: dict) -> dict:
    home_name = str(event.get("home_team") or "")
    away_name = str(event.get("away_team") or "")
    home_prices: list[int] = []
    away_prices: list[int] = []
    home_spreads: list[float] = []
    total_lines: list[float] = []
    books: dict[str, dict] = {}

    for bookmaker in event.get("bookmakers") or []:
        key = str(bookmaker.get("key") or "?")
        book = books.setdefault(key, {"last_update": bookmaker.get("last_update")})
        for market in bookmaker.get("markets") or []:
            outcomes = market.get("outcomes") or []
            if market.get("key") == "h2h":
                for outcome in outcomes:
                    if outcome.get("name") == home_name:
                        home_prices.append(int(outcome["price"]))
                        book["ml_home"] = int(outcome["price"])
                    elif outcome.get("name") == away_name:
                        away_prices.append(int(outcome["price"]))
                        book["ml_away"] = int(outcome["price"])
            elif market.get("key") == "spreads":
                for outcome in outcomes:
                    if outcome.get("point") is None:
                        continue
                    if outcome.get("name") == home_name:
                        point = float(outcome["point"])
                        home_spreads.append(point)
                        book["spread_home"] = point
                        book["spread_home_price"] = outcome.get("price")
                    elif outcome.get("name") == away_name:
                        book["spread_away"] = float(outcome["point"])
                        book["spread_away_price"] = outcome.get("price")
            elif market.get("key") == "totals":
                over = next((outcome for outcome in outcomes if outcome.get("name") == "Over"), None)
                under = next((outcome for outcome in outcomes if outcome.get("name") == "Under"), None)
                if over and over.get("point") is not None:
                    line = float(over["point"])
                    total_lines.append(line)
                    book["total_line"] = line
                    book["over"] = over.get("price")
                    book["under"] = under.get("price") if under else None

    home_ml = consensus_american(home_prices)
    away_ml = consensus_american(away_prices)
    home_spread_raw = sum(home_spreads) / len(home_spreads) if home_spreads else None
    total_raw = sum(total_lines) / len(total_lines) if total_lines else None
    return {
        "home_ml": home_ml,
        "away_ml": away_ml,
        "home_spread": round(home_spread_raw * 2) / 2 if home_spread_raw is not None else None,
        "vegas_total": round(total_raw * 2) / 2 if total_raw is not None else None,
        "vegas_total_raw": total_raw,
        "books": books,
        "bookmaker_count": len(event.get("bookmakers") or []),
    }


def fetch_odds(
    db: DatabaseManager,
    api_key: str,
    game_date: str | None = None,
    *,
    monitoring_hours: int | None = None,
    event_ids: set[str] | None = None,
    refresh_events: bool = True,
    bookmakers: str | None = None,
    markets: str = "h2h,spreads,totals",
    request_audit: dict | None = None,
) -> int:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for NFL odds ingestion")
    if refresh_events:
        fetch_events(db, api_key, None if monitoring_hours else game_date)
    if event_ids is not None:
        eligible_rows = db.execute(
            """
            SELECT event_id, season_type FROM nfl_matchups
            WHERE event_id = ANY(%s) AND commence_time > NOW() AND NOT completed
            """,
            (sorted(event_ids),),
        )
    elif monitoring_hours:
        eligible_rows = db.execute(
            """
            SELECT event_id, season_type FROM nfl_matchups
            WHERE commence_time > NOW()
              AND commence_time <= NOW() + (%s || ' hours')::interval
              AND NOT completed
            """,
            (max(1, int(monitoring_hours)),),
        )
    else:
        eligible_rows = db.execute(
            """
            SELECT event_id, season_type FROM nfl_matchups
            WHERE game_date = %s AND commence_time > NOW() AND NOT completed
            """,
            (game_date,),
        )
    eligible_by_type: dict[str, set[str]] = {}
    for row in eligible_rows:
        eligible_by_type.setdefault(str(row.get("season_type") or "regular"), set()).add(str(row["event_id"]))
    eligible_event_ids = set().union(*eligible_by_type.values()) if eligible_by_type else set()
    if not eligible_event_ids:
        horizon = f"within {monitoring_hours} hours" if monitoring_hours else f"for {game_date}"
        print(f"NFL odds: no events {horizon}; paid odds request skipped")
        return 0
    games: list[dict] = []
    audit_calls: list[dict] = []
    for sport_key, season_type in NFL_SPORT_KEYS.items():
        if not eligible_by_type.get(season_type):
            continue
        requested_ids = sorted(eligible_by_type[season_type])
        params = {
            "apiKey": api_key,
            "markets": markets,
            "oddsFormat": "american",
            "dateFormat": "iso",
            "eventIds": ",".join(requested_ids),
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        else:
            params["regions"] = NFL_ODDS_REGIONS
        response = requests.get(
            f"{ODDS_API_BASE}/sports/{sport_key}/odds/",
            params=params,
            timeout=20,
        )
        response.raise_for_status()
        _log_quota(response, f"NFL {season_type} odds")
        audit_calls.append({
            "endpoint": str(getattr(response, "url", f"{ODDS_API_BASE}/sports/{sport_key}/odds/")).split("?", 1)[0],
            "season_type": season_type,
            "event_ids": requested_ids,
            "status": getattr(response, "status_code", 200),
            "requests_last": response.headers.get("x-requests-last"),
            "requests_used": response.headers.get("x-requests-used"),
            "requests_remaining": response.headers.get("x-requests-remaining"),
        })
        games.extend(response.json() or [])
    if request_audit is not None and audit_calls:
        def _header_int(name: str) -> int | None:
            try:
                return int(audit_calls[-1].get(name))
            except (TypeError, ValueError):
                return None

        request_audit.update({
            "endpoint": audit_calls[0]["endpoint"],
            "status": audit_calls[-1]["status"],
            "requests_last": sum(int(call.get("requests_last") or 0) for call in audit_calls),
            "requests_used": _header_int("requests_used"),
            "requests_remaining": _header_int("requests_remaining"),
            "request_count": len(audit_calls),
            "calls": audit_calls,
        })
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = captured_at.isoformat()
    history_rows: list[dict] = []
    updated = 0

    for event in games:
        event_id = str(event.get("id") or "").strip()
        if event_id not in eligible_event_ids:
            continue
        commence = _parse_iso(event.get("commence_time"))
        eastern_game_date = _eastern_date(commence)
        if game_date and eastern_game_date != game_date:
            continue
        matchup = db.execute_one(
            """
            SELECT m.*, ht.name AS home_name, at.name AS away_name
            FROM nfl_matchups m
            JOIN nfl_teams ht ON ht.team_id = m.home_team_id
            JOIN nfl_teams at ON at.team_id = m.away_team_id
            WHERE m.event_id = %s
            """,
            (event_id,),
        )
        if not matchup:
            raise ValueError(f"NFL odds event {event_id} has no matchup row")
        if matchup["home_name"] != event.get("home_team") or matchup["away_name"] != event.get("away_team"):
            raise ValueError(f"NFL odds event {event_id} team identity changed")
        try:
            _require_pregame_capture(
                event_commence=commence,
                stored_commence=matchup["commence_time"],
                captured_at=captured_at,
            )
        except ValueError as exc:
            logger.info("Skipping in-play NFL odds event %s: %s", event_id, exc)
            continue

        validate_event_prices(event)
        market = _extract_markets(event)
        home_prob = _vig_free_home_probability(market["home_ml"], market["away_ml"])
        total = market["vegas_total"]
        spread = market["home_spread"]
        home_implied = (total - spread) / 2 if total is not None and spread is not None else None
        away_implied = (total + spread) / 2 if total is not None and spread is not None else None
        db.execute(
            """
            UPDATE nfl_matchups SET
                vegas_total = %s, home_ml = %s, away_ml = %s,
                home_spread = %s, vegas_prob_home = %s,
                home_implied = %s, away_implied = %s, fetched_at = NOW()
            WHERE id = %s
            """,
            (total, market["home_ml"], market["away_ml"], spread, home_prob,
             home_implied, away_implied, matchup["id"]),
        )
        history_rows.append({
            "sport": "nfl",
            "matchup_id": matchup["id"],
            "event_id": event_id,
            "game_date": eastern_game_date,
            "home_team_id": matchup["home_team_id"],
            "away_team_id": matchup["away_team_id"],
            "home_team_name": matchup["home_name"],
            "away_team_name": matchup["away_name"],
            "bookmaker_count": market["bookmaker_count"],
            "home_ml": market["home_ml"],
            "away_ml": market["away_ml"],
            "home_spread": spread,
            "vegas_total": total,
            "vegas_prob_home": home_prob,
            "home_implied": home_implied,
            "away_implied": away_implied,
            "capture_key": capture_key,
            "captured_at": captured_at,
            "books": market["books"] or None,
            "vegas_total_raw": market["vegas_total_raw"],
        })
        updated += 1

    insert_game_odds_history_rows(db, history_rows)
    print(f"NFL odds: {updated} pregame matchups captured" + (f" for {game_date}" if game_date else ""))
    return updated


def fetch_scores(db: DatabaseManager, api_key: str, days_from: int = 3) -> int:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for NFL score ingestion")
    days = max(1, min(3, int(days_from)))
    candidate = db.execute_one(
        """
        SELECT COUNT(*)::int AS n,
               ARRAY_AGG(DISTINCT COALESCE(season_type, 'regular')) AS season_types
        FROM nfl_matchups
        WHERE commence_time <= NOW()
          AND commence_time >= NOW() - (%s || ' days')::interval
        """,
        (days,),
    ) or {}
    if int(candidate.get("n") or 0) == 0:
        print("NFL scores: no recently started matchups; paid scores request skipped")
        return 0
    season_types = set(candidate.get("season_types") or ["regular"])
    games: list[dict] = []
    for sport_key, season_type in NFL_SPORT_KEYS.items():
        if season_type not in season_types:
            continue
        response = requests.get(
            f"{ODDS_API_BASE}/sports/{sport_key}/scores/",
            params={"apiKey": api_key, "daysFrom": days, "dateFormat": "iso"},
            timeout=20,
        )
        response.raise_for_status()
        _log_quota(response, f"NFL {season_type} scores")
        games.extend(response.json() or [])
    updated = 0
    for event in games:
        if not event.get("completed"):
            continue
        event_id = str(event.get("id") or "").strip()
        matchup = db.execute_one(
            "SELECT id, home_score, away_score FROM nfl_matchups WHERE event_id = %s",
            (event_id,),
        )
        if not matchup:
            logger.warning("Completed NFL score event %s has no matchup row", event_id)
            continue
        score_map = {str(item.get("name")): item.get("score") for item in event.get("scores") or []}
        try:
            home_score = int(score_map[str(event.get("home_team"))])
            away_score = int(score_map[str(event.get("away_team"))])
        except (KeyError, TypeError, ValueError):
            logger.warning("Completed NFL event %s has incomplete scores", event_id)
            continue
        corrected = (
            matchup["home_score"] is not None
            and matchup["away_score"] is not None
            and (int(matchup["home_score"]) != home_score or int(matchup["away_score"]) != away_score)
        )
        db.execute(
            """
            UPDATE nfl_matchups SET
                game_status = 'Final', completed = TRUE,
                home_score = %s, away_score = %s,
                score_fetched_at = NOW(), final_at = COALESCE(final_at, NOW())
            WHERE id = %s
            """,
            (home_score, away_score, matchup["id"]),
        )
        if corrected:
            db.execute(
                """
                UPDATE line_alerts SET outcome = NULL, settled_at = NULL
                WHERE sport = 'nfl' AND matchup_id = %s AND outcome IS NOT NULL
                """,
                (matchup["id"],),
            )
        updated += 1
    print(f"NFL scores: {updated} completed games updated")
    return updated


def verify_fresh_upcoming_odds(
    db: DatabaseManager,
    game_date: str,
    *,
    max_age_minutes: int = 35,
) -> bool:
    row = db.execute_one(
        """
        SELECT
            COUNT(DISTINCT m.id) FILTER (
                WHERE m.commence_time > NOW() AND NOT m.completed
                  AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
            ) AS upcoming_games,
            COUNT(DISTINCT m.id) FILTER (
                WHERE m.commence_time > NOW() AND NOT m.completed
                  AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
                  AND h.captured_at >= NOW() - (%s || ' minutes')::interval
            ) AS fresh_games,
            MAX(h.captured_at) FILTER (WHERE m.commence_time > NOW()) AS latest_capture
        FROM nfl_matchups m
        LEFT JOIN game_odds_history h
          ON h.matchup_id = m.id AND h.sport = 'nfl'
         AND h.captured_at <= m.commence_time
        WHERE m.game_date = %s
        """,
        (max_age_minutes, game_date),
    ) or {}
    upcoming = int(row.get("upcoming_games") or 0)
    fresh = int(row.get("fresh_games") or 0)
    if upcoming == 0:
        logger.info("NFL odds freshness passed: no upcoming games for %s", game_date)
        return True
    if fresh < upcoming:
        logger.error(
            "NFL odds freshness failed for %s: %d/%d upcoming games fresh; latest=%s",
            game_date, fresh, upcoming, row.get("latest_capture"),
        )
        return False
    return True


def verify_fresh_monitoring_window(
    db: DatabaseManager,
    *,
    monitoring_hours: int = 48,
    max_age_minutes: int = 35,
) -> bool:
    row = db.execute_one(
        """
        SELECT
          COUNT(DISTINCT m.id)::int AS upcoming_games,
          COUNT(DISTINCT m.id) FILTER (
            WHERE h.captured_at >= NOW() - (%s || ' minutes')::interval
          )::int AS fresh_games
        FROM nfl_matchups m
        LEFT JOIN game_odds_history h ON h.matchup_id = m.id AND h.sport = 'nfl'
          AND h.captured_at <= m.commence_time
        WHERE m.commence_time > NOW()
          AND m.commence_time <= NOW() + (%s || ' hours')::interval
          AND NOT m.completed
          AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
        """,
        (max_age_minutes, max(1, int(monitoring_hours))),
    ) or {}
    upcoming = int(row.get("upcoming_games") or 0)
    fresh = int(row.get("fresh_games") or 0)
    if upcoming == 0:
        return True
    if fresh < upcoming:
        logger.error("NFL monitoring freshness failed: %d/%d games fresh", fresh, upcoming)
        return False
    return True


def collect_nfl_data_health(db: DatabaseManager, game_date: str) -> dict:
    """Return an operational verdict for identity, freshness, and settlement."""
    row = db.execute_one(
        """
        WITH latest AS (
          SELECT matchup_id, MAX(captured_at) AS captured_at
          FROM game_odds_history WHERE sport = 'nfl' GROUP BY matchup_id
        )
        SELECT
          COUNT(*) FILTER (WHERE m.commence_time > NOW() AND l.captured_at IS NULL)::int AS missing_capture,
          COUNT(*) FILTER (WHERE m.commence_time > NOW()
                            AND l.captured_at < NOW() - INTERVAL '35 minutes')::int AS stale_capture,
          COUNT(*) FILTER (WHERE m.commence_time < NOW() - INTERVAL '24 hours'
                            AND (m.home_score IS NULL OR m.away_score IS NULL))::int AS missing_score,
          (SELECT COUNT(DISTINCT h.matchup_id)::int FROM game_odds_history h
             JOIN nfl_matchups x ON x.id = h.matchup_id
             WHERE h.sport = 'nfl' AND x.game_date = %s AND h.captured_at > x.commence_time) AS post_kickoff,
          (SELECT COUNT(*)::int FROM line_alerts a JOIN nfl_matchups x ON x.id = a.matchup_id
             WHERE a.sport = 'nfl' AND x.game_date = %s
               AND x.home_score IS NOT NULL AND x.away_score IS NOT NULL
               AND a.settled_at IS NULL) AS unsettled_alerts
        FROM nfl_matchups m LEFT JOIN latest l ON l.matchup_id = m.id
        WHERE m.game_date = %s
        """,
        (game_date, game_date, game_date),
    ) or {}
    counts = {key: int(row.get(key) or 0) for key in (
        "missing_capture", "stale_capture", "missing_score", "post_kickoff", "unsettled_alerts",
    )}
    hard_errors = counts["missing_capture"] + counts["stale_capture"] + counts["missing_score"] + counts["post_kickoff"]
    return {"status": "pass" if hard_errors == 0 else "fail", **counts}


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh NFL events, odds, and scores")
    parser.add_argument("--date", default=None, help="Eastern game date YYYY-MM-DD")
    parser.add_argument("--scores-only", action="store_true")
    parser.add_argument("--skip-scores", action="store_true")
    parser.add_argument("--days-from", type=int, default=3)
    parser.add_argument("--require-fresh-upcoming-odds", action="store_true")
    parser.add_argument("--monitoring-hours", type=int, default=None)
    args = parser.parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)
    if args.scores_only:
        fetch_scores(db, config.odds_api.api_key, args.days_from)
        return 0
    target_date = args.date or datetime.now(EASTERN).date().isoformat()
    fetch_odds(
        db,
        config.odds_api.api_key,
        None if args.monitoring_hours else target_date,
        monitoring_hours=args.monitoring_hours,
    )
    if not args.skip_scores:
        fetch_scores(db, config.odds_api.api_key, args.days_from)
    if args.require_fresh_upcoming_odds:
        fresh = (
            verify_fresh_monitoring_window(db, monitoring_hours=args.monitoring_hours)
            if args.monitoring_hours
            else verify_fresh_upcoming_odds(db, target_date)
        )
        if not fresh:
            return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
