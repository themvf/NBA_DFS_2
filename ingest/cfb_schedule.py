"""Canonical CFB schedule, exact-book odds, and quota-aware checkpoints.

CFBD owns game identity and final scores. The Odds API owns sportsbook quotes.
Only accepted, mapped, pre-kickoff quotes reach ``game_odds_history``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_cfb_team_name_cache,
    insert_game_odds_history_rows,
    map_cfb_odds_event,
    quarantine_cfb_event,
    upsert_cfb_matchup,
    upsert_cfb_team,
    upsert_cfb_team_alias,
    upsert_cfb_venue,
)
from ingest.game_odds_market import (
    eastern_date,
    extract_game_markets,
    parse_iso,
    require_pregame_capture,
    vig_free_home_probability,
)
from ingest.mlb_odds_policy import validate_event_prices

logger = logging.getLogger(__name__)

CFBD_BASE = "https://api.collegefootballdata.com"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
CFB_SPORT_KEY = "americanfootball_ncaaf"
CFB_BOOKMAKERS = (
    "draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics",
    "espnbet", "hardrockbet", "betrivers", "pinnacle", "bovada",
)
CFB_MARKETS = "h2h,spreads,totals"
CHECKPOINTS = (
    ("t_minus_48h", 42 * 60, 48 * 60),
    ("t_minus_24h", 20 * 60, 24 * 60),
    ("t_minus_6h", 330, 360),
    ("t_minus_90m", 60, 90),
    ("t_minus_15m", 5, 15),
    ("t_minus_2m", 0, 2),
)


def _normal_name(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", text.casefold())


def official_team_aliases(teams: list[dict], canonical: dict[int, int], existing: dict[str, int]) -> list[tuple[str, int]]:
    """Exact official names + mascots, rejecting cross-team collisions (no fuzzy matching)."""
    owners: dict[str, set[int]] = {}
    candidates: dict[str, int] = {}
    for name, team_id in existing.items():
        owners.setdefault(_normal_name(name), set()).add(team_id)
    for team in teams:
        team_id = canonical.get(team.get("id"))
        if team_id is None:
            continue
        names = {str(team[key]).strip() for key in (
            "school", "alt_name_1", "alt_name_2", "alt_name_3", "altName1", "altName2", "altName3"
        ) if team.get(key)}
        # Current CFBD API uses an array; retain scalar support for old exports.
        names.update(name.strip() for name in (team.get("alternateNames") or [])
                     if isinstance(name, str) and name.strip())
        mascot = str(team.get("mascot") or "").strip()
        aliases = names | {f"{name} {mascot}" for name in names if mascot}
        for alias in aliases:
            owners.setdefault(_normal_name(alias), set()).add(team_id)
            candidates[alias] = team_id
    return sorted((alias, team_id) for alias, team_id in candidates.items()
                  if len(owners[_normal_name(alias)]) == 1)


def refresh_team_aliases(db: DatabaseManager, api_key: str) -> int:
    from psycopg2.extras import execute_values
    response = requests.get(f"{CFBD_BASE}/teams", headers=_cfbd_headers(api_key), timeout=45)
    response.raise_for_status()
    canonical = {int(row["cfbd_team_id"]): int(row["team_id"]) for row in db.execute(
        "SELECT cfbd_team_id, team_id FROM cfb_teams WHERE active=TRUE"
    )}
    aliases = official_team_aliases(response.json(), canonical, build_cfb_team_name_cache(db))
    if aliases:
        with db.connect() as connection:
            execute_values(connection.cursor(),
                """INSERT INTO cfb_team_aliases (provider, alias, team_id, reviewed)
                   VALUES %s ON CONFLICT (provider, alias) DO NOTHING""",
                [("odds_api", alias, team_id, True) for alias, team_id in aliases])
    print(f"CFB aliases: {len(aliases)} unambiguous official name candidates synchronized")
    return len(aliases)


def _cfbd_headers(api_key: str) -> dict[str, str]:
    if not api_key:
        raise ValueError("CFBD_API_KEY is required for CFB schedule ingestion")
    return {"Authorization": f"Bearer {api_key}"}


def _log_quota(response: requests.Response, label: str) -> None:
    logger.info(
        "%s quota: remaining=%s used=%s last=%s",
        label,
        response.headers.get("x-requests-remaining", "?"),
        response.headers.get("x-requests-used", "?"),
        response.headers.get("x-requests-last", "?"),
    )


def _media_by_game(
    api_key: str, *, year: int, week: int | None, season_type: str,
) -> dict[int, str]:
    params: dict[str, object] = {"year": year, "seasonType": season_type, "classification": "fbs"}
    if week is not None:
        params["week"] = week
    try:
        response = requests.get(
            f"{CFBD_BASE}/games/media",
            params=params,
            headers=_cfbd_headers(api_key),
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("CFBD media unavailable; continuing without networks: %s", exc)
        return {}
    result: dict[int, str] = {}
    for item in response.json() or []:
        if item.get("mediaType") != "tv" or item.get("id") is None:
            continue
        result[int(item["id"])] = str(item.get("outlet") or "")
    return result


def fetch_schedule(
    db: DatabaseManager,
    api_key: str,
    *,
    year: int,
    week: int | None = None,
    season_type: str = "regular",
) -> int:
    params: dict[str, object] = {
        "year": year,
        "seasonType": season_type,
        "classification": "fbs",
    }
    if week is not None:
        params["week"] = week
    response = requests.get(
        f"{CFBD_BASE}/games",
        params=params,
        headers=_cfbd_headers(api_key),
        timeout=45,
    )
    response.raise_for_status()
    media = _media_by_game(api_key, year=year, week=week, season_type=season_type)
    # Reuse the historical ingest's transaction facade; opening a fresh TLS
    # connection for every team/alias/venue/game write can take several minutes.
    from ingest.cfb_history import _TransactionDb
    with db.connect() as connection:
        return _store_schedule(_TransactionDb(connection), response.json() or [], media,
                               year=year, week=week, season_type=season_type)


def _store_schedule(db, games: list[dict], media: dict, *, year: int,
                    week: int | None, season_type: str) -> int:
    upserted = 0
    for game in games:
        game_id = game.get("id")
        if game_id is None:
            raise ValueError("CFBD game missing id")
        commence = parse_iso(game.get("startDate"))
        home_id = upsert_cfb_team(
            db,
            cfbd_team_id=int(game["homeId"]),
            name=str(game["homeTeam"]),
            conference=game.get("homeConference"),
            classification=game.get("homeClassification"),
        )
        away_id = upsert_cfb_team(
            db,
            cfbd_team_id=int(game["awayId"]),
            name=str(game["awayTeam"]),
            conference=game.get("awayConference"),
            classification=game.get("awayClassification"),
        )
        upsert_cfb_team_alias(
            db, provider="cfbd", alias=str(game["homeTeam"]), team_id=home_id, reviewed=True,
        )
        upsert_cfb_team_alias(
            db, provider="cfbd", alias=str(game["awayTeam"]), team_id=away_id, reviewed=True,
        )
        venue_id = None
        if game.get("venue"):
            venue_id = upsert_cfb_venue(
                db,
                cfbd_venue_id=int(game["venueId"]) if game.get("venueId") is not None else None,
                name=str(game["venue"]),
            )
        completed = bool(game.get("completed"))
        matchup_id = upsert_cfb_matchup(
            db,
            cfbd_game_id=int(game_id),
            season=int(game.get("season") or year),
            season_type=str(game.get("seasonType") or season_type),
            week=int(game.get("week") or week or 0),
            game_date=eastern_date(commence),
            commence_time=commence,
            start_time_tbd=bool(game.get("startTimeTBD")),
            home_team_id=home_id,
            away_team_id=away_id,
            venue_id=venue_id,
            neutral_site=bool(game.get("neutralSite")),
            conference_game=bool(game.get("conferenceGame")),
            network=media.get(int(game_id)),
            completed=completed,
            home_score=int(game["homePoints"]) if game.get("homePoints") is not None else None,
            away_score=int(game["awayPoints"]) if game.get("awayPoints") is not None else None,
            home_line_scores=game.get("homeLineScores"),
            away_line_scores=game.get("awayLineScores"),
            game_status="final" if completed else "scheduled",
        )
        upserted += int(bool(matchup_id))
    print(f"CFB schedule: {upserted} canonical games upserted for {year}" + (f" week {week}" if week else ""))
    return upserted


def _team_cache(db: DatabaseManager) -> dict[str, int]:
    return {
        _normal_name(name): team_id
        for name, team_id in build_cfb_team_name_cache(db).items()
    }


def _candidate_matchups(db: DatabaseManager, home_id: int, away_id: int) -> list[dict]:
    return db.execute(
        """
        SELECT id, cfbd_game_id, odds_event_id, game_date, commence_time,
               start_time_tbd
        FROM cfb_matchups
        WHERE home_team_id=%s AND away_team_id=%s AND completed=FALSE
          AND game_date BETWEEN (CURRENT_DATE - INTERVAL '7 days')::date
                            AND (CURRENT_DATE + INTERVAL '30 days')::date
        ORDER BY commence_time NULLS LAST, id
        """,
        (home_id, away_id),
    )


def _resolve_event_matchup(db: DatabaseManager, event: dict, cache: dict[str, int]) -> dict | None:
    event_id = str(event.get("id") or "").strip()
    if not event_id:
        return None
    existing = db.execute_one(
        "SELECT * FROM cfb_matchups WHERE odds_event_id=%s",
        (event_id,),
    )
    if existing:
        return existing
    home_name = str(event.get("home_team") or "")
    away_name = str(event.get("away_team") or "")
    home_id = cache.get(_normal_name(home_name))
    away_id = cache.get(_normal_name(away_name))
    commence = parse_iso(event.get("commence_time"))
    if home_id is None or away_id is None:
        quarantine_cfb_event(
            db, event_id=event_id, home_name=home_name, away_name=away_name,
            commence_time=commence, reason="unknown team alias", raw_json=event,
        )
        return None
    candidates = _candidate_matchups(db, home_id, away_id)
    eligible: list[tuple[float, dict]] = []
    for candidate in candidates:
        stored = candidate.get("commence_time")
        if stored is None:
            continue
        stored_utc = stored if stored.tzinfo else stored.replace(tzinfo=timezone.utc)
        delta_hours = abs((stored_utc.astimezone(timezone.utc) - commence).total_seconds()) / 3600
        same_date = str(candidate["game_date"]) == eastern_date(commence)
        if delta_hours <= 6 or (candidate.get("start_time_tbd") and same_date):
            eligible.append((delta_hours, candidate))
    eligible.sort(key=lambda item: item[0])
    if not eligible or (len(eligible) > 1 and eligible[0][0] == eligible[1][0]):
        quarantine_cfb_event(
            db, event_id=event_id, home_name=home_name, away_name=away_name,
            commence_time=commence,
            reason="no unique canonical matchup within kickoff tolerance",
            raw_json=event,
        )
        return None
    matchup = eligible[0][1]
    map_cfb_odds_event(db, matchup_id=int(matchup["id"]), event_id=event_id)
    db.execute(
        "UPDATE cfb_unmapped_events SET resolved_at=NOW() WHERE provider='odds_api' AND provider_event_id=%s",
        (event_id,),
    )
    return {**matchup, "odds_event_id": event_id}


def fetch_events(db: DatabaseManager, api_key: str) -> int:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for CFB event ingestion")
    response = requests.get(
        f"{ODDS_API_BASE}/sports/{CFB_SPORT_KEY}/events",
        params={"apiKey": api_key, "dateFormat": "iso"},
        timeout=25,
    )
    response.raise_for_status()
    _log_quota(response, "CFB events")
    cache = _team_cache(db)
    mapped = 0
    for event in response.json() or []:
        mapped += int(_resolve_event_matchup(db, event, cache) is not None)
    print(f"CFB events: {mapped} provider events mapped")
    return mapped


def fetch_odds(
    db: DatabaseManager,
    api_key: str,
    *,
    event_ids: set[str] | None = None,
    refresh_events: bool = True,
    request_audit: dict | None = None,
) -> int:
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for CFB odds ingestion")
    if refresh_events:
        fetch_events(db, api_key)
    if event_ids is None:
        rows = db.execute(
            """
            SELECT odds_event_id FROM cfb_matchups
            WHERE odds_event_id IS NOT NULL AND completed=FALSE
              AND commence_time > NOW() AND commence_time <= NOW() + INTERVAL '72 hours'
            """
        )
        event_ids = {str(row["odds_event_id"]) for row in rows}
    if not event_ids:
        print("CFB odds: no mapped due events; paid request skipped")
        return 0
    response = requests.get(
        f"{ODDS_API_BASE}/sports/{CFB_SPORT_KEY}/odds",
        params={
            "apiKey": api_key,
            "bookmakers": ",".join(CFB_BOOKMAKERS),
            "markets": CFB_MARKETS,
            "oddsFormat": "american",
            "dateFormat": "iso",
        },
        timeout=30,
    )
    response.raise_for_status()
    _log_quota(response, "CFB odds")
    if request_audit is not None:
        request_audit.update({
            "requests_remaining": response.headers.get("x-requests-remaining"),
            "requests_used": response.headers.get("x-requests-used"),
            "requests_last": response.headers.get("x-requests-last"),
            "returned_events": len(response.json() or []),
        })
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = captured_at.isoformat()
    history_rows: list[dict] = []
    team_cache = _team_cache(db)
    for event in response.json() or []:
        event_id = str(event.get("id") or "")
        if event_id not in event_ids:
            continue
        matchup = db.execute_one(
            """
            SELECT m.*, ht.name AS home_name, at.name AS away_name
            FROM cfb_matchups m
            JOIN cfb_teams ht ON ht.team_id=m.home_team_id
            JOIN cfb_teams at ON at.team_id=m.away_team_id
            WHERE m.odds_event_id=%s
            """,
            (event_id,),
        )
        if not matchup:
            continue
        event_home_id = team_cache.get(_normal_name(event.get("home_team")))
        event_away_id = team_cache.get(_normal_name(event.get("away_team")))
        if (
            event_home_id != int(matchup["home_team_id"])
            or event_away_id != int(matchup["away_team_id"])
        ):
            quarantine_cfb_event(
                db, event_id=event_id, home_name=event.get("home_team"),
                away_name=event.get("away_team"), commence_time=parse_iso(event.get("commence_time")),
                reason="mapped event team identity changed", raw_json=event,
            )
            continue
        event_commence = parse_iso(event.get("commence_time"))
        try:
            require_pregame_capture(
                event_commence=event_commence,
                stored_commence=matchup["commence_time"],
                captured_at=captured_at,
            )
        except ValueError as exc:
            logger.info("Skipping in-play CFB event %s: %s", event_id, exc)
            continue
        validate_event_prices(event)
        market = extract_game_markets(event)
        if not market["books"]:
            continue
        home_prob = vig_free_home_probability(market["home_ml"], market["away_ml"])
        total, spread = market["vegas_total"], market["home_spread"]
        home_implied = (total - spread) / 2 if total is not None and spread is not None else None
        away_implied = (total + spread) / 2 if total is not None and spread is not None else None
        db.execute(
            """
            UPDATE cfb_matchups SET
                vegas_total=%s, home_ml=%s, away_ml=%s, home_spread=%s,
                vegas_prob_home=%s, home_implied=%s, away_implied=%s,
                odds_fetched_at=%s
            WHERE id=%s
            """,
            (total, market["home_ml"], market["away_ml"], spread, home_prob,
             home_implied, away_implied, captured_at, matchup["id"]),
        )
        history_rows.append({
            "sport": "cfb",
            "matchup_id": matchup["id"],
            "event_id": event_id,
            "game_date": matchup["game_date"],
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
            "books": market["books"],
            "vegas_total_raw": market["vegas_total_raw"],
        })
    inserted = insert_game_odds_history_rows(db, history_rows)
    print(f"CFB odds: {inserted} pregame event captures written")
    return inserted


def due_checkpoints(db: DatabaseManager) -> list[dict]:
    values = ", ".join(
        f"('{name}', {minimum}, {maximum})" for name, minimum, maximum in CHECKPOINTS
    )
    return db.execute(
        f"""
        WITH windows(checkpoint, min_lead, max_lead) AS (VALUES {values})
        SELECT m.id, m.odds_event_id, m.commence_time, w.checkpoint,
               w.min_lead, w.max_lead
        FROM cfb_matchups m
        CROSS JOIN windows w
        WHERE m.completed=FALSE AND m.start_time_tbd=FALSE
          AND m.odds_event_id IS NOT NULL AND m.commence_time > NOW()
          AND EXTRACT(EPOCH FROM (m.commence_time - NOW())) / 60.0
                BETWEEN w.min_lead AND w.max_lead
          AND NOT EXISTS (
              SELECT 1 FROM game_odds_history h
              WHERE h.sport='cfb' AND h.matchup_id=m.id AND h.books IS NOT NULL
                AND h.captured_at < m.commence_time
                AND EXTRACT(EPOCH FROM (m.commence_time - h.captured_at)) / 60.0
                      BETWEEN w.min_lead AND w.max_lead
          )
        ORDER BY m.commence_time, w.min_lead
        """
    )


def capture_due_checkpoints(db: DatabaseManager, api_key: str, *, dry_run: bool = False) -> dict:
    due = due_checkpoints(db)
    event_ids = {str(row["odds_event_id"]) for row in due}
    result = {
        "due_games": len({int(row["id"]) for row in due}),
        "due_checkpoints": len(due),
        "checkpoints": sorted({str(row["checkpoint"]) for row in due}),
        "paid_request": False,
        "captured_events": 0,
        "dry_run": dry_run,
    }
    if not event_ids or dry_run:
        return result
    audit: dict = {}
    result["paid_request"] = True
    result["captured_events"] = fetch_odds(
        db, api_key, event_ids=event_ids, refresh_events=False, request_audit=audit,
    )
    result["quota"] = audit
    return result


def refresh_recent_scores(db: DatabaseManager, api_key: str) -> int:
    """Refresh weeks with recent games instead of rewriting the whole season."""
    weeks = db.execute(
        """SELECT DISTINCT season, week, season_type FROM cfb_matchups
           WHERE commence_time BETWEEN NOW() - INTERVAL '48 hours' AND NOW()
             AND season_type IN ('regular', 'postseason')"""
    )
    return sum(fetch_schedule(db, api_key, year=int(row["season"]),
                              week=int(row["week"]), season_type=row["season_type"])
               for row in weeks)


def collect_data_health(db: DatabaseManager) -> dict:
    row = db.execute_one(
        """
        SELECT
          COUNT(*) FILTER (
            WHERE completed=FALSE AND commence_time > NOW()
              AND commence_time <= NOW() + INTERVAL '72 hours'
              AND odds_event_id IS NULL
          )::int AS unmapped_upcoming,
          (SELECT COUNT(*) FROM cfb_unmapped_events WHERE resolved_at IS NULL)::int AS quarantined,
          (SELECT COUNT(*) FROM game_odds_history h JOIN cfb_matchups m ON m.id=h.matchup_id
             WHERE h.sport='cfb' AND m.commence_time IS NOT NULL
               AND h.captured_at >= m.commence_time)::int AS post_kickoff,
          COUNT(*) FILTER (
            WHERE completed=TRUE AND (home_score IS NULL OR away_score IS NULL)
          )::int AS missing_final_score
        FROM cfb_matchups
        """
    ) or {}
    result = {key: int(row.get(key) or 0) for key in (
        "unmapped_upcoming", "quarantined", "post_kickoff", "missing_final_score"
    )}
    result["status"] = "fail" if result["post_kickoff"] or result["missing_final_score"] else (
        "warn" if result["unmapped_upcoming"] or result["quarantined"] else "pass"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh canonical CFB schedule and sportsbook checkpoints")
    parser.add_argument("--year", type=int, default=datetime.now().year)
    parser.add_argument("--week", type=int)
    parser.add_argument("--season-type", default="regular")
    parser.add_argument("--refresh-schedule", action="store_true")
    parser.add_argument("--refresh-scores", action="store_true")
    parser.add_argument("--refresh-team-aliases", action="store_true")
    parser.add_argument("--refresh-events", action="store_true")
    parser.add_argument("--capture-due", action="store_true")
    parser.add_argument("--capture-now", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--health", action="store_true")
    args = parser.parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)
    cfbd_key = getattr(config, "cfbd_api_key", None) or __import__("os").getenv("CFBD_API_KEY", "")
    odds_key = config.odds_api.api_key
    ran = False
    if args.refresh_team_aliases:
        refresh_team_aliases(db, cfbd_key)
        ran = True
    if args.refresh_schedule:
        fetch_schedule(db, cfbd_key, year=args.year, week=args.week, season_type=args.season_type)
        ran = True
    if args.refresh_scores:
        refresh_recent_scores(db, cfbd_key)
        ran = True
    if args.refresh_events:
        fetch_events(db, odds_key)
        ran = True
    if args.capture_now:
        fetch_odds(db, odds_key)
        ran = True
    if args.capture_due:
        print(json.dumps(capture_due_checkpoints(db, odds_key, dry_run=args.dry_run), indent=2, default=str))
        ran = True
    if args.health:
        print(json.dumps(collect_data_health(db), indent=2, default=str))
        ran = True
    if not ran:
        fetch_schedule(db, cfbd_key, year=args.year, week=args.week, season_type=args.season_type)
        fetch_events(db, odds_key)
        print(json.dumps(capture_due_checkpoints(db, odds_key), indent=2, default=str))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
