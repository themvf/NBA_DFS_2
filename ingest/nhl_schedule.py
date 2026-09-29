"""Canonical NHL schedule, free event mapping, and exact-book odds captures.

The NHL's public API (api-web.nhle.com, free, no key) owns game identity,
scheduled start, status and final score. The Odds API owns sportsbook quotes.
Only accepted, mapped, pre-puck-drop quotes reach ``game_odds_history`` with
``sport='nhl'`` -- the same ledger design as ``ingest/cfb_schedule.py``.

Paid captures are made by the shared checkpoint worker
(``ingest/event_closing_lines.py``); this module's scheduled job only refreshes
the free schedule, scores and provider-event mappings.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from datetime import date, datetime, timedelta, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_nhl_team_name_cache,
    insert_game_odds_history_rows,
    map_nhl_odds_event,
    quarantine_nhl_event,
    upsert_nhl_matchup,
    upsert_nhl_team,
)
from db.schema import CLOSE_CAPTURE_CONSTRAINT_DDLS, NHL_INDEXES, NHL_TABLES
from ingest.game_odds_market import (
    EASTERN,
    eastern_date,
    extract_game_markets,
    parse_iso,
    require_pregame_capture,
    vig_free_home_probability,
)
from ingest.mlb_odds_policy import MlbOddsPolicyError, validate_event_prices
from ingest.sportsbook_policy import BOOKMAKER_KEYS, selected_event

logger = logging.getLogger(__name__)

NHL_API_BASE = "https://api-web.nhle.com/v1"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
NHL_SPORT_KEY = "icehockey_nhl"
NHL_BOOKMAKERS = BOOKMAKER_KEYS
# h2h = moneyline (includes overtime and shootout), spreads = puck line,
# totals = game total. Six named books bill as one group: 3 credits per call.
NHL_MARKETS = "h2h,spreads,totals"
# Regular season (2) and playoffs (3). Preseason (1) has no Odds API market.
NHL_GAME_TYPES = (2, 3)
SCHEDULE_DAYS = 14
# The bulk /odds call is billed per market, not per event, so each paid call
# records every mapped game inside this horizon -- not only the games whose
# checkpoint is due. Extra tape at zero marginal cost.
CAPTURE_HORIZON_HOURS = 72
# The Odds API lists puck drop ~10 minutes after the NHL's scheduled start
# (21:10 vs 21:00 on 2026-09-29); a wide tolerance still requires both teams.
EVENT_MATCH_TOLERANCE_HOURS = 6
FINAL_STATES = frozenset({"FINAL", "OFF"})
PLAYABLE_SCHEDULE_STATES = frozenset({"OK"})


def _normal_name(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", text.casefold())


def _localized(value: object) -> str:
    if isinstance(value, dict):
        value = value.get("default")
    return str(value or "").strip()


def team_full_name(team: dict) -> str:
    """"Florida" + "Panthers" -> "Florida Panthers" (the Odds API's spelling)."""
    place, common = _localized(team.get("placeName")), _localized(team.get("commonName"))
    if not place or not common:
        raise ValueError(f"NHL team {team.get('abbrev')!r} is missing placeName/commonName")
    return f"{place} {common}"


def _networks(game: dict) -> str | None:
    """National broadcasts first, then regional; unique, in NHL order."""
    ordered = sorted(
        (item for item in game.get("tvBroadcasts") or [] if item.get("network")),
        key=lambda item: (item.get("market") != "N", item.get("sequenceNumber") or 0),
    )
    names: list[str] = []
    for item in ordered:
        if item["network"] not in names:
            names.append(str(item["network"]))
    return ", ".join(names) or None


def parse_schedule_games(payload: dict) -> list[dict]:
    """Normalize one ``/schedule/{date}`` gameWeek payload into matchup rows."""
    games: dict[int, dict] = {}
    for day in payload.get("gameWeek") or []:
        for game in day.get("games") or []:
            if int(game.get("gameType") or 0) not in NHL_GAME_TYPES:
                continue
            commence = parse_iso(game.get("startTimeUTC"))
            state = str(game.get("gameState") or "")
            completed = state in FINAL_STATES
            home, away = game["homeTeam"], game["awayTeam"]
            outcome = game.get("gameOutcome") or {}
            games[int(game["id"])] = {
                "nhl_game_id": int(game["id"]),
                "season": int(game.get("season") or 0),
                "game_type": int(game["gameType"]),
                "game_date": eastern_date(commence),
                "commence_time": commence,
                "venue": _localized(game.get("venue")) or None,
                "neutral_site": bool(game.get("neutralSite")),
                "networks": _networks(game),
                "game_state": state or None,
                "schedule_state": str(game.get("gameScheduleState") or "OK"),
                "completed": completed,
                # Live payloads carry running scores. Only a final is stored:
                # alert settlement reads these columns.
                "home_score": int(home["score"]) if completed and home.get("score") is not None else None,
                "away_score": int(away["score"]) if completed and away.get("score") is not None else None,
                "last_period_type": outcome.get("lastPeriodType") if completed else None,
                "home_team": home,
                "away_team": away,
            }
    return sorted(games.values(), key=lambda row: (row["commence_time"], row["nhl_game_id"]))


def _store_games(db: DatabaseManager, games: list[dict]) -> int:
    team_ids: dict[int, int] = {}

    def team_id(team: dict) -> int:
        nhl_id = int(team["id"])
        if nhl_id not in team_ids:
            team_ids[nhl_id] = upsert_nhl_team(
                db,
                nhl_team_id=nhl_id,
                abbreviation=str(team.get("abbrev") or ""),
                name=team_full_name(team),
                place_name=_localized(team.get("placeName")) or None,
                common_name=_localized(team.get("commonName")) or None,
                logo_url=str(team.get("logo") or ""),
            )
        return team_ids[nhl_id]

    stored = 0
    for game in games:
        row = {key: value for key, value in game.items() if key not in ("home_team", "away_team")}
        row["home_team_id"] = team_id(game["home_team"])
        row["away_team_id"] = team_id(game["away_team"])
        stored += int(bool(upsert_nhl_matchup(db, **row)))
    return stored


def fetch_schedule(db: DatabaseManager, *, start: date, days: int = SCHEDULE_DAYS) -> int:
    """Upsert every regular-season/playoff game in [start, start + days)."""
    games: dict[int, dict] = {}
    for offset in range(0, max(days, 1), 7):  # each call returns a 7-day gameWeek
        day = start + timedelta(days=offset)
        response = requests.get(f"{NHL_API_BASE}/schedule/{day.isoformat()}", timeout=30)
        response.raise_for_status()
        for game in parse_schedule_games(response.json() or {}):
            if game["commence_time"].astimezone(EASTERN).date() < start + timedelta(days=days):
                games[game["nhl_game_id"]] = game
    stored = _store_games(db, sorted(games.values(), key=lambda g: g["commence_time"]))
    print(f"NHL schedule: {stored} canonical games upserted from {start} (+{days}d)")
    return stored


def refresh_recent_scores(db: DatabaseManager, *, today: date | None = None) -> int:
    """One free call covering the last two days (finals) through the next four."""
    today = today or datetime.now(EASTERN).date()
    return fetch_schedule(db, start=today - timedelta(days=2), days=7)


def _team_cache(db: DatabaseManager) -> dict[str, int]:
    """Normalized full name -> team_id; a name two teams share is dropped."""
    owners: dict[str, set[int]] = {}
    for name, team_id in build_nhl_team_name_cache(db).items():
        owners.setdefault(_normal_name(name), set()).add(team_id)
    return {key: next(iter(ids)) for key, ids in owners.items() if len(ids) == 1}


def _candidate_matchups(db: DatabaseManager, home_id: int, away_id: int) -> list[dict]:
    return db.execute(
        """
        SELECT id, odds_event_id, game_date, commence_time
        FROM nhl_matchups
        WHERE home_team_id=%s AND away_team_id=%s AND completed=FALSE
          AND commence_time BETWEEN NOW() - INTERVAL '1 day' AND NOW() + INTERVAL '30 days'
        ORDER BY commence_time, id
        """,
        (home_id, away_id),
    )


def _resolve_event_matchup(db: DatabaseManager, event: dict, cache: dict[str, int]) -> dict | None:
    event_id = str(event.get("id") or "").strip()
    if not event_id:
        return None
    existing = db.execute_one("SELECT * FROM nhl_matchups WHERE odds_event_id=%s", (event_id,))
    if existing:
        return existing
    home_name, away_name = str(event.get("home_team") or ""), str(event.get("away_team") or "")
    commence = parse_iso(event.get("commence_time"))

    def quarantine(reason: str) -> None:
        quarantine_nhl_event(
            db, event_id=event_id, home_name=home_name, away_name=away_name,
            commence_time=commence, reason=reason, raw_json=event,
        )

    home_id, away_id = cache.get(_normal_name(home_name)), cache.get(_normal_name(away_name))
    if home_id is None or away_id is None:
        quarantine("unknown team name")
        return None
    eligible: list[tuple[float, dict]] = []
    for candidate in _candidate_matchups(db, home_id, away_id):
        stored = candidate.get("commence_time")
        if stored is None:
            continue
        stored = stored if stored.tzinfo else stored.replace(tzinfo=timezone.utc)
        delta_hours = abs((stored.astimezone(timezone.utc) - commence).total_seconds()) / 3600
        if delta_hours <= EVENT_MATCH_TOLERANCE_HOURS:
            eligible.append((delta_hours, candidate))
    eligible.sort(key=lambda item: item[0])
    if not eligible or (len(eligible) > 1 and eligible[0][0] == eligible[1][0]):
        quarantine("no unique canonical matchup within start-time tolerance")
        return None
    matchup = eligible[0][1]
    try:
        map_nhl_odds_event(db, matchup_id=int(matchup["id"]), event_id=event_id)
    except ValueError as exc:  # the game already maps to another provider event
        quarantine(str(exc))
        return None
    db.execute(
        "UPDATE nhl_unmapped_events SET resolved_at=NOW() WHERE provider='odds_api' AND provider_event_id=%s",
        (event_id,),
    )
    return {**matchup, "odds_event_id": event_id}


def _log_quota(response: requests.Response, label: str) -> None:
    logger.info(
        "%s quota: remaining=%s used=%s last=%s", label,
        response.headers.get("x-requests-remaining", "?"),
        response.headers.get("x-requests-used", "?"),
        response.headers.get("x-requests-last", "?"),
    )


def fetch_events(db: DatabaseManager, api_key: str) -> int:
    """Map free Odds API events onto canonical games. Costs no credits."""
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for NHL event mapping")
    response = requests.get(
        f"{ODDS_API_BASE}/sports/{NHL_SPORT_KEY}/events",
        params={"apiKey": api_key, "dateFormat": "iso"},
        timeout=25,
    )
    response.raise_for_status()
    _log_quota(response, "NHL events")
    cache = _team_cache(db)
    mapped = sum(int(_resolve_event_matchup(db, event, cache) is not None)
                 for event in response.json() or [])
    print(f"NHL events: {mapped} provider events mapped")
    return mapped


def _mapped_upcoming(db: DatabaseManager, horizon_hours: int) -> dict[str, dict]:
    rows = db.execute(
        """
        SELECT m.*, ht.name AS home_name, at.name AS away_name
        FROM nhl_matchups m
        JOIN nhl_teams ht ON ht.team_id=m.home_team_id
        JOIN nhl_teams at ON at.team_id=m.away_team_id
        WHERE m.odds_event_id IS NOT NULL AND m.completed=FALSE
          AND COALESCE(m.schedule_state, 'OK') = 'OK'
          AND m.commence_time > NOW()
          AND m.commence_time <= NOW() + %s * INTERVAL '1 hour'
        """,
        (horizon_hours,),
    )
    return {str(row["odds_event_id"]): row for row in rows}


def fetch_odds(
    db: DatabaseManager,
    api_key: str,
    *,
    event_ids: set[str] | None = None,
    request_audit: dict | None = None,
    horizon_hours: int = CAPTURE_HORIZON_HOURS,
) -> int:
    """One bulk paid call; append a capture for every mapped upcoming game.

    ``event_ids`` (the checkpoint worker's due games) only decides whether the
    call is worth making: when none of them is still a mapped upcoming game,
    no credits are spent.
    """
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for NHL odds ingestion")
    mapped = _mapped_upcoming(db, horizon_hours)
    if not mapped or (event_ids is not None and not set(event_ids) & set(mapped)):
        print("NHL odds: no mapped upcoming game is due; paid request skipped")
        return 0
    url = f"{ODDS_API_BASE}/sports/{NHL_SPORT_KEY}/odds"
    response = requests.get(
        url,
        params={
            "apiKey": api_key,
            "bookmakers": ",".join(NHL_BOOKMAKERS),
            "markets": NHL_MARKETS,
            "oddsFormat": "american",
            "dateFormat": "iso",
        },
        timeout=30,
    )
    if request_audit is not None:
        request_audit.update({
            "endpoint": url,
            "status": response.status_code,
            "request_count": 1,
            "requests_remaining": response.headers.get("x-requests-remaining"),
            "requests_used": response.headers.get("x-requests-used"),
            "requests_last": response.headers.get("x-requests-last"),
        })
    response.raise_for_status()
    _log_quota(response, "NHL odds")
    payload = response.json() or []
    if request_audit is not None:
        request_audit["returned_events"] = len(payload)
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = captured_at.isoformat()
    team_cache = _team_cache(db)
    history_rows: list[dict] = []
    for raw_event in payload:
        event = selected_event(raw_event)
        event_id = str(event.get("id") or "")
        matchup = mapped.get(event_id)
        if matchup is None:
            continue
        if (
            team_cache.get(_normal_name(event.get("home_team"))) != int(matchup["home_team_id"])
            or team_cache.get(_normal_name(event.get("away_team"))) != int(matchup["away_team_id"])
        ):
            quarantine_nhl_event(
                db, event_id=event_id, home_name=event.get("home_team"),
                away_name=event.get("away_team"), commence_time=parse_iso(event.get("commence_time")),
                reason="mapped event team identity changed", raw_json=event,
            )
            continue
        try:
            require_pregame_capture(
                event_commence=parse_iso(event.get("commence_time")),
                stored_commence=matchup["commence_time"],
                captured_at=captured_at,
            )
            validate_event_prices(event)
        except (ValueError, MlbOddsPolicyError) as exc:
            logger.info("Skipping NHL event %s: %s", event_id, exc)
            continue
        market = extract_game_markets(event)
        if not market["books"]:
            continue
        home_prob = vig_free_home_probability(market["home_ml"], market["away_ml"])
        db.execute(
            """
            UPDATE nhl_matchups SET
                vegas_total=%s, home_ml=%s, away_ml=%s, home_spread=%s,
                vegas_prob_home=%s, odds_fetched_at=%s
            WHERE id=%s
            """,
            (market["vegas_total"], market["home_ml"], market["away_ml"],
             market["home_spread"], home_prob, captured_at, matchup["id"]),
        )
        history_rows.append({
            "sport": "nhl",
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
            "home_spread": market["home_spread"],
            "vegas_total": market["vegas_total"],
            "vegas_prob_home": home_prob,
            "capture_key": capture_key,
            "captured_at": captured_at,
            "books": market["books"],
            "vegas_total_raw": market["vegas_total_raw"],
        })
    inserted = insert_game_odds_history_rows(db, history_rows)
    print(f"NHL odds: {inserted} pregame event captures written ({len(mapped)} mapped upcoming)")
    return inserted


def ensure_nhl_schema(db: DatabaseManager) -> dict:
    """Lock-light bootstrap for jobs that skip the global schema pass.

    The shared close worker runs with ``--existing-schema`` yet seeds NHL
    checkpoints from ``nhl_matchups`` and writes ``sport='nhl'`` rows under two
    CHECK constraints. Its first run after a deploy must not depend on another
    job having applied ``db/schema.py`` first. When everything is present this
    is one catalog read; DDL runs only for what is missing.
    """
    state = db.execute_one(
        """
        SELECT to_regclass('public.nhl_teams') IS NOT NULL
               AND to_regclass('public.nhl_matchups') IS NOT NULL
               AND to_regclass('public.nhl_unmapped_events') IS NOT NULL AS tables_present,
               (SELECT COUNT(*) FROM pg_constraint
                 WHERE conname IN ('odds_capture_checkpoints_sport_check',
                                   'odds_capture_checkpoints_checkpoint_check',
                                   'event_closing_lines_sport_check')
                   AND position('nhl' IN pg_get_constraintdef(oid)) > 0)::int AS nhl_constraints
        """
    ) or {}
    applied: list[str] = []
    if not state.get("tables_present"):
        with db.connect() as connection:
            cursor = connection.cursor()
            for ddl in (*NHL_TABLES, *NHL_INDEXES):
                cursor.execute(ddl)
        applied.append("tables")
    if int(state.get("nhl_constraints") or 0) < 3:
        with db.connect() as connection:  # drop + re-add atomically
            cursor = connection.cursor()
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            for ddl in CLOSE_CAPTURE_CONSTRAINT_DDLS:
                cursor.execute(ddl)
        applied.append("close_capture_constraints")
    return {"applied": applied}


def collect_data_health(db: DatabaseManager) -> dict:
    row = db.execute_one(
        """
        SELECT
          COUNT(*) FILTER (
            WHERE completed=FALSE AND COALESCE(schedule_state, 'OK')='OK'
              AND commence_time > NOW() AND commence_time <= NOW() + INTERVAL '48 hours'
              AND odds_event_id IS NULL
          )::int AS unmapped_upcoming,
          (SELECT COUNT(*) FROM nhl_unmapped_events WHERE resolved_at IS NULL)::int AS quarantined,
          (SELECT COUNT(*) FROM game_odds_history h JOIN nhl_matchups m ON m.id=h.matchup_id
             WHERE h.sport='nhl' AND h.captured_at >= m.commence_time)::int AS post_start,
          COUNT(*) FILTER (
            WHERE completed=TRUE AND (home_score IS NULL OR away_score IS NULL)
          )::int AS missing_final_score,
          COUNT(*) FILTER (
            WHERE completed=FALSE AND COALESCE(schedule_state, 'OK')='OK'
              AND commence_time < NOW() - INTERVAL '8 hours'
          )::int AS overdue_final,
          COUNT(*) FILTER (WHERE commence_time > NOW())::int AS upcoming
        FROM nhl_matchups
        """
    ) or {}
    result = {key: int(row.get(key) or 0) for key in (
        "unmapped_upcoming", "quarantined", "post_start", "missing_final_score",
        "overdue_final", "upcoming",
    )}
    # Integrity failures are "fail"; coverage gaps are "warn". An empty
    # upcoming schedule mid-season means the schedule refresh wrote nothing.
    if result["post_start"] or result["missing_final_score"]:
        result["status"] = "fail"
    elif result["unmapped_upcoming"] or result["quarantined"] or result["overdue_final"]:
        result["status"] = "warn"
    else:
        result["status"] = "pass"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh the canonical NHL schedule and market mappings")
    parser.add_argument("--refresh-schedule", action="store_true")
    parser.add_argument("--start", type=date.fromisoformat, help="first Eastern date (default: today)")
    parser.add_argument("--days", type=int, default=SCHEDULE_DAYS)
    parser.add_argument("--refresh-scores", action="store_true")
    parser.add_argument("--refresh-events", action="store_true")
    parser.add_argument("--capture-now", action="store_true",
                        help="one paid bulk odds call (3 credits), outside the checkpoint worker")
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--ensure-schema", action="store_true",
                        help="bootstrap NHL tables/constraints only; skips the global schema pass")
    args = parser.parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url, initialize_schema=not args.ensure_schema)
    status = 0
    with db.reuse_connection():
        if args.ensure_schema:
            print(json.dumps(ensure_nhl_schema(db)))
        if args.refresh_schedule:
            fetch_schedule(db, start=args.start or datetime.now(EASTERN).date(), days=args.days)
        if args.refresh_scores:
            refresh_recent_scores(db)
        if args.refresh_events:
            fetch_events(db, config.odds_api.api_key)
        if args.capture_now:
            audit: dict = {}
            fetch_odds(db, config.odds_api.api_key, request_audit=audit)
            print(json.dumps({"quota": audit}, default=str))
        if args.health:
            health = collect_data_health(db)
            print(json.dumps(health, indent=2))
            status = 1 if health["status"] == "fail" else 0
    return status


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
