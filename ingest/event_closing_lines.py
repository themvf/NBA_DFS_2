"""Event-driven, quality-graded closing lines for MLB, Tennis, NFL, and CFB.

This worker is cheap to poll: it seeds durable checkpoint rows from schedules
and calls The Odds API only when one or more events are actually due. Existing
game_odds_history rows remain the source of truth for the captured prices.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.cfb_schedule import fetch_odds as fetch_cfb_odds
from ingest.mlb_schedule import fetch_odds as fetch_mlb_odds
from ingest.nfl_schedule import fetch_events as fetch_nfl_events, fetch_odds as fetch_nfl_odds
from ingest.tennis_schedule import discover_tournaments, fetch_tournament

logger = logging.getLogger(__name__)

MARKETS = "h2h,totals,spreads"
# Ten or fewer explicit books are billed as one region by The Odds API.
BOOKMAKERS = (
    "draftkings,fanduel,betmgm,fanatics,caesars,betrivers,"
    "betonlineag,pinnacle"
)
VERIFIED_CLV_START_AT = datetime(2026, 8, 31, 4, 0, tzinfo=timezone.utc)
CLV_METHODOLOGY_VERSION = "event-close-v1"
VERIFIED_CLV_COHORT = "verified_clv_v1"
EASTERN = ZoneInfo("America/New_York")
CORE_CHECKPOINTS = (
    ("t_minus_6h", 360, 330),
    ("t_minus_90m", 90, 60),
    ("t_minus_15m", 15, 5),
    ("t_minus_2m", 2, 0),
)
CHECKPOINTS_BY_SPORT = {
    "mlb": CORE_CHECKPOINTS,
    "tennis": CORE_CHECKPOINTS,
    "cfb": (
        ("t_minus_48h", 48 * 60, 42 * 60),
        ("t_minus_24h", 24 * 60, 20 * 60),
        *CORE_CHECKPOINTS,
        # Hourly on game day, then 15-minute observations for the final six
        # hours. Durable windows tolerate scheduler delay; missed slots remain
        # missed rather than being backfilled with a later quote.
        *((f"cfb_t_minus_{lead}m", lead, lead - 60) for lead in range(720, 360, -60)),
        *((f"cfb_t_minus_{lead}m", lead, lead - 15)
          for lead in range(345, 15, -15) if lead != 90),
        ("closing_candidate", 5, 0),
    ),
}
DAILY_CREDIT_CAP = int(os.getenv("ODDS_CLOSE_DAILY_CREDIT_CAP", "240"))
MIN_REMAINING_RESERVE = int(os.getenv("ODDS_CLOSE_MIN_REMAINING", "250"))
ESTIMATED_GROUP_COST = 3  # three markets x one <=10-book group
NFL_STANDARD_WINDOW_MINUTES = 20


def _as_utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def nfl_checkpoint_schedule(scheduled_start: datetime | str) -> list[dict]:
    """Build the frozen ET calendar cadence for one NFL kickoff.

    Calendar-day slots express the requested D-3/D-2/D-1/game-day cadence.
    The final three jobs are event-relative. Each target has a durable window
    so a five-minute scheduler does not need to fire on the exact minute.
    """
    kickoff = _as_utc(scheduled_start)
    kickoff_et = kickoff.astimezone(EASTERN)
    jobs: list[dict] = []

    def add(name: str, target: datetime, due_until: datetime | None = None) -> None:
        target_utc = _as_utc(target)
        due_utc = _as_utc(due_until or (target_utc + timedelta(minutes=NFL_STANDARD_WINDOW_MINUTES)))
        if target_utc < kickoff:
            jobs.append({
                "checkpoint": name,
                "target_at": target_utc,
                "due_until": min(due_utc, kickoff),
            })

    for days_before, hours_between in ((3, 6), (2, 6), (1, 3)):
        local_date = kickoff_et.date() - timedelta(days=days_before)
        for hour in range(0, 24, hours_between):
            local_target = datetime.combine(local_date, time(hour=hour), tzinfo=EASTERN)
            add(f"d_minus_{days_before}_{hour:02d}", local_target)

    # Game-day hourly captures end at least 60 minutes before kickoff. The
    # event-relative 30m/15m/close-candidate jobs own the final hour.
    for hour in range(0, 24):
        local_target = datetime.combine(kickoff_et.date(), time(hour=hour), tzinfo=EASTERN)
        if _as_utc(local_target) <= kickoff - timedelta(hours=1):
            add(f"game_day_{hour:02d}", local_target)

    add("t_minus_30m", kickoff - timedelta(minutes=30), kickoff - timedelta(minutes=20))
    add("t_minus_15m", kickoff - timedelta(minutes=15), kickoff - timedelta(minutes=5))
    add("closing_candidate", kickoff - timedelta(minutes=5), kickoff)
    return jobs


def _seed_nfl_checkpoints(db: DatabaseManager, now: datetime) -> int:
    events = db.execute(
        """
        SELECT id AS matchup_id, event_id, commence_time AS scheduled_start_at
        FROM nfl_matchups
        WHERE event_id IS NOT NULL
          AND commence_time BETWEEN %s - INTERVAL '8 hours' AND %s + INTERVAL '4 days'
          AND completed = FALSE
          AND COALESCE(game_status, '') NOT IN ('Postponed', 'Cancelled')
        """,
        (now, now),
    )
    values: list[tuple] = []
    for event in events:
        for job in nfl_checkpoint_schedule(event["scheduled_start_at"]):
            values.append((
                "nfl", event["matchup_id"], str(event["event_id"]), job["checkpoint"],
                event["scheduled_start_at"], job["target_at"], job["due_until"],
            ))
    if not values:
        return 0
    placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s)"] * len(values))
    params = tuple(item for row in values for item in row)
    inserted = db.execute(
        f"""
        INSERT INTO odds_capture_checkpoints (
            sport, matchup_id, event_id, checkpoint, scheduled_start_at, target_at, due_until
        ) VALUES {placeholders}
        ON CONFLICT (sport, matchup_id, checkpoint, scheduled_start_at) DO NOTHING
        RETURNING id
        """,
        params,
    )
    return len(inserted)


def close_quality(lead_seconds: int) -> str:
    if lead_seconds <= 5 * 60:
        return "A"
    if lead_seconds <= 15 * 60:
        return "B"
    if lead_seconds <= 30 * 60:
        return "C"
    return "stale"


def classify_clv_cohort(
    *, scheduled_start: datetime, quality: str, boundary_source: str,
) -> dict[str, str | bool | datetime]:
    primary = _as_utc(scheduled_start) >= VERIFIED_CLV_START_AT and quality in {"A", "B", "C"}
    return {
        "methodology_version": CLV_METHODOLOGY_VERSION,
        "clv_cohort": VERIFIED_CLV_COHORT if primary else "non_primary",
        "verification_level": (
            "actual_start"
            if boundary_source in {"mlb_first_pitch", "mlb_first_play"}
            else "scheduled_boundary"
        ),
        "cohort_started_at": VERIFIED_CLV_START_AT,
        "primary_clv_eligible": primary,
    }


def parse_mlb_actual_start(payload: dict[str, Any]) -> tuple[datetime | None, str | None, dict]:
    """Extract the first-pitch boundary from one official MLB live-feed payload."""
    game_data = payload.get("gameData") or {}
    status = game_data.get("status") or {}
    abstract_state = str(status.get("abstractGameState") or "")
    detailed_state = str(status.get("detailedState") or "")
    evidence = {"abstract_state": abstract_state, "detailed_state": detailed_state}

    first_pitch = (game_data.get("datetime") or {}).get("firstPitch")
    if first_pitch:
        try:
            return _as_utc(first_pitch), "mlb_first_pitch", {**evidence, "first_pitch": first_pitch}
        except ValueError:
            pass

    plays = ((payload.get("liveData") or {}).get("plays") or {}).get("allPlays") or []
    if plays:
        first_play = (plays[0].get("about") or {}).get("startTime")
        if first_play:
            try:
                return _as_utc(first_play), "mlb_first_play", {**evidence, "first_play": first_play}
            except ValueError:
                pass

    # A preview or delayed game has not crossed a verified boundary. Even for
    # a malformed live/final feed, fail closed instead of inventing first pitch.
    return None, None, evidence


def fetch_mlb_actual_start(game_id: str) -> tuple[datetime | None, str | None, dict]:
    try:
        response = requests.get(
            f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live",
            timeout=20,
        )
        response.raise_for_status()
        return parse_mlb_actual_start(response.json() or {})
    except (requests.RequestException, ValueError) as exc:
        return None, None, {"error": str(exc)}


def seed_checkpoints(db: DatabaseManager, now: datetime | None = None) -> int:
    """Create checkpoint jobs for events with a known provider event id."""
    now = now or datetime.now(timezone.utc)
    inserted = 0
    sources = {
        "mlb": """
            SELECT m.id AS matchup_id, h.event_id, m.commence_time AS scheduled_start_at
            FROM mlb_matchups m
            JOIN LATERAL (
                SELECT event_id FROM game_odds_history
                WHERE sport='mlb' AND matchup_id=m.id AND event_id IS NOT NULL
                ORDER BY captured_at DESC LIMIT 1
            ) h ON TRUE
            WHERE m.commence_time BETWEEN %s - INTERVAL '8 hours' AND %s + INTERVAL '36 hours'
              AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
        """,
        "tennis": """
            SELECT id AS matchup_id, game_id AS event_id, commence_time AS scheduled_start_at
            FROM tennis_matches
            WHERE game_id IS NOT NULL
              AND commence_time BETWEEN %s - INTERVAL '8 hours' AND %s + INTERVAL '36 hours'
              AND completion_status = 'scheduled'
        """,
        "cfb": """
            SELECT id AS matchup_id, odds_event_id AS event_id,
                   commence_time AS scheduled_start_at
            FROM cfb_matchups
            WHERE odds_event_id IS NOT NULL
              AND commence_time BETWEEN %s - INTERVAL '8 hours' AND %s + INTERVAL '54 hours'
              AND completed = FALSE AND start_time_tbd = FALSE
        """,
    }
    for sport, source_sql in sources.items():
        values_sql = ", ".join(
            f"('{name}', {target_lead}, {due_lead})"
            for name, target_lead, due_lead in CHECKPOINTS_BY_SPORT[sport]
        )
        rows = db.execute(
            f"""
            INSERT INTO odds_capture_checkpoints (
                sport, matchup_id, event_id, checkpoint, scheduled_start_at,
                target_at, due_until
            )
            SELECT %s, e.matchup_id, e.event_id::text, w.checkpoint,
                   e.scheduled_start_at,
                   e.scheduled_start_at - w.target_lead * INTERVAL '1 minute',
                   e.scheduled_start_at - w.due_lead * INTERVAL '1 minute'
            FROM ({source_sql}) e
            CROSS JOIN (VALUES {values_sql}) AS w(checkpoint, target_lead, due_lead)
            ON CONFLICT (sport, matchup_id, checkpoint, scheduled_start_at) DO NOTHING
            RETURNING id
            """,
            (sport, now, now),
        )
        inserted += len(rows)
    return inserted + _seed_nfl_checkpoints(db, now)


def reconcile_checkpoints(db: DatabaseManager, now: datetime | None = None) -> int:
    """Attach already-recorded history rows and expire windows that were missed."""
    now = now or datetime.now(timezone.utc)
    # Provider event refreshes update nfl_matchups.commence_time in place. Old
    # schedule jobs remain as audit evidence but must never become due after a
    # kickoff change; the newly seeded scheduled_start_at owns the cadence.
    db.execute(
        """
        UPDATE odds_capture_checkpoints c
        SET status='missed', failure_reason='superseded by kickoff reschedule'
        FROM nfl_matchups m
        WHERE c.sport='nfl' AND m.id=c.matchup_id
          AND c.status IN ('pending', 'attempted', 'failed')
          AND c.scheduled_start_at IS DISTINCT FROM m.commence_time
        """
    )
    db.execute(
        """UPDATE odds_capture_checkpoints c
           SET status='missed', failure_reason='superseded by kickoff reschedule'
           FROM cfb_matchups m
           WHERE c.sport='cfb' AND m.id=c.matchup_id
             AND c.status IN ('pending', 'attempted', 'failed')
             AND c.scheduled_start_at IS DISTINCT FROM m.commence_time"""
    )
    captured = db.execute(
        """
        WITH candidates AS (
            SELECT DISTINCT ON (c.id) c.id AS checkpoint_id,
                   h.id AS history_id, h.captured_at
            FROM odds_capture_checkpoints c
            JOIN game_odds_history h
              ON h.sport=c.sport AND h.matchup_id=c.matchup_id
             AND h.captured_at BETWEEN c.target_at AND c.due_until
             AND COALESCE(h.books, '{}'::jsonb) - 'polymarket' <> '{}'::jsonb
            WHERE c.status IN ('pending', 'attempted', 'failed')
            ORDER BY c.id, h.captured_at DESC, h.id DESC
        )
        UPDATE odds_capture_checkpoints c
        SET status='captured', captured_at=x.captured_at, history_id=x.history_id,
            failure_reason=NULL
        FROM candidates x
        WHERE c.id=x.checkpoint_id
        RETURNING c.id
        """
    )
    db.execute(
        """
        UPDATE odds_capture_checkpoints
        SET status='missed', failure_reason=COALESCE(failure_reason, 'checkpoint window elapsed')
        WHERE status IN ('pending', 'attempted', 'failed') AND due_until < %s
        """,
        (now,),
    )
    return len(captured)


def due_checkpoints(db: DatabaseManager, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(timezone.utc)
    return [dict(row) for row in db.execute(
        """
        SELECT c.*, CASE WHEN c.sport='tennis' THEN tm.tour END AS tour,
               CASE WHEN c.sport='tennis' THEN tm.tournament END AS tournament,
               CASE WHEN c.sport='nfl' THEN nm.season_type END AS season_type
        FROM odds_capture_checkpoints c
        LEFT JOIN tennis_matches tm ON c.sport='tennis' AND tm.id=c.matchup_id
        LEFT JOIN nfl_matchups nm ON c.sport='nfl' AND nm.id=c.matchup_id
        WHERE c.status IN ('pending', 'attempted', 'failed')
          AND c.target_at <= %s AND c.due_until >= %s
          AND c.scheduled_start_at > %s
        ORDER BY c.sport, c.scheduled_start_at, c.matchup_id
        """,
        (now, now, now),
    )]


def _audit_usage(
    db: DatabaseManager, *, sport: str, event_count: int, audit: dict, metadata: dict,
) -> None:
    def number(name: str) -> int | None:
        try:
            return int(audit.get(name))
        except (TypeError, ValueError):
            return None

    db.execute(
        """
        INSERT INTO odds_api_usage (
            sport, purpose, endpoint, event_count, markets, bookmakers,
            requests_last, requests_used, requests_remaining, response_status, metadata
        ) VALUES (%s, 'closing_checkpoint', %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            sport, audit.get("endpoint") or "unknown", event_count, MARKETS, BOOKMAKERS,
            number("requests_last"), number("requests_used"), number("requests_remaining"),
            number("status"), json.dumps(metadata, default=str),
        ),
    )


def quota_allows(db: DatabaseManager, estimated_cost: int = ESTIMATED_GROUP_COST) -> tuple[bool, str | None]:
    row = db.execute_one(
        """
        SELECT COALESCE(SUM(requests_last), 0)::int AS credits_today,
               (SELECT requests_remaining FROM odds_api_usage
                WHERE requests_remaining IS NOT NULL
                ORDER BY requested_at DESC, id DESC LIMIT 1) AS remaining
        FROM odds_api_usage
        WHERE purpose='closing_checkpoint'
          AND requested_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'America/New_York')
                            AT TIME ZONE 'America/New_York'
        """
    ) or {}
    credits_today = int(row.get("credits_today") or 0)
    remaining = row.get("remaining")
    if credits_today + estimated_cost > DAILY_CREDIT_CAP:
        return False, f"daily close-capture cap {DAILY_CREDIT_CAP} credits"
    if remaining is not None and int(remaining) - estimated_cost < MIN_REMAINING_RESERVE:
        return False, f"monthly reserve {MIN_REMAINING_RESERVE} credits"
    return True, None


def _mark_attempt(db: DatabaseManager, jobs: list[dict], now: datetime) -> None:
    for job in jobs:
        db.execute(
            "UPDATE odds_capture_checkpoints SET status='attempted', attempted_at=%s WHERE id=%s",
            (now, job["id"]),
        )


def _mark_failure(db: DatabaseManager, jobs: list[dict], reason: str) -> None:
    for job in jobs:
        db.execute(
            "UPDATE odds_capture_checkpoints SET status='failed', failure_reason=%s WHERE id=%s",
            (reason[:1000], job["id"]),
        )


def _norm(value: str | None) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def _tennis_provider_key(job: dict, tournaments: list[tuple[str, str, str]]) -> tuple[str, str] | None:
    wanted_tour = str(job.get("tour") or "").upper()
    wanted_title = _norm(job.get("tournament"))
    candidates = [item for item in tournaments if item[0] == wanted_tour]
    exact = [item for item in candidates if _norm(item[2]) == wanted_title]
    if exact:
        return exact[0][1], exact[0][2]
    contains = [item for item in candidates if wanted_title and (
        wanted_title in _norm(item[2]) or _norm(item[2]) in wanted_title
    )]
    return (contains[0][1], contains[0][2]) if len(contains) == 1 else None


def capture_due_checkpoints(
    db: DatabaseManager, api_key: str, *, now: datetime | None = None, dry_run: bool = False,
) -> dict:
    now = now or datetime.now(timezone.utc)
    if not dry_run:
        seed_checkpoints(db, now)
        reconcile_checkpoints(db, now)
    due = due_checkpoints(db, now)
    result = {
        "due": len(due), "groups": 0, "paid_requests": 0,
        "quota_deferred": 0, "dry_run": dry_run,
    }
    if not due or dry_run:
        return result

    mlb_groups: dict[str, list[dict]] = defaultdict(list)
    tennis_jobs: list[dict] = []
    cfb_jobs: list[dict] = []
    nfl_groups: dict[str, list[dict]] = defaultdict(list)
    for job in due:
        if job["sport"] == "mlb":
            mlb_groups[_as_utc(job["scheduled_start_at"]).date().isoformat()].append(job)
        elif job["sport"] == "tennis":
            tennis_jobs.append(job)
        elif job["sport"] == "cfb":
            cfb_jobs.append(job)
        elif job["sport"] == "nfl":
            nfl_groups[str(job.get("season_type") or "regular")].append(job)

    for game_date, jobs in mlb_groups.items():
        allowed, reason = quota_allows(db)
        if not allowed:
            _mark_failure(db, jobs, f"quota deferred: {reason}")
            result["quota_deferred"] += len(jobs)
            continue
        event_ids = sorted({str(job["event_id"]) for job in jobs})
        _mark_attempt(db, jobs, now)
        audit: dict = {}
        updated = fetch_mlb_odds(
            db, api_key, game_date, event_ids=event_ids, bookmakers=BOOKMAKERS,
            markets=MARKETS, request_audit=audit,
        )
        _audit_usage(db, sport="mlb", event_count=len(event_ids), audit=audit,
                     metadata={"game_date": game_date, "event_ids": event_ids})
        result["groups"] += 1
        result["paid_requests"] += 1
        if updated == 0:
            _mark_failure(db, jobs, "provider returned no accepted prestart events")

    if tennis_jobs:
        tournaments = discover_tournaments(api_key)
        grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
        unresolved: list[dict] = []
        for job in tennis_jobs:
            provider = _tennis_provider_key(job, tournaments)
            if provider is None:
                unresolved.append(job)
            else:
                grouped[(str(job["tour"]), provider[0], provider[1])].append(job)
        if unresolved:
            _mark_failure(db, unresolved, "active provider tournament key not resolved")
        for (tour, sport_key, title), jobs in grouped.items():
            allowed, reason = quota_allows(db)
            if not allowed:
                _mark_failure(db, jobs, f"quota deferred: {reason}")
                result["quota_deferred"] += len(jobs)
                continue
            event_ids = sorted({str(job["event_id"]) for job in jobs})
            _mark_attempt(db, jobs, now)
            audit = {}
            updated = fetch_tournament(
                db, api_key, tour, sport_key, title, game_date=None,
                event_ids=event_ids, bookmakers=BOOKMAKERS, markets=MARKETS,
                request_audit=audit,
            )
            _audit_usage(db, sport="tennis", event_count=len(event_ids), audit=audit,
                         metadata={"sport_key": sport_key, "event_ids": event_ids})
            result["groups"] += 1
            result["paid_requests"] += 1
            if updated == 0:
                _mark_failure(db, jobs, "provider returned no accepted prestart events")

    if cfb_jobs:
        allowed, reason = quota_allows(db)
        if not allowed:
            _mark_failure(db, cfb_jobs, f"quota deferred: {reason}")
            result["quota_deferred"] += len(cfb_jobs)
        else:
            event_ids = {str(job["event_id"]) for job in cfb_jobs}
            _mark_attempt(db, cfb_jobs, now)
            audit = {}
            updated = fetch_cfb_odds(
                db, api_key, event_ids=event_ids, refresh_events=False,
                request_audit=audit,
            )
            _audit_usage(
                db, sport="cfb", event_count=len(event_ids), audit=audit,
                metadata={"event_ids": sorted(event_ids), "cadence_version": "cfb-dense-v1",
                          "daily_credit_cap": DAILY_CREDIT_CAP},
            )
            result["groups"] += 1
            result["paid_requests"] += 1
            if updated == 0:
                _mark_failure(db, cfb_jobs, "provider returned no accepted prestart events")

    for season_type, jobs in nfl_groups.items():
        allowed, reason = quota_allows(db)
        if not allowed:
            _mark_failure(db, jobs, f"quota deferred: {reason}")
            result["quota_deferred"] += len(jobs)
            continue
        event_ids = {str(job["event_id"]) for job in jobs}
        _mark_attempt(db, jobs, now)
        audit = {}
        try:
            updated = fetch_nfl_odds(
                db, api_key, event_ids=event_ids, refresh_events=False,
                bookmakers=BOOKMAKERS, markets=MARKETS, request_audit=audit,
            )
        except requests.RequestException as exc:
            response = exc.response
            if response is not None:
                audit.update({
                    "endpoint": str(response.url).split("?", 1)[0],
                    "status": response.status_code,
                    "requests_last": response.headers.get("x-requests-last"),
                    "requests_used": response.headers.get("x-requests-used"),
                    "requests_remaining": response.headers.get("x-requests-remaining"),
                    "request_count": 1,
                })
            _audit_usage(
                db, sport="nfl", event_count=len(event_ids), audit=audit,
                metadata={"season_type": season_type, "event_ids": sorted(event_ids),
                          "error": str(exc)},
            )
            _mark_failure(db, jobs, f"provider request failed: {exc}")
            result["groups"] += 1
            result["paid_requests"] += int(audit.get("request_count") or 0)
            continue
        _audit_usage(
            db, sport="nfl", event_count=len(event_ids), audit=audit,
            metadata={"season_type": season_type, "event_ids": sorted(event_ids)},
        )
        result["groups"] += 1
        result["paid_requests"] += int(audit.get("request_count") or 1)
        if updated == 0:
            _mark_failure(db, jobs, "provider returned no accepted prestart events")

    reconcile_checkpoints(db, datetime.now(timezone.utc))
    return result


def _eligible_book_updates(books: dict | None, boundary: datetime) -> list[str]:
    eligible: list[str] = []
    for key, quote in (books or {}).items():
        if key == "polymarket" or not isinstance(quote, dict):
            continue
        updated = quote.get("last_update")
        if not updated:
            eligible.append(key)
            continue
        try:
            if _as_utc(updated) < boundary:
                eligible.append(key)
        except ValueError:
            continue
    return eligible


def _freeze_one(
    db: DatabaseManager, *, sport: str, matchup_id: int, event_id: str | None,
    scheduled_start: datetime, actual_start: datetime | None, boundary: datetime,
    boundary_source: str, evidence: dict,
) -> bool:
    histories = db.execute(
        """
        SELECT id, captured_at, books
        FROM game_odds_history
        WHERE sport=%s AND matchup_id=%s AND captured_at < %s
          AND COALESCE(books, '{}'::jsonb) - 'polymarket' <> '{}'::jsonb
        ORDER BY captured_at DESC, id DESC LIMIT 20
        """,
        (sport, matchup_id, boundary),
    )
    selected = None
    eligible_books: list[str] = []
    for row in histories:
        eligible_books = _eligible_book_updates(row.get("books"), boundary)
        if eligible_books:
            selected = row
            break
    if selected is None:
        return False
    captured = _as_utc(selected["captured_at"])
    lead_seconds = max(0, int((boundary - captured).total_seconds()))
    quality = close_quality(lead_seconds)
    cohort = classify_clv_cohort(
        scheduled_start=scheduled_start, quality=quality, boundary_source=boundary_source,
    )
    verification = {**evidence, "eligible_bookmakers": eligible_books}
    inserted = db.execute_one(
        """
        INSERT INTO event_closing_lines (
            sport, matchup_id, event_id, scheduled_start_at, actual_start_at,
            boundary_at, boundary_source, history_id, captured_at, lead_seconds,
            quality, methodology_version, clv_cohort, verification_level,
            cohort_started_at, primary_clv_eligible, verification_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s::jsonb
        )
        ON CONFLICT (sport, matchup_id) DO NOTHING
        RETURNING id
        """,
        (
            sport, matchup_id, event_id, scheduled_start, actual_start, boundary,
            boundary_source, selected["id"], captured, lead_seconds,
            quality, cohort["methodology_version"], cohort["clv_cohort"],
            cohort["verification_level"], cohort["cohort_started_at"],
            cohort["primary_clv_eligible"], json.dumps(verification, default=str),
        ),
    )
    return bool(inserted)


def freeze_due_closes(db: DatabaseManager, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    rows = db.execute(
        """
        SELECT 'mlb' AS sport, m.id AS matchup_id, m.game_id AS official_game_id,
               h.event_id, m.commence_time AS scheduled_start_at
        FROM mlb_matchups m
        LEFT JOIN LATERAL (
            SELECT event_id FROM game_odds_history
            WHERE sport='mlb' AND matchup_id=m.id AND event_id IS NOT NULL
            ORDER BY captured_at DESC LIMIT 1
        ) h ON TRUE
        WHERE m.commence_time <= %s AND m.commence_time >= %s - INTERVAL '12 hours'
          AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
          AND NOT EXISTS (SELECT 1 FROM event_closing_lines c WHERE c.sport='mlb' AND c.matchup_id=m.id)
        UNION ALL
        SELECT 'tennis', t.id, NULL, t.game_id, t.commence_time
        FROM tennis_matches t
        WHERE t.commence_time <= %s AND t.commence_time >= %s - INTERVAL '12 hours'
          AND NOT EXISTS (SELECT 1 FROM event_closing_lines c WHERE c.sport='tennis' AND c.matchup_id=t.id)
        UNION ALL
        SELECT 'cfb', m.id, NULL, m.odds_event_id, m.commence_time
        FROM cfb_matchups m
        WHERE m.commence_time <= %s AND m.commence_time >= %s - INTERVAL '12 hours'
          AND m.completed = FALSE AND m.start_time_tbd = FALSE
          AND NOT EXISTS (SELECT 1 FROM event_closing_lines c WHERE c.sport='cfb' AND c.matchup_id=m.id)
        UNION ALL
        SELECT 'nfl', m.id, NULL, m.event_id, m.commence_time
        FROM nfl_matchups m
        WHERE m.commence_time <= %s AND m.commence_time >= %s - INTERVAL '12 hours'
          AND COALESCE(m.game_status, '') NOT IN ('Postponed', 'Cancelled')
          AND NOT EXISTS (SELECT 1 FROM event_closing_lines c WHERE c.sport='nfl' AND c.matchup_id=m.id)
        """,
        (now, now, now, now, now, now, now, now),
    )
    result = {"eligible": len(rows), "frozen": 0, "awaiting_boundary": 0, "missing_history": 0}
    for row in rows:
        scheduled = _as_utc(row["scheduled_start_at"])
        if row["sport"] == "mlb":
            actual, source, evidence = fetch_mlb_actual_start(str(row["official_game_id"]))
            if actual is None or source is None:
                result["awaiting_boundary"] += 1
                continue
            boundary = actual
        elif row["sport"] == "tennis":
            actual = None
            boundary = scheduled
            source = "scheduled_provider"
            evidence = {"limitation": "no verified point-level first-serve timestamp"}
        elif row["sport"] == "cfb":
            actual = None
            boundary = scheduled
            source = "scheduled_cfbd"
            evidence = {"limitation": "no verified play-level CFB kickoff timestamp"}
        else:
            actual = None
            boundary = scheduled
            source = "scheduled_nfl"
            evidence = {"limitation": "no verified play-level NFL kickoff timestamp"}
        if _freeze_one(
            db, sport=row["sport"], matchup_id=int(row["matchup_id"]),
            event_id=row.get("event_id"), scheduled_start=scheduled,
            actual_start=actual, boundary=boundary, boundary_source=source,
            evidence=evidence,
        ):
            result["frozen"] += 1
        else:
            result["missing_history"] += 1
    return result


def health_report(db: DatabaseManager) -> dict:
    rows = db.execute(
        """
        SELECT sport, clv_cohort, verification_level, quality, COUNT(*)::int AS n,
               ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lead_seconds) / 60.0)::numeric, 1) AS median_lead_min,
               ROUND((PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY lead_seconds) / 60.0)::numeric, 1) AS p90_lead_min
        FROM event_closing_lines
        GROUP BY sport, clv_cohort, verification_level, quality
        ORDER BY sport, clv_cohort, verification_level, quality
        """
    )
    checkpoints = db.execute(
        """
        SELECT sport, status, COUNT(*)::int AS n
        FROM odds_capture_checkpoints GROUP BY sport, status ORDER BY sport, status
        """
    )
    return {"closes": [dict(row) for row in rows], "checkpoints": [dict(row) for row in checkpoints]}


def run(db: DatabaseManager, api_key: str, *, dry_run: bool = False) -> dict:
    # The previous scheduled NFL capture discovered only the current game
    # date. Refreshing the provider's free event list here is required before
    # D-3 checkpoints can be seeded. It does not write an odds observation.
    if not dry_run:
        try:
            fetch_nfl_events(db, api_key)
        except requests.RequestException as exc:
            # Existing mapped games and due checkpoints remain usable. Most
            # importantly, an event-list outage cannot block close freezing.
            logger.warning("NFL event discovery failed; continuing with mapped games: %s", exc)
    captures = capture_due_checkpoints(db, api_key, dry_run=dry_run)
    closes = {"skipped": "dry_run"} if dry_run else freeze_due_closes(db)
    return {"captures": captures, "closes": closes, "health": health_report(db)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Capture and freeze MLB/Tennis/NFL/CFB closing lines")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--health-only", action="store_true")
    args = parser.parse_args()
    config = load_config()
    database = DatabaseManager(config.database_url)
    with database.reuse_connection():
        output = health_report(database) if args.health_only else run(
            database, config.odds_api.api_key, dry_run=args.dry_run,
        )
    print(json.dumps(output, indent=2, default=str))
