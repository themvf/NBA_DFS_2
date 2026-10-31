"""Bounded, per-game NBA points captures at T-5 and T-1. Never certified closes.

Free discovery in dry run; --apply authorizes at most two main-market calls per
event/day. A durable ledger reserves each attempt before the request, including
failures. A process lock prevents overlapping local workers. No schema changes.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import time

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.nba_points_props import (
    ODDS_BASE, SPORT, BOOKMAKER_KEYS, fetch_events, eastern_date, persist_rows,
    _header_int,
)
from model.nba_points_ladder import team_key

CHECKPOINTS = (("t_minus_5m", 300, 120), ("t_minus_1m", 60, 5))


def utcnow():
    return datetime.now(timezone.utc)


def aware(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("naive timestamp")
    return result.astimezone(timezone.utc)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


@contextmanager
def worker_lock(path):
    """OS releases the lock after a crash; the durable attempt ledger survives."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def official_games(payload, target):
    games = []
    for event in payload.get("events", []):
        for competition in event.get("competitions", []):
            teams = {c["homeAway"]: c["team"]["displayName"]
                     for c in competition.get("competitors", [])}
            if set(teams) != {"home", "away"}:
                continue
            start = aware(competition.get("date", event["date"]))
            if eastern_date(start) != target:
                continue
            games.append({"id": event["id"], "home": teams["home"],
                          "away": teams["away"], "start": start.isoformat(),
                          "state": competition.get("status", event.get("status", {}))
                          .get("type", {}).get("state"),
                          "status": competition.get("status", event.get("status", {}))
                          .get("type", {}).get("name")})
    return games


def game_cutoff(event, games):
    matches = [g for g in games if team_key(g["home"]) == team_key(event.get("home_team"))
               and team_key(g["away"]) == team_key(event.get("away_team"))]
    if len(matches) != 1:
        return None, "missing_or_ambiguous_official_join"
    game = matches[0]
    if game["state"] != "pre" or game["status"] != "STATUS_SCHEDULED":
        return None, "official_game_not_scheduled_pregame"
    return min(aware(event["commence_time"]), aware(game["start"])), None


def due_checkpoints(event, cutoff, now, attempts):
    seconds = (cutoff - now).total_seconds()
    return [(name, f"{event['id']}:{name}") for name, lead, end in CHECKPOINTS
            if end < seconds <= lead and f"{event['id']}:{name}" not in attempts]


def valid_payload(payload, event, cutoff, captured_at):
    """Do not admit live/late responses or silently changed game identities."""
    if captured_at >= cutoff:
        return False, "response_at_or_after_cutoff"
    if str(payload.get("id")) != str(event["id"]):
        return False, "payload_event_mismatch"
    if any(team_key(payload.get(k)) != team_key(event.get(k))
           for k in ("home_team", "away_team")):
        return False, "payload_team_mismatch"
    if not payload.get("commence_time"):
        return False, "payload_missing_start"
    if captured_at >= aware(payload["commence_time"]):
        return False, "payload_start_already_passed"
    for book in payload.get("bookmakers", []):
        for item in [book, *book.get("markets", [])]:
            timestamp = item.get("last_update")
            if timestamp and aware(timestamp) > captured_at:
                return False, "provider_update_after_capture"
    return True, None


def paid_capture(db, api_key, event, cutoff, directory):
    # Recheck immediately before dispatch, after preceding games/network work.
    if utcnow() >= cutoff - timedelta(seconds=5):
        return {"status": "skipped", "reason": "too_late_to_request"}
    response = requests.get(
        f"{ODDS_BASE}/sports/{SPORT}/events/{event['id']}/odds",
        params={"apiKey": api_key, "markets": "player_points",
                "bookmakers": ",".join(BOOKMAKER_KEYS),
                "oddsFormat": "american", "dateFormat": "iso"}, timeout=15)
    captured = utcnow()
    # Do not log request URLs, which include the account key.
    db.execute("""INSERT INTO odds_api_usage
        (sport,purpose,endpoint,event_count,markets,bookmakers,requests_last,
         requests_used,requests_remaining,response_status,metadata)
        VALUES ('nba','nba_points_near_tip',%s,1,'player_points',%s,%s,%s,%s,%s,%s::jsonb)""",
        (f"{ODDS_BASE}/sports/{SPORT}/events/{{id}}/odds", ",".join(BOOKMAKER_KEYS),
         _header_int(response, "x-requests-last"), _header_int(response, "x-requests-used"),
         _header_int(response, "x-requests-remaining"), response.status_code,
         json.dumps({"event_ids": [event["id"]], "capture_kind": "near_tip_not_verified_close"})))
    if response.status_code != 200:
        return {"status": "failed", "http_status": response.status_code}
    payload = response.json()
    raw_path = directory / f"{event['id']}-{captured.strftime('%H%M%S%f')}.json"
    schedule_file = directory / "latest-schedule.json"
    schedule = json.loads(schedule_file.read_text(encoding="utf-8")) if schedule_file.exists() else None
    write_json(raw_path, {"captured_at": captured.isoformat(),
                         "cutoff": cutoff.isoformat(), "data": payload,
                         "schedule_observation": schedule})
    valid, reason = valid_payload(payload, event, cutoff, captured)
    if not valid:
        return {"status": "rejected", "reason": reason, "raw_path": str(raw_path)}
    # Store the conservative official/market cutoff, never a later advertised tip.
    stored_event = {**event, "commence_time": cutoff.isoformat()}
    rows = persist_rows(db, stored_event, payload, captured)
    return {"status": "captured" if rows else "empty", "rows": rows,
            "captured_at": captured.isoformat(), "cutoff": cutoff.isoformat(),
            "seconds_before_tip": (cutoff - captured).total_seconds(),
            "raw_path": str(raw_path), "verified_closing_line": False,
            "credits": _header_int(response, "x-requests-last")}


def run_once(*, target, root, state, api_key, db=None, apply=False, max_credits=40,
             now=None):
    now = now or utcnow()
    events = fetch_events(api_key)  # free listing
    response = requests.get("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
                            params={"dates": target.strftime("%Y%m%d"), "limit": 100}, timeout=15)
    response.raise_for_status()
    payload = response.json()
    games = official_games(payload, target)
    directory = root / target.isoformat()
    write_json(directory / "latest-schedule.json", {"observed_at": utcnow().isoformat(),
               "odds_events": events, "espn_scoreboard": payload})
    plan = []
    cost = math.ceil(len(BOOKMAKER_KEYS) / 10)
    for event in events:
        if eastern_date(aware(event["commence_time"])) != target:
            continue
        cutoff, error = game_cutoff(event, games)
        previous_cutoff = state.get("games", {}).get(event["id"], {}).get("cutoff")
        if cutoff and previous_cutoff:
            # A later advertised start must never extend an earlier observed
            # pregame boundary and accidentally admit an in-play quote.
            cutoff = min(cutoff, aware(previous_cutoff))
        item = {"event_id": event["id"], "home": event.get("home_team"),
                "away": event.get("away_team"), "cutoff": cutoff.isoformat() if cutoff else None,
                "join_error": error}
        plan.append(item)
        if error:
            continue
        due = due_checkpoints(event, cutoff, now, state["attempts"])
        item["due"] = [name for name, _ in due]
        for name, key in due:
            if not apply:
                continue
            if state["reserved_credits"] + cost > max_credits:
                item["budget_blocked"] = True
                continue
            # Persist reservation BEFORE any request; crash/restart cannot double spend.
            state["reserved_credits"] += cost
            state["attempts"][key] = {"status": "reserved", "at": utcnow().isoformat(),
                                       "cutoff": cutoff.isoformat(), "estimated_credits": cost}
            write_json(directory / "state.json", state)
            try:
                result = paid_capture(db, api_key, event, cutoff, directory)
            except Exception as exc:
                # An attempted failed call is not automatically repeated or hidden.
                result = {"status": "failed", "error_type": type(exc).__name__}
            state["attempts"][key].update(result)
            write_json(directory / "state.json", state)
    state.update({"updated_at": utcnow().isoformat(), "plan": plan,
                  "official_game_count": len(games), "worker_pid": os.getpid(),
                  "max_credits": max_credits, "applied": apply})
    known = state.setdefault("games", {})
    for item in plan:
        known[item["event_id"]] = item
    for event_id, item in known.items():
        if item.get("cutoff"):
            remaining = (aware(item["cutoff"]) - utcnow()).total_seconds()
            item["checkpoints"] = {name: state["attempts"].get(
                f"{event_id}:{name}", {}).get("status", "missed" if remaining <= end else "pending")
                for name, _, end in CHECKPOINTS}
    state["official_last_start"] = max((g["start"] for g in games), default=None)
    if apply:
        write_json(directory / "state.json", state)
    write_json(directory / "latest-plan.json", state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date")
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--max-credits", type=int, default=40)
    args = parser.parse_args()
    if args.apply and args.max_credits <= 0:
        parser.error("positive credit ceiling required")
    target = date.fromisoformat(args.date) if args.date else eastern_date(utcnow())
    config = load_config()
    if not config.odds_api.api_key:
        raise ValueError("ODDS_API_KEY missing")
    directory = args.state_dir / target.isoformat()
    with worker_lock(args.state_dir / "worker.lock"):
        file = directory / "state.json"
        state = json.loads(file.read_text(encoding="utf-8")) if file.exists() else {
            "version": "nba-points-near-tip-v1", "date": target.isoformat(),
            "attempts": {}, "reserved_credits": 0}
        db = DatabaseManager(config.database_url, initialize_schema=False) if args.apply else None
        if db:
            db.require_tables(["prop_odds_history", "odds_api_usage"])
        # Stop by 5 AM next Eastern day, or after all known official starts.
        from ingest.nba_points_props import EASTERN
        end = datetime.combine(target + timedelta(days=1), datetime.min.time(), tzinfo=EASTERN) + timedelta(hours=5)
        while True:
            try:
                run_once(target=target, root=args.state_dir, state=state,
                         api_key=config.odds_api.api_key, db=db, apply=args.apply,
                         max_credits=args.max_credits)
                print(json.dumps({"date": str(target), "games": state["official_game_count"],
                                  "attempts": len(state["attempts"]),
                                  "reserved_credits": state["reserved_credits"]}), flush=True)
            except Exception as exc:
                write_json(directory / "last-error.json", {"at": utcnow().isoformat(),
                           "error_type": type(exc).__name__})
                if not args.watch:
                    raise RuntimeError(f"capture discovery failed: {type(exc).__name__}") from None
            if not args.watch or utcnow() >= end:
                break
            if state.get("official_last_start") and utcnow() > aware(state["official_last_start"]) + timedelta(minutes=2):
                break
            starts = [aware(p["cutoff"]) for p in state.get("plan", []) if p.get("cutoff")]
            # Poll closely near starts; quiet hours need only free discovery every
            # five minutes. An empty/mismatched listing never implies completion.
            near = any(0 < (start - utcnow()).total_seconds() < 600 for start in starts)
            time.sleep(20 if near else 300)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
