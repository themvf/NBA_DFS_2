"""Read-only pregame CFB exception list for the capture operator.

This report uses accepted history and current-schedule checkpoint rows. It
never treats a dispatched workflow or an API response as a saved line.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from config import load_config

# Early pilot checkpoints the provider often cannot serve: The Odds API lists
# most CFB games only 2-5 days out (measured 2026-10-07: 15 of ~50 games listed
# 9-10 days ahead, none 4-8 days). A checkpoint is created only once the game
# has a provider event, so one created after its own window closed was never
# capturable. That is a source limit, reported but not alerted.
EARLY_CHECKPOINTS = ("cfb_t_minus_7d", "cfb_t_minus_4d")


def listed_after_window(checkpoint: dict) -> bool:
    created = checkpoint.get("created_at")
    return (checkpoint["checkpoint"] in EARLY_CHECKPOINTS and created is not None
            and created > checkpoint["due_until"])


def classify_game(game: dict, checkpoints: list[dict], now: datetime) -> list[str]:
    kickoff = game["commence_time"]
    lead = kickoff - now
    issues: list[str] = []
    if game["odds_event_id"] is None and lead <= timedelta(hours=24):
        issues.append("unmapped_within_24h")
    if game["start_time_tbd"]:
        issues.append("kickoff_unconfirmed")
    latest = game["last_capture"]
    if lead <= timedelta(hours=6):
        if latest is None and lead <= timedelta(hours=6) - timedelta(minutes=3):
            issues.append("no_pregame_capture")
        elif latest is not None and now - latest > timedelta(minutes=25):
            issues.append("capture_overdue")
    elif lead <= timedelta(hours=12) and latest is not None and now - latest > timedelta(minutes=90):
        issues.append("capture_overdue")
    for checkpoint in checkpoints:
        if checkpoint["status"] == "missed" and checkpoint["failure_reason"] != "superseded by kickoff reschedule":
            if not listed_after_window(checkpoint):
                issues.append(f"missed:{checkpoint['checkpoint']}")
        elif (checkpoint["status"] in ("pending", "attempted", "failed")
              and checkpoint["target_at"] + timedelta(minutes=1 if checkpoint["checkpoint"] == "t_minus_2m" else 3)
              <= now <= checkpoint["due_until"]):
            issues.append(f"due_now:{checkpoint['checkpoint']}")
    return sorted(set(issues))


def build_report(database_url: str, now: datetime | None = None) -> dict:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    now = now or datetime.now(timezone.utc)
    with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("""SELECT m.id,m.commence_time,m.start_time_tbd,m.odds_event_id,
                       a.name AS away_team,h.name AS home_team,
                       (SELECT MAX(o.captured_at) FROM game_odds_history o
                        WHERE o.sport='cfb' AND o.matchup_id=m.id
                          AND o.captured_at<m.commence_time) AS last_capture
                FROM cfb_matchups m
                JOIN cfb_teams a ON a.team_id=m.away_team_id
                JOIN cfb_teams h ON h.team_id=m.home_team_id
                WHERE m.completed=FALSE AND m.commence_time>%s
                  AND m.commence_time<=%s+INTERVAL '72 hours'
                ORDER BY m.commence_time,m.id""", (now, now))
            games = [dict(row) for row in cursor.fetchall()]
            ids = [game["id"] for game in games]
            checkpoints: dict[int, list[dict]] = {}
            if ids:
                cursor.execute("""SELECT c.matchup_id,c.checkpoint,c.status,c.target_at,
                           c.due_until,c.failure_reason,c.created_at
                    FROM odds_capture_checkpoints c JOIN cfb_matchups m ON m.id=c.matchup_id
                    WHERE c.sport='cfb' AND c.matchup_id=ANY(%s)
                      AND c.scheduled_start_at=m.commence_time""", (ids,))
                for row in cursor.fetchall():
                    checkpoints.setdefault(row["matchup_id"], []).append(dict(row))
    entries = []
    not_listed: dict[str, int] = {}
    for game in games:
        game_checkpoints = checkpoints.get(game["id"], [])
        issues = classify_game(game, game_checkpoints, now)
        if issues:
            entries.append({"matchup_id": game["id"], "game": f"{game['away_team']} at {game['home_team']}",
                            "kickoff": game["commence_time"].isoformat(), "issues": issues})
        for checkpoint in game_checkpoints:
            if checkpoint["status"] == "missed" and listed_after_window(checkpoint):
                not_listed[checkpoint["checkpoint"]] = not_listed.get(checkpoint["checkpoint"], 0) + 1
    return {"as_of": now.isoformat(), "upcoming_games": len(games),
            "games_needing_review": len(entries), "exceptions": entries,
            "provider_listed_after_window": not_listed}


def main() -> int:
    parser = argparse.ArgumentParser(description="List actionable upcoming CFB line-coverage gaps")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fail-on-alert", action="store_true")
    args = parser.parse_args()
    report = build_report(load_config().database_url)
    text = json.dumps(report, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    return 1 if args.fail_on_alert and report["games_needing_review"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
