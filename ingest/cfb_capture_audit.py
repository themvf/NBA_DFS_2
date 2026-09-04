"""Read-only integrity audit. Missing observations are warnings, never fabricated."""
from __future__ import annotations

import json
import math
from datetime import datetime

from config import load_config


def quote_issues(books: dict, captured_at: datetime) -> list[str]:
    issues = []
    for name, quote in books.items():
        for field in ("spread_home", "spread_away", "total_line", "ml_home", "ml_away",
                      "spread_home_price", "spread_away_price", "over", "under"):
            value = quote.get(field)
            if value is not None:
                try:
                    if not math.isfinite(float(value)):
                        issues.append(f"{name}:{field}:nonfinite")
                except (ValueError, TypeError):
                    issues.append(f"{name}:{field}:nonnumeric")
        try:
            if quote.get("spread_home") is not None and quote.get("spread_away") is not None:
                if abs(float(quote["spread_home"]) + float(quote["spread_away"])) > 1e-8:
                    issues.append(f"{name}:asymmetric_spread")
            if quote.get("last_update"):
                updated = datetime.fromisoformat(str(quote["last_update"]).replace("Z", "+00:00"))
                if (updated - captured_at).total_seconds() > 90:
                    issues.append(f"{name}:future_quote_timestamp")
        except (ValueError, TypeError):
            issues.append(f"{name}:invalid_quote_or_timestamp")
    return issues


def audit(connection) -> dict:
    from collections import defaultdict
    from model.line_alerts import _cfb_market_signals, _nfl_line_outcome
    from psycopg2.extras import RealDictCursor
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    def rows(sql):
        cursor.execute(sql)
        return [dict(row) for row in cursor.fetchall()]
    checks = {
        "capture_identity_or_boundary": """SELECT h.id FROM game_odds_history h
            LEFT JOIN cfb_matchups m ON m.id=h.matchup_id WHERE h.sport='cfb'
            AND (m.id IS NULL OR m.commence_time IS NULL OR h.captured_at>=m.commence_time
                 OR h.event_id IS DISTINCT FROM m.odds_event_id)""",
        "duplicate_capture_keys": """SELECT matchup_id, capture_key FROM game_odds_history
            WHERE sport='cfb' GROUP BY matchup_id,capture_key HAVING COUNT(*)>1""",
        "invalid_checkpoint_evidence": """SELECT c.id FROM odds_capture_checkpoints c
            LEFT JOIN game_odds_history h ON h.id=c.history_id
            WHERE c.sport='cfb' AND c.status='captured' AND (h.id IS NULL
              OR h.sport<>'cfb' OR h.matchup_id<>c.matchup_id
              OR h.captured_at NOT BETWEEN c.target_at AND c.due_until
              OR h.captured_at>=c.scheduled_start_at)""",
        "invalid_close_evidence": """SELECT c.id FROM event_closing_lines c
            LEFT JOIN game_odds_history h ON h.id=c.history_id WHERE c.sport='cfb'
            AND (h.id IS NULL OR h.sport<>'cfb' OR h.matchup_id<>c.matchup_id
              OR h.captured_at>=c.boundary_at OR h.captured_at<>c.captured_at
              OR ABS(EXTRACT(EPOCH FROM c.boundary_at-h.captured_at)-c.lead_seconds)>1
              OR (c.primary_clv_eligible AND c.quality='stale'))""",
        "invalid_signal_trigger": """SELECT a.id FROM line_alerts a
            LEFT JOIN game_odds_history h ON h.id=a.trigger_history_id
            LEFT JOIN cfb_matchups m ON m.id=a.matchup_id
            WHERE a.sport='cfb' AND (h.id IS NULL OR h.sport<>'cfb'
              OR h.matchup_id<>a.matchup_id OR h.captured_at>=m.commence_time
              OR h.captured_at>a.created_at)""",
        "premature_settlement": """SELECT a.id FROM line_alerts a JOIN cfb_matchups m
            ON m.id=a.matchup_id WHERE a.sport='cfb' AND a.outcome IS NOT NULL
            AND (NOT m.completed OR m.home_score IS NULL OR m.away_score IS NULL)""",
    }
    errors = {name: found for name, sql in checks.items() if (found := rows(sql))}
    bad_quotes = []
    for row in rows("SELECT id,books,captured_at FROM game_odds_history WHERE sport='cfb'"):
        issues = quote_issues(row["books"] or {}, row["captured_at"])
        if issues:
            bad_quotes.append({"history_id": row["id"], "issues": issues})
    if bad_quotes:
        errors["invalid_quotes"] = bad_quotes
    alerts = rows("""SELECT a.*,m.home_score,m.away_score FROM line_alerts a
        JOIN cfb_matchups m ON m.id=a.matchup_id WHERE a.sport='cfb'""")
    incorrect_results = []
    for alert in alerts:
        details = alert["details_json"] or {}
        if alert["outcome"] is None or details.get("market") not in ("spread", "total"):
            continue
        entry = details.get("entry_home_line", details.get("trigger_line"))
        if entry is None or alert["home_score"] is None or alert["away_score"] is None:
            incorrect_results.append({"id": alert["id"], "reason": "missing grading inputs"})
        elif _nfl_line_outcome(details["market"], alert["side"], float(entry),
                              alert["home_score"], alert["away_score"]) != alert["outcome"]:
            incorrect_results.append({"id": alert["id"], "reason": "result disagrees with frozen entry"})
    if incorrect_results:
        errors["incorrect_results"] = incorrect_results
    histories = defaultdict(list)
    for row in rows("""SELECT h.id AS history_id,h.matchup_id,h.books,h.captured_at,h.capture_key
        FROM game_odds_history h JOIN cfb_matchups m ON m.id=h.matchup_id
        WHERE h.sport='cfb' AND h.captured_at<m.commence_time ORDER BY h.captured_at,h.id"""):
        histories[row["matchup_id"]].append(row)
    recorded = {(a["matchup_id"], a["alert_type"], a["side"]): a for a in alerts}
    gaps = []
    for matchup_id, history in histories.items():
        first_seen = set()
        for end in range(1, len(history) + 1):
            for signal in _cfb_market_signals(history[:end]):
                key = (matchup_id, signal["alert_type"], signal["side"])
                if key in first_seen:
                    continue
                first_seen.add(key)
                actual = recorded.get(key)
                expected_id = history[end - 1]["history_id"]
                if actual is None or actual["trigger_history_id"] != expected_id:
                    gaps.append({"matchup_id": matchup_id, "type": key[1], "side": key[2],
                                 "expected_history_id": expected_id,
                                 "recorded_history_id": actual["trigger_history_id"] if actual else None})
    coverage = rows("""SELECT m.id, m.game_date, m.commence_time, m.odds_event_id,
        COUNT(h.id)::int AS captures, MAX(h.captured_at) AS last_capture
        FROM cfb_matchups m LEFT JOIN game_odds_history h ON h.matchup_id=m.id AND h.sport='cfb'
        WHERE m.commence_time BETWEEN NOW() AND NOW()+INTERVAL '72 hours'
        GROUP BY m.id ORDER BY m.commence_time,m.id""")
    checkpoints = rows("""SELECT status,COUNT(*)::int AS n FROM odds_capture_checkpoints
        WHERE sport='cfb' AND scheduled_start_at>NOW() AND target_at<=NOW() GROUP BY status""")
    closes = rows("SELECT quality,COUNT(*)::int AS n FROM event_closing_lines WHERE sport='cfb' GROUP BY quality")
    return {"integrity_status": "fail" if errors else "pass", "errors": errors,
            "first_breach_replay_gaps": gaps,
            "upcoming": coverage, "due_or_elapsed_checkpoints": checkpoints, "close_quality": closes,
            "limitations": ["Passed integrity checks do not prove uninterrupted capture coverage.",
                            "Signals record the first qualifying breach per game/type/side, not every repeated pulse.",
                            "Stale closes and missed windows are not repaired with later prices."]}


if __name__ == "__main__":
    import psycopg2
    with psycopg2.connect(load_config().database_url) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        result = audit(connection)
    print(json.dumps(result, indent=2, default=str))
    raise SystemExit(1 if result["errors"] else 0)
