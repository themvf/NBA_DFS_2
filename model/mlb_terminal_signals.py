"""Prospective MLB game-line observations, isolated from other sport detectors.

Frozen v1: >=3 matched books; total line movement >=0.5 runs; run-line
same-handicap fair-price movement >=1.5pp. Steam uses adjacent observations
<=40 minutes apart; walking uses >=3 observations over 40m..6h. Reversal
requires an initial move and a retrace of the same threshold. These are
descriptive hypotheses, not validated betting recommendations.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone

VERSION = "mlb-terminal-v1"
TYPES = ("mlb_total_steam", "mlb_total_walking", "mlb_total_reversal",
         "mlb_total_price_steam", "mlb_total_price_walking", "mlb_total_price_reversal",
         "mlb_run_line_steam", "mlb_run_line_walking", "mlb_run_line_reversal",
         "mlb_run_line_points_steam", "mlb_run_line_points_walking", "mlb_run_line_points_reversal",
         "mlb_moneyline_reversal")
RETAIL = ("draftkings", "fanduel", "betmgm", "williamhill_us", "caesars", "fanatics", "betrivers", "bovada", "hardrockbet")
EXECUTION = ("draftkings", "betmgm", "fanatics", "williamhill_us", "fanduel", "betrivers")
FINAL = {"Final", "Game Over"}


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def prob(price):
    if not numeric(price) or abs(price) < 100 or int(price) != price:
        return None
    return 100 / (100 + price) if price > 0 else -price / (100 - price)


def quote(book, market, side):
    other = {"home": "away", "away": "home", "over": "under", "under": "over"}[side]
    if market == "moneyline":
        price, paired, line, other_line = book.get(f"ml_{side}"), book.get(f"ml_{other}"), None, None
    elif market == "total":
        price, paired = book.get(side), book.get(other)
        line, other_line = book.get(f"{side}_line", book.get("total_line")), book.get(f"{other}_line", book.get("total_line"))
    else:
        price = book.get(f"spread_{side}_price", book.get("spread_price") if side == "home" else None)
        paired = book.get(f"spread_{other}_price", book.get("spread_price") if other == "home" else None)
        line, other_line = book.get(f"spread_{side}"), book.get(f"spread_{other}")
    p, q = prob(price), prob(paired)
    if p is None or q is None:
        return None
    if market != "moneyline" and (not numeric(line) or not numeric(other_line) or (line != other_line if market == "total" else line != -other_line)):
        return None
    return {"price": price, "line": line, "fair": p / (p + q)}


def fresh(book, market, observed):
    field = {"moneyline": "h2h", "run_line": "spreads", "total": "totals"}[market]
    value = book.get(f"{field}_last_update") or book.get("last_update")
    if not value:
        return False
    try:
        return 0 <= (utc(observed) - utc(value)).total_seconds() <= 35 * 60
    except (ValueError, TypeError):
        return False


def candidates(history, now=None):
    """Pure detector. History must be pregame, chronologically sorted."""
    if len(history) < 2:
        return []
    now = now or datetime.now(timezone.utc)
    current = history[-1]
    if not 0 <= (utc(now) - utc(current["captured_at"])).total_seconds() <= 35 * 60:
        return []
    history = [h for h in history if 0 <= (utc(current["captured_at"]) - utc(h["captured_at"])).total_seconds() <= 6 * 3600]
    if len(history) < 2:
        return []
    output = []
    for market, base_side, metric, suffix in (
        ("total", "over", "runs", ""), ("total", "over", "fair_probability", "_price"),
        ("run_line", "home", "fair_probability", ""), ("run_line", "home", "runs", "_points"),
        ("moneyline", "home", "fair_probability", ""),
    ):
        threshold = .5 if metric == "runs" else .015
        for kind in ("steam", "walking", "reversal"):
            if market == "moneyline" and kind != "reversal":
                continue  # Existing MLB moneyline steam/walking stay authoritative.
            window = history[-2:] if kind == "steam" else history
            seconds = (utc(window[-1]["captured_at"]) - utc(window[0]["captured_at"])).total_seconds()
            if seconds <= 0 or (kind == "steam" and seconds > 40 * 60) or (kind != "steam" and (len(window) < 3 or seconds < 40 * 60)):
                continue
            movements = []
            for key in RETAIL:
                books = [h["books"].get(key, {}) for h in window]
                quotes = [quote(b, market, base_side) if fresh(b, market, h["captured_at"]) else None for b, h in zip(books, window)]
                if not all(quotes):
                    continue
                if market != "moneyline" and metric == "fair_probability" and len({q["line"] for q in quotes}) != 1:
                    continue
                values = [(q["line"] * (-1 if market == "run_line" else 1)) if metric == "runs" else q["fair"] for q in quotes]
                delta = values[-1] - values[0]
                if kind == "walking" and any((b-a) * delta < -1e-9 for a, b in zip(values, values[1:])):
                    continue
                if kind == "reversal":
                    up = max(values[1:-1]) - values[0] >= threshold and max(values[1:-1]) - values[-1] >= threshold
                    down = values[0] - min(values[1:-1]) >= threshold and values[-1] - min(values[1:-1]) >= threshold
                    if up == down:
                        continue
                    delta = -threshold if up else threshold
                if abs(delta) + 1e-9 >= threshold:
                    movements.append((key, delta))
            for direction in (1, -1):
                supporting = [key for key, delta in movements if delta * direction > 0]
                if len(supporting) < 3:
                    continue
                side = ("over" if direction == 1 else "under") if market == "total" else ("home" if direction == 1 else "away")
                execution = [(key, quote(current["books"][key], market, side)) for key in supporting if key in EXECUTION]
                execution = [(key, q) for key, q in execution if q]
                if not execution:
                    continue
                # Fix a real observed line before choosing the best price at that line.
                lines = [q["line"] for _, q in execution if q["line"] is not None]
                line = sorted(lines)[(len(lines) - 1) // 2] if lines else None
                execution = [(key, q) for key, q in execution if q["line"] == line]
                key, entry = max(execution, key=lambda item: 1 / prob(item[1]["price"]))
                output.append({"type": f"mlb_{market}{suffix}_{kind}", "side": side, "prob": entry["fair"], "details": {
                    "signal_version": VERSION, "market": market, "exec_book": key, "exec_line": line,
                    "capture_policy": current["books"][key].get("capture_policy", "legacy-capture"),
                    "dk_odds": entry["price"], "dk_decimal": 1 / prob(entry["price"]), "trigger_history_id": current["id"],
                    "observed_at": utc(current["captured_at"]).isoformat(), "supporting_books": supporting,
                    "threshold": threshold, "metric": metric, "window_seconds": seconds,
                }})
    return output


def settlement(market, side, line, status, home, away, *, rescheduled=False):
    if market not in ("moneyline", "run_line", "total") or side not in (("over", "under") if market == "total" else ("home", "away")):
        return None, "invalid_selection"
    if rescheduled or status in ("Postponed", "Cancelled"):
        return "void", "rescheduled" if rescheduled else status.lower()
    if status == "Completed Early":
        return None, "shortened_game_requires_review"
    if status not in FINAL or not numeric(home) or not numeric(away) or home < 0 or away < 0:
        return None, "awaiting_final"
    if market != "moneyline" and not numeric(line):
        return None, "missing_entry_line"
    if market == "moneyline":
        if home == away:
            return "void", "tied_game"
        margin = (home - away) * (1 if side == "home" else -1)
    elif market == "total":
        margin = (home + away - line) * (1 if side == "over" else -1)
    else:
        margin = (home - away if side == "home" else away - home) + line
    return ("void", "push") if abs(margin) < 1e-9 else ("won", "final_score") if margin > 0 else ("lost", "final_score")


def run(db, *, scan_only=False, settle_only=False):
    rows = [] if settle_only else db.execute("""SELECT h.*, m.commence_time FROM game_odds_history h
        JOIN mlb_matchups m ON m.id=h.matchup_id WHERE h.sport='mlb'
        AND m.commence_time>NOW() AND h.captured_at<m.commence_time
        AND h.game_date=m.game_date AND h.captured_at>NOW()-INTERVAL '6 hours'
        AND COALESCE(m.game_status,'') NOT IN ('Postponed','Cancelled') AND h.books IS NOT NULL
        AND h.books ?| ARRAY['draftkings','fanduel','betmgm','betrivers','bovada','fanatics']
        ORDER BY h.matchup_id,h.captured_at,h.id""")
    groups = {}
    for row in rows:
        groups.setdefault(row["matchup_id"], []).append(row)
    inserted = 0
    for history in groups.values():
        latest = history[-1]
        for signal in candidates(history):
            added = db.execute("""INSERT INTO line_alerts
              (sport,matchup_id,game_date,matchup,commence_time,alert_type,side,capture_key,alert_prob,details_json)
              VALUES ('mlb',%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT (sport,matchup_id,alert_type,side) DO NOTHING RETURNING id""",
              (latest["matchup_id"], latest["game_date"], f"{latest['away_team_name']} @ {latest['home_team_name']}",
               latest["commence_time"], signal["type"], signal["side"], latest["capture_key"], signal["prob"], json.dumps(signal["details"])))
            inserted += len(added)
    if scan_only:
        return inserted
    # Revisit settled entries without a verified close: outcomes can arrive before close verification.
    alerts = db.execute("""SELECT a.*, m.game_status, m.home_score, m.away_score, m.game_date AS current_date,
        h.books AS closing_books, h.captured_at AS close_at, c.quality AS close_quality, c.history_id AS close_history_id
        FROM line_alerts a JOIN mlb_matchups m ON m.id=a.matchup_id
        LEFT JOIN verified_clv_closes c ON c.sport='mlb' AND c.matchup_id=a.matchup_id
        LEFT JOIN game_odds_history h ON h.id=c.history_id AND h.game_date=a.game_date
        WHERE a.sport='mlb' AND a.alert_type=ANY(%s)
        AND (a.settled_at IS NULL OR (a.grading_json->>'verified_clv') IS NULL)
        AND a.game_date>=CURRENT_DATE-90""", (list(TYPES),))
    for alert in alerts:
        d = alert["details_json"] or {}
        if d.get("signal_version") != VERSION:
            continue
        outcome, reason = settlement(d["market"], alert["side"], d.get("exec_line"), alert["game_status"],
            alert["home_score"], alert["away_score"], rescheduled=alert["game_date"] != alert["current_date"])
        previous_grade = alert["grading_json"] or {}
        grade = {**previous_grade, "settlement_reason": previous_grade.get("settlement_reason", reason) if alert["outcome"] else reason, "signal_version": VERSION}
        closing = (alert.get("closing_books") or {}).get(d["exec_book"])
        close_is_later = alert.get("close_at") and utc(alert["close_at"]) > utc(d["observed_at"])
        q = quote(closing, d["market"], alert["side"]) if closing and close_is_later and fresh(closing, d["market"], alert["close_at"]) else None
        if q and reason not in ("rescheduled", "postponed", "cancelled", "tied_game"):
            if d["market"] == "moneyline":
                clv, unit = (q["fair"] - float(alert["alert_prob"])) * 100, "pp"
            elif d["metric"] == "fair_probability":
                clv, unit = ((q["fair"] - float(alert["alert_prob"])) * 100, "pp") if q["line"] == d["exec_line"] else (None, "pp")
            elif d["market"] == "run_line":
                clv, unit = d["exec_line"] - q["line"], "runs"
            else:
                clv, unit = (q["line"] - d["exec_line"]) * (1 if alert["side"] == "over" else -1), "runs"
            grade.update(verified_clv=clv, clv_unit=unit, close_quality=alert["close_quality"], close_history_id=alert["close_history_id"])
        db.execute("""UPDATE line_alerts SET outcome=COALESCE(outcome,%s),
          settled_at=CASE WHEN %s::text IS NOT NULL THEN COALESCE(settled_at,NOW()) ELSE settled_at END,
          grading_json=%s,grading_version=%s WHERE id=%s AND sport='mlb'""",
          (outcome, outcome, json.dumps(grade), VERSION, alert["id"]))
    return inserted
