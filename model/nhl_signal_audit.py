"""NHL-only exact-close grading and decision-time evidence. No execution claims."""
from __future__ import annotations

import math
from datetime import datetime, timezone

GRADING_VERSION = "nhl-moneyline-audit-v2"
SIGNAL_VERSION = "nhl-moneyline-v1"  # Existing thresholds; no fitting to outcomes.
FRESH_SECONDS = 300


def utc(value):
    if value is None:
        return None
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def decimal(price):
    try:
        odds = float(price)
        if not math.isfinite(odds) or abs(odds) < 100:
            return None
        return 1 + (odds / 100 if odds > 0 else 100 / -odds)
    except (TypeError, ValueError):
        return None


def pair_probability(book, side):
    if side not in ("home", "away") or not isinstance(book, dict):
        return None
    home, away = decimal(book.get("ml_home")), decimal(book.get("ml_away"))
    if home is None or away is None or book.get("ml_draw") is not None:
        return None
    p = (1 / home) / (1 / home + 1 / away)
    return p if side == "home" else 1 - p


def timing_evidence(capture, updated, decision, start):
    c, u, a, s = map(utc, (capture, updated, decision, start))
    evidence = {"trigger_capture_at": c.isoformat() if c else None,
                "book_updated_at": u.isoformat() if u else None,
                "alert_created_at": a.isoformat() if a else None,
                "scheduled_start_at": s.isoformat() if s else None,
                "freshness_limit_seconds": FRESH_SECONDS,
                "capture_age_seconds": (a-c).total_seconds() if a and c else None,
                "book_age_seconds": (a-u).total_seconds() if a and u else None,
                "quote_status": "unknown", "quote_fresh": False,
                "execution_verified": False}
    if not all((c, u, a, s)):
        return evidence
    if c > a or u > c or a >= s or c >= s:
        evidence["quote_status"] = "invalid_time"
    elif (a-c).total_seconds() > FRESH_SECONDS or (a-u).total_seconds() > FRESH_SECONDS:
        evidence["quote_status"] = "stale"
    else:
        evidence.update(quote_status="fresh", quote_fresh=True)
    return evidence


def insert_evidence(row, details, as_of=None):
    as_of = as_of or datetime.now(timezone.utc)
    book = (row.get("books") or {}).get(details.get("exec_book")) or {}
    return {"signal_version": SIGNAL_VERSION,
            "trigger_history_id": row.get("history_id"),
            **timing_evidence(row.get("captured_at"), book.get("last_update"), as_of, row.get("commence_time")),
            "context_scope": "market_only", "goalie_evidence": "not_captured",
            "injury_evidence": "not_captured"}


def grade_prices(alert, trigger, close, history=()):
    """Pure grader: close includes its exact books; history is persistence only."""
    details = alert.get("details_json") or {}
    book_key = details.get("exec_book") or details.get("clv_book")
    side = alert.get("side")
    trigger_book = ((trigger or {}).get("books") or {}).get(book_key) or {}
    timing = timing_evidence((trigger or {}).get("captured_at"), trigger_book.get("last_update"),
                             alert.get("created_at"), alert.get("commence_time"))
    entry = details.get("exec_decimal")
    try:
        entry = float(entry)
        if not math.isfinite(entry) or entry <= 1:
            entry = None
    except (TypeError, ValueError):
        entry = None
    meta = {"grading_version": GRADING_VERSION, "timing": timing, "exec_book": book_key,
            "entry_decimal": entry, "trigger_history_id": (trigger or {}).get("id"),
            "close_source": "unavailable", "close_status": "NO_CLOSE", "close_history_id": None,
            "price_clv_pct": None, "close_decimal": None, "close_prob": None, "clv_pp": None,
            "pnl_basis": "hypothetical_frozen_quote", "context_scope": "market_only",
            "goalie_evidence": "not_captured", "injury_evidence": "not_captured",
            "convergence_eligible": False, "survival_lower_min": None, "survival_upper_min": None}
    out = {"dk_close_decimal": None, "dk_clv_pct": None, "pin_close_prob": None,
           "convergence": None, "dk_survival_min": None, "grading_json": meta,
           "comparison_status": "NO_CLOSE", "grading_version": GRADING_VERSION}
    trigger_valid = (trigger is not None and trigger.get("sport") == "nhl"
                     and trigger.get("matchup_id") == alert.get("matchup_id")
                     and trigger.get("id") == alert.get("trigger_history_id")
                     and utc(trigger.get("captured_at")) is not None
                     and utc(alert.get("commence_time")) is not None
                     and utc(alert.get("created_at")) is not None
                     and utc(trigger["captured_at"]) <= utc(alert["created_at"]) < utc(alert["commence_time"])
                     and utc(trigger["captured_at"]) < utc(alert["commence_time"]))
    quoted_entry = decimal(trigger_book.get("ml_"+str(side)))
    meta["entry_verified"] = bool(trigger_valid and entry and quoted_entry and abs(entry-quoted_entry) <= 0.00011
                                  and pair_probability(trigger_book, side) is not None
                                  and timing["quote_status"] != "invalid_time")
    if not trigger or trigger.get("id") != alert.get("trigger_history_id") or trigger.get("matchup_id") != alert.get("matchup_id"):
        timing.update(quote_status="unknown", quote_fresh=False)
    if not close:
        return out
    capture, boundary, start = utc(close.get("captured_at")), utc(close.get("boundary_at")), utc(alert.get("commence_time"))
    valid_close = (close.get("sport") == "nhl" and close.get("matchup_id") == alert.get("matchup_id")
                   and close.get("history_matchup_id") == alert.get("matchup_id")
                   and close.get("history_sport") == "nhl"
                   and close.get("history_id") == close.get("history_row_id")
                   and close.get("quality") in ("A", "B", "C")
                   and close.get("primary_clv_eligible") is True
                   and close.get("clv_cohort") == "verified_clv_v1"
                   and capture is not None and boundary is not None and start is not None
                   and capture < boundary <= start and utc(close.get("scheduled_start_at")) == start
                   and utc(close.get("history_captured_at")) == capture)
    if not valid_close:
        meta["close_status"] = "INVALID_CLOSE"
        out["comparison_status"] = "INVALID_CLOSE"
        return out
    meta.update(close_source="event_closing_lines", close_history_id=close["history_id"],
                close_quality=close["quality"], close_boundary_source=close.get("boundary_source"),
                close_lead_seconds=close.get("lead_seconds"), close_cohort=close.get("clv_cohort"),
                close_verification_level=close.get("verification_level"),
                close_captured_at=capture.isoformat(), close_status="CLOSE_UNAVAILABLE")
    books = close.get("books") or {}
    # Probability CLV retains the existing retail-mean definition.
    probs = [p for key,b in books.items() if key not in ("pinnacle", "polymarket")
             and utc(b.get("last_update")) is not None and utc(b["last_update"]) <= capture
             and (p := pair_probability(b, side)) is not None]
    if probs:
        meta["close_prob"] = sum(probs)/len(probs)
        if alert.get("alert_prob") is not None and trigger_valid and timing["quote_status"] != "invalid_time":
            meta["clv_pp"] = 100*(meta["close_prob"]-float(alert["alert_prob"]))
    out["pin_close_prob"] = pair_probability(books.get("pinnacle"), side)
    close_book = books.get(book_key) or {}
    close_decimal = decimal(close_book.get("ml_"+str(side)))
    update = utc(close_book.get("last_update"))
    if entry is None or not meta["entry_verified"]:
        meta["close_status"] = "ENTRY_UNVERIFIED"
    elif (close_decimal is not None and pair_probability(close_book, side) is not None
          and update is not None and update <= capture):
        out["dk_close_decimal"] = close_decimal
        out["dk_clv_pct"] = (entry/close_decimal-1)*100
        meta.update(close_status="OBSERVED", close_decimal=close_decimal, price_clv_pct=out["dk_clv_pct"])
    out["comparison_status"] = "SAME_BOOK_MONEYLINE" if out["dk_close_decimal"] else meta["close_status"]
    if meta["entry_verified"] and utc(trigger["captured_at"]) <= capture:
        begin = utc(trigger["captured_at"])
        last_same = begin
        for h in sorted(history, key=lambda h: (utc(h["captured_at"]), h["id"])):
            at = utc(h["captured_at"])
            if h.get("sport") != "nhl" or h.get("matchup_id") != alert["matchup_id"] or not begin < at <= capture:
                continue
            value = decimal((h.get("books") or {}).get(book_key, {}).get("ml_"+side))
            if value is None or abs(value-entry) > 0.00011:
                upper = (at-begin).total_seconds()/60
                meta.update(survival_lower_min=(last_same-begin).total_seconds()/60,
                            survival_upper_min=upper, first_changed_at=at.isoformat(),
                            last_same_at=last_same.isoformat(), survival_time_basis="trigger_capture",
                            quote_changed_before_alert=at <= utc(alert["created_at"]))
                out["dk_survival_min"] = upper
                break
            last_same = at
        else:
            meta.update(survival_lower_min=(last_same-begin).total_seconds()/60,
                        first_changed_at=None, last_same_at=last_same.isoformat(),
                        survival_time_basis="trigger_capture", quote_changed_before_alert=False)
    return out


def load_grade(db, alert):
    trigger = db.execute_one("SELECT * FROM game_odds_history WHERE id=%s AND sport='nhl'",
                             (alert.get("trigger_history_id"),))
    close = db.execute_one("""SELECT c.*, h.books, h.id AS history_row_id,
        h.matchup_id AS history_matchup_id, h.sport AS history_sport,
        h.captured_at AS history_captured_at FROM verified_clv_closes c
        JOIN game_odds_history h ON h.id=c.history_id
        WHERE c.sport='nhl' AND c.matchup_id=%s""", (alert["matchup_id"],))
    history = db.execute("""SELECT id,sport,matchup_id,captured_at,books FROM game_odds_history
        WHERE sport='nhl' AND matchup_id=%s AND captured_at >= %s AND captured_at <= %s
        ORDER BY captured_at,id""", (alert["matchup_id"], trigger["captured_at"], close["captured_at"])) if trigger and close else []
    return grade_prices(alert, trigger, close, history)
