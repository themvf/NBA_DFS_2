"""Prospective WTA walking enrollment and read-only research reporting."""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone

import numpy as np

VERSION = "wta-walking-60-70-v1"
TARGET = 200


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    return value.astimezone(timezone.utc)


def enrollment(*, context, opening, details, probability, now=None):
    """Metadata only: caller inserts it immutably with the first walking row."""
    now = utc(now or datetime.now(timezone.utc))
    if context.get("tour") != "WTA" or not .60 <= probability < .70:
        return {}
    if details.get("drift_pp", 0) < 2 or details.get("overlap_books", 0) < 3:
        return {}
    books = context.get("books") or {}
    if "polymarket" in books or "polymarket" in (opening.get("books") or {}):
        return {}
    try:
        shared = (set(books) & set(opening.get("books") or {})) - {"pinnacle", "polymarket"}
        usable = []
        for key in shared:
            try:
                for quote in (books[key], opening["books"][key]):
                    _prob(quote["ml_home"])
                    _prob(quote["ml_away"])
                usable.append(key)
            except (KeyError, TypeError, ValueError):
                pass
        # Do not enroll a probability computed from incomplete moneyline pairs.
        if len(usable) < 3 or len(usable) != details["overlap_books"]:
            return {}
        captured = utc(context["captured_at"])
        starts = utc(context["commence_time"])
        updated = utc(books[details["exec_book"]]["last_update"])
        if not (0 <= (now - captured).total_seconds() <= 900
                and 0 <= (now - updated).total_seconds() <= 900
                and updated <= captured < starts and now < starts):
            return {}
        decimal = float(details["exec_decimal"])
        if not details.get("exec_price_available") or not math.isfinite(decimal) or decimal <= 1:
            return {}
    except (KeyError, TypeError, ValueError):
        return {}
    return {
        "walking_study_version": VERSION,
        "walking_study_target": TARGET,
        "walking_study_enrolled_at": now.isoformat(),
        "walking_study_tour": "WTA",
        "walking_study_tournament": context.get("tournament"),
        "walking_study_surface": context.get("surface"),
        "walking_study_players": [context.get("home_team_name"), context.get("away_team_name")],
        "walking_study_probability": probability,
        "walking_study_probability_basis": "mean_proportional_devig_overlapping_retail",
        "walking_study_boundary": "scheduled_not_verified_first_serve",
        "walking_study_trigger_at": captured.isoformat(),
        "walking_study_exec_updated_at": updated.isoformat(),
        "walking_study_trigger_history_id": context["history_id"],
        "walking_study_opening_history_id": opening["id"],
        "walking_study_research_only": True,
        "walking_study_retail_books": sorted(usable),
    }


def interval(rows, field):
    """Date-clustered bootstrap; null instead of spurious tiny-sample CIs."""
    groups = defaultdict(list)
    for row in rows:
        if row.get(field) is not None:
            groups[str(row["game_date"])].append(row[field])
    if sum(map(len, groups.values())) < 20 or len(groups) < 5:
        return None
    sums = np.array([sum(v) for v in groups.values()])
    counts = np.array([len(v) for v in groups.values()])
    picks = np.random.default_rng(20260905).integers(0, len(groups), (4000, len(groups)))
    samples = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)
    return np.quantile(samples, [.025, .975]).tolist()


def _prob(american):
    odds = float(american)
    if not math.isfinite(odds) or abs(odds) < 100:
        raise ValueError("Invalid American odds")
    return -odds / (100 - odds) if odds < 0 else 100 / (100 + odds)


def summarize(rows):
    def avg(field):
        values = [r[field] for r in rows if r.get(field) is not None]
        return sum(values) / len(values) if values else None
    return {
        "n": len(rows), "wins": sum(r["won"] for r in rows),
        "win_rate": avg("won"), "expected_win_rate": avg("probability"),
        "roi": avg("pnl"), "roi_95ci": interval(rows, "pnl"),
        "win_excess": avg("excess"), "win_excess_95ci": interval(rows, "excess"),
        "paired_primary_closes": sum(r.get("close_ev") is not None for r in rows),
        "closing_fair_ticket_ev": avg("close_ev"),
        "closing_fair_ticket_ev_95ci": interval(rows, "close_ev"),
        "same_book_execution_clv": avg("execution_clv"),
    }


def sensitivity(rows):
    if not rows:
        return None
    profits = [r["pnl"] for r in rows]
    wins = sorted((r["pnl"] + 1 for r in rows if r["won"]), reverse=True)
    excluded = set(sorted(range(len(rows)), key=lambda i: profits[i], reverse=True)[:2])
    excluded.update(sorted((i for i, r in enumerate(rows) if not r["won"]),
                           key=lambda i: profits[i])[:2])
    remaining = [p for i, p in enumerate(profits) if i not in excluded]
    return {
        "roi_after_highest_paying_wins_become_losses": {
            str(k): (sum(profits) - sum(wins[:k])) / len(rows)
            for k in (1, 2, 3) if len(wins) >= k
        },
        "roi_without_largest_two_wins_and_losses": sum(remaining) / len(remaining) if remaining else None,
    }


def build_report(raw):
    buckets = defaultdict(list)
    seen = set()
    invalid = 0
    for original in sorted(raw, key=lambda r: (r["created_at"], r["id"])):
        d = original.get("details_json") or {}
        if d.get("walking_study_version") != VERSION:
            continue
        if original["matchup_id"] in seen:
            continue
        seen.add(original["matchup_id"])
        r = dict(original)
        try:
            p, dec = float(d["walking_study_probability"]), float(d["exec_decimal"])
            if not .6 <= p < .7 or not math.isfinite(dec) or dec <= 1:
                raise ValueError("Invalid frozen inputs")
            if not (utc(d["walking_study_trigger_at"]) <= utc(d["walking_study_enrolled_at"])
                    < utc(r["commence_time"]) and utc(r["created_at"]) < utc(r["commence_time"])):
                raise ValueError("Not pre-schedule")
        except (KeyError, TypeError, ValueError):
            invalid += 1
            continue
        status = r.get("completion_status")
        if status not in ("completed", "unknown", "scheduled", None):
            buckets["excluded_status"].append(r)
            continue
        if r.get("winner") not in ("home", "away"):
            buckets["pending"].append(r)
            continue
        r.update(won=int(r["winner"] == r["side"]), probability=p,
                 tournament=d.get("walking_study_tournament") or "unknown",
                 surface=d.get("walking_study_surface") or "unknown")
        r.update(pnl=r["won"] * dec - 1, excess=r["won"] - p)
        # Read the exact execution book's MONEYLINE pair, never totals or
        # stored legacy dk_clv_pct. Joined closes are primary-cohort only.
        try:
            book = (r.get("close_books") or {})[d["exec_book"]]
            at = utc(r["close_captured_at"])
            update = utc(book["last_update"])
            if not (utc(d["walking_study_trigger_at"]) < at < utc(r["close_boundary_at"])
                    and 0 <= (at - update).total_seconds() <= 900):
                raise ValueError("Unusable close timing")
            own = _prob(book["ml_" + r["side"]])
            other = _prob(book["ml_away" if r["side"] == "home" else "ml_home"])
            r.update(close_ev=dec * own / (own + other) - 1, execution_clv=dec * own - 1)
        except (KeyError, TypeError, ValueError):
            pass
        buckets["completed" if status == "completed" else "uncertain_winner"].append(r)
    primary = buckets["completed"]
    dates = len({str(r["game_date"]) for r in primary})
    tournaments = len({r["tournament"] for r in primary if r["tournament"] != "unknown"})
    known = len(primary) + len(buckets["uncertain_winner"])
    main = summarize(primary)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "study_version": VERSION, "research_only": True,
        "status": "review_due_freeze_dataset" if len(primary) >= TARGET and dates >= 20 and tournaments >= 5 else "collecting",
        "target_completed": TARGET, "enrolled_unique_matches": len(seen),
        "invalid_frozen_records": invalid,
        "primary_completed": main,
        "uncertain_winners_sensitivity_only": summarize(buckets["uncertain_winner"]),
        "pending_no_winner": len(buckets["pending"]),
        "excluded_statuses": dict(Counter(r.get("completion_status") for r in buckets["excluded_status"])),
        "completed_match_dates": dates, "completed_tournament_labels": tournaments,
        "completion_coverage_among_nonretired_winners": len(primary) / known if known else None,
        "paired_close_coverage": main["paired_primary_closes"] / len(primary) if primary else None,
        "primary_sensitivity": sensitivity(primary),
        "probability_bands_completed": {
            f"{lo:.3f}-{hi:.3f}": summarize([r for r in primary if lo <= r["probability"] < hi])
            for lo, hi in ((.6, .625), (.625, .65), (.65, .675), (.675, .7))
        },
        "player_completed_counts": dict(Counter(
            p for r in primary for p in r["details_json"].get("walking_study_players", []) if p)),
        "calendar_week_completed_counts": dict(Counter(
            str(r["game_date"].isocalendar()[:2]) for r in primary)),
        "surface_completed": {s: summarize([r for r in primary if r["surface"] == s]) for s in sorted({r["surface"] for r in primary})},
        "tournament_completed": {t: summarize([r for r in primary if r["tournament"] == t]) for t in sorted({r["tournament"] for r in primary})},
        "notes": ["Hypothetical one-unit returns at frozen prices; not executed wagers.",
                  "Unknown completion never enters primary results.",
                  "Closes use recorded scheduled boundaries, not verified first serve.",
                  "Review due is not validation or permission to bet; see the registered study."],
    }


def main():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from config import load_config
    # Avoid DatabaseManager's automatic schema writes for this reporting CLI.
    with psycopg2.connect(load_config().database_url, cursor_factory=RealDictCursor,
                          connect_timeout=20) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '30s'")
            cur.execute("""
                SELECT a.*, m.winner, m.completion_status,
                       h.books AS close_books, c.captured_at AS close_captured_at,
                       c.boundary_at AS close_boundary_at
                FROM line_alerts a JOIN tennis_matches m ON m.id=a.matchup_id
                LEFT JOIN verified_clv_closes c ON c.sport=a.sport AND c.matchup_id=a.matchup_id
                LEFT JOIN game_odds_history h ON h.id=c.history_id
                WHERE a.sport='tennis' AND a.alert_type='walking'
                  AND a.origin='prospective'
                  AND a.details_json->>'walking_study_version'=%s
                  AND NOT EXISTS (
                      SELECT 1 FROM line_alerts earlier
                      WHERE earlier.sport=a.sport AND earlier.matchup_id=a.matchup_id
                        AND earlier.alert_type='walking'
                        AND (earlier.created_at, earlier.id) < (a.created_at, a.id)
                  )
                ORDER BY a.created_at, a.id
            """, (VERSION,))
            print(json.dumps(build_report(cur.fetchall()), indent=2, default=str))


if __name__ == "__main__":
    main()
