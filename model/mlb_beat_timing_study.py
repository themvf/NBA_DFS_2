"""P1 (Stage A) of the MLB Beat-Writer Information-Latency Pilot (CLAUDE.md,
2026-07-05): does market movement around beat-writer facts differ from
typical day-to-day movement?

Scope discipline: this is deliberately the COARSE first question, not the
full P1 spec. The original P1 definition asks whether the market "moves in
the implied direction" after a fact -- answering that requires a per-fact
expected-direction label (e.g. is this injury_status fact good or bad news
for the team), which is a real design decision this file has NOT yet made.
Assigning that label only after looking at movement data would be exactly
the kind of after-the-fact rationalization this project's pre-registration
discipline exists to prevent. So Stage A asks a narrower, honest, already-
answerable question first: is there MORE market movement (in either
direction) around games linked to a beat-writer fact than around a
baseline of all other games in the same window? If Stage A shows nothing,
there is no point building the harder directional (Stage B) analysis.

Entry/close convention mirrors model/clv_report.py exactly: entry = first
game_odds_history snapshot, close = last snapshot before the game. Here,
"entry" for a fact-linked game is the first snapshot at or after the
fact's published_at (not just the first snapshot of the day), since the
whole point is measuring movement AFTER the fact posted.

Sample-size reality check (pre-registered, before running): as of
2026-07-05 there are ~111 beat-writer facts across 58 articles spanning
5.5 weeks for one team from one source. This run is a mechanical
correctness check -- confirming the linking/measurement machinery works
end-to-end -- NOT a statistical conclusion. No minimum-sample gate has been
reached; none of this file's normal walk-forward/CI discipline applies yet
because there isn't remotely enough data. Revisit for real analysis once
volume grows over the season.

Usage:
    python -m model.mlb_beat_timing_study
"""

from __future__ import annotations

import argparse
import logging
from datetime import timedelta

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

_SPORT = "mlb"
_MAX_DAYS_TO_NEXT_GAME = 7  # facts referencing a game further out are skipped


def _team_prob_delta(entry: dict, close: dict, team_id: int) -> float | None:
    """Movement in the fact's own team's implied win probability (not just
    home team's), so an away-team fact and a home-team fact are comparable."""
    if entry["vegas_prob_home"] is None or close["vegas_prob_home"] is None:
        return None
    entry_p, close_p = float(entry["vegas_prob_home"]), float(close["vegas_prob_home"])
    if team_id == entry["away_team_id"]:
        entry_p, close_p = 1 - entry_p, 1 - close_p
    return close_p - entry_p


def _total_delta(entry: dict, close: dict) -> float | None:
    if entry["vegas_total"] is None or close["vegas_total"] is None:
        return None
    return float(close["vegas_total"]) - float(entry["vegas_total"])


def _find_next_game(db: DatabaseManager, team_id: int, after_date: str) -> dict | None:
    rows = db.execute(
        """
        SELECT id, game_date, home_team_id, away_team_id
        FROM mlb_matchups
        WHERE (home_team_id = %s OR away_team_id = %s) AND game_date >= %s
        ORDER BY game_date ASC LIMIT 1
        """,
        (team_id, team_id, after_date),
    )
    if not rows:
        return None
    game = rows[0]
    if (game["game_date"] - _to_date(after_date)).days > _MAX_DAYS_TO_NEXT_GAME:
        return None
    return game


def _to_date(d):
    from datetime import date, datetime
    if isinstance(d, (date,)) and not isinstance(d, datetime):
        return d
    return datetime.fromisoformat(str(d)).date()


def _entry_close(db: DatabaseManager, matchup_id: int, after_ts=None) -> tuple[dict, dict] | None:
    """First snapshot (at/after after_ts if given) and last snapshot for a matchup."""
    rows = db.execute(
        """
        SELECT captured_at, vegas_prob_home, vegas_total, home_team_id, away_team_id
        FROM game_odds_history
        WHERE sport = %s AND matchup_id = %s
        ORDER BY captured_at ASC
        """,
        (_SPORT, matchup_id),
    )
    if after_ts is not None:
        rows = [r for r in rows if r["captured_at"] >= after_ts]
    if len(rows) < 2:
        return None
    entry, close = rows[0], rows[-1]
    if entry["captured_at"] >= close["captured_at"]:
        return None
    return entry, close


def collect_fact_linked_movement(db: DatabaseManager, model_version: str) -> list[dict]:
    """One record per fact that could be linked to a next game with a
    measurable entry->close odds window."""
    facts = db.execute(
        """
        SELECT f.id, f.fact_type, f.team_id, f.player_name, a.published_at
        FROM mlb_beat_facts f
        JOIN mlb_beat_articles a ON a.id = f.article_id
        WHERE f.model_version = %s AND f.fact_type != '_none'
          AND f.team_id IS NOT NULL AND a.published_at IS NOT NULL
        ORDER BY a.published_at ASC
        """,
        (model_version,),
    )

    records = []
    for f in facts:
        game = _find_next_game(db, f["team_id"], str(f["published_at"].date()))
        if game is None:
            continue
        window = _entry_close(db, game["id"], after_ts=f["published_at"])
        if window is None:
            continue
        entry, close = window
        delta_team_prob = _team_prob_delta(entry, close, f["team_id"])
        delta_total = _total_delta(entry, close)
        records.append({
            "fact_id": f["id"],
            "fact_type": f["fact_type"],
            "player_name": f["player_name"],
            "published_at": f["published_at"],
            "matchup_id": game["id"],
            "delta_team_prob": delta_team_prob,
            "delta_total": delta_total,
            "hours_from_publish_to_entry": (entry["captured_at"] - f["published_at"]).total_seconds() / 3600,
            "hours_entry_to_close": (close["captured_at"] - entry["captured_at"]).total_seconds() / 3600,
        })
    return records


def collect_baseline_movement(db: DatabaseManager, team_id: int, start_date: str, end_date: str) -> list[dict]:
    """One record per Orioles game in the window, using the day's own first
    and last snapshot -- the "typical day" comparison population, independent
    of whether any beat-writer fact was linked to that game."""
    games = db.execute(
        """
        SELECT id FROM mlb_matchups
        WHERE (home_team_id = %s OR away_team_id = %s)
          AND game_date >= %s AND game_date <= %s
        """,
        (team_id, team_id, start_date, end_date),
    )
    records = []
    for g in games:
        window = _entry_close(db, g["id"])
        if window is None:
            continue
        entry, close = window
        delta_team_prob = _team_prob_delta(entry, close, team_id)
        delta_total = _total_delta(entry, close)
        records.append({"delta_team_prob": delta_team_prob, "delta_total": delta_total})
    return records


def _mean_abs(xs: list[float | None]) -> float | None:
    vals = [abs(x) for x in xs if x is not None]
    return sum(vals) / len(vals) if vals else None


def report(db: DatabaseManager, model_version: str, team_abbrev: str = "BAL") -> None:
    from db.queries import build_mlb_team_abbrev_cache
    team_id = build_mlb_team_abbrev_cache(db).get(team_abbrev)

    fact_records = collect_fact_linked_movement(db, model_version)
    if not fact_records:
        print("No facts could be linked to a measurable entry->close odds window yet.")
        return

    dates = [r["published_at"] for r in fact_records]
    baseline = collect_baseline_movement(
        db, team_id, str(min(dates).date()), str(max(dates).date())
    )

    print(f"=== MLB Beat-Writer Timing Study -- Stage A (descriptive only) ===")
    print(f"Model version: {model_version}")
    print(f"Facts linked to a measurable next-game odds window: {len(fact_records)}")
    print(f"Baseline games in the same window: {len(baseline)}")
    print()
    print("*** SAMPLE SIZE REALITY CHECK: this is nowhere near a statistical")
    print("*** sample. This run confirms the linking/measurement mechanism")
    print("*** works end-to-end. No conclusion should be drawn from these")
    print("*** numbers -- see CLAUDE.md for the pre-registered minimum sample.")
    print()

    fact_mean = _mean_abs([r["delta_team_prob"] for r in fact_records])
    base_mean = _mean_abs([r["delta_team_prob"] for r in baseline])
    print(f"Avg |team win-prob movement| on fact-linked games:  "
          f"{fact_mean*100:.2f}pp" if fact_mean is not None else "n/a")
    print(f"Avg |team win-prob movement| on baseline games:     "
          f"{base_mean*100:.2f}pp" if base_mean is not None else "n/a")
    print()

    by_type: dict[str, list[dict]] = {}
    for r in fact_records:
        by_type.setdefault(r["fact_type"], []).append(r)
    for ft, recs in sorted(by_type.items()):
        m = _mean_abs([r["delta_team_prob"] for r in recs])
        avg_lag = sum(r["hours_from_publish_to_entry"] for r in recs) / len(recs)
        print(f"  {ft:<22} n={len(recs):<3} avg|delta prob|={m*100:.2f}pp  "
              f"avg hrs publish->first odds snapshot={avg_lag:.1f}" if m is not None
              else f"  {ft:<22} n={len(recs)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="MLB beat-writer timing study (Stage A)")
    parser.add_argument("--model-version", default="beat-extract-deepseek-v2")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    report(db, model_version=args.model_version)
