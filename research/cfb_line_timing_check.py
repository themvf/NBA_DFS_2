"""Step 1 of cfb-market-weight-v1: what time does a CFBD line represent?

CFBD's /lines payload carries no timestamp.  For 2026 games we also hold our
own timestamped quotes (game_odds_history), so CFBD's value can be compared
with our first and last pregame capture of the same book.  Only 2026 line
values are read; no 2026 score is touched.  The decision rule is fixed in
docs/cfb-market-weight-study.md before this runs.

    python -m research.cfb_line_timing_check [--output PATH]

Needs CFBD_API_KEY (GitHub secret) and DATABASE_URL.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from ingest.cfb_history import fetch_cfbd

VERSION = "cfb-line-timing-check-v1"
SEASON = 2026
SETTLE_DAYS = 2
MIN_MOVED_GAMES = 50
CLOSE_SHARE = 0.70
OPEN_SHARE_MAX = 0.15
# CFBD provider name -> our book key, or the median across all our books.
PROVIDER_BOOK = {
    "draftkings": "draftkings",
    "william hill (new jersey)": "williamhill_us",
    "consensus": "__median__",
}
DEFAULT_OUTPUT = Path("artifacts/cfb-market-weight-v1/line-timing-check.json")


def classify(cfbd_value: float | None, series: list[tuple[datetime, float]]) -> str | None:
    """Compare one CFBD value with our pregame series (sorted by time)."""
    if cfbd_value is None or not series:
        return None
    opened, closed = series[0][1], series[-1][1]
    if opened == closed:
        return "no_move"
    if cfbd_value == closed:
        return "close_only"
    if cfbd_value == opened:
        return "open_only"
    return "neither"


def latest_match_hours(cfbd_value: float | None, series: list[tuple[datetime, float]],
                       kickoff: datetime) -> float | None:
    """Hours before kickoff of the latest capture whose value equals CFBD's."""
    if cfbd_value is None:
        return None
    hits = [at for at, value in series if value == cfbd_value]
    return round((kickoff - max(hits)).total_seconds() / 3600, 2) if hits else None


def verdict(counts: Counter) -> dict:
    moved = counts["close_only"] + counts["open_only"] + counts["neither"]
    if moved == 0:
        return {"moved": 0, "label": "INDETERMINATE"}
    close, opened = counts["close_only"] / moved, counts["open_only"] / moved
    if close >= CLOSE_SHARE and opened <= OPEN_SHARE_MAX:
        label = "CLOSE_LIKE"
    elif opened >= CLOSE_SHARE and close <= OPEN_SHARE_MAX:
        label = "OPEN_LIKE"
    else:
        label = "INDETERMINATE"
    return {"moved": moved, "close_share": round(close, 4), "open_share": round(opened, 4), "label": label}


def _series_by_book(rows) -> dict:
    """{(matchup_id, book, market): [(captured_at, value)]}, plus the cross-book median."""
    by_capture: dict = defaultdict(lambda: defaultdict(list))
    series: dict = defaultdict(list)
    for row in rows:
        for market in ("spread", "total"):
            value = row[market]
            if value is None:
                continue
            series[(row["matchup_id"], row["book"], market)].append((row["captured_at"], float(value)))
            by_capture[(row["matchup_id"], market)][row["captured_at"]].append(float(value))
    for (matchup_id, market), captures in by_capture.items():
        for at in sorted(captures):
            series[(matchup_id, "__median__", market)].append((at, float(statistics.median(captures[at]))))
    for key in series:
        series[key].sort()
    return series


def run(db, lines: list[dict], now: datetime) -> dict:
    games = {
        int(r["cfbd_game_id"]): dict(r) for r in db.execute(
            """SELECT id, cfbd_game_id, commence_time FROM cfb_matchups
               WHERE season=%s AND cfbd_game_id IS NOT NULL
                 AND commence_time < %s - make_interval(days => %s)""",
            (SEASON, now, SETTLE_DAYS))
    }
    rows = db.execute(
        """SELECT h.matchup_id, h.captured_at, b.key AS book,
                  (b.value->>'spread_home')::float AS spread,
                  (b.value->>'total_line')::float AS total
           FROM game_odds_history h
           JOIN cfb_matchups m ON m.id = h.matchup_id
           CROSS JOIN LATERAL jsonb_each(h.books) b
           WHERE h.sport='cfb' AND m.season=%s AND h.books IS NOT NULL
             AND h.captured_at < m.commence_time
             AND m.commence_time < %s - make_interval(days => %s)""",
        (SEASON, now, SETTLE_DAYS))
    series = _series_by_book(rows)

    counts: dict = defaultdict(Counter)
    hours: dict = defaultdict(list)
    sign_flip: Counter = Counter()
    providers_seen: Counter = Counter()
    unmapped_games = 0
    for game in lines:
        matchup = games.get(int(game["id"])) if game.get("id") is not None else None
        if matchup is None:
            unmapped_games += 1
            continue
        for line in game.get("lines") or []:
            provider = str(line.get("provider") or "unknown").strip().lower()
            providers_seen[provider] += 1
            book = PROVIDER_BOOK.get(provider)
            if book is None:
                continue
            checks = (
                ("spread_reference", "spread", line.get("spread")),
                ("spread_open", "spread", line.get("spreadOpen")),
                ("total_reference", "total", line.get("overUnder")),
                ("total_open", "total", line.get("overUnderOpen")),
            )
            for label, market, raw in checks:
                value = float(raw) if raw is not None else None
                ours = series.get((matchup["id"], book, market), [])
                outcome = classify(value, ours)
                if outcome is None:
                    counts[(provider, label)]["no_series_or_value"] += 1
                    continue
                counts[(provider, label)][outcome] += 1
                hit = latest_match_hours(value, ours, matchup["commence_time"])
                if hit is not None:
                    hours[(provider, label)].append(hit)
                if label == "spread_reference" and value is not None and ours:
                    # A home/away sign mismatch would read as 'neither' everywhere.
                    sign_flip["same" if value == ours[-1][1] else
                              "flipped" if -value == ours[-1][1] else "other"] += 1

    table = {}
    for (provider, label), c in sorted(counts.items()):
        table[f"{provider}|{label}"] = {
            "counts": dict(c), **verdict(c),
            "latest_match_hours_median": (round(statistics.median(hours[(provider, label)]), 2)
                                          if hours[(provider, label)] else None),
            "games_with_a_matching_capture": len(hours[(provider, label)]),
        }

    dk = table.get("draftkings|spread_reference", {})
    basis = "draftkings" if dk.get("moved", 0) >= MIN_MOVED_GAMES else "consensus"
    decision = table.get(f"{basis}|spread_reference", {"label": "INDETERMINATE", "moved": 0})
    return {
        "version": VERSION, "season": SEASON, "run_at": now.isoformat(),
        "settle_days": SETTLE_DAYS, "cfbd_games": len(lines), "cfbd_games_unmapped": unmapped_games,
        "settled_matchups": len(games), "providers_seen": dict(providers_seen),
        "spread_sign_check_vs_close": dict(sign_flip),
        "rule": {"min_moved_games": MIN_MOVED_GAMES, "close_share": CLOSE_SHARE,
                 "open_share_max": OPEN_SHARE_MAX},
        "decision_basis": basis, "decision": decision.get("label", "INDETERMINATE"),
        "decision_detail": decision, "table": table,
        "limits": [
            "Our first capture is not the true market open; The Odds API lists most CFB games 2-5 days ahead.",
            "2026 provider behaviour may differ from the 2022 consensus feed and 2023-2025 DraftKings rows.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    api_key = os.getenv("CFBD_API_KEY", "")
    if not api_key:
        raise SystemExit("CFBD_API_KEY is required")
    with tempfile.TemporaryDirectory() as cache:
        lines = fetch_cfbd("lines", api_key=api_key, season=SEASON, cache_dir=Path(cache), use_cache=False)
    db = DatabaseManager(load_config().database_url or "", initialize_schema=False)
    report = run(db, lines, datetime.now(timezone.utc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in (
        "decision", "decision_basis", "decision_detail", "providers_seen",
        "spread_sign_check_vs_close", "cfbd_games", "cfbd_games_unmapped", "settled_matchups")},
        indent=2, default=str))
    for key, row in report["table"].items():
        print(f"{key:45s} {row.get('label', ''):14s} moved={row.get('moved', 0):4d} "
              f"close={row.get('close_share')} open={row.get('open_share')} "
              f"match_h={row['latest_match_hours_median']} {row['counts']}")


if __name__ == "__main__":
    main()
