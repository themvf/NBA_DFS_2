"""Report what the CFB signal ledger has actually established, so far.

Answers one question honestly: has any CFB line-movement signal produced
positive closing-line value on the PROSPECTIVE sample, with a game-clustered
confidence interval that excludes zero?

It applies the promotion contract in docs/cfb-signal-research.md rather than
summarising it:

  * ``origin`` cohorts are never pooled. Retrospective replays (the Week 0
    historical pilot) are reported separately and are excluded from every
    verdict -- backtest rule 1.
  * ``signal_version`` cohorts are never pooled. A threshold change is a new
    version, not a continuation -- backtest rule 2.
  * Uncertainty resamples GAMES, not alerts. Two signals on one game are one
    observation of that game's line behaviour -- backtest rule 4.
  * Line CLV is primary; units and ROI are reported but do not carry a
    verdict.
  * Pushes, voids and missing verified closes are counted explicitly and
    never dropped or imputed -- backtest rule 7 and the settlement contract.

Nothing here promotes a signal. The strongest verdict it can return is
CI-EXCLUDES-ZERO, which is one gate of several; the remaining gates
(two scheduling regimes, a second untouched window, manual review) are
reported as outstanding.

Usage:
    python -m research.cfb_week_report
    python -m research.cfb_week_report --season 2026 --through 2026-09-18
    python -m research.cfb_week_report --by-week
"""

from __future__ import annotations

import argparse
from datetime import date

from config import load_config
from db.database import DatabaseManager

# Reused rather than reimplemented: resamples clusters, not rows.
from model.nfl_walking_fade_study import cluster_bootstrap

# Below this many DISTINCT GAMES a bootstrap interval is not reported at all.
# A CI from a handful of correlated clusters is a shape, not a measurement.
MIN_GAMES_FOR_CI = 20

# Mirrors model/line_alerts.py's own floor for quoting a rate.
MIN_SETTLED_FOR_RATE = 30

SPREAD_TOTAL_TYPES = (
    "spread_steam", "total_steam", "spread_walking", "total_walking",
    "key_cross", "price_pressure", "reversal", "reference_led",
)


def load_signals(db: DatabaseManager, season: int, through: date | None) -> list[dict]:
    """One row per CFB alert, carrying its CURRENT grade and game context.

    Joins alert_grades on is_current so a regraded alert reports the grade in
    force now, while the append-only history behind it stays intact.
    """
    return db.execute(
        """
        SELECT la.id,
               la.alert_type,
               la.side,
               COALESCE(la.signal_version, '(unversioned)') AS signal_version,
               la.origin,
               la.matchup_id,
               la.created_at,
               m.game_date,
               m.week,
               m.season_type,
               m.completed,
               m.went_to_overtime,
               ag.outcome,
               ag.line_clv,
               ag.pnl_units,
               ag.close_history_id,
               ag.grading_version
        FROM line_alerts la
        JOIN cfb_matchups m ON m.id = la.matchup_id
        LEFT JOIN alert_grades ag ON ag.alert_id = la.id AND ag.is_current
        WHERE la.sport = 'cfb'
          AND m.season = %s
          AND (%s::date IS NULL OR m.game_date <= %s::date)
        ORDER BY m.game_date, la.created_at, la.id
        """,
        (season, through, through),
    )


def summarise(rows: list[dict]) -> dict:
    """Collapse one cohort. Every row lands in exactly one outcome bucket."""
    games = {r["matchup_id"] for r in rows}

    won = [r for r in rows if r["outcome"] == "won"]
    lost = [r for r in rows if r["outcome"] == "lost"]
    push = [r for r in rows if r["outcome"] == "push"]
    void = [r for r in rows if r["outcome"] == "void"]
    pending = [r for r in rows if r["outcome"] is None]

    decisions = len(won) + len(lost)
    settled = decisions + len(push)

    units = sum(r["pnl_units"] for r in rows if r["pnl_units"] is not None)
    roi = units / decisions if decisions else None

    # Missing verified closes stay missing. They can still carry an ATS
    # outcome; they cannot contribute to the CLV estimate.
    clv_rows = [r for r in rows if r["line_clv"] is not None]
    clv_values = [float(r["line_clv"]) for r in clv_rows]
    clv_games = {r["matchup_id"] for r in clv_rows}

    mean_clv = sum(clv_values) / len(clv_values) if clv_values else None
    beat_close = (
        sum(1 for v in clv_values if v > 0) / len(clv_values) if clv_values else None
    )

    ci = None
    if len(clv_games) >= MIN_GAMES_FOR_CI:
        ci = cluster_bootstrap(clv_values, [r["matchup_id"] for r in clv_rows])

    return {
        "n": len(rows),
        "games": len(games),
        "won": len(won), "lost": len(lost),
        "push": len(push), "void": len(void), "pending": len(pending),
        "decisions": decisions, "settled": settled,
        "units": units, "roi": roi,
        "clv_n": len(clv_values), "clv_games": len(clv_games),
        "no_close": len(rows) - len(clv_values),
        "mean_clv": mean_clv, "clv_ci": ci, "beat_close": beat_close,
        "overtime": sum(1 for r in rows if r["went_to_overtime"]),
    }


def clv_verdict(s: dict) -> str:
    """The only verdict this script is permitted to reach.

    Deliberately cannot return anything resembling "edge": positive CLV with a
    clean interval clears ONE promotion gate. The rest are named in the
    caller's outstanding-gates list.
    """
    if s["clv_n"] == 0:
        return "NO VERIFIED CLOSES — nothing measurable yet"
    if s["clv_games"] < MIN_GAMES_FOR_CI:
        return (
            f"DESCRIPTIVE ONLY — {s['clv_games']} game clusters "
            f"< {MIN_GAMES_FOR_CI} floor, no interval computed"
        )
    low, high = s["clv_ci"]
    if low > 0:
        return "CI EXCLUDES ZERO (positive) — clears the CLV gate only"
    if high < 0:
        return "CI EXCLUDES ZERO (NEGATIVE) — signal runs backwards"
    return "CI INCLUDES ZERO — no directional CLV demonstrated"


def _fmt(value, spec: str = "+.3f", dash: str = "     —") -> str:
    return dash if value is None else format(value, spec)


def print_cohort(title: str, rows: list[dict], *, verdict: bool) -> dict | None:
    if not rows:
        return None
    s = summarise(rows)
    print(f"\n  {title}")
    print(
        f"    observed {s['n']:>4} alerts over {s['games']:>3} games"
        f"   |  settled {s['settled']:>4}  pending {s['pending']:>4}"
    )
    print(
        f"    W-L-P     {s['won']}-{s['lost']}-{s['push']}"
        f"   void {s['void']}   overtime {s['overtime']}"
    )

    if s["decisions"] >= MIN_SETTLED_FOR_RATE:
        print(f"    units    {s['units']:+.3f}   ROI/bet {_fmt(s['roi'], '+.2%')}")
    elif s["decisions"]:
        print(
            f"    units    {s['units']:+.3f} over {s['decisions']} decisions"
            f"   (< {MIN_SETTLED_FOR_RATE} — no rate quoted)"
        )

    print(
        f"    line CLV  mean {_fmt(s['mean_clv'])} pts"
        f"   on {s['clv_n']} alerts / {s['clv_games']} games"
        f"   ({s['no_close']} without a verified close)"
    )
    if s["clv_ci"]:
        low, high = s["clv_ci"]
        print(
            f"              game-clustered 95% CI [{low:+.3f}, {high:+.3f}]"
            f"   beat close {_fmt(s['beat_close'], '.1%')}"
        )
    if verdict:
        print(f"    verdict   {clv_verdict(s)}")
    return s


def hypothesis_status(db: DatabaseManager) -> None:
    rows = db.execute(
        """
        SELECT h.id, h.hypothesis_key, h.version, h.name, h.status,
               h.min_sample_json, h.notes,
               (SELECT COUNT(*) FROM cfb_game_signal_snapshots s
                 WHERE s.hypothesis_id = h.id AND s.qualified_for_tracking) AS tracked,
               (SELECT COUNT(*) FROM cfb_game_signal_snapshots s
                  JOIN cfb_matchups m ON m.id = s.game_id
                 WHERE s.hypothesis_id = h.id AND s.qualified_for_tracking
                   AND m.completed AND m.home_score IS NOT NULL) AS settled
        FROM cfb_hypotheses h
        ORDER BY h.hypothesis_key, h.version
        """
    )
    print(f"\n{'=' * 74}")
    print("REGISTERED HYPOTHESES")
    print(f"{'=' * 74}")
    if not rows:
        print("  none registered — run: python -m research.cfb_hypotheses --register-all")
        return

    # Imported lazily so the ledger section still runs if the registry module
    # is mid-edit.
    from research.cfb_hypotheses import is_evaluable

    for r in rows:
        minimums = r["min_sample_json"] or {}
        floor = minimums.get("prospective_n")
        evaluable = is_evaluable(r["hypothesis_key"], r["version"])
        print(f"\n  {r['hypothesis_key']} {r['version']}  {r['name']}")
        print(f"    status    {r['status']}")
        if not evaluable:
            print("    EVALUATOR  none — definition frozen, not yet scoreable.")
            print("               Preregistration working as intended; the claim")
            print("               was fixed before the data existed to test it.")
        gap = "" if floor is None else f" / {floor} floor"
        print(f"    tracked   {r['tracked']} qualified snapshots, {r['settled']} settled{gap}")
        if floor and r["settled"] < floor:
            print(
                f"    GATE      short by {floor - r['settled']} — "
                "no conclusion permitted at this sample"
            )

        latest = db.execute(
            """
            SELECT evaluation_type, n, wins, losses, pushes, roi, avg_clv,
                   ci_low, ci_high, evaluated_at
            FROM cfb_hypothesis_results
            WHERE hypothesis_id = %s
            ORDER BY evaluated_at DESC LIMIT 1
            """,
            (r["id"],),
        )
        if latest:
            x = latest[0]
            print(
                f"    last eval {x['evaluation_type']} n={x['n']} "
                f"{x['wins']}-{x['losses']}-{x['pushes']}  "
                f"ROI {_fmt(x['roi'], '+.2%')}  avg CLV {_fmt(x['avg_clv'])} pts  "
                f"({x['evaluated_at']:%Y-%m-%d})"
            )


def report(db: DatabaseManager, season: int, through: date | None, by_week: bool) -> dict:
    rows = load_signals(db, season, through)

    print(f"\n{'=' * 74}")
    print(f"CFB SIGNAL LEDGER · season {season}" + (f" · through {through}" if through else ""))
    print(f"{'=' * 74}")
    print("Line CLV is the primary metric. Units and ROI are reported but")
    print("carry no verdict — a winning record at flat CLV is variance.")

    if not rows:
        print("\n  0 CFB alerts found. Either none have fired, or capture is not running.")
        hypothesis_status(db)
        return {"n": 0}

    prospective = [r for r in rows if r["origin"] == "prospective"]
    other = [r for r in rows if r["origin"] != "prospective"]

    print(f"\n{'-' * 74}")
    print("PROSPECTIVE  (the only cohort a promotion decision may use)")
    print(f"{'-' * 74}")
    if not prospective:
        print("\n  none — nothing here can support any decision.")
    else:
        by_version: dict[str, list[dict]] = {}
        for r in prospective:
            by_version.setdefault(r["signal_version"], []).append(r)

        for version, vrows in sorted(by_version.items()):
            print(f"\n  ── signal_version {version} ──")
            print_cohort("ALL TYPES POOLED", vrows, verdict=True)

            by_type: dict[str, list[dict]] = {}
            for r in vrows:
                by_type.setdefault(r["alert_type"], []).append(r)
            for alert_type, trows in sorted(by_type.items()):
                print_cohort(f"· {alert_type}", trows, verdict=True)

    if other:
        print(f"\n{'-' * 74}")
        print("RETROSPECTIVE / REPLAY  (excluded from every verdict, rule 1)")
        print(f"{'-' * 74}")
        by_origin: dict[str, list[dict]] = {}
        for r in other:
            by_origin.setdefault(r["origin"], []).append(r)
        for origin, orows in sorted(by_origin.items()):
            print_cohort(f"origin = {origin}", orows, verdict=False)

    if by_week and prospective:
        print(f"\n{'-' * 74}")
        print("PER-WEEK DIAGNOSTIC  (rule 5: a diagnostic, not a strategy)")
        print(f"{'-' * 74}")
        weeks: dict[tuple, list[dict]] = {}
        for r in prospective:
            weeks.setdefault((r["season_type"], r["week"]), []).append(r)
        header = "  week                n  games     W-L-P    units   meanCLV"
        print()
        print(header)
        for (season_type, week), wrows in sorted(weeks.items()):
            s = summarise(wrows)
            label = f"{season_type} wk {week}"
            record = f"{s['won']}-{s['lost']}-{s['push']}"
            print(
                f"  {label:<16}{s['n']:>5}{s['games']:>7}{record:>10}"
                f"{s['units']:>+9.2f}{_fmt(s['mean_clv']):>10}"
            )

    print(f"\n{'-' * 74}")
    print("OUTSTANDING PROMOTION GATES  (all required, docs/cfb-signal-research.md)")
    print(f"{'-' * 74}")
    print("  [ ] definition frozen before the evaluation window")
    print("  [ ] mapping accuracy 100% on accepted provider events")
    print("  [ ] capture + close health met throughout the window")
    print("  [ ] results span >= 2 distinct scheduling regimes")
    print("  [ ] directional line CLV, game-clustered CI excluding zero")
    print("  [ ] ROI after a conservative slippage/execution-cost sensitivity")
    print("  [ ] a SECOND untouched window confirms it")
    print("\n  Until every box is ticked the terminal reads NO EDGE CLAIM,")
    print("  regardless of nominal ROI. This script ticks none of them for you.")

    hypothesis_status(db)
    return {"n": len(rows), "prospective": len(prospective)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=date.today().year)
    parser.add_argument("--through", type=date.fromisoformat, default=None,
                        help="only games on or before this date (YYYY-MM-DD)")
    parser.add_argument("--by-week", action="store_true",
                        help="per-week diagnostic breakdown")
    args = parser.parse_args()

    db = DatabaseManager(load_config().database_url)
    report(db, args.season, args.through, args.by_week)


if __name__ == "__main__":
    main()
