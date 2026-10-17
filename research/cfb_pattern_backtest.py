"""Historical replay of the frozen CFB pattern-watch triggers (read-only).

Runs the six cfb-pattern-watch-v2 triggers (model/cfb_pattern_watch.py,
docs/cfb-early-season-patterns-study.md) over completed FBS-vs-FBS games in
past seasons, using the closing lines CFBD reports (cfb_historical_game_lines,
canonical reference rows: consensus first, then DraftKings, FanDuel, BetMGM,
Caesars). Nothing is written. This is a backtest of patterns found on 2026
data; it cannot confirm or kill them (the prospective study does that).

Differences from the live study, stated so nobody mistakes one for the other:
  * Conference membership is the one CFBD recorded on the game's line row
    (point-in-time), not cfb_teams.conference (current, post-realignment).
  * T2 (FBS over FCS) cannot run: the history ingest keeps FBS-vs-FBS only.
  * Spread and total bets grade at -110, as in the study. Moneyline bets grade
    at the canonical provider's closing moneyline. A game with no moneyline is
    still eligible for the spread/total triggers; its favourite comes from the
    spread (a pick'em game with no moneyline is skipped).
  * T5's Saturday 5:00-8:29 pm window is read in true US Eastern time. The live
    classifier uses a fixed UTC-4, which is one hour off after DST ends.

Usage:
    python -m research.cfb_pattern_backtest --seasons 2023 2024 2025
"""

from __future__ import annotations

import argparse
import collections
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import load_config
from db.database import DatabaseManager
from model import cfb_pattern_watch as pw

ET = ZoneInfo("America/New_York")
ML_TRIGGERS = {"T1_g5_mid_fav_ml", "T5_sat_evening_g5_fav_ml"}

_SQL = """
WITH canon AS (
  SELECT game_id, market_type, home_value, away_value, provider,
         home_conference, away_conference, home_classification, away_classification
  FROM cfb_historical_game_lines
  WHERE is_canonical_reference AND line_designation = 'historical_reference'
)
SELECT m.id AS matchup_id, m.season, m.season_type, m.week, m.game_date, m.commence_time,
       m.home_score, m.away_score, ht.name AS home, at.name AS away,
       sp.home_value AS home_spread, sp.provider AS spread_provider,
       tt.home_value AS vegas_total,
       ml.home_value AS home_ml, ml.away_value AS away_ml, ml.provider AS ml_provider,
       COALESCE(sp.home_conference, ml.home_conference, tt.home_conference) AS hconf,
       COALESCE(sp.away_conference, ml.away_conference, tt.away_conference) AS aconf,
       lower(COALESCE(sp.home_classification, ml.home_classification, tt.home_classification)) AS hclass,
       lower(COALESCE(sp.away_classification, ml.away_classification, tt.away_classification)) AS aclass
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
LEFT JOIN canon sp ON sp.game_id = m.id AND sp.market_type = 'spread'
LEFT JOIN canon tt ON tt.game_id = m.id AND tt.market_type = 'total'
LEFT JOIN canon ml ON ml.game_id = m.id AND ml.market_type = 'moneyline'
WHERE m.season = ANY(%s) AND m.completed
  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.commence_time IS NOT NULL
ORDER BY m.commence_time, m.id
"""


def _true_eastern_for_classifier(ts: datetime) -> datetime:
    """Shift ts so the classifier's fixed UTC-4 lands on the real Eastern wall clock."""
    off = ts.astimezone(ET).utcoffset()
    return ts + (off + timedelta(hours=4)) if off is not None else ts


def load(db: DatabaseManager, seasons: list[int]) -> tuple[list[dict], collections.Counter]:
    skipped = collections.Counter()
    games = []
    for r in db.execute(_SQL, (seasons,)):
        g = dict(r)
        if g["home_spread"] is None:
            skipped["no spread"] += 1
            continue
        g["home_ml"] = int(g["home_ml"]) if g["home_ml"] is not None else None
        g["away_ml"] = int(g["away_ml"]) if g["away_ml"] is not None else None
        g["ml_missing"] = g["home_ml"] is None or g["away_ml"] is None
        if g["ml_missing"]:
            if g["home_spread"] == 0:
                skipped["pick'em without moneyline"] += 1
                continue
            # placeholder prices only to let classify/grade pick the favourite;
            # moneyline triggers are dropped for these games below.
            g["home_ml"], g["away_ml"] = -110, -110
        g["classifier_time"] = _true_eastern_for_classifier(g["commence_time"])
        games.append(g)
    return games, skipped


def build(games: list[dict]) -> list[dict]:
    rows = []
    for g in games:
        probe = {**g, "commence_time": g["classifier_time"]}
        for trig in pw.classify(probe):
            if trig in ML_TRIGGERS and g["ml_missing"]:
                continue
            gr = pw.grade(trig, g)
            rows.append({"trigger": trig, "matchup_id": g["matchup_id"], "season": g["season"],
                         "season_type": g["season_type"], "week": g["week"],
                         "game_date": str(g["game_date"]), **gr})
    return rows


def line(trig: str, rows: list[dict]) -> str:
    bet = pw.TRIGGERS[trig][0]
    n = len(rows)
    if n == 0:
        return f"  {trig:26s} {bet:19s} n=0"
    w = sum(r["result"] == "won" for r in rows); l = sum(r["result"] == "lost" for r in rows)
    units = sum(r["pnl_units"] for r in rows)
    implied = sum(r["implied_prob"] for r in rows) / n
    dates = len({r["game_date"] for r in rows})
    lo, hi = pw._bootstrap_roi(rows) if dates >= 3 else (float("nan"), float("nan"))
    return (f"  {trig:26s} {bet:19s} n={n:4d} {w}-{l}-{n - w - l:<2d} win {w / max(1, w + l):5.1%} "
            f"(implied {implied:5.1%}) units {units:+7.2f} ROI {roi_fmt(units / n)} "
            f"CI[{roi_fmt(lo)},{roi_fmt(hi)}] dates={dates}")


def roi_fmt(x: float) -> str:
    return f"{x:+6.1%}" if x == x else "  nan "


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", type=int, nargs="+", default=[2023, 2024, 2025])
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url)
    games, skipped = load(db, args.seasons)
    rows = build(games)
    print(f"# CFB pattern backtest ({pw.STUDY_VERSION} triggers, read-only) "
          f"{datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    print(f"games with a closing spread: {len(games)}; skipped {dict(skipped)}; "
          f"no moneyline (ML triggers off): {sum(g['ml_missing'] for g in games)}")
    print("Spread/total at -110; moneyline at canonical close; 95% CI resamples game dates.")
    blocks = [(f"{s} regular season", [r for r in rows if r["season"] == s and r["season_type"] == "regular"])
              for s in args.seasons]
    blocks += [(f"{s} weeks 1-5 only", [r for r in rows if r["season"] == s and r["season_type"] == "regular"
                                         and r["week"] <= 5]) for s in args.seasons]
    blocks += [(f"{s} postseason", [r for r in rows if r["season"] == s and r["season_type"] != "regular"])
               for s in args.seasons]
    blocks.append((f"ALL {min(args.seasons)}-{max(args.seasons)} regular season",
                   [r for r in rows if r["season_type"] == "regular"]))
    for label, sub in blocks:
        print(f"\n== {label} ==")
        for trig in pw.TRIGGERS:
            print(line(trig, [r for r in sub if r["trigger"] == trig]))
    print("\nT2 cannot run: history holds FBS-vs-FBS games only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
