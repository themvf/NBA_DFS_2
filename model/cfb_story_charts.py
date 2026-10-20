"""The nine reader charts (model/story_charts.py) for college football, season to date.

Games are the FBS-vs-FBS regular-season population of the scenario charts;
2023-25 comes from cfb_pattern_history. The late-flip chart reads only scoring
plays and keeps a game only when its accepted scoring plays add up to its
regulation score (and quarter line scores), the same check the comeback charts
use, because CFBD's running score on plays can be wrong for whole drives.

    python -m model.cfb_story_charts --out-dir artifacts/cfb_pattern_watch/charts
"""

from __future__ import annotations

import argparse
from pathlib import Path

from model import story_charts as sc

DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "charts"
LATE_MINUTE = 55.0   # five minutes left in regulation


def late_score(steps: list[tuple[float, int, int]]) -> tuple[int, int]:
    """(home, away) after the last accepted score at or before 55:00 elapsed."""
    before = [(a, b) for m, a, b in steps if m <= LATE_MINUTE]
    return before[-1] if before else (0, 0)


def late_flip_games(base_games: list[dict], comeback_games: list[dict], plays: list[dict]) -> tuple[list[dict], dict]:
    """Games with a verified score at 5:00 left; (games, {'checked', 'excluded'})."""
    from collections import defaultdict

    from model.cfb_team_charts import _steps_match_lines, score_steps

    lines = {int(g["matchup_id"]): g for g in base_games}
    by_game: dict = defaultdict(list)
    for p in plays:
        by_game[p["game_id"]].append(p)
    out, checked, excluded = [], 0, 0
    for g in comeback_games:
        base = lines.get(int(g["game_id"]))
        if base is None or base.get("home_spread") is None:
            continue
        # The same population as every other chart: FBS against FBS, regular season.
        if base.get("hclass") != "fbs" or base.get("aclass") != "fbs" or (base.get("season_type") or "regular") != "regular":
            continue
        checked += 1
        reg = (g.get("reg_home"), g.get("reg_away"))
        if reg[0] is None or reg[1] is None:
            if g.get("overtime"):
                excluded += 1
                continue
            reg = (g["home_score"], g["away_score"])
        steps, _ = score_steps(by_game.get(g["game_id"], []), g["home_team_id"])
        if not _steps_match_lines(steps, g, True, tuple(reg)):
            excluded += 1
            continue
        h, a = late_score(steps)
        out.append({"home": g["home"], "away": g["away"], "week": g["week"],
                    "home_spread": float(base["home_spread"]), "vegas_total": base.get("vegas_total"),
                    "home_score": g["home_score"], "away_score": g["away_score"], "late_home": h, "late_away": a})
    return out, {"checked": checked, "excluded": excluded}


def build_charts(db, out_dir: Path) -> dict[str, bool]:
    from model import cfb_pattern_csv as cc
    from model import cfb_pattern_history as history
    from model import cfb_pattern_measures as measures
    from model import cfb_scenario_charts as scen
    from model import cfb_team_charts as tc

    out_dir = Path(out_dir)
    games = measures.load_base_rate_games(db)
    rec = measures._ml_eligible(games)
    prior_games = history.load_prior_games(db)
    rows = cc.market_breakdown_table(measures.market_breakdown(games))
    prior_rows = history.prior_breakdown_table(db)
    label = "2023-25"
    o = lambda n: out_dir / f"story_{n}.png"   # noqa: E731
    made = {
        "story_every_game_spread.png": sc.margin_chart(rec, o("every_game_spread"), "college"),
        "story_every_game_total.png": sc.total_chart(rec, o("every_game_total"), "college"),
        "story_line_miss.png": sc.line_miss_chart(rec, o("line_miss"), "college"),
        "story_bankroll.png": sc.bankroll_chart(rec, o("bankroll"), "college"),
        "story_what_changed.png": sc.slope_chart(rows, prior_rows, o("what_changed"), "college", label,
                                                 exclude=scen.HOME_LENS | {"By halftime score"}),
        "story_calibration.png": sc.calibration_chart(rec, measures._ml_eligible(prior_games), o("calibration"),
                                                      "college", label),
        "story_team_calendar.png": sc.team_calendar_chart(rec, o("team_calendar"), "college", extra="Games against FBS opponents only."),
        "story_team_quadrant.png": sc.team_quadrant_chart(rec, o("team_quadrant"), "College", min_games=4,
                                                          n_labels=16),
        "story_outcomes.png": sc.outcomes_chart(rec, measures.SPREAD_BANDS, o("outcomes"), "College"),
    }
    comeback_games = tc._plain(db.execute(tc._COMEBACK_GAMES_SQL, {"season": tc.SEASON}))
    for g in comeback_games:
        for k in ("home_score", "away_score", "home_team_id", "away_team_id", "week", "reg_home", "reg_away"):
            g[k] = None if g[k] is None else int(g[k])
        for k in ("home_line", "away_line"):
            g[k] = [int(v) for v in g[k]] if isinstance(g.get(k), list) else None
    plays = tc._plain(db.execute(tc._COMEBACK_PLAYS_SQL, {"season": tc.SEASON})) if comeback_games else []
    flips, _audit = late_flip_games(games, comeback_games, plays)
    made["story_late_flips.png"] = sc.late_flips_chart(flips, o("late_flips"), "college")
    return made


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    for name, ok in build_charts(db, Path(args.out_dir)).items():
        print(f"{Path(args.out_dir) / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
