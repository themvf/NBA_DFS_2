"""Score Showdown ownership variants against imported contests.

    python -m research.nfl_showdown_ownership_eval \\
        --contest 195786073=path/to/contest-standings-195786073.csv \\
        --contest 195943238=path/to/contest-standings-195943238.csv

Each contest must already be imported (nfl_dfs_field_contests, attached to a
slate upload). Variants are defined in VARIANTS below; every variant is also
scored with ACTUAL ownership (a perfect forecast) and with none, because the
question is how much of the available gain a forecast captures, not its score
in isolation. Read-only: SELECTs and the standings file.

v4 reads the game script (favorite / underdog) the way the page does, from the
game's moneyline (nfl_matchups, then nfl_season_games market, then quoted), but
for a past game that is the line as stored NOW, not a frozen pregame read.
`--compare A B` prints B's projected points minus A's at A's real-field
duplication, interpolated along B's leverage curve (each a mean over --seeds).
"""

from __future__ import annotations

import argparse
import collections
import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_field_audit import normalize_name
from model.nfl_dfs_field_structure import parse_lineup
from model.nfl_showdown_ownership_eval import (
    VERSION, fit_metrics, portfolio, prior_showdown, prior_showdown_v4, score_portfolio,
)

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

#: name -> (slate, game script) -> (flex %, captain %). v2 is what shipped before v3.
VARIANTS = {
    "v2": lambda slate, fav, dog: prior_showdown(slate, value_exponent=1.5),
    "v3": lambda slate, fav, dog: prior_showdown(slate, value_exponent=0.5),
    "v4": lambda slate, fav, dog: prior_showdown_v4(slate, fav, dog)[:2],
}
LEVERAGE = (0.5, 1.0)
SEEDS = (1, 2, 3)
_TEAM_ALIASES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}


def _team_key(team) -> str:
    key = str(team or "").strip().upper()
    return _TEAM_ALIASES.get(key, key)


def load_slate(db: PipelineDatabase, contest_id: str) -> tuple[int, pd.DataFrame, str | None, str | None]:
    contest = db.execute("SELECT season, week, slate_upload_id FROM nfl_dfs_field_contests WHERE contest_id = %s", (contest_id,))
    if not contest or not contest[0]["slate_upload_id"]:
        sys.exit(f"contest {contest_id} is not imported or has no slate attached")
    season, week, upload = contest[0]["season"], contest[0]["week"], contest[0]["slate_upload_id"]
    slate = pd.DataFrame([dict(r) for r in db.execute(
        """SELECT normalized_name, name, position, team, salary, captain_salary, avg_fpts_dk, dk_status, is_out, our_proj
           FROM nfl_dfs_slate_players WHERE upload_id = %s""", (upload,))])
    own = {r["normalized_name"]: r["drafted_by_slot"] or {} for r in db.execute(
        "SELECT normalized_name, drafted_by_slot FROM nfl_dfs_field_ownership WHERE contest_id = %s", (contest_id,))}
    slate["flex_act"] = slate["normalized_name"].map(lambda n: float(own.get(n, {}).get("FLEX", 0) or 0))
    slate["cpt_act"] = slate["normalized_name"].map(lambda n: float(own.get(n, {}).get("CPT", 0) or 0))
    slate = slate.drop_duplicates("normalized_name")
    slate = slate[~slate["is_out"].astype(bool)].reset_index(drop=True)
    proj, avg = slate["our_proj"], slate["avg_fpts_dk"]
    # The number the field drafts on: the same 60/40 blend the prior uses.
    pts = np.where(proj.notna() & avg.notna(), 0.6 * proj + 0.4 * avg, proj.fillna(avg)).astype(float)
    slate["pts"] = np.where(np.isnan(pts) | (pts <= 0), 0.0, pts)
    # Portfolio building needs a strictly positive objective for every player it may pick.
    slate["pts"] = slate["pts"].clip(lower=0.0)
    favorite, underdog = game_script(db, season, week, slate["team"].dropna().unique())
    return int(week), slate, favorite, underdog


def game_script(db: PipelineDatabase, season, week, slate_teams) -> tuple[str | None, str | None]:
    """(favorite, underdog) in the slate's own team codes, as actions.ts resolves it."""
    if season is None or week is None or len(slate_teams) != 2:
        return None, None
    by_key = {_team_key(t): str(t) for t in slate_teams}
    for g in db.execute(
            """SELECT h.abbreviation AS home, a.abbreviation AS away,
                      COALESCE(m.home_ml, g.market_home_ml, g.quoted_home_ml) AS home_ml,
                      COALESCE(m.away_ml, g.market_away_ml, g.quoted_away_ml) AS away_ml
               FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id
               JOIN nfl_teams a ON a.team_id = g.away_team_id
               LEFT JOIN nfl_matchups m ON m.id = g.matchup_id AND m.home_ml IS NOT NULL AND m.away_ml IS NOT NULL
               WHERE g.season = %s AND g.week = %s AND g.game_type = 'REG'""", (season, week)):
        home, away = by_key.get(_team_key(g["home"])), by_key.get(_team_key(g["away"]))
        if not home or not away or home == away:
            continue
        if g["home_ml"] is None or g["away_ml"] is None or float(g["home_ml"]) == float(g["away_ml"]):
            return None, None
        favorite = home if float(g["home_ml"]) < float(g["away_ml"]) else away
        return favorite, (away if favorite == home else home)
    return None, None


def real_lineup_counts(path: Path) -> tuple[collections.Counter, int]:
    counts: collections.Counter = collections.Counter()
    total = 0
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            if len(row) > 5 and row[5].strip() and row[0].strip().isdigit():
                parsed = parse_lineup(row[5])
                cpt = [normalize_name(n) for s, n in parsed if s == "CPT"]
                flex = sorted(normalize_name(n) for s, n in parsed if s == "FLEX")
                if len(parsed) == 6 and len(cpt) == 1 and len(flex) == 5:
                    counts[(cpt[0], tuple(flex))] += 1
                    total += 1
    return counts, total


def points_at_equal_duplication(rows: pd.DataFrame, base: str, other: str) -> list[dict]:
    """For each of `base`'s leverage points, `other`'s projected points at the same
    share_duplicated (linear interpolation along `other`'s leverage curve; NaN
    outside its range) minus `base`'s."""
    a = rows[rows["ownership"] == base].sort_values("k")
    b = rows[rows["ownership"] == other].sort_values("share_duplicated")
    out = []
    for _, r in a.iterrows():
        x, xs, ys = r["share_duplicated"], b["share_duplicated"].to_numpy(), b["proj_pts"].to_numpy()
        inside = len(xs) > 1 and xs.min() <= x <= xs.max()
        other_pts = float(np.interp(x, xs, ys)) if inside else float("nan")
        out.append({"k": r["k"], "share_duplicated": x, f"{base} proj_pts": r["proj_pts"],
                    f"{other} proj_pts at equal dup": other_pts, "delta": other_pts - r["proj_pts"]})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contest", action="append", required=True, metavar="ID=PATH")
    parser.add_argument("--variants", nargs="+", default=list(VARIANTS), choices=list(VARIANTS))
    parser.add_argument("--leverage", nargs="+", type=float, default=list(LEVERAGE))
    parser.add_argument("--seeds", nargs="+", type=int, default=list(SEEDS))
    parser.add_argument("--compare", nargs=2, metavar=("BASE", "OTHER"),
                        help="report OTHER's projected points minus BASE's at BASE's duplication")
    args = parser.parse_args()
    db = PipelineDatabase(load_config().database_url)

    fit_rows, frontier_rows, compare_rows = [], [], []
    for spec in args.contest:
        contest_id, _, path = spec.partition("=")
        week, slate, favorite, underdog = load_slate(db, contest_id)
        counts, total = real_lineup_counts(Path(path))
        print(f"week {week} showdown {contest_id}: pool {len(slate)}, {total:,} real lineups, "
              f"{len(counts):,} distinct, favorite {favorite or 'unknown'}   [{VERSION}]")
        inputs = {name: VARIANTS[name](slate, favorite, underdog) for name in args.variants}
        inputs["ACTUAL ownership (perfect forecast)"] = (slate["flex_act"].to_numpy(float), slate["cpt_act"].to_numpy(float))
        zero = np.zeros(len(slate))
        for name, (flex, cap) in inputs.items():
            if not name.startswith("ACTUAL"):
                fit_rows.append({"contest": contest_id, "week": week, "variant": name, **fit_metrics(slate, flex, cap)})
        contest_rows = []
        for leverage in args.leverage:
            for name, (flex, cap) in {**inputs, "none": (zero, zero)}.items():
                scored = [score_portfolio(slate, portfolio(slate, flex, cap, leverage, seed), counts) for seed in args.seeds]
                contest_rows.append({"contest": contest_id, "week": week, "k": leverage, "ownership": name,
                                     **{key: float(np.mean([s[key] for s in scored])) for key in
                                        ("proj_pts", "share_duplicated", "mean_real_copies")}})
        frontier_rows += contest_rows
        if args.compare:
            for row in points_at_equal_duplication(pd.DataFrame(contest_rows), *args.compare):
                compare_rows.append({"contest": contest_id, "week": week, **row})
    print("\nForecast accuracy (lower MAE better; rho_chalk = rank among players the field used)")
    print(pd.DataFrame(fit_rows).round(3).to_string(index=False))
    print(f"\nPortfolio trade-off, mean of seeds {tuple(args.seeds)}, 80 lineups each "
          "(compare proj_pts at similar share_duplicated)")
    print(pd.DataFrame(frontier_rows).round(3).to_string(index=False))
    if compare_rows:
        print(f"\n{args.compare[1]} minus {args.compare[0]}: projected points at equal real-field duplication")
        print(pd.DataFrame(compare_rows).round(3).to_string(index=False))


if __name__ == "__main__":
    main()
