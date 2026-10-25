"""Reader chart: what a win model built on offensive yards alone says about a day's games, beside the market.

An experiment, not a forecast. Each team's average offensive yards per game (pass and run plays, regular season,
games before this one) is the only input:

    p_home = 1 / (1 + exp(-b * (home_ypg - away_ypg)))

No home-field term, no defense, no injuries. The one number b is fitted on 2023-2025 games from week 4 on (each
team has at least three prior games), and the same seasons give the yardstick: Brier score of this model against
the betting market's closing no-vig moneyline and a coin flip. 2026 weeks 4 onward to the day before the slate are
scored out of sample.

  nfl_take_yards_win_prob_<date>.png   one row per game: home team's win chance from the market (grey), our pick'em
                                       model (blue) and the yards-only model (orange), with each team's yards a game

    python -m model.nfl_yards_win_prob_chart --date 2026-10-25 --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_win_prob_chart import ET, home_win  # noqa: E402
from model.story_charts import WIDTH_IN, _finish  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
FIT_SEASONS = (2023, 2024, 2025)
SEASON = 2026
MIN_PRIOR_GAMES = 3
TEAM_ALIAS = {"LA": "LAR"}
MARKET_COLOUR, MODEL_COLOUR, YARDS_COLOUR = "#8a8a8a", "#2a78d6", "#e3833b"
PBP_COLS = ("game_id", "season_type", "posteam", "play_type", "yards_gained")


def _t(team: str) -> str:
    return TEAM_ALIAS.get(team, team)


# ── pure computations ────────────────────────────────────────────────────────
def game_yards(pbp: list[dict]) -> dict[str, dict[str, float]]:
    """game_id -> {team: offensive yards on pass and run plays} (regular season)."""
    out: dict[str, dict[str, float]] = collections.defaultdict(lambda: collections.defaultdict(float))
    for p in pbp:
        if p.get("season_type") != "REG" or p.get("play_type") not in ("pass", "run") or not p.get("posteam"):
            continue
        y = p.get("yards_gained")
        if isinstance(y, (int, float)) and not math.isnan(y):
            out[p["game_id"]][_t(p["posteam"])] += y
    return out


def pregame_rows(schedule: list[dict], yards: dict[str, dict[str, float]]) -> list[dict]:
    """Each game with both teams' average yards a game over their earlier games that season (point in time).

    Games are taken in week order; a game's own yards join the averages only after it is recorded, so nothing from
    the game or later reaches its inputs. A game without play-by-play adds nothing to the averages."""
    out = []
    tot: dict[tuple[int, str], list[float]] = collections.defaultdict(list)
    for g in sorted(schedule, key=lambda g: (g["season"], g["week"], g["game_id"])):
        s, h, a = g["season"], _t(g["home_team"]), _t(g["away_team"])
        hy, ay = tot[(s, h)], tot[(s, a)]
        out.append({**g, "home": h, "away": a, "home_n": len(hy), "away_n": len(ay),
                    "home_ypg": sum(hy) / len(hy) if hy else None, "away_ypg": sum(ay) / len(ay) if ay else None})
        gy = yards.get(g["game_id"])
        if gy and h in gy and a in gy:
            hy.append(gy[h])
            ay.append(gy[a])
    return out


def usable(r: dict) -> bool:
    return min(r["home_n"], r["away_n"]) >= MIN_PRIOR_GAMES and r["home_ypg"] is not None


def predict(b: float, home_ypg: float, away_ypg: float) -> float:
    return 1 / (1 + math.exp(-b * (home_ypg - away_ypg)))


def fit_slope(rows: list[dict]) -> float:
    """Maximum-likelihood b for a logistic model with one input and no intercept (Newton's method)."""
    pts = [(r["home_ypg"] - r["away_ypg"], 1.0 if r["home_score"] > r["away_score"] else 0.0)
           for r in rows if r["home_score"] != r["away_score"]]
    b = 0.0
    for _ in range(50):
        grad = sum(x * (y - 1 / (1 + math.exp(-b * x))) for x, y in pts)
        hess = sum(x * x * (p := 1 / (1 + math.exp(-b * x))) * (1 - p) for x, y in pts)
        step = grad / hess if hess else 0.0
        b += step
        if abs(step) < 1e-10:
            break
    return b


def scores(b: float, rows: list[dict]) -> dict:
    """Brier score of the yards model, the market's no-vig moneyline and a coin flip on the same decided games."""
    n, yb, mb, yw, mw = 0, 0.0, 0.0, 0, 0
    for r in rows:
        if r["home_score"] == r["away_score"] or r.get("home_moneyline") is None:
            continue
        won = 1.0 if r["home_score"] > r["away_score"] else 0.0
        py = predict(b, r["home_ypg"], r["away_ypg"])
        pm = home_win(r["home_moneyline"], r["away_moneyline"])
        n += 1
        yb += (py - won) ** 2
        mb += (pm - won) ** 2
        yw += (py > 0.5) == (won == 1.0)
        mw += (pm > 0.5) == (won == 1.0)
    if not n:
        return {"n": 0}
    return {"n": n, "yards_brier": yb / n, "market_brier": mb / n, "coin_brier": 0.25,
            "yards_right": yw / n, "market_right": mw / n}


# ── chart ────────────────────────────────────────────────────────────────────
def _pick(away: str, home: str, p_home: float) -> tuple[str, float]:
    if abs(p_home - 0.5) < 0.005:
        return ("Even", 0.5)   # a coin flip favors nobody, so it is never a different winner
    return (home, p_home) if p_home >= 0.5 else (away, 1 - p_home)


def yards_chart(games: list[dict], path: Path, day_label: str, b: float, fit: dict, oos: dict) -> bool:
    """A grid: one row per game, one column per model, each cell the team that model favors and its chance."""
    if not games:
        return False
    rows = []
    for g in games:
        mk = _pick(g["away"], g["home"], g["market_home"])
        md = _pick(g["away"], g["home"], g["model_home"]) if g.get("model_home") is not None else None
        yd = _pick(g["away"], g["home"], g["yards_home"])
        rows.append({**g, "mk": mk, "md": md, "yd": yd, "flip": yd[0] not in (mk[0], "Even"),
                     "gap": abs(g["yards_home"] - g["market_home"])})
    rows.sort(key=lambda r: (not r["flip"], -r["gap"]))
    n_flip = sum(r["flip"] for r in rows)

    height = 0.42 * len(rows) + 2.9
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_axis_off()
    ax.set_xlim(0, 100)
    ax.set_ylim(len(rows) - 0.4, -1.3)
    cols = [("Betting market", 24, MARKET_COLOUR, "mk"), ("Our pick'em model", 46, MODEL_COLOUR, "md"),
            ("Yards only", 68, YARDS_COLOUR, "yd")]
    width = 19
    for label, x, colour, _ in cols:
        ax.text(x, -0.95, label, ha="left", va="center", fontsize=8.2, color=colour, fontweight="bold")
    ax.text(0, -0.95, "Game", ha="left", va="center", fontsize=8.2, color=INK, fontweight="bold")
    ax.text(90, -0.95, "Yards a game", ha="left", va="center", fontsize=8.2, color=INK, fontweight="bold")
    for y, r in enumerate(rows):
        if r["flip"]:
            ax.add_patch(plt.Rectangle((-1, y - 0.46), 102, 0.92, color=YARDS_COLOUR, alpha=0.10, lw=0, zorder=0))
        ax.text(0, y, f"{r['away']} @ {r['home']}", ha="left", va="center", fontsize=8, color=INK)
        for _, x, colour, key in cols:
            pick = r[key]
            if pick is None:
                ax.text(x, y, "n/a", ha="left", va="center", fontsize=7.5, color=INK_2)
                continue
            team, p = pick
            ax.add_patch(plt.Rectangle((x, y - 0.3), width, 0.6, color="#e6e4df", lw=0, zorder=1))
            ax.add_patch(plt.Rectangle((x, y - 0.3), width * (p - 0.5) / 0.5, 0.6, color=colour, alpha=0.85,
                                       lw=0, zorder=2))
            ax.text(x + 0.6, y, f"{team} {p:.0%}", ha="left", va="center", fontsize=7.8, color=INK,
                    fontweight="bold", zorder=3)
        ax.text(90, y, f"{r['away']} {r['away_ypg']:.0f} / {r['home']} {r['home_ypg']:.0f}", ha="left",
                va="center", fontsize=7, color=INK_2)
    if n_flip:
        ax.text(0, len(rows) - 0.1, f"Shaded rows: yards alone picks a different winner than the market "
                f"({n_flip} of {len(rows)} games).", ha="left", va="top", fontsize=7, color=INK_2)
    fig.tight_layout()
    per100 = predict(b, 100, 0) - 0.5
    sub = (f"{day_label}. Each cell: the team that model favors and its chance to win (bar fills from 50% to 100%).\n"
           f"Yards only = each team's offensive yards a game this season, nothing else. 100 more yards a game than "
           f"the opponent is worth {per100 * 100:+.0f} points of win chance.\n"
           f"Track record, 2023-25 ({fit['n']} games): yards alone picked the winner {fit['yards_right']:.0%} of the "
           f"time, the market {fit['market_right']:.0%}. This season ({oos.get('n', 0)} games) it has been close to "
           f"a coin flip.\nAn experiment, not a forecast.")
    return _finish(fig, path, "Who wins? Market vs our model vs yards alone", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_pbp(season: int) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in PBP_COLS, compression="gzip", low_memory=False)
    return frame.to_dict("records")


def load_schedule(seasons: tuple[int, ...]) -> list[dict]:
    import pandas as pd

    from ingest.ff_independent import NFLVERSE_SCHEDULE_URL

    f = pd.read_csv(NFLVERSE_SCHEDULE_URL)
    f = f[f.season.isin(seasons) & (f.game_type == "REG")]
    rows = f[["game_id", "season", "week", "gameday", "home_team", "away_team", "home_score", "away_score",
              "home_moneyline", "away_moneyline"]].to_dict("records")
    for r in rows:
        for k in ("home_score", "away_score", "home_moneyline", "away_moneyline"):
            if isinstance(r[k], float) and math.isnan(r[k]):
                r[k] = None
    return rows


def build_all(out_dir: Path, day: dt.date) -> list[tuple[str, bool]]:
    from model.nfl_win_prob_chart import game_rows, load_forecasts, load_games, model_rows

    seasons = FIT_SEASONS + (SEASON,)
    schedule = load_schedule(seasons)
    yards: dict[str, dict[str, float]] = {}
    for s in seasons:
        yards.update(game_yards(load_pbp(s)))
    rows = pregame_rows(schedule, yards)
    done = [r for r in rows if usable(r) and r["home_score"] is not None]
    train = [r for r in done if r["season"] in FIT_SEASONS]
    b = fit_slope(train)
    oos_rows = [r for r in done if r["season"] == SEASON and r["gameday"] < day.isoformat()]
    oos = scores(b, oos_rows)
    ws = sorted({r["week"] for r in oos_rows})
    oos["weeks"] = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    fit = scores(b, train)
    today = {(r["away"], r["home"]): r for r in rows if r["gameday"] == day.isoformat()}

    market = game_rows(load_games(day))
    forecasts, _ = load_forecasts(day)
    model = {(g["away"], g["home"]): g for g in model_rows(market, forecasts)}
    games = []
    for g in market:
        r = today.get((g["away"], g["home"]))
        if not r or r["home_ypg"] is None:
            continue
        m = model.get((g["away"], g["home"]))
        games.append({**g, "market_home": g["p_home"], "model_home": m["p_home"] if m else None,
                      "home_ypg": r["home_ypg"], "away_ypg": r["away_ypg"],
                      "yards_home": predict(b, r["home_ypg"], r["away_ypg"])})
    label = day.strftime("%A, %B %d").replace(" 0", " ")
    name = f"nfl_take_yards_win_prob_{day.isoformat()}.png"
    print(f"b = {b:.5f} per yard; fit {fit}; 2026 out of sample {oos}")
    return [(name, yards_chart(games, Path(out_dir) / name, label, b, fit, oos))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default=(dt.datetime.now(ET).date() + dt.timedelta(days=1)).isoformat())
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), dt.date.fromisoformat(args.date)):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
