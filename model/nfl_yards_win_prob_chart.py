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
from matplotlib.lines import Line2D  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_win_prob_chart import ET, home_win  # noqa: E402
from model.story_charts import WIDTH_IN, _axes, _finish  # noqa: E402

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
def yards_chart(games: list[dict], path: Path, day_label: str, b: float, fit: dict, oos: dict) -> bool:
    if not games:
        return False
    height = 0.5 * len(games) + 3.2
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    for y, g in enumerate(games):
        vals = [v for v in (g["market_home"], g.get("model_home"), g["yards_home"]) if v is not None]
        ax.plot([min(vals), max(vals)], [y, y], color=INK_2, linewidth=0.8, alpha=0.5, zorder=1)
        ax.scatter(g["market_home"], y, s=46, color=MARKET_COLOUR, zorder=3)
        if g.get("model_home") is not None:
            ax.scatter(g["model_home"], y, s=46, color=MODEL_COLOUR, zorder=4)
        ax.scatter(g["yards_home"], y, s=46, color=YARDS_COLOUR, marker="D", zorder=5)
        ax.text(1.02, y, f"{g['away']} {g['away_ypg']:.0f}  ·  {g['home']} {g['home_ypg']:.0f}",
                ha="left", va="center", fontsize=6.8, color=INK_2, transform=ax.get_yaxis_transform())
    ax.axvline(0.5, color=INK_2, linewidth=0.7, linestyle=":", zorder=0)
    ax.set_yticks(range(len(games)), [f"{g['away']} @ {g['home']}" for g in games], fontsize=7.4)
    ax.set_ylim(len(games) - 0.5, -0.7)
    ax.set_xlim(0, 1)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1], ["0%", "25%", "50%", "75%", "100%"], fontsize=7)
    ax.set_xlabel("Home team's chance to win", fontsize=8, color=INK_2)
    ax.legend(handles=[Line2D([], [], marker="o", linestyle="", color=MARKET_COLOUR, label="Betting market"),
                       Line2D([], [], marker="o", linestyle="", color=MODEL_COLOUR, label="Our pick'em model"),
                       Line2D([], [], marker="D", linestyle="", color=YARDS_COLOUR, label="Yards only")],
              loc="lower left", bbox_to_anchor=(0, 1.0), ncol=3, fontsize=7, frameon=False)
    fig.tight_layout(rect=(0, 0, 0.8, 1))
    per100 = predict(b, 100, 0) - 0.5
    sub = (f"{day_label}. One input: each team's offensive yards a game so far this season (right column, away then "
           f"home).\nNo home field, defense or injuries. Every 100 yards a game of difference is worth "
           f"{per100 * 100:+.0f} points of win chance (fitted on 2023-25).\n"
           f"Accuracy, Brier score (lower is better, 0.250 = coin flip): 2023-25 yards only {fit['yards_brier']:.3f} vs "
           f"market {fit['market_brier']:.3f} over {fit['n']} games; 2026 {oos.get('weeks', '')} "
           f"{oos.get('yards_brier', float('nan')):.3f} vs {oos.get('market_brier', float('nan')):.3f} ({oos.get('n', 0)} games).\n"
           f"Picked the winner: yards only {fit['yards_right']:.0%}, market {fit['market_right']:.0%} (2023-25). "
           f"An experiment, not a forecast.")
    return _finish(fig, path, "If yards were all that mattered", sub, height)


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
