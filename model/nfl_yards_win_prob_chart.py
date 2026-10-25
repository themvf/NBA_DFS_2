"""Reader chart: what win models built on one offensive stat each say about a day's games, beside the market.

An experiment, not a forecast. Two single-input models, each using one team average over the games before this one
(regular season):

    yards only:       offensive yards a game (pass and run plays)
    touchdowns only:  offensive touchdowns a game (touchdowns on the team's own pass and run plays; defensive and
                      return touchdowns are left out)

    p_home = 1 / (1 + exp(-b * (home_avg - away_avg)))

No home-field term, no defense, no injuries. Each model's one number b is fitted on 2023-2025 games from week 4 on
(each team has at least three prior games), and the same seasons give the yardstick: how often it picked the winner
and its Brier score, beside the betting market's closing no-vig moneyline. 2026 games before the slate are scored out
of sample.

  nfl_take_yards_win_prob_<date>.png   a grid, one row per game: the team the market, our pick'em model, the
                                       yards-only model and the touchdowns-only model each favor, and its chance

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
MARKET_COLOUR, MODEL_COLOUR, YARDS_COLOUR, TDS_COLOUR = "#8a8a8a", "#2a78d6", "#e3833b", "#3a9a5b"
FLIP_COLOUR = "#c0392b"
PBP_COLS = ("game_id", "season_type", "posteam", "play_type", "yards_gained", "touchdown", "td_team")
OFFENSE_PLAYS = ("pass", "run")


def _t(team: str) -> str:
    return TEAM_ALIAS.get(team, team)


def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not math.isnan(v) else None


# ── pure computations ────────────────────────────────────────────────────────
def game_stats(pbp: list[dict]) -> tuple[dict, dict]:
    """(yards, touchdowns): each game_id -> {team: total}, regular season, offense only.

    Every team with the ball in a game gets an entry, so a game without a touchdown records 0 rather than nothing."""
    yards: dict = collections.defaultdict(lambda: collections.defaultdict(float))
    tds: dict = collections.defaultdict(lambda: collections.defaultdict(float))
    for p in pbp:
        if p.get("season_type") != "REG" or not isinstance(p.get("posteam"), str):
            continue
        g, t = p["game_id"], _t(p["posteam"])
        yards[g][t] += 0.0
        tds[g][t] += 0.0
        if p.get("play_type") not in OFFENSE_PLAYS:
            continue
        yards[g][t] += _num(p.get("yards_gained")) or 0.0
        if _num(p.get("touchdown")) == 1.0 and isinstance(p.get("td_team"), str) and _t(p["td_team"]) == t:
            tds[g][t] += 1
    return yards, tds


def pregame_rows(schedule: list[dict], stat: dict[str, dict[str, float]]) -> list[dict]:
    """Each game with both teams' average of ``stat`` over their earlier games that season (point in time).

    Games are taken in week order; a game's own numbers join the averages only after it is recorded, so nothing from
    the game or later reaches its inputs. A game without play-by-play adds nothing to the averages."""
    out = []
    tot: dict[tuple[int, str], list[float]] = collections.defaultdict(list)
    for g in sorted(schedule, key=lambda g: (g["season"], g["week"], g["game_id"])):
        s, h, a = g["season"], _t(g["home_team"]), _t(g["away_team"])
        hy, ay = tot[(s, h)], tot[(s, a)]
        out.append({**g, "home": h, "away": a, "home_n": len(hy), "away_n": len(ay),
                    "home_avg": sum(hy) / len(hy) if hy else None, "away_avg": sum(ay) / len(ay) if ay else None})
        gy = stat.get(g["game_id"])
        if gy and h in gy and a in gy:
            hy.append(gy[h])
            ay.append(gy[a])
    return out


def usable(r: dict) -> bool:
    return min(r["home_n"], r["away_n"]) >= MIN_PRIOR_GAMES and r["home_avg"] is not None


def predict(b: float, home_avg: float, away_avg: float) -> float:
    return 1 / (1 + math.exp(-b * (home_avg - away_avg)))


def fit_slope(rows: list[dict]) -> float:
    """Maximum-likelihood b for a logistic model with one input and no intercept (Newton's method)."""
    pts = [(r["home_avg"] - r["away_avg"], 1.0 if r["home_score"] > r["away_score"] else 0.0)
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
    """Brier score and winner-picking rate of the one-stat model and the market's no-vig moneyline, same games."""
    n, sb, mb, sw, mw = 0, 0.0, 0.0, 0, 0
    for r in rows:
        if r["home_score"] == r["away_score"] or r.get("home_moneyline") is None:
            continue
        won = 1.0 if r["home_score"] > r["away_score"] else 0.0
        ps = predict(b, r["home_avg"], r["away_avg"])
        pm = home_win(r["home_moneyline"], r["away_moneyline"])
        n += 1
        sb += (ps - won) ** 2
        mb += (pm - won) ** 2
        sw += (ps > 0.5) == (won == 1.0)
        mw += (pm > 0.5) == (won == 1.0)
    if not n:
        return {"n": 0}
    return {"n": n, "stat_brier": sb / n, "market_brier": mb / n, "stat_right": sw / n, "market_right": mw / n}


def evaluate(schedule: list[dict], stat: dict, day: dt.date) -> dict:
    """Fit one single-stat model and score it: b, the 2023-25 fit, the 2026 games before ``day``, and the rows."""
    rows = pregame_rows(schedule, stat)
    done = [r for r in rows if usable(r) and r["home_score"] is not None]
    train = [r for r in done if r["season"] in FIT_SEASONS]
    b = fit_slope(train)
    oos = scores(b, [r for r in done if r["season"] == SEASON and r["gameday"] < day.isoformat()])
    today = {(r["away"], r["home"]): r for r in rows if r["gameday"] == day.isoformat()}
    return {"b": b, "fit": scores(b, train), "oos": oos, "today": today}


# ── chart ────────────────────────────────────────────────────────────────────
def _pick(away: str, home: str, p_home: float) -> tuple[str, float]:
    if abs(p_home - 0.5) < 0.005:
        return ("Even", 0.5)   # a coin flip favors nobody, so it is never a different winner
    return (home, p_home) if p_home >= 0.5 else (away, 1 - p_home)


COLUMNS = (("Betting market", MARKET_COLOUR, "market_home"), ("Our pick'em model", MODEL_COLOUR, "model_home"),
           ("Yards only", YARDS_COLOUR, "yards_home"), ("Touchdowns only", TDS_COLOUR, "tds_home"))


def picks_chart(games: list[dict], path: Path, day_label: str, yards: dict, tds: dict) -> bool:
    """A grid: one row per game, one column per model, each cell the team that model favors and its chance.

    A yards-only or touchdowns-only cell that picks a different winner than the market is outlined in red, and
    games are sorted by how many of those two disagree."""
    if not games:
        return False
    rows = []
    for g in games:
        picks = {key: (_pick(g["away"], g["home"], g[key]) if g.get(key) is not None else None)
                 for _, _, key in COLUMNS}
        market_team = picks["market_home"][0]
        flips = {key for key in ("yards_home", "tds_home")
                 if picks[key] and picks[key][0] not in (market_team, "Even")}
        rows.append({**g, "picks": picks, "flips": flips,
                     "gap": abs(g["yards_home"] - g["market_home"]) + abs(g["tds_home"] - g["market_home"])})
    rows.sort(key=lambda r: (-len(r["flips"]), -r["gap"]))

    height = 0.42 * len(rows) + 3.1
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_axis_off()
    ax.set_xlim(0, 100)
    ax.set_ylim(len(rows) - 0.2, -1.3)
    xs, width = (14, 31, 48, 65), 15.5
    head = dict(ha="left", va="center", fontsize=7.8, fontweight="bold")
    ax.text(0, -0.95, "Game", color=INK, **head)
    for (label, colour, _), x in zip(COLUMNS, xs):
        ax.text(x, -0.95, label, color=colour, **head)
    ax.text(83, -0.95, "Yards a game", color=INK, **head)
    ax.text(93, -0.95, "TDs a game", color=INK, **head)
    for y, r in enumerate(rows):
        ax.text(0, y, f"{r['away']} @ {r['home']}", ha="left", va="center", fontsize=7.8, color=INK)
        for (_, colour, key), x in zip(COLUMNS, xs):
            pick = r["picks"][key]
            if pick is None:
                ax.text(x, y, "n/a", ha="left", va="center", fontsize=7.4, color=INK_2)
                continue
            team, p = pick
            flip = key in r["flips"]
            ax.add_patch(plt.Rectangle((x, y - 0.3), width, 0.6, color="#e6e4df", lw=0, zorder=1))
            ax.add_patch(plt.Rectangle((x, y - 0.3), width * (p - 0.5) / 0.5, 0.6, color=colour, alpha=0.85,
                                       lw=0, zorder=2))
            if flip:
                ax.add_patch(plt.Rectangle((x, y - 0.3), width, 0.6, fill=False, edgecolor=FLIP_COLOUR, lw=1.6,
                                           zorder=4))
            ax.text(x + 0.6, y, f"{team} {p:.0%}", ha="left", va="center", fontsize=7.6,
                    color=FLIP_COLOUR if flip else INK, fontweight="bold", zorder=3)
        ax.text(83, y, f"{r['away_ypg']:.0f} / {r['home_ypg']:.0f}", ha="left", va="center", fontsize=7,
                color=INK_2)
        ax.text(93, y, f"{r['away_tpg']:.1f} / {r['home_tpg']:.1f}", ha="left", va="center", fontsize=7,
                color=INK_2)
    n_y = sum("yards_home" in r["flips"] for r in rows)
    n_t = sum("tds_home" in r["flips"] for r in rows)
    ax.text(0, len(rows) - 0.25, f"Red outline: picks a different winner than the market (yards only {n_y} of "
            f"{len(rows)} games, touchdowns only {n_t}). Stats are away / home.", ha="left", va="top", fontsize=7,
            color=INK_2)
    fig.tight_layout()
    yf, tf = yards["fit"], tds["fit"]
    sub = (f"{day_label}. Each cell: the team that model favors and its chance to win (bar fills from 50% to 100%).\n"
           f"Yards only and touchdowns only each use one number: that team's offensive average this season, nothing "
           f"else.\n100 more yards a game than the opponent is worth {(predict(yards['b'], 100, 0) - .5) * 100:+.0f} "
           f"points of win chance; 1 more touchdown a game is worth {(predict(tds['b'], 1, 0) - .5) * 100:+.0f}.\n"
           f"Picked the winner, 2023-25 ({yf['n']} games): market {yf['market_right']:.0%}, touchdowns only "
           f"{tf['stat_right']:.0%}, yards only {yf['stat_right']:.0%}. "
           f"2026 so far ({yards['oos'].get('n', 0)} games): market {yards['oos'].get('market_right', 0):.0%}, "
           f"touchdowns {tds['oos'].get('stat_right', 0):.0%}, yards {yards['oos'].get('stat_right', 0):.0%}.\n"
           f"An experiment, not a forecast.")
    return _finish(fig, path, "Who wins? Market vs our model vs one stat", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_pbp(season: int) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in PBP_COLS, compression="gzip", low_memory=False)
    missing = set(PBP_COLS) - set(frame.columns)
    if missing:
        raise RuntimeError(f"play-by-play {season} is missing columns: {sorted(missing)}")
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
    yards_by_game: dict = {}
    tds_by_game: dict = {}
    for s in seasons:
        y, t = game_stats(load_pbp(s))
        yards_by_game.update(y)
        tds_by_game.update(t)
    yards = evaluate(schedule, yards_by_game, day)
    tds = evaluate(schedule, tds_by_game, day)

    market = game_rows(load_games(day))
    forecasts, _ = load_forecasts(day)
    model = {(g["away"], g["home"]): g for g in model_rows(market, forecasts)}
    games = []
    for g in market:
        k = (g["away"], g["home"])
        ry, rt = yards["today"].get(k), tds["today"].get(k)
        if not ry or not rt or ry["home_avg"] is None or rt["home_avg"] is None:
            continue
        m = model.get(k)
        games.append({**g, "market_home": g["p_home"], "model_home": m["p_home"] if m else None,
                      "home_ypg": ry["home_avg"], "away_ypg": ry["away_avg"],
                      "home_tpg": rt["home_avg"], "away_tpg": rt["away_avg"],
                      "yards_home": predict(yards["b"], ry["home_avg"], ry["away_avg"]),
                      "tds_home": predict(tds["b"], rt["home_avg"], rt["away_avg"])})
    for name, e in (("yards", yards), ("touchdowns", tds)):
        print(f"{name}: b = {e['b']:.5f}; fit {e['fit']}; 2026 out of sample {e['oos']}")
    label = day.strftime("%A, %B %d").replace(" 0", " ")
    name = f"nfl_take_yards_win_prob_{day.isoformat()}.png"
    return [(name, picks_chart(games, Path(out_dir) / name, label, yards, tds))]


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
