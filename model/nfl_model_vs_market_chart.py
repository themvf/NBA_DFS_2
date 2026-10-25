"""Reader chart: our pick'em model's win chances against the betting market's, game by game, with the results.

Descriptive; a few dozen games say nothing reliable about whether the model beats the market.

  nfl_take_model_vs_market_<season>.png   a scorecard (winners picked, Brier score, how the teams the model
                                          liked more than the market did) above one row per finished game: a
                                          grey dot at the market's chance for the team the model leaned
                                          toward, a coloured dot at the model's, green if that team won and
                                          red if it lost, sorted by how far the model moved off the market

Model = the default ``pickem-matchup-residual-shadow-v1`` forecast, the latest one built before kickoff, ties
removed (``homeConditional``). Market = the no-vig moneyline frozen in that same forecast, so both numbers are
from the same moment. Results from ``nfl_season_games``. A tie counts as neither a win nor a loss.

    python -m model.nfl_model_vs_market_chart --season 2026 --out-dir DIR
"""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
TEAM_ALIAS = {"LA": "LAR"}
LEAN_PTS = 2.0   # "the model liked a team more" means by at least this many points


# ── pure computations ────────────────────────────────────────────────────────
def game_rows(raw: list[dict]) -> list[dict]:
    """Orient each game on the team the model liked more than the market (its favourite when they agree)."""
    out = []
    for g in raw:
        m, k = g["model_home"], g["market_home"]
        if g.get("home_score") is None or g.get("away_score") is None:
            continue
        edge = m - k
        lean_home = edge > 0 if abs(edge) >= 0.0005 else m >= 0.5
        team, opp = (g["home"], g["away"]) if lean_home else (g["away"], g["home"])
        pts_for, pts_against = ((g["home_score"], g["away_score"]) if lean_home
                                else (g["away_score"], g["home_score"]))
        result = "tie" if pts_for == pts_against else ("won" if pts_for > pts_against else "lost")
        out.append({**g, "team": team, "opp": opp, "at_home": lean_home,
                    "model": m if lean_home else 1 - m, "market": k if lean_home else 1 - k,
                    "edge": abs(edge), "result": result, "score": f"{pts_for}-{pts_against}"})
    return sorted(out, key=lambda r: (-r["edge"], r["week"], r["team"]))


def _brier(p: float, won: bool) -> float:
    return (p - (1.0 if won else 0.0)) ** 2


def _logloss(p: float, won: bool) -> float:
    p = min(max(p, 1e-6), 1 - 1e-6)
    return -math.log(p if won else 1 - p)


def scorecard(rows: list[dict]) -> dict:
    """Winners picked, Brier and log loss for both, and how the model's leans did (ties left out)."""
    decided = [r for r in rows if r["result"] != "tie"]
    n = len(decided)
    if not n:
        return {"n": 0}
    won = [r["result"] == "won" for r in decided]
    picks = lambda key: sum((r[key] > 0.5) == w for r, w in zip(decided, won))
    leans = [(r, w) for r, w in zip(decided, won) if r["edge"] * 100 >= LEAN_PTS]
    return {
        "n": n, "ties": len(rows) - n,
        "market_picks": picks("market"), "model_picks": picks("model"),
        "market_brier": sum(_brier(r["market"], w) for r, w in zip(decided, won)) / n,
        "model_brier": sum(_brier(r["model"], w) for r, w in zip(decided, won)) / n,
        "market_logloss": sum(_logloss(r["market"], w) for r, w in zip(decided, won)) / n,
        "model_logloss": sum(_logloss(r["model"], w) for r, w in zip(decided, won)) / n,
        "lean_n": len(leans),
        "lean_won": sum(w for _, w in leans),
        "lean_market": sum(r["market"] for r, _ in leans) / len(leans) if leans else None,
        "lean_model": sum(r["model"] for r, _ in leans) / len(leans) if leans else None,
    }


# ── chart ────────────────────────────────────────────────────────────────────
def _tile(ax, x: float, head: str, market: str, model: str, note: str) -> None:
    ax.text(x, 0.92, head, ha="left", va="top", fontsize=7.6, color=INK_2, transform=ax.transAxes)
    ax.text(x, 0.66, f"Market  {market}", ha="left", va="top", fontsize=9.4, color=INK, fontweight="bold",
            transform=ax.transAxes)
    ax.text(x, 0.40, f"Model   {model}", ha="left", va="top", fontsize=9.4, color="#2a78d6", fontweight="bold",
            transform=ax.transAxes)
    ax.text(x, 0.13, note, ha="left", va="top", fontsize=6.6, color=INK_2, transform=ax.transAxes)


def chart(rows: list[dict], path: Path, season: int, weeks: str) -> bool:
    if not rows:
        return False
    sc = scorecard(rows)
    n_rows = len(rows)
    height = 0.27 * n_rows + 3.6
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    grid = fig.add_gridspec(2, 1, height_ratios=[1.0, 0.27 * n_rows], hspace=0.06)
    top, ax = fig.add_subplot(grid[0]), fig.add_subplot(grid[1])
    top.set_facecolor(SURFACE)
    top.axis("off")
    n = sc["n"]
    _tile(top, 0.0, "Picked the winner", f"{sc['market_picks']} of {n}", f"{sc['model_picks']} of {n}",
          "the side each gave over 50%")
    _tile(top, 0.34, "Brier score (lower is better)", f"{sc['market_brier']:.3f}", f"{sc['model_brier']:.3f}",
          "squared miss; a coin flip scores 0.250")
    if sc["lean_n"]:
        _tile(top, 0.68, f"Teams the model liked {LEAN_PTS:.0f}+ pts more ({sc['lean_n']} games)",
              f"said {sc['lean_market']:.0%}", f"said {sc['lean_model']:.0%}",
              f"they actually won {sc['lean_won']} of {sc['lean_n']} ({sc['lean_won'] / sc['lean_n']:.0%})")

    _axes(ax, ygrid=False)
    colour = {"won": GOOD, "lost": BAD, "tie": NEUTRAL}
    for y, r in enumerate(rows):
        ax.plot([r["market"], r["model"]], [y, y], color=INK_2, linewidth=1.0, alpha=0.55, zorder=1)
        ax.scatter(r["market"], y, s=26, color=NEUTRAL, edgecolor=INK_2, linewidth=0.5, zorder=2)
        ax.scatter(r["model"], y, s=34, color=colour[r["result"]], edgecolor=INK, linewidth=0.5, zorder=3)
        where = f"vs {r['opp']}" if r["at_home"] else f"at {r['opp']}"
        ax.text(1.015, y, f"{r['result']} {r['score']}   model +{r['edge'] * 100:.1f}", ha="left", va="center",
                fontsize=6.4, color=colour[r["result"]] if r["result"] != "tie" else INK_2,
                transform=ax.get_yaxis_transform(), clip_on=False, fontweight="bold")
        r["label"] = f"Wk {r['week']}  {r['team']} {where}"
    ax.axvline(0.5, color=INK_2, linewidth=0.6, linestyle=":", zorder=0)
    ax.set_yticks(range(n_rows), [r["label"] for r in rows], fontsize=6.6)
    ax.set_ylim(n_rows - 0.4, -0.6)
    lo = min(min(r["market"], r["model"]) for r in rows)
    hi = max(max(r["market"], r["model"]) for r in rows)
    ax.set_xlim(max(0.0, lo - 0.04), min(1.0, hi + 0.04))
    ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
    ax.tick_params(axis="x", labelsize=7)
    ax.set_xlabel("Win chance for the team on each row", fontsize=8, color=INK_2)
    ax.legend(handles=[Line2D([], [], marker="o", linestyle="", color=NEUTRAL, markeredgecolor=INK_2,
                              label="Market"),
                       Line2D([], [], marker="o", linestyle="", color=GOOD, markeredgecolor=INK,
                              label="Model, team won"),
                       Line2D([], [], marker="o", linestyle="", color=BAD, markeredgecolor=INK,
                              label="Model, team lost")],
              loc="lower right", bbox_to_anchor=(1.0, 1.0), ncol=3, fontsize=6.6, frameon=False,
              handletextpad=0.3, columnspacing=1.0)
    fig.subplots_adjust(left=0.2, right=0.8, bottom=0.6 / height, top=1 - 1.2 / height)
    ties = f" {sc['ties']} tie left out of the scores." if sc.get("ties") else ""
    sub = (f"{season} season, {weeks}: every finished game with a frozen pregame forecast ({len(rows)}). Each row is "
           f"the team our model liked more than the market.\nGrey dot: the market's chance (moneyline, margin "
           f"removed). Coloured dot: our model's, green if that team won, red if it lost. Biggest moves at the "
           f"top.\nModel = our pick'em matchup model (shadow): it starts from the market and nudges it by pass rush "
           f"and yards before and after contact.\n{len(rows)} games is far too few to say whether it beats the "
           f"market; this shows what it did, not that it works.{ties}")
    return _finish(fig, path, "Our model vs the betting market vs what happened", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_games(season: int) -> list[dict]:
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    sql = """
        SELECT DISTINCT ON (f.game_id) f.game_id, s.week, s.home_score, s.away_score,
               (f.payload->'candidate'->>'homeConditional')::float,
               (f.payload->'baseline'->>'home')::float, (f.payload->'baseline'->>'away')::float
        FROM nfl_pickem_matchup_forecasts f
        JOIN nfl_season_games s ON s.nflverse_game_id = f.game_id
        WHERE f.model_version = 'pickem-matchup-residual-shadow-v1' AND (f.payload->>'covered')::boolean
          AND (f.payload->>'selectedForDefault')::boolean AND f.available_at < s.kickoff
          AND s.season = %s AND s.completed AND s.home_score IS NOT NULL AND s.away_score IS NOT NULL
        ORDER BY f.game_id, f.available_at DESC
    """
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(sql, (season,))
        rows = cur.fetchall()
    out = []
    for game_id, week, hs, as_, m, bh, ba in rows:
        _, _, away, home = game_id.split("_")
        out.append({"week": int(week), "home": TEAM_ALIAS.get(home, home), "away": TEAM_ALIAS.get(away, away),
                    "home_score": int(hs), "away_score": int(as_), "model_home": m, "market_home": bh / (bh + ba)})
    return out


def build_all(out_dir: Path, season: int) -> list[tuple[str, bool]]:
    rows = game_rows(load_games(season))
    ws = sorted({r["week"] for r in rows})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    name = f"nfl_take_model_vs_market_{season}.png"
    return [(name, chart(rows, Path(out_dir) / name, season, weeks))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
