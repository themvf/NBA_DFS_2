"""Reader chart: the season's finished games three ways (market, our model, what happened), then slice by slice.

Descriptive; a few dozen games say nothing reliable about whether the model beats the market.

  nfl_take_three_ways_<season>.png   top: three half-circle cards, one dot per game, coloured by whom each side
                                     favoured (home / toss-up / away; for results, who won), one colour each;
                                     bottom: one row per slice (all games, betting-total bands, spread bands, which
                                     side the model leaned to) with a strip of the market's and the model's Brier
                                     score and columns for games, both scores, the gap and how the model's leans did

Model = the default ``pickem-matchup-residual-shadow-v1`` forecast, latest before kickoff, ties removed. Market =
the no-vig moneyline frozen in that same forecast. Totals and spreads are the market lines in
``nfl_season_games`` (quoted lines when the market line is missing). A tie counts as neither a win nor a loss.

    python -m model.nfl_three_ways_chart --season 2026 --out-dir DIR
"""

from __future__ import annotations

import argparse
import math
import os
import random
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

from model import nfl_model_vs_market_chart as mv  # noqa: E402
from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
TOSS_UP = 0.05    # within 45-55% is a toss-up
STRONG = 0.65     # 65%+ is a strong lean
BIG_WIN = 8       # a win by 8+ is a clear result
# One colour per side: strong and lean seats share it, so each card reads as three blocs.
COLOURS = {"home_strong": "#2c4f9e", "home": "#2c4f9e", "toss": "#2f7d5b",
           "away": "#a12d3a", "away_strong": "#a12d3a", "tie": "#2f7d5b"}
BLUE, GREEN, RED = COLOURS["home_strong"], COLOURS["toss"], COLOURS["away_strong"]
CARD = "#fbfaf7"
MODEL_DOT = "#2a78d6"


# ── pure computations ────────────────────────────────────────────────────────
def lean_class(p_home: float) -> str:
    """Seat colour for one game from a home win chance."""
    if abs(p_home - 0.5) < TOSS_UP:
        return "toss"
    if p_home >= 0.5:
        return "home_strong" if p_home >= STRONG else "home"
    return "away_strong" if p_home <= 1 - STRONG else "away"


def result_class(home_score: int, away_score: int) -> str:
    if home_score == away_score:
        return "tie"
    margin = home_score - away_score
    if margin > 0:
        return "home_strong" if margin >= BIG_WIN else "home"
    return "away_strong" if -margin >= BIG_WIN else "away"


def bloc_counts(classes: list[str]) -> tuple[int, int, int]:
    """(home, middle, away) where middle is toss-ups or ties."""
    home = sum(c.startswith("home") for c in classes)
    away = sum(c.startswith("away") for c in classes)
    return home, len(classes) - home - away, away


ORDER = ["home_strong", "home", "toss", "tie", "away", "away_strong"]


def seat_positions(n: int, rows: int | None = None) -> list[tuple[float, float]]:
    """Parliament-style seats on concentric arcs, ordered left to right by angle."""
    if n <= 0:
        return []
    rows = rows or max(2, round(math.sqrt(n / 3)))
    radii = [1.0 - 0.17 * i * (4 / rows) for i in range(rows)]
    total_r = sum(radii)
    counts = [max(1, round(n * r / total_r)) for r in radii]
    counts[0] += n - sum(counts)
    seats = []
    for r, k in zip(radii, counts):
        for j in range(k):
            ang = math.pi * (1 - (j + 0.5) / k) if k > 1 else math.pi / 2
            seats.append((ang, r))
    seats.sort(key=lambda s: (-s[0], -s[1]))
    return [(r * math.cos(a), r * math.sin(a)) for a, r in seats]


def _bootstrap(pairs: list[tuple[dict, bool]], seed: int = 1, draws: int = 4000) -> tuple[float, float]:
    rng = random.Random(seed)
    diffs = []
    for _ in range(draws):
        s = [rng.choice(pairs) for _ in pairs]
        diffs.append(sum(mv._brier(r["model"], w) - mv._brier(r["market"], w) for r, w in s) / len(s))
    diffs.sort()
    return diffs[int(0.025 * draws)], diffs[int(0.975 * draws) - 1]


def slice_stats(name: str, rows: list[dict]) -> dict | None:
    decided = [r for r in rows if r["result"] != "tie"]
    if not decided:
        return None
    pairs = [(r, r["result"] == "won") for r in decided]
    n = len(pairs)
    bm = sum(mv._brier(r["market"], w) for r, w in pairs) / n
    bo = sum(mv._brier(r["model"], w) for r, w in pairs) / n
    lo, hi = _bootstrap(pairs)
    leans = [(r, w) for r, w in pairs if r["edge"] * 100 >= mv.LEAN_PTS]
    return {"name": name, "n": n, "market": bm, "model": bo, "diff": bo - bm, "lo": lo, "hi": hi,
            "lean_won": sum(w for _, w in leans), "lean_n": len(leans)}


def slices(rows: list[dict]) -> list[tuple[str, list[dict]]]:
    out = [("All games", rows)]
    for lo, hi, label in [(0, 43, "Total under 43"), (43, 47, "Total 43 to 46.5"), (47, 99, "Total 47+")]:
        out.append((label, [r for r in rows if r.get("total") is not None and lo <= r["total"] < hi]))
    for lo, hi, label in [(0, 3, "Spread under 3"), (3, 7, "Spread 3 to 6.5"), (7, 99, "Spread 7+")]:
        out.append((label, [r for r in rows if r.get("spread") is not None and lo <= r["spread"] < hi]))
    out.append(("Model leaned home", [r for r in rows if r["at_home"]]))
    out.append(("Model leaned away", [r for r in rows if not r["at_home"]]))
    return out


# ── chart ────────────────────────────────────────────────────────────────────
def _card(fig, rect, title, classes, labels, highlight=False):
    ax = fig.add_axes(rect)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((0.01, 0.01), 0.98, 0.98, boxstyle="round,pad=0,rounding_size=0.04",
                                facecolor=CARD, edgecolor="#a12d3a" if highlight else "#e7e3dc",
                                linewidth=1.6 if highlight else 0.8, transform=ax.transAxes))
    ax.text(0.5, 0.9, title, ha="center", va="center", fontsize=11, fontweight="bold", color=INK)
    seats = seat_positions(len(classes))
    ordered = sorted(classes, key=ORDER.index)
    hx = fig.add_axes([rect[0] + rect[2] * 0.12, rect[1] + rect[3] * 0.38, rect[2] * 0.76, rect[3] * 0.44])
    hx.set_aspect("equal")
    hx.axis("off")
    hx.scatter([s[0] for s in seats], [s[1] for s in seats], s=58, c=[COLOURS[c] for c in ordered],
               edgecolors="none")
    hx.set_xlim(-1.08, 1.08)
    hx.set_ylim(-0.05, 1.08)
    counts = bloc_counts(classes)
    for x, v, lab, col in zip((0.22, 0.5, 0.78), counts, labels, (BLUE, GREEN, RED)):
        ax.text(x, 0.22, str(v), ha="center", va="center", fontsize=19, fontweight="bold", color=col,
                family="serif")
        ax.text(x, 0.09, lab, ha="center", va="center", fontsize=7.2, color=INK_2)


def chart(rows: list[dict], path: Path, season: int, weeks: str) -> bool:
    if not rows:
        return False
    stats = [s for s in (slice_stats(n, rs) for n, rs in slices(rows)) if s]
    W, H = 13.2, 7.0 + 0.42 * len(stats)
    fig = plt.figure(figsize=(W, H), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    top = 1 - 0.35 / H

    fig.text(0.05, top, f"THE {len(rows)} FINISHED GAMES, THREE WAYS", fontsize=9, color=INK_2,
             fontweight="bold", va="top")
    fig.text(0.05, top - 0.28 / H, f"NFL {season}, {weeks}: our pick'em model vs the betting market vs what happened",
             fontsize=15, color=INK, fontweight="bold", va="top")
    card_h, card_w = 3.0 / H, 0.27
    card_y = top - 0.85 / H - card_h
    markets = [lean_class(r["market_home"]) for r in rows]
    models = [lean_class(r["model_home"]) for r in rows]
    results = [result_class(r["home_score"], r["away_score"]) for r in rows]
    fav = ("Home fav", "Toss-up", "Away fav")
    _card(fig, [0.05, card_y, card_w, card_h], "Betting market", markets, fav)
    _card(fig, [0.365, card_y, card_w, card_h], "Our model", models, fav)
    _card(fig, [0.68, card_y, card_w, card_h], "What happened", results, ("Home won", "Tie", "Away won"),
          highlight=True)
    ly = card_y - 0.3 / H
    items = [("home_strong", "Home side"), ("toss", "Toss-up (45-55%) / tie"), ("away_strong", "Away side")]
    x = 0.2
    for key, lab in items:
        fig.text(x, ly, "●", color=COLOURS[key], fontsize=11, va="center")
        fig.text(x + 0.013, ly, lab, color=INK_2, fontsize=8.5, va="center")
        x += 0.16
    fig.text(x + 0.02, ly, "Each dot is one game.", color=INK_2,
             fontsize=8.2, va="center")

    # ── slice table ─────────────────────────────────────────────────────────
    ty = ly - 0.55 / H
    fig.text(0.05, ty, "SLICE BY SLICE", fontsize=9.5, color=INK, fontweight="bold", va="center")
    fig.add_artist(plt.Line2D([0.17, 0.95], [ty, ty], color="#d9d4cc", linewidth=0.8))
    hy = ty - 0.42 / H
    cols = {"slice": 0.05, "strip0": 0.25, "strip1": 0.5, "games": 0.555, "market": 0.635, "model": 0.715,
            "diff": 0.815, "leans": 0.915}
    for key, lab in [("slice", "SLICE"), ("games", "GAMES"), ("market", "MARKET"), ("model", "MODEL"),
                     ("diff", "MODEL − MARKET"), ("leans", "MODEL'S LEANS")]:
        fig.text(cols[key], hy, lab, fontsize=7.6, color=INK_2, fontweight="bold", va="center",
                 ha="left" if key == "slice" else "center")
    lo_b, hi_b = 0.15, 0.32

    def bx(v):
        return cols["strip0"] + (cols["strip1"] - cols["strip0"]) * (min(max(v, lo_b), hi_b) - lo_b) / (hi_b - lo_b)

    for v in (0.15, 0.20, 0.25, 0.30):
        fig.text(bx(v), hy, f"{v:.2f}".lstrip("0"), fontsize=7, color=INK_2, ha="center", va="center")
    row_h = 0.42 / H
    first = hy - 0.38 / H
    last = first - row_h * (len(stats) - 1)
    fig.add_artist(plt.Line2D([bx(0.25), bx(0.25)], [last - row_h * 0.5, first + row_h * 0.5], color=INK_2,
                              linewidth=0.7, linestyle=":"))
    for i, s in enumerate(stats):
        y = first - i * row_h
        if i:
            fig.add_artist(plt.Line2D([0.05, 0.95], [y + row_h / 2, y + row_h / 2], color="#ebe7e0",
                                      linewidth=0.6))
        fig.text(cols["slice"], y, s["name"], fontsize=10.2, color=INK, fontweight="bold", va="center")
        fig.add_artist(plt.Line2D([cols["strip0"], cols["strip1"]], [y, y], color="#e3dfd8", linewidth=0.6,
                                  linestyle=(0, (1, 2))))
        fig.add_artist(plt.Line2D([bx(s["market"]), bx(s["model"])], [y, y], color="#c9c4bc", linewidth=2.4))
        fig.add_artist(plt.Line2D([bx(s["market"])], [y], marker="o", markersize=7, color="#9a958d",
                                  linestyle=""))
        fig.add_artist(plt.Line2D([bx(s["model"])], [y], marker="o", markersize=7, color=MODEL_DOT,
                                  linestyle=""))
        clear = s["lo"] > 0 or s["hi"] < 0
        better = s["diff"] < 0
        dcol = (GREEN if better else RED) if clear else INK
        fig.text(cols["games"], y, str(s["n"]), fontsize=10.5, color=INK, ha="center", va="center")
        fig.text(cols["market"], y, f"{s['market']:.3f}", fontsize=10.5, color=INK, ha="center", va="center")
        fig.text(cols["model"], y, f"{s['model']:.3f}", fontsize=10.5, color=MODEL_DOT, ha="center",
                 va="center", fontweight="bold")
        fig.text(cols["diff"], y, f"{s['diff']:+.3f}" + (" *" if clear else ""), fontsize=10.5, color=dcol,
                 ha="center", va="center", fontweight="bold" if clear else "normal")
        lean = f"{s['lean_won']} of {s['lean_n']} won" if s["lean_n"] else "none"
        fig.text(cols["leans"], y, lean, fontsize=10.2, color=INK, ha="center", va="center")
    fy = last - row_h * 0.9
    fig.add_artist(plt.Line2D([0.05, 0.95], [fy + 0.12 / H, fy + 0.12 / H], color=INK, linewidth=0.8))
    note = ("Brier score: squared miss, lower is better; dotted line = a coin flip (0.25). Grey dot = market, blue = "
            "model. * = the 95% bootstrap range of the gap excludes zero (green: model better; red: worse).\n"
            "\"Model's leans\" = games where the model liked a team 2+ points more than the market. Nine slices of the "
            "same games, cut after looking: expect one or two * by chance alone.\n"
            "Model = our pick'em matchup model (shadow), which starts from the market. Market = the no-vig moneyline "
            "frozen in the same pregame forecast. A tie is left out of the scores.")
    fig.text(0.05, fy - 0.08 / H, note, fontsize=7.6, color=INK_2, va="top", linespacing=1.5)
    fig.savefig(path, dpi=DPI, facecolor=SURFACE)
    plt.close(fig)
    return True


# ── data ─────────────────────────────────────────────────────────────────────
def load_games(season: int) -> list[dict]:
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    sql = """
        SELECT DISTINCT ON (f.game_id) f.game_id, s.week, s.home_score, s.away_score,
               (f.payload->'candidate'->>'homeConditional')::float,
               (f.payload->'baseline'->>'home')::float, (f.payload->'baseline'->>'away')::float,
               COALESCE(s.market_total_line, s.quoted_total_line),
               COALESCE(s.market_spread_line, s.quoted_spread_line)
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
    for game_id, week, hs, as_, m, bh, ba, total, spread in rows:
        _, _, away, home = game_id.split("_")
        out.append({"week": int(week), "home": mv.TEAM_ALIAS.get(home, home), "away": mv.TEAM_ALIAS.get(away, away),
                    "home_score": int(hs), "away_score": int(as_), "model_home": m, "market_home": bh / (bh + ba),
                    "total": None if total is None else float(total),
                    "spread": None if spread is None else abs(float(spread))})
    return out


def build_all(out_dir: Path, season: int) -> list[tuple[str, bool]]:
    rows = mv.game_rows(load_games(season))
    ws = sorted({r["week"] for r in rows})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    name = f"nfl_take_three_ways_{season}.png"
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
