"""Reader chart: every NFL game this season that finished under its betting total.

Descriptive, season to date; nothing here claims an edge.

  nfl_take_unders.png   one row per game that went under, grouped by week: the betting total (open circle),
                        the points actually scored (filled dot) and the gap between them

Source: the nflverse schedule (``games.csv``), regular season, finished games only. ``total_line`` is the
schedule's recorded game total, a market consensus near kickoff, not one sportsbook's price. Final scores include
overtime. A game landing exactly on the total is a push and is counted separately, never as an under.

    python -m model.nfl_unders_chart --out-dir DIR
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.story_charts import BAD, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
TEAM_ALIAS = {"LA": "LAR"}


def _team(t: str) -> str:
    return TEAM_ALIAS.get(t, t)


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


# ── pure computations ────────────────────────────────────────────────────────
def finished_games(rows: list[dict], season: int = SEASON) -> list[dict]:
    """Regular-season games of ``season`` with a final score and a betting total."""
    out = []
    for r in rows:
        if int(r.get("season") or 0) != season or r.get("game_type") != "REG":
            continue
        hs, as_, line = _num(r.get("home_score")), _num(r.get("away_score")), _num(r.get("total_line"))
        if hs is None or as_ is None or line is None:
            continue
        total = hs + as_
        out.append({"week": int(r["week"]), "game_id": r.get("game_id"), "away": _team(r["away_team"]),
                    "home": _team(r["home_team"]), "line": line, "total": total, "gap": total - line,
                    "result": "under" if total < line else "over" if total > line else "push"})
    return out


def week_summary(games: list[dict]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for g in games:
        w = out.setdefault(g["week"], {"games": 0, "under": 0, "over": 0, "push": 0})
        w["games"] += 1
        w[g["result"]] += 1
    return dict(sorted(out.items()))


# ── chart ────────────────────────────────────────────────────────────────────
def unders_chart(games: list[dict], path: Path) -> bool:
    unders = [g for g in games if g["result"] == "under"]
    if not unders:
        return False
    weeks = week_summary(games)
    # Rows grouped by week, biggest miss first within each week, with a gap row between weeks.
    rows: list[dict | None] = []
    for w in weeks:
        wk = sorted((g for g in unders if g["week"] == w), key=lambda g: g["gap"])
        if not wk:
            continue
        if rows:
            rows.append(None)
        rows.extend(wk)
    height = 0.2 * len(rows) + 2.4
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    labels, ys = [], []
    for y, g in enumerate(rows):
        if g is None:
            continue
        ax.plot([g["total"], g["line"]], [y, y], color=BAD, alpha=0.35, linewidth=2.2, solid_capstyle="round")
        ax.scatter(g["line"], y, s=26, facecolor=SURFACE, edgecolor=INK_2, linewidth=1.0, zorder=3)
        ax.scatter(g["total"], y, s=26, color=BAD, zorder=4)
        ax.text(g["line"] + 0.8, y, f"{g['gap']:+.1f}", va="center", fontsize=6.4, color=INK_2)
        ys.append(y)
        labels.append(f"W{g['week']}  {g['away']} @ {g['home']}  {g['line']:g} / {g['total']:.0f}")
    # Week headers in the blank rows (and above the first week).
    first = 0
    for w, s in weeks.items():
        if not any(g["week"] == w for g in unders):
            continue
        header_y = first - 0.75
        ax.text(1.0, header_y, f"Week {w}: {s['under']} of {s['games']} under", transform=ax.get_yaxis_transform(),
                ha="right", va="center", fontsize=7, color=INK, fontweight="bold")
        first += s["under"] + 1
    ax.set_yticks(ys, labels, fontsize=6.6)
    ax.set_ylim(len(rows) - 0.4, -1.4)
    lo = min(g["total"] for g in unders)
    hi = max(g["line"] for g in unders)
    ax.set_xlim(lo - 3, hi + 6)
    ax.set_xlabel("Points in the game", fontsize=8, color=INK_2)
    ax.tick_params(axis="x", labelsize=7)
    ax.scatter([], [], s=26, facecolor=SURFACE, edgecolor=INK_2, label="Betting total")
    ax.scatter([], [], s=26, color=BAD, label="Points scored")
    ax.legend(loc="lower right", fontsize=7, frameon=False)
    fig.tight_layout()
    n = len(games)
    nu = len(unders)
    no = sum(g["result"] == "over" for g in games)
    npush = n - nu - no
    span = f"weeks {min(weeks)}-{max(weeks)}"
    avg = sum(g["gap"] for g in unders) / nu
    big = min(unders, key=lambda g: g["gap"])
    sub = (f"{nu} of {n} finished games went under the betting total, {span} ({no} over"
           f"{'' if not npush else f', {npush} push'}). Each row: the total (open circle) and the points scored "
           f"(red dot);\nthe number is how far under it finished. The average under missed by {abs(avg):.1f} points; "
           f"the biggest was {big['away']} @ {big['home']} (W{big['week']}), {big['total']:.0f} against "
           f"{big['line']:g}.\nTotal: the nflverse schedule's game total, a consensus near kickoff, not one book's "
           f"number. Scores include overtime. Describes what happened; not a betting angle.")
    return _finish(fig, path, "Every NFL game that went under the total this season", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_schedule() -> list[dict]:
    import pandas as pd

    from ingest.ff_independent import NFLVERSE_SCHEDULE_URL

    last = None
    for attempt in range(3):
        try:
            return pd.read_csv(NFLVERSE_SCHEDULE_URL, low_memory=False).to_dict("records")
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse schedule download failed: {last}")


def build_all(out_dir: Path, season: int = SEASON) -> list[tuple[str, bool]]:
    games = finished_games(load_schedule(), season)
    path = Path(out_dir) / "nfl_take_unders.png"
    return [("nfl_take_unders.png", unders_chart(games, path))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
