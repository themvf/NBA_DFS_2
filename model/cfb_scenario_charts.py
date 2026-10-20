"""One chart per betting scenario: moneyline, spread and total side by side.

Every single cut of the market breakdown (home or away, spread band, kickoff
window, rest, ...) gets a three-panel image: favorites vs underdogs on the
moneyline at the closing price, favorites vs underdogs against the spread at
-110, and overs vs unders at -110. A black tick on each bar marks the same
group over 2023-25. Every two-way grid (spread x total, tier x spread, ...)
gets a three-panel heatmap of the favorite (or over) return.

Rows come from ``cfb_pattern_csv.market_breakdown_table`` and the pooled
prior-season table from ``cfb_pattern_history``, the same rows the private
page renders, so the images and the page cannot disagree. Descriptive,
season to date; small groups are faded, never hidden.

    python -m model.cfb_scenario_charts --out-dir artifacts/cfb_pattern_watch/charts
"""

from __future__ import annotations

import argparse
import re
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "charts"
WIDTH_IN = 9.6        # three panels need more room than the single-chart width
THIN = 20             # groups with fewer games are faded
CLIP = 60.0           # ROI axis limit; a tiny group's +269% is labelled, not drawn to scale

# (level name in the breakdown table) -> (file slug, title, extra subtitle note)
SINGLE_CUTS = {
    "By venue": ("venue", "Home or away favorite", "Neutral-site games are their own group."),
    "By conference game": ("conference", "Conference vs. non-conference games", ""),
    "By matchup tier": ("tier", "Matchup tier", "Power: SEC, Big Ten, Big 12, ACC (and the Pac-12 through 2023) plus Notre Dame."),
    "By kickoff": ("kickoff", "Kickoff window", "Eastern time."),
    "By spread": ("spread", "Size of the spread", ""),
    "By total": ("total", "Size of the total", ""),
    "By price": ("price", "The favorite's moneyline price", ""),
    "By line movement": ("line_movement", "How the line moved before kickoff",
                         "Favorite's win chance, first line 12+ hours out to the close. No 2023-25 comparison."),
    "By last result": ("last_result", "The favorite's last result", ""),
    "By rest": ("rest", "Rest", "More rest: 3+ extra days. Short week: 5 days or fewer for either team."),
    "Home team ladder": ("home_ladder_markets", "The home team, from big favorite to big underdog",
                         "Told from the home team's side. Neutral sites left out."),
    "Home underdog price": ("home_dog_price", "Home underdogs by price", "Told from the home team's side."),
    "By halftime score": ("halftime", "By halftime score: not a pregame bet",
                          "Known only at halftime; still graded at the closing price."),
}
GRIDS = {
    "Venue and spread": ("venue_spread", "Home or away favorite, by spread"),
    "Venue and total": ("venue_total", "Home or away favorite, by total"),
    "Spread and total": ("spread_total", "Spread and total together"),
    "Tier and spread": ("tier_spread", "Matchup tier, by spread"),
    "Kickoff and tier": ("kickoff_tier", "Kickoff window, by matchup tier"),
}
HOME_LENS = {"Home team ladder", "Home underdog price"}

# (panel title, side A label, side B label, A column, B column, games column)
MARKETS = (
    ("Moneyline", "Favorites", "Underdogs", "ROI (%)", "Underdog ROI (%)", "Games"),
    ("Spread at -110", "Favorites", "Underdogs", "Fav ATS ROI (%)", "Dog ATS ROI (%)", "Games"),
    ("Total at -110", "Overs", "Unders", "Over ROI (%)", "Under ROI (%)", "Games with total"),
)
SUBTITLE = ("Return per unit, season to date: moneyline at the closing price, spread and total at -110. "
            "Black ticks: the same group in 2023-25.\nGame counts in brackets; faded rows have fewer than "
            f"{THIN} games. Bars beyond ±{CLIP:.0f}% are cut off and labelled.")


def short(label: str | None) -> str:
    s = label or ""
    for pat, rep in (("^Spread ", ""), ("^Total ", ""), ("^Home favored by ", "Fav "),
                     ("^Home underdog by ", "Dog "), ("^Home underdog ", ""), (" or more$", "+"),
                     (" or later$", "+")):
        s = re.sub(pat, rep, s)
    return s


def market_values(row: dict, market: tuple, home_lens: bool = False) -> tuple:
    """(side A ROI, side B ROI, games) for one market; the home lens swaps sides
    where the home team is the underdog (rows are stored from the favorite's side)."""
    _, _, _, a_col, b_col, n_col = market
    a, b = row.get(a_col), row.get(b_col)
    if home_lens and n_col == "Games" and not str(row.get("Split", "")).startswith("Home favored"):
        a, b = b, a
    return a, b, row.get(n_col) or 0


def _panel_labels(market: tuple, home_lens: bool) -> tuple[str, str]:
    if home_lens and market[5] == "Games":
        return "Home team", "Road team"
    return market[1], market[2]


def _clip(v: float | None) -> float:
    return 0.0 if v is None else max(-CLIP, min(CLIP, v))


def _save(fig, path: Path, top: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=(0, 0, 1, top))
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def _heading(fig, title: str, subtitle: str) -> None:
    fig.text(0.01, 0.995, title, fontsize=12, fontweight="bold", color=INK, va="top")
    fig.text(0.01, 0.995 - 0.33 / fig.get_figheight(), subtitle, fontsize=7.5, color=INK_2, va="top")


def scenario_chart(rows: list[dict], prior: dict, level: str, path: Path) -> bool:
    """Three-panel grouped bars for one single cut; ``prior`` maps Split -> 2023-25 row."""
    rows = [r for r in rows if r.get("Level name") == level]
    if not rows or level not in SINGLE_CUTS:
        return False
    _, title, note = SINGLE_CUTS[level]
    home = level in HOME_LENS
    n = len(rows)
    height = 0.42 * n + 2.3
    fig, axes = plt.subplots(1, 3, figsize=(WIDTH_IN, height), dpi=DPI, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    ys = list(range(n))[::-1]
    w = 0.36
    for k, (ax, market) in enumerate(zip(axes, MARKETS)):
        ax.set_facecolor(SURFACE)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=INK_2, labelsize=7, length=0)
        ax.grid(axis="x", color=GRID, linewidth=0.6)
        ax.set_axisbelow(True)
        ax.axvline(0, color=INK_2, linewidth=0.8)
        a_lab, b_lab = _panel_labels(market, home)
        for y, r in zip(ys, rows):
            a, b, games = market_values(r, market, home)
            alpha = 1.0 if games >= THIN else 0.35
            p = prior.get(r.get("Split"))
            pa, pb, _ = market_values(p, market, home) if p else (None, None, 0)
            for off, v, pv, colour in ((w / 2, a, pa, SERIES[0]), (-w / 2, b, pb, SERIES[1])):
                if v is None or games == 0:
                    continue
                ax.barh(y + off, _clip(v), height=w, color=colour, alpha=alpha)
                if pv is not None:
                    ax.plot([_clip(pv)] * 2, [y + off - w / 2, y + off + w / 2], color=INK, linewidth=1.4)
                text = f"{v:+.0f}%" + ("»" if abs(v) > CLIP else "")
                x = _clip(v)
                # Sit the label past both the bar end and the 2023-25 tick on the same side.
                if pv is not None and (_clip(pv) >= 0) == (x >= 0):
                    x = max(x, _clip(pv)) if x >= 0 else min(x, _clip(pv))
                ax.text(x + (1.5 if x >= 0 else -1.5), y + off, text, va="center",
                        ha="left" if x >= 0 else "right", fontsize=5.8, color=INK, alpha=max(alpha, 0.6))
        ax.set_xlim(-CLIP * 1.25, CLIP * 1.25)
        ax.set_xticks([-50, -25, 0, 25, 50], ["-50%", "-25%", "0", "+25%", "+50%"])
        ax.set_title(market[0], loc="left", fontsize=9, color=INK, pad=14)
        ax.text(0, 1.005, f"■ {a_lab}", color=SERIES[0], fontsize=7, transform=ax.transAxes, va="bottom")
        ax.text(0.38, 1.005, f"■ {b_lab}", color=SERIES[1], fontsize=7, transform=ax.transAxes, va="bottom")
        if k == 0:
            labels = []
            for r in rows:
                games = r.get("Games") or 0
                labels.append(f"{short(r.get('Group') or r.get('Split'))} ({games})")
            ax.set_yticks(ys, labels, fontsize=7.5, color=INK)
    _heading(fig, title, SUBTITLE + (f" {note}" if note else ""))
    _save(fig, path, top=1 - 0.72 / height)
    return True


_CMAP = LinearSegmentedColormap.from_list("roi", ["#e34948", "#f6f6f4", "#1baf7a"])


def grid_chart(rows: list[dict], level: str, path: Path) -> bool:
    """Three heatmaps (favorite moneyline, favorite spread, over) for one two-way grid."""
    rows = [r for r in rows if r.get("Level name") == level]
    if not rows or level not in GRIDS:
        return False
    _, title = GRIDS[level]
    row_vals = list(dict.fromkeys(r["Group"] for r in rows))
    col_vals = list(dict.fromkeys(r["Column"] for r in rows))
    cell = {(r["Group"], r["Column"]): r for r in rows}
    height = 0.55 * len(row_vals) + 2.2
    fig, axes = plt.subplots(1, 3, figsize=(WIDTH_IN, height), dpi=DPI, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    sides = ((MARKETS[0], "Favorites, moneyline"), (MARKETS[1], "Favorites, spread"), (MARKETS[2], "Overs"))
    for k, (ax, (market, label)) in enumerate(zip(axes, sides)):
        ax.set_facecolor(SURFACE)
        for side in ax.spines.values():
            side.set_visible(False)
        ax.tick_params(colors=INK_2, labelsize=7, length=0)
        for i, rv in enumerate(row_vals):
            for j, cv in enumerate(col_vals):
                r = cell.get((rv, cv))
                v, _, games = market_values(r, market) if r else (None, None, 0)
                y = len(row_vals) - 1 - i
                if not games or v is None:
                    ax.add_patch(plt.Rectangle((j, y), 0.95, 0.9, color="#f6f6f4", linewidth=0))
                    ax.text(j + 0.475, y + 0.45, "no games", ha="center", va="center", fontsize=5.5, color=INK_2)
                    continue
                colour = _CMAP((max(-40, min(40, v)) + 40) / 80)
                ax.add_patch(plt.Rectangle((j, y), 0.95, 0.9, color=colour, linewidth=0, alpha=1.0 if games >= THIN else 0.4))
                ax.text(j + 0.475, y + 0.55, f"{v:+.0f}%", ha="center", va="center", fontsize=7.5,
                        fontweight="bold", color=INK if games >= THIN else INK_2)
                ax.text(j + 0.475, y + 0.25, f"{games} g", ha="center", va="center", fontsize=5.5, color=INK_2)
        ax.set_xlim(0, len(col_vals))
        ax.set_ylim(0, len(row_vals))
        ax.set_xticks([j + 0.475 for j in range(len(col_vals))], [textwrap.fill(short(c), 14) for c in col_vals],
                      fontsize=6.5, color=INK, rotation=0)
        ax.set_title(label, loc="left", fontsize=9, color=INK, pad=6)
        if k == 0:
            ax.set_yticks([len(row_vals) - 1 - i + 0.45 for i in range(len(row_vals))],
                          [short(r) for r in row_vals], fontsize=7.5, color=INK)
    _heading(fig, title, "Return per unit, season to date: favorites on the moneyline at the closing price, "
                         "favorites against the spread and overs at -110.\nGreen: positive, red: negative; faded "
                         f"cells have fewer than {THIN} games. Underdogs and unders are roughly the mirror, minus the margin.")
    _save(fig, path, top=1 - 0.7 / height)
    return True


def build_charts(db, out_dir: Path) -> dict[str, bool]:
    from model import cfb_pattern_csv as cc
    from model import cfb_pattern_history as history
    from model import cfb_pattern_measures as measures

    rows = cc.market_breakdown_table(measures.market_breakdown(measures.load_base_rate_games(db)))
    prior_rows = history.prior_breakdown_table(db)
    made: dict[str, bool] = {}
    for level, (slug, _, _) in SINGLE_CUTS.items():
        prior = {r["Split"]: r for r in prior_rows if r.get("Level name") == level}
        made[f"scenario_{slug}.png"] = scenario_chart(rows, prior, level, out_dir / f"scenario_{slug}.png")
    for level, (slug, _) in GRIDS.items():
        made[f"grid_{slug}.png"] = grid_chart(rows, level, out_dir / f"grid_{slug}.png")
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
