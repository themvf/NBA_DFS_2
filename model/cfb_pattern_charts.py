"""PNG charts of the CFB pattern watch for pasting into a newsletter.

Reads the same tables as the CSV export (model/cfb_pattern_csv.py) and writes
three images sized for Substack's column (1456 px wide at 2x):

  cumulative_units.png   running units by game date, one line per pattern
  pattern_units.png      units to date by pattern, with each record
  weekly_favorites.png   favorites' win rate against the rate their prices implied

Colors are the dataviz reference palette (categorical slots in fixed order,
light surface). Images are snapshots; the site page is the live view.

    python -m model.cfb_pattern_charts [--out-dir artifacts/cfb_pattern_watch/charts]
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from config import load_config  # noqa: E402
from db.database import DatabaseManager  # noqa: E402
from model import cfb_pattern_csv as cc  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "charts"
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
WIDTH_IN, DPI = 7.28, 200   # 1456 px wide


def _style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold", color=INK, pad=22)
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=8.5, color=INK_2, va="bottom")


def _figure(height: float = 4.0):
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    return fig, ax


def _save(fig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def _day(d: str) -> str:
    x = date.fromisoformat(d)
    return f"{x:%b} {x.day}"


def cumulative_chart(cum: list[dict], path: Path) -> bool:
    if not cum:
        return False
    codes = [k for k in cum[0] if k not in ("Date", "All patterns")]
    xs = list(range(len(cum)))
    fig, ax = _figure()
    ax.axhline(0, color=INK_2, linewidth=0.8)
    for i, code in enumerate(codes):
        ax.plot(xs, [r[code] for r in cum], color=SERIES[i], linewidth=2, marker="o", markersize=4, label=code)
    # Direct-label the end of each line, largest moves first, skipping any that
    # would collide with one already placed; the legend still names every line.
    ends = sorted(((cum[-1][c], c) for c in codes), key=lambda t: -abs(t[0]))
    lo, hi = ax.get_ylim()
    gap = (hi - lo) * 0.06
    placed: list[float] = []
    for y, code in ends:
        if all(abs(y - p) >= gap for p in placed):
            placed.append(y)
            ax.annotate(f"{code} {y:+.1f}", (xs[-1], y), xytext=(6, 0), textcoords="offset points",
                        fontsize=7.5, color=INK, va="center")
    ax.set_xticks(xs, [_day(r["Date"]) for r in cum])
    ax.set_xlim(-0.3, len(xs) - 1 + 0.9)
    ax.set_ylabel("units", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", ncol=len(codes), fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, "Running units by pattern", "One unit per qualifying game, graded at the closing line.")
    _save(fig, path)
    return True


def pattern_chart(patterns: list[dict], path: Path) -> bool:
    if not patterns:
        return False
    labels = [f"{p['Pattern']}  {p['Rule']}" for p in patterns]
    units = [p["Units"] for p in patterns]
    fig, ax = _figure(3.6)
    ys = list(range(len(patterns)))[::-1]
    ax.barh(ys, units, color=SERIES[0], height=0.55)
    ax.axvline(0, color=INK_2, linewidth=0.8)
    span = max(1.0, max(abs(u) for u in units))
    for y, p in zip(ys, patterns):
        u = p["Units"]
        txt = f"{u:+.2f}  ({p['Record']}, {p['Graded']}/{p['Sample floor']} games)" if p["Graded"] else "no graded games yet"
        # Labels always sit right of the bar end or the zero line, so a negative
        # bar never pushes its label into the row names.
        ax.text(max(u, 0.0) + 0.03 * span, y, txt, va="center", ha="left", fontsize=7.5, color=INK)
    ax.set_yticks(ys, labels, fontsize=7.5, color=INK)
    ax.set_xlim(min(0.0, min(units)) - span * 0.1, max(0.0, max(units)) + span * 1.3)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    _style(ax, "Units to date by pattern", "Record and progress toward each pattern's minimum sample in brackets.")
    _save(fig, path)
    return True


def weekly_chart(weeks: list[dict], path: Path) -> bool:
    weeks = [w for w in weeks if w["Favorites won (%)"] is not None and w["Price implied (%)"] is not None]
    if not weeks:
        return False
    xs = [w["Week"] for w in weeks]
    won = [w["Favorites won (%)"] for w in weeks]
    implied = [w["Price implied (%)"] for w in weeks]
    fig, ax = _figure()
    ax.plot(xs, won, color=SERIES[0], linewidth=2, marker="o", markersize=4, label="Favorites won")
    ax.plot(xs, implied, color=SERIES[1], linewidth=2, marker="o", markersize=4, label="Price implied")
    ax.annotate(f"won {won[-1]:.1f}%", (xs[-1], won[-1]), xytext=(6, 4), textcoords="offset points", fontsize=7.5, color=INK)
    ax.annotate(f"implied {implied[-1]:.1f}%", (xs[-1], implied[-1]), xytext=(6, -10), textcoords="offset points", fontsize=7.5, color=INK)
    ax.set_xticks(xs, [f"Wk {x}" for x in xs])
    ax.set_xlim(min(xs) - 0.3, max(xs) + 1.1)
    ax.set_ylabel("percent of games", color=INK_2, fontsize=8)
    ax.legend(loc="lower left", fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, "Are favorites still beating their prices?",
           "All FBS games with a closing line. Implied = closing moneyline with the bookmaker's margin removed.")
    _save(fig, path)
    return True


def roi_band_chart(rows: list[dict], key: str, title: str, path: Path) -> bool:
    """Favorite and underdog ROI side by side for each band (rows from market_breakdown_table)."""
    rows = [r for r in rows if r["Level name"] == key and r["Games"]]
    if not rows:
        return False
    labels = [f"{r['Split'].replace('Spread ', '').replace('Total ', '')}\n{r['Games']} games" for r in rows]
    fav = [r["ROI (%)"] or 0.0 for r in rows]
    dog = [r["Underdog ROI (%)"] or 0.0 for r in rows]
    xs = list(range(len(rows)))
    fig, ax = _figure(3.8)
    w = 0.38
    ax.bar([x - w / 2 for x in xs], fav, width=w, color=SERIES[0], label="Favorites")
    ax.bar([x + w / 2 for x in xs], dog, width=w, color=SERIES[1], label="Underdogs")
    ax.axhline(0, color=INK_2, linewidth=0.8)
    for x, f, d in zip(xs, fav, dog):
        for xx, v in ((x - w / 2, f), (x + w / 2, d)):
            ax.annotate(f"{v:+.1f}%", (xx, v), xytext=(0, 3 if v >= 0 else -10), textcoords="offset points",
                        ha="center", fontsize=7, color=INK)
    ax.set_xticks(xs, labels, fontsize=7.5, color=INK)
    ax.margins(y=0.12)
    ax.set_ylabel("ROI per unit, %", color=INK_2, fontsize=8)
    ax.legend(loc="lower left", fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, title, "One unit on each side at its closing moneyline, all FBS games with a closing line.")
    _save(fig, path)
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    from model import cfb_pattern_measures as measures
    from model import cfb_pattern_store as store

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    rows = store.read_ledger(db)
    base_games = measures.load_base_rate_games(db)
    rates = measures.weekly_base_rates(base_games)
    breakdown = cc.market_breakdown_table(measures.market_breakdown(base_games))
    out = Path(args.out_dir)
    made = {
        "cumulative_units.png": cumulative_chart(cc.cumulative_table(rows), out / "cumulative_units.png"),
        "pattern_units.png": pattern_chart(cc.patterns_table(rows), out / "pattern_units.png"),
        "weekly_favorites.png": weekly_chart(cc.weekly_table(rates), out / "weekly_favorites.png"),
        "roi_by_spread.png": roi_band_chart(breakdown, "By spread", "Moneyline ROI by spread", out / "roi_by_spread.png"),
        "roi_by_total.png": roi_band_chart(breakdown, "By total", "Moneyline ROI by total", out / "roi_by_total.png"),
    }
    for name, ok in made.items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
