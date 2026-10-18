"""Key numbers: how often college football games land on each final margin.

Descriptive, evergreen content for the CFB pattern-watch page. Nothing here is
a signal or a bet. Four tables:

  key_margins     share of games by final margin (1..40, then 41+), college
                  FBS-vs-FBS regular season pooled 2016-2026, with the NFL
                  regular season beside it where its scores exist
  key_numbers     the most common margins, college share next to NFL share
                  and the ratio (is 3 or 7 really less special in college?)
  spread_landing  2026 games with a closing spread: how often the final
                  margin landed within a point of the spread, how often a
                  cover was decided by 3 points or less, and push rates at
                  integer lines
  total_landing   2026 games with a closing total: combined score minus the
                  total, in 3-point bins

Margin is abs(home - away). Overtime games are included (college overtime
cannot end tied). Tied games (NFL only) are counted but have no margin row.

Closing lines come only from 2026 (`load_base_rate_games`); earlier seasons'
lines are not used here.

    python -m model.cfb_key_numbers [--chart artifacts/cfb_pattern_watch/charts/key_margins.png]
"""
from __future__ import annotations

import argparse
import math
from collections import Counter
from pathlib import Path

from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, _figure, _save, _style

KEY_VERSION = "cfb-key-numbers-v1"
CFB_FIRST_SEASON = 2016
MAX_MARGIN = 40           # rows 1..40, then "41+"
CHART_MAX = 30
KEY_LINES = (3, 7, 10, 14, 17, 21)
TOTAL_BIN = 3
TOTAL_EDGE = 24            # bins run from -24 to +24, open at both ends
LEAGUES = ("cfb", "nfl")


def _r(v: float | None, d: int = 1) -> float | None:
    return None if v is None else round(v, d)


def _margins(games: list[dict], league: str) -> tuple[Counter, int, int]:
    """(margin counter, games with a margin, ties) for one league."""
    c: Counter = Counter()
    ties = 0
    for g in games:
        if (g.get("league") or "cfb") != league:
            continue
        hs, as_ = g.get("home_score"), g.get("away_score")
        if hs is None or as_ is None:
            continue
        m = abs(int(hs) - int(as_))
        if m == 0:
            ties += 1
        else:
            c[min(m, MAX_MARGIN + 1)] += 1
    return c, sum(c.values()), ties


def _label(m: int) -> str:
    return f"{MAX_MARGIN + 1}+" if m > MAX_MARGIN else str(m)


# ── 1. margin distribution ───────────────────────────────────────────────────
def margin_distribution(games: list[dict]) -> list[dict]:
    """One row per final margin. Each game carries `league` ('cfb' or 'nfl', default 'cfb').

    Shares are of games with a nonzero margin in that league. NFL columns are
    None when no NFL games are supplied.
    """
    per = {lg: _margins(games, lg) for lg in LEAGUES}
    if per["cfb"][1] == 0 and per["nfl"][1] == 0:
        return []
    rows, cum = [], {lg: 0 for lg in LEAGUES}
    for m in range(1, MAX_MARGIN + 2):
        row: dict = {"Margin": _label(m)}
        for lg in LEAGUES:
            c, n, _ = per[lg]
            name = lg.upper()
            if n == 0:
                row[f"{name} games"] = row[f"{name} share (%)"] = row[f"{name} cumulative (%)"] = None
                continue
            k = c.get(m, 0)
            cum[lg] += k
            row[f"{name} games"] = k
            row[f"{name} share (%)"] = _r(100 * k / n, 2)
            row[f"{name} cumulative (%)"] = _r(100 * cum[lg] / n, 1)
        rows.append(row)
    return rows


# ── 2. key numbers ───────────────────────────────────────────────────────────
def key_number_table(games: list[dict], top: int = 10) -> list[dict]:
    """The most common margins in either league, ranked by college share.

    Union of each league's top `top` margins (41+ excluded), so a margin that
    matters in only one league still shows. `CFB vs NFL` is CFB share / NFL share.
    """
    cc, cn, _ = _margins(games, "cfb")
    nc, nn, _ = _margins(games, "nfl")
    if cn == 0:
        return []
    def tops(c: Counter) -> list[int]:
        ranked = sorted((m for m in c if m <= MAX_MARGIN), key=lambda m: (-c[m], m))
        return ranked[:top]
    pick = set(tops(cc)) | (set(tops(nc)) if nn else set())
    cfb_rank = {m: i + 1 for i, m in enumerate(sorted((m for m in cc if m <= MAX_MARGIN), key=lambda m: (-cc[m], m)))}
    nfl_rank = {m: i + 1 for i, m in enumerate(sorted((m for m in nc if m <= MAX_MARGIN), key=lambda m: (-nc[m], m)))}
    rows = []
    for m in sorted(pick, key=lambda m: (-cc.get(m, 0), m)):
        cs = 100 * cc.get(m, 0) / cn
        ns = 100 * nc.get(m, 0) / nn if nn else None
        rows.append({
            "Margin": m,
            "CFB rank": cfb_rank.get(m),
            "CFB games": cc.get(m, 0),
            "CFB share (%)": _r(cs, 2),
            "NFL rank": nfl_rank.get(m) if nn else None,
            "NFL games": nc.get(m, 0) if nn else None,
            "NFL share (%)": _r(ns, 2),
            "CFB vs NFL": _r(cs / ns, 2) if ns else None,
        })
    return rows


# ── 3. spread landing ────────────────────────────────────────────────────────
def _row(measure: str, line: str, games: int, count: int) -> dict:
    return {"Measure": measure, "Line": line, "Games": games, "Count": count,
            "Share (%)": _r(100 * count / games, 1) if games else None}


def spread_landing(games: list[dict]) -> list[dict]:
    """Final margin against the closing spread (home_spread < 0 = home favored)."""
    gs = [g for g in games if g.get("home_spread") is not None
          and g.get("home_score") is not None and g.get("away_score") is not None]
    if not gs:
        return []
    # cover margin: home margin plus home spread; 0 = push, >0 home covered
    cover = [(g["home_score"] - g["away_score"]) + float(g["home_spread"]) for g in gs]
    rows = [_row("Final margin within 1 point of the spread", "All lines", len(gs),
                 sum(1 for c in cover if abs(c) <= 1))]
    decided = [c for c in cover if c != 0]
    rows.append(_row("Cover decided by 3 points or less", "All lines (pushes left out)", len(decided),
                     sum(1 for c in decided if abs(c) <= 3)))
    rows.append(_row("Cover decided by 7 points or less", "All lines (pushes left out)", len(decided),
                     sum(1 for c in decided if abs(c) <= 7)))
    integer = [(abs(float(g["home_spread"])), c) for g, c in zip(gs, cover)
               if float(g["home_spread"]).is_integer() and float(g["home_spread"]) != 0]
    for line in KEY_LINES:
        sub = [c for s, c in integer if s == line]
        rows.append(_row("Push at the closing line", f"{line}", len(sub), sum(1 for c in sub if c == 0)))
    other = [c for s, c in integer if s not in KEY_LINES]
    rows.append(_row("Push at the closing line", "Other integer lines", len(other), sum(1 for c in other if c == 0)))
    rows.append(_row("Push at the closing line", "All integer lines", len(integer), sum(1 for _, c in integer if c == 0)))
    return rows


# ── 4. total landing ─────────────────────────────────────────────────────────
def _total_bin(miss: float) -> int:
    lo = math.floor(miss / TOTAL_BIN) * TOTAL_BIN
    return max(-TOTAL_EDGE - TOTAL_BIN, min(TOTAL_EDGE, lo))


def _total_label(lo: int) -> str:
    if lo < -TOTAL_EDGE:
        return f"Under by more than {TOTAL_EDGE}"
    if lo >= TOTAL_EDGE:
        return f"Over by {TOTAL_EDGE} or more"
    return f"{_signed(lo)} to {_signed(lo + TOTAL_BIN)}"


def _signed(v: int) -> str:
    return "0" if v == 0 else f"{v:+d}"


def total_landing(games: list[dict]) -> list[dict]:
    """Combined score minus the closing total, in 3-point bins [lo, lo+3)."""
    misses = [(g["home_score"] + g["away_score"]) - float(g["vegas_total"]) for g in games
              if g.get("vegas_total") is not None and g.get("home_score") is not None
              and g.get("away_score") is not None]
    if not misses:
        return []
    c = Counter(_total_bin(m) for m in misses)
    n, cum, rows = len(misses), 0, []
    for lo in range(-TOTAL_EDGE - TOTAL_BIN, TOTAL_EDGE + 1, TOTAL_BIN):
        k = c.get(lo, 0)
        cum += k
        rows.append({"Miss (points)": _total_label(lo), "Games": k,
                     "Share (%)": _r(100 * k / n, 1), "Cumulative (%)": _r(100 * cum / n, 1)})
    return rows


# ── loaders ──────────────────────────────────────────────────────────────────
_CFB_SQL = """
SELECT m.season, m.home_score, m.away_score
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
WHERE m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND COALESCE(m.season_type, 'regular') = 'regular'
  AND ht.classification = 'fbs' AND at.classification = 'fbs'
  AND m.season >= %s
"""

_NFL_SQL = """
SELECT season, home_score, away_score
FROM nfl_season_games
WHERE game_type = 'REG' AND completed AND home_score IS NOT NULL AND away_score IS NOT NULL
  AND season >= %s
"""


def load_games(db) -> list[dict]:
    games = [dict(r, league="cfb") for r in db.execute(_CFB_SQL, (CFB_FIRST_SEASON,))]
    games += [dict(r, league="nfl") for r in db.execute(_NFL_SQL, (CFB_FIRST_SEASON,))]
    return games


def load_lined_2026(db) -> list[dict]:
    """2026 FBS-vs-FBS regular-season games with closing lines (pattern-watch loader)."""
    from model.cfb_pattern_measures import load_base_rate_games
    return [g for g in load_base_rate_games(db)
            if g.get("hclass") == "fbs" and g.get("aclass") == "fbs"
            and (g.get("season_type") or "regular") == "regular"]


def season_span(games: list[dict], league: str) -> tuple[int | None, int | None, int]:
    ss = [int(g["season"]) for g in games if (g.get("league") or "cfb") == league and g.get("season") is not None]
    n = sum(1 for g in games if (g.get("league") or "cfb") == league)
    return (min(ss) if ss else None, max(ss) if ss else None, n)


def build_tables(db) -> dict[str, list[dict]]:
    games = load_games(db)
    lined = load_lined_2026(db)
    return {
        "key_margins": margin_distribution(games),
        "key_numbers": key_number_table(games),
        "spread_landing": spread_landing(lined),
        "total_landing": total_landing(lined),
    }


# ── chart ────────────────────────────────────────────────────────────────────
def margin_chart(rows: list[dict], path: Path, subtitle: str | None = None,
                 annotate: tuple[int, ...] = (3, 7, 10, 14)) -> bool:
    """Share of games by final margin 1..30: CFB bars, NFL as an outline where present."""
    rows = [r for r in rows if r.get("Margin", "").isdigit() and int(r["Margin"]) <= CHART_MAX]
    if not rows or not any(r.get("CFB games") for r in rows):
        return False
    xs = [int(r["Margin"]) for r in rows]
    cfb = [r.get("CFB share (%)") or 0.0 for r in rows]
    has_nfl = any(r.get("NFL games") for r in rows)
    fig, ax = _figure(4.0)
    ax.bar(xs, cfb, width=0.8, color=SERIES[0], label="College (FBS vs FBS)")
    if has_nfl:
        nfl = [r.get("NFL share (%)") or 0.0 for r in rows]
        ax.bar(xs, nfl, width=0.8, facecolor="none", edgecolor=SERIES[1], linewidth=1.2, label="NFL")
    for k in annotate:
        if k in xs:
            i = xs.index(k)
            top = max(cfb[i], nfl[i] if has_nfl else 0.0)
            text = f"{cfb[i]:.1f}%" + (f"\n{nfl[i]:.1f}%" if has_nfl else "")
            ax.annotate(text, (k, top), xytext=(0, 3), textcoords="offset points",
                        ha="center", fontsize=6, color=INK, linespacing=1.1)
    ax.set_xticks(xs, [str(x) for x in xs], fontsize=6.5, color=INK)
    ax.set_xlim(0.3, CHART_MAX + 0.7)
    ax.margins(y=0.15)
    ax.set_xlabel("final margin, points", color=INK_2, fontsize=8)
    ax.set_ylabel("share of games, %", color=INK_2, fontsize=8)
    ax.legend(loc="upper right", fontsize=7.5, frameon=False, labelcolor=INK)
    for sp in ("left", "bottom"):
        ax.spines[sp].set_color(GRID)
    title = "Where college football games end"
    sub = subtitle or "Share of games decided by each final margin."
    _style(ax, title, sub)
    if chr(10) in sub:   # make room for a two-line subtitle
        ax.set_title(title, loc="left", fontsize=12, fontweight="bold", color=INK, pad=34)
    _save(fig, path)
    return True


def chart_subtitle(games: list[dict]) -> str:
    c0, c1, cn = season_span(games, "cfb")
    n0, n1, nn = season_span(games, "nfl")
    s = f"Bars: college, {cn:,} FBS-vs-FBS regular-season games, {c0}-{c1}. Overtime included."
    if nn:
        s += "\n" + f"Outline: NFL, {nn:,} regular-season games, {n0}-{n1}. Labels: college share, NFL share."
    return s


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--chart", type=Path, default=None)
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    games = load_games(db)
    lined = load_lined_2026(db)
    print(chart_subtitle(games))
    for r in key_number_table(games):
        print(r)
    for r in spread_landing(lined):
        print(r)
    for r in total_landing(lined):
        print(r)
    if args.chart:
        print("chart:", margin_chart(margin_distribution(games), args.chart, chart_subtitle(games)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
