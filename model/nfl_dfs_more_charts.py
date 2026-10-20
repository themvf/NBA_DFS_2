"""More NFL DFS reader charts from the flagship contests stored from stat-api.

Five images, season to date:

    nfl_take_winning_scores      what it took: the winner's score and 100th place, by week
    nfl_take_winners_ownership   how contrarian the top 100 Millionaire lineups were vs the field
    nfl_take_showdown_captains   who the top Showdown lineups made captain, by position
    nfl_take_stack_shapes        how winners stacked their quarterback vs the whole field
    nfl_take_us_vs_winners       our best lineup against the winner and 100th place

Reads nfl_dfs_field_contests / _top_entries / _user_builds / _ownership /
_structure / _build_reviews (SELECT only). From week 6, stat-api opens only
each flagship's top 100 standings rows, so anything that needs the whole
field (the field's stack shapes) uses weeks 1-5. Descriptive, no edge claimed.

    python -m model.nfl_dfs_more_charts --out-dir artifacts/nfl_market_scenarios
"""

from __future__ import annotations

import argparse
import statistics
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import INK, INK_2, SERIES  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, _axes, _fig, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
TOP = 100
POS_ORDER = ("QB", "RB", "WR", "TE", "K", "DST")
POS_COLOUR = {"QB": SERIES[0], "RB": SERIES[1], "WR": SERIES[2], "TE": SERIES[4], "K": SERIES[3], "DST": SERIES[6]}
SHOWDOWN_KEYS = ("thursday night", "monday night", "sunday night", "kickoff")


# ── pure helpers ─────────────────────────────────────────────────────────────
def kind(contest: dict) -> str | None:
    """'millionaire' for the Classic flagship, 'showdown' for the Thursday/Monday/Sunday/Kickoff flagships."""
    name = str(contest.get("contest_name") or "").lower()
    if contest.get("format") == "classic" and "millionaire" in name and "mega" not in name:
        return "millionaire"
    if contest.get("format") == "showdown" and any(k in name for k in SHOWDOWN_KEYS):
        return "showdown"
    return None


def game_label(contest: dict) -> str:
    name = str(contest.get("contest_name") or "")
    teams = name[name.rfind("(") + 1:name.rfind(")")] if "(" in name else ""
    day = next((d for d in ("Thursday", "Monday", "Sunday", "Kickoff") if d.lower() in name.lower()), "")
    return f"Wk {contest['week']} {day} {teams}".strip()


def top_scores(entries: list[dict], top: int = TOP) -> dict | None:
    """Winner's points and the 100th-place points from rank-ordered top entries."""
    ranked = sorted((e for e in entries if e.get("rank") is not None and e.get("points") is not None),
                    key=lambda e: e["rank"])
    if not ranked:
        return None
    inside = [e for e in ranked if e["rank"] <= top]
    return {"winner": ranked[0]["points"], "cut": min(e["points"] for e in inside) if inside else None,
            "cut_rank": max(e["rank"] for e in inside) if inside else None}


def field_avg_ownership(own_pcts: list[float]) -> float | None:
    """A random field lineup's expected ownership total: sum of own% squared / 100.

    A player in p% of lineups appears in a given lineup with chance p/100 and
    then adds p to its total, so the expectation is the sum of p * p / 100.
    """
    vals = [float(p) for p in own_pcts if p is not None]
    return sum(p * p for p in vals) / 100 if vals else None


def captain_mix(lineups: list[dict], top: int = TOP) -> dict | None:
    """Position counts of captains among lineups ranked within ``top`` (rosters as stat-api seats)."""
    picked = [l for l in lineups if l.get("rank") is not None and l["rank"] <= top and l.get("roster")]
    if not picked:
        return None
    counts = Counter()
    names = Counter()
    for l in picked:
        cpt = next((s for s in l["roster"] if s[0] == "CPT"), None)
        if cpt:
            counts[cpt[3]] += 1
            names[cpt[1]] += 1
    return {"n": sum(counts.values()), "by_position": dict(counts), "names": names.most_common(3)}


def stack_rates(summaries: list[dict]) -> dict:
    """Entries-weighted stack rates for the field, the top 1% and the top 100 across complete Classic contests."""
    out = {}
    for group in ("all", "top_1pct", "top_100"):
        tot = 0
        acc: dict = defaultdict(float)
        for s in summaries:
            g = (s.get("by_rank") or {}).get(group)
            if not g or not g.get("entries"):
                continue
            n = g["entries"]
            tot += n
            for k in ("0", "1", "2", "3+"):
                acc[k] += n * float((g.get("qb_plus_catchers") or {}).get(k, 0))
            acc["bring_back"] += n * float(g.get("bring_back_any") or 0)
            acc["qb_plus_rb"] += n * float(g.get("qb_plus_rb") or 0)
        if tot:
            out[group] = {k: v / tot for k, v in acc.items()} | {"entries": tot}
    return out


# ── charts ───────────────────────────────────────────────────────────────────
def winning_scores_chart(rows: list[dict], path: Path) -> bool:
    """rows: {kind, label, week, winner, cut, entries}."""
    mil = sorted((r for r in rows if r["kind"] == "millionaire"), key=lambda r: r["week"])
    sd = sorted((r for r in rows if r["kind"] == "showdown"), key=lambda r: (r["week"], r["label"]))
    if not mil and not sd:
        return False
    height = 4.9
    fig, (a1, a2) = _fig(height, 2, gridspec_kw={"width_ratios": [1, 1.35]})
    for ax, data, title in ((a1, mil, "Millionaire (Sunday main slate)"), (a2, sd, "Thursday and Monday Showdowns")):
        _axes(ax, xgrid=False)
        xs = list(range(len(data)))
        ax.bar(xs, [r["winner"] for r in data], color=SERIES[0], width=0.6, label="Winner")
        cuts = [(x, r["cut"]) for x, r in zip(xs, data) if r["cut"] is not None]
        ax.scatter([c[0] for c in cuts], [c[1] for c in cuts], color=SERIES[1], zorder=3, s=36, marker="D",
                   label="100th place")
        for x, r in zip(xs, data):
            ax.text(x, r["winner"] + 2, f"{r['winner']:.0f}", ha="center", fontsize=7, color=INK)
        ax.set_xticks(xs, [r["label"] for r in data], fontsize=6.5, rotation=0 if ax is a1 else 35,
                      ha="center" if ax is a1 else "right")
        ax.set_title(title, loc="left", fontsize=9, color=INK)
        ax.set_ylim(0, max(r["winner"] for r in data) * 1.15)
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch

        ax.legend(handles=[Patch(color=SERIES[0], label="Winner"),
                           Line2D([], [], color=SERIES[1], marker="D", linestyle="", label="100th place")],
                  fontsize=7, frameon=False, loc="lower right")
    sub = ("DraftKings points of each flagship's first-place lineup (bars) and of 100th place (diamonds). "
           "The Millionaire pays $1M to first;\nfrom week 6 stat-api shows only the top 100 rows, which still "
           "covers both numbers.")
    return _finish(fig, path, "What it took to win, week by week", sub, height)


def winners_ownership_chart(rows: list[dict], path: Path) -> bool:
    """rows: {week, sums (top-100 lineup ownership totals), winner_sum, field_avg}."""
    rows = sorted((r for r in rows if r["sums"]), key=lambda r: r["week"])
    if not rows:
        return False
    height = 4.8
    fig, ax = _fig(height)
    _axes(ax, xgrid=False)
    for x, r in enumerate(rows):
        n = len(r["sums"])
        jitter = [((i * 37) % 21 - 10) / 50 for i in range(n)]
        ax.scatter([x + j for j in jitter], r["sums"], s=10, color=SERIES[0], alpha=0.35, linewidth=0)
        if r["field_avg"] is not None:
            ax.plot([x - 0.35, x + 0.35], [r["field_avg"]] * 2, color=INK, linewidth=2)
        if r["winner_sum"] is not None:
            ax.scatter([x], [r["winner_sum"]], s=70, color=GOOD, edgecolor=INK, zorder=4)
    ax.set_xticks(range(len(rows)), [f"Week {r['week']}\ntop-100 median {statistics.median(r['sums']):.0f}%"
                                     + (f"\nfield {r['field_avg']:.0f}%" if r["field_avg"] is not None else "")
                                     for r in rows], fontsize=7.5)
    ax.set_xlim(-0.6, len(rows) - 0.2)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("Lineup's ownership total (sum of its 9 players' ownership)", fontsize=8, color=INK_2)
    ax.scatter([], [], s=10, color=SERIES[0], label="Each top-100 lineup")
    ax.scatter([], [], s=70, color=GOOD, edgecolor=INK, label="First place")
    ax.plot([], [], color=INK, linewidth=2, label="Average field lineup")
    ax.legend(fontsize=7, frameon=False, loc="upper right", ncol=3)
    sub = ("DraftKings Millionaire. Ownership = the share of lineups that drafted a player; a lineup's total adds its "
           "nine players.\nLower than the black line = a lineup built from less popular players than the field's typical one.")
    return _finish(fig, path, "Did the Millionaire's best lineups fade the crowd?", sub, height)


def captains_chart(rows: list[dict], path: Path) -> bool:
    """rows: {label, n, by_position, names, winner_captain}."""
    rows = [r for r in rows if r and r.get("n")]
    if not rows:
        return False
    height = 0.48 * len(rows) + 2.3
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = list(range(len(rows)))[::-1]
    for y, r in zip(ys, rows):
        left = 0.0
        for pos in POS_ORDER:
            share = r["by_position"].get(pos, 0) / r["n"]
            if share <= 0:
                continue
            ax.barh(y, share, left=left, color=POS_COLOUR[pos], height=0.62, linewidth=0)
            if share >= 0.08:
                ax.text(left + share / 2, y, f"{pos} {share:.0%}", ha="center", va="center", fontsize=6.8,
                        color="white", fontweight="bold")
            left += share
        top_name = f"{r['names'][0][0]} ({r['names'][0][1]})" if r["names"] else "?"
        ax.text(1.01, y, f"winner: {r['winner_captain'] or '?'}\nmost used: {top_name}", fontsize=6.3,
                color=INK_2, va="center")
    ax.set_yticks(ys, [f"{r['label']} ({r['n']})" for r in rows], fontsize=7.5, color=INK)
    ax.set_xlim(0, 1.75)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1], ["0", "25%", "50%", "75%", "100%"])
    sub = ("Position of the captain (scores 1.5x) in each flagship Showdown's top-100 lineups (count in brackets), "
           "with the winning lineup's captain\nand the most-used captain (number of lineups). Pink: tight end.")
    return _finish(fig, path, "Who the best Showdown lineups made captain", sub, height)


def stack_shapes_chart(rates: dict, weeks: list[int], path: Path) -> bool:
    if "all" not in rates:
        return False
    groups = [(g, lab, c) for g, lab, c in (("all", "Whole field", NEUTRAL), ("top_1pct", "Top 1%", SERIES[0]),
                                            ("top_100", "Top 100", GOOD)) if g in rates]
    cats = [("0", "QB alone\n(no teammate)"), ("1", "QB + 1\npass catcher"), ("2", "QB + 2"), ("3+", "QB + 3 or more"),
            ("bring_back", "Bring-back\n(an opponent too)"), ("qb_plus_rb", "QB + his RB")]
    height = 4.6
    fig, ax = _fig(height)
    _axes(ax, xgrid=False)
    w = 0.8 / len(groups)
    for i, (g, lab, colour) in enumerate(groups):
        xs = [k + (i - (len(groups) - 1) / 2) * w for k in range(len(cats))]
        vals = [rates[g].get(c, 0) for c, _ in cats]
        ax.bar(xs, vals, width=w * 0.95, color=colour, label=f"{lab} ({rates[g]['entries']:,.0f} lineups)")
        for x, v in zip(xs, vals):
            ax.text(x, v + 0.01, f"{v:.0%}", ha="center", fontsize=6.3, color=INK)
    ax.set_xticks(range(len(cats)), [lab for _, lab in cats], fontsize=7.5)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_ylim(0, 0.8)
    ax.legend(fontsize=7, frameon=False, loc="upper center", ncol=3)
    sub = (f"DraftKings Millionaire, weeks {min(weeks)}-{max(weeks)} pooled (weeks with every entry's stack shape stored). "
           "Pass catchers = his own WR/TE.\nShare of lineups with each shape: whole field, top 1% and top 100 by score.")
    return _finish(fig, path, "How the winners stacked their quarterback", sub, height)


def us_vs_winners_chart(rows: list[dict], path: Path) -> bool:
    """rows: {label, ours, winner, cut, lineups}."""
    rows = [r for r in rows if r.get("ours") is not None]
    if not rows:
        return False
    height = 0.5 * len(rows) + 2.2
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = list(range(len(rows)))[::-1]
    for y, r in zip(ys, rows):
        lo = min(v for v in (r["ours"], r["cut"], r["winner"]) if v is not None)
        ax.plot([lo, r["winner"]], [y, y], color="#e4e3df", linewidth=6, solid_capstyle="round", zorder=1)
        ax.scatter([r["winner"]], [y], s=60, color=SERIES[0], zorder=3)
        if r["cut"] is not None:
            ax.scatter([r["cut"]], [y], s=50, color=SERIES[1], marker="D", zorder=3)
        colour = GOOD if r["cut"] is not None and r["ours"] >= r["cut"] else BAD
        ax.scatter([r["ours"]], [y], s=60, color=colour, edgecolor=INK, zorder=4)
        gap = r["winner"] - r["ours"]
        n = r["lineups"]
        ax.text(r["winner"] + 4, y, f"{gap:.0f} pts short of first ({n} lineup{'' if n == 1 else 's'})", fontsize=6.5,
                color=INK_2, va="center")
    ax.set_yticks(ys, [r["label"] for r in rows], fontsize=7.5, color=INK)
    hi = max(r["winner"] for r in rows)
    ax.set_xlim(min(r["ours"] for r in rows) * 0.85, hi * 1.3)
    ax.scatter([], [], s=60, color=SERIES[0], label="Winner")
    ax.scatter([], [], s=50, color=SERIES[1], marker="D", label="100th place")
    ax.scatter([], [], s=60, color=BAD, edgecolor=INK, label="Our best lineup")
    ax.legend(fontsize=7, frameon=False, loc="lower right")
    ax.set_xlabel("DraftKings points", fontsize=8, color=INK_2)
    sub = ("Our saved optimizer build for each flagship contest's slate, scored with the players' actual points: "
           "our best lineup (green if it reached the top 100).\nLineups with a player whose points are unknown are "
           "left out, not counted as zero.")
    return _finish(fig, path, "Our lineups against the winners", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def _plain(v):
    return float(v) if v is not None and not isinstance(v, (bool, int, float, str, list, dict)) else v


def load(db, season: int = SEASON) -> dict:
    contests = [dict(r) for r in db.execute(
        """SELECT contest_id, contest_name, format, week, entry_count, winning_score
           FROM nfl_dfs_field_contests WHERE season = %s AND file_name LIKE 'stat-api:%%' AND week IS NOT NULL
           ORDER BY week, contest_id""", (season,))]
    picked = [c | {"kind": kind(c)} for c in contests if kind(c)]
    # One Millionaire per week: the largest field.
    best: dict = {}
    for c in picked:
        key = (c["kind"], c["week"]) if c["kind"] == "millionaire" else (c["kind"], c["contest_id"])
        if key not in best or (c["entry_count"] or 0) > (best[key]["entry_count"] or 0):
            best[key] = c
    picked = sorted(best.values(), key=lambda c: (c["week"], c["kind"], c["contest_id"]))
    out = {"contests": picked, "entries": {}, "builds": {}, "own": {}, "structure": {}, "reviews": {}}
    for c in picked:
        cid = c["contest_id"]
        out["entries"][cid] = [dict(r) for r in db.execute(
            "SELECT rank, points, username, ownership_sum FROM nfl_dfs_field_top_entries WHERE contest_id = %s", (cid,))]
        out["builds"][cid] = [l for r in db.execute(
            "SELECT lineups FROM nfl_dfs_field_user_builds WHERE contest_id = %s", (cid,)) for l in (r["lineups"] or [])]
        out["own"][cid] = [_plain(r["drafted_pct"]) for r in db.execute(
            "SELECT drafted_pct FROM nfl_dfs_field_ownership WHERE contest_id = %s", (cid,))]
        s = db.execute("SELECT summary FROM nfl_dfs_field_structure WHERE contest_id = %s ORDER BY computed_at DESC LIMIT 1",
                       (cid,))
        out["structure"][cid] = s[0]["summary"] if s else None
        rv = db.execute("SELECT ours FROM nfl_dfs_field_build_reviews WHERE contest_id = %s ORDER BY computed_at DESC LIMIT 1",
                        (cid,))
        out["reviews"][cid] = rv[0]["ours"] if rv else None
    return out


def _float(v):
    return None if v is None else float(v)


def build_charts(out_dir: Path, db=None, season: int = SEASON) -> dict[str, bool]:
    if db is None:
        from config import load_config
        from db.database import DatabaseManager

        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out_dir = Path(out_dir)
    d = load(db, season)
    score_rows, own_rows, cap_rows, us_rows, summaries, stack_weeks = [], [], [], [], [], []
    for c in d["contests"]:
        cid = c["contest_id"]
        entries = [e | {"points": _float(e["points"]), "ownership_sum": _float(e["ownership_sum"])} for e in d["entries"][cid]]
        ts = top_scores(entries)
        label = f"Wk {c['week']}" if c["kind"] == "millionaire" else game_label(c)
        if ts:
            score_rows.append({"kind": c["kind"], "label": label, "week": c["week"], "winner": ts["winner"],
                               "cut": ts["cut"], "entries": c["entry_count"]})
        if c["kind"] == "millionaire":
            sums = [e["ownership_sum"] for e in entries if e["rank"] is not None and e["rank"] <= TOP
                    and e["ownership_sum"] is not None]
            win = min((e for e in entries if e["ownership_sum"] is not None), key=lambda e: e["rank"], default=None)
            own_rows.append({"week": c["week"], "sums": sums, "winner_sum": win["ownership_sum"] if win else None,
                             "field_avg": field_avg_ownership(d["own"][cid])})
            s = d["structure"][cid]
            if s and ((s.get("by_rank") or {}).get("all") or {}).get("entries", 0) >= (c["entry_count"] or 0) * 0.99:
                summaries.append(s)
                stack_weeks.append(c["week"])
        else:
            lineups = [l | {"points": _float(l.get("points"))} for l in d["builds"][cid]]
            mix = captain_mix(lineups)
            if mix:
                first = min((l for l in lineups if l.get("rank") is not None), key=lambda l: l["rank"], default=None)
                cpt = next((s for s in (first or {}).get("roster") or [] if s[0] == "CPT"), None)
                cap_rows.append(mix | {"label": game_label(c), "winner_captain": f"{cpt[1]} ({cpt[3]})" if cpt else None})
        ours = d["reviews"][cid]
        if ours and ours.get("best_points") is not None and ts:
            us_rows.append({"label": label if c["kind"] == "showdown" else f"Wk {c['week']} Millionaire",
                            "ours": float(ours["best_points"]), "winner": ts["winner"], "cut": ts["cut"],
                            "lineups": ours.get("lineups_scored") or ours.get("lineups")})
    o = lambda n: out_dir / f"nfl_take_{n}.png"   # noqa: E731
    return {
        "nfl_take_winning_scores.png": winning_scores_chart(score_rows, o("winning_scores")),
        "nfl_take_winners_ownership.png": winners_ownership_chart(own_rows, o("winners_ownership")),
        "nfl_take_showdown_captains.png": captains_chart(cap_rows, o("showdown_captains")),
        "nfl_take_stack_shapes.png": stack_shapes_chart(stack_rates(summaries), stack_weeks, o("stack_shapes"))
        if stack_weeks else False,
        "nfl_take_us_vs_winners.png": us_vs_winners_chart(us_rows, o("us_vs_winners")),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--season", type=int, default=SEASON)
    args = ap.parse_args(argv)
    for name, ok in build_charts(Path(args.out_dir), season=args.season).items():
        print(f"{Path(args.out_dir) / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
