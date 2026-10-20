"""Reader charts on NFL DFS ownership, for a newsletter. Season to date, descriptive.

Nothing here claims an edge. Three images:

  nfl_take_low_owned_smashes.png        the season's best low-owned plays: every DraftKings
                                        Millionaire player in under 5% of lineups who scored 20+
  nfl_take_low_owned_vs_projection.png  our pre-lock ownership estimate against what the field did,
                                        with the low-owned corner drawn out
  nfl_take_qb_ownership.png             quarterbacks only: projected vs actual ownership, by week

Words used on the images:

* ownership: the share of lineups in the contest that drafted the player (``drafted_pct``)
* projected ownership: our estimate of that share, made before lock (the optimizer's input)

Sources (SELECT only; see CLAUDE.md "stat-api.com" and "Field-ownership prior"):

* ``nfl_dfs_field_contests`` / ``nfl_dfs_field_ownership``: each week's Classic Millionaire from
  stat-api, with every drafted player's ownership and DK points (``fpts``). From week 6 stat-api
  opened only the top 100 users, so ownership covers the players those users drafted.
* ``nfl_dfs_field_user_builds.exposure``: position, team and salary of the players top users drafted
* ``nfl_dfs_optimizer_runs.input_snapshot``: the latest pregame, non-research optimizer run on the
  contest's slate. ``ownership.total`` is our projected ownership; name, team, position and salary
  fill in what the contest rows lack.

A slate player missing from the contest's ownership is counted as 0% owned only when the
contest's ownership adds to at least 99.5% of the 900% a nine-player lineup holds; otherwise he
is left out and counted.

    python -m model.nfl_ownership_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import math
import re
import statistics as st
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.cfb_scenario_charts import _heading  # noqa: E402
from model.cfb_team_charts import _place_labels  # noqa: E402
from model.nfl_dfs_story_charts import _plain, _short, load_build_points, load_millionaires  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, _axes, _fig, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_dfs_story" / "charts"
LINEUP_MASS = 900.0        # nine slots x 100%
MIN_MASS = 0.995           # ownership share needed before an absent slate player counts as 0%
LOW_OWN = 5.0              # "low-owned": under 5% of lineups
SMASH_POINTS = 20.0
RELEVANT_OWN = 0.5         # chart 6 plots players projected or drafted at 0.5%+
POS_COLOUR = {"QB": SERIES[0], "RB": SERIES[1], "WR": SERIES[2], "TE": SERIES[4], "DST": SERIES[6]}
MISS_HIGH = SERIES[3]
OWN_NOTE = "Ownership = the share of lineups that drafted the player."
PROJ_NOTE = "Projected ownership = our estimate of that share, made before lock."
ET = ZoneInfo("America/New_York")
_SUFFIX = re.compile(r"(jr|sr|ii|iii|iv|v)$")


# ── pure computations ────────────────────────────────────────────────────────
def norm(name) -> str:
    """Lowercase alphanumerics: 'Ja'Marr Chase' -> 'jamarrchase'."""
    return "".join(ch for ch in str(name or "").lower() if ch.isalnum())


def norm_loose(name) -> str:
    """``norm`` without a trailing Jr/Sr/II/III/IV/V, for names one source suffixes and one does not."""
    parts = [p for p in re.split(r"[\s.]+", str(name or "").lower()) if p]
    if len(parts) > 2 and _SUFFIX.fullmatch(norm(parts[-1])):
        parts = parts[:-1]
    return norm(" ".join(parts))


def parse_kickoff(game_info: str | None) -> datetime | None:
    """'ARI@LAR 10/18/2026 04:05PM ET' -> the kickoff in UTC; None when unreadable."""
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4}) (\d{1,2}:\d{2}[AP]M)", str(game_info or ""))
    if not m:
        return None
    local = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%m/%d/%Y %I:%M%p").replace(tzinfo=ET)
    return local.astimezone(timezone.utc)


def first_kickoff(snapshot: list[dict]) -> datetime | None:
    times = [t for t in (parse_kickoff(p.get("gameInfo")) for p in snapshot or []) if t]
    return min(times) if times else None


def ownership_mass(own_rows: list[dict]) -> float:
    """Share of the 900% a classic lineup holds that the contest's ownership rows account for."""
    return sum(float(r.get("drafted_pct") or 0) for r in own_rows) / LINEUP_MASS


def projected_own(p: dict) -> float | None:
    own = p.get("ownership")
    if not isinstance(own, dict) or own.get("total") is None:
        return None
    return float(own["total"])


def _index(snapshot: list[dict]) -> tuple[dict, dict]:
    strict, loose = collections.defaultdict(list), collections.defaultdict(list)
    for p in snapshot or []:
        strict[norm(p.get("name"))].append(p)
        loose[norm_loose(p.get("name"))].append(p)
    return strict, loose


def _resolve(cands: list[dict], info: dict) -> tuple[dict | None, bool]:
    """(the one candidate, ambiguous?). Several candidates are split by the contest's team, then position."""
    if len(cands) == 1:
        return cands[0], False
    if not cands:
        return None, False
    for key in ("team", "position"):
        if info.get(key):
            narrowed = [c for c in cands if c.get(key) == info[key]]
            if len(narrowed) == 1:
                return narrowed[0], False
            if narrowed:
                cands = narrowed
    return None, True


def match_week(own_rows: list[dict], snapshot: list[dict], info: dict[str, dict] | None = None,
               min_mass: float = MIN_MASS) -> tuple[list[dict], dict]:
    """Join one contest's ownership to one optimizer snapshot.

    ``own_rows``: player_name, normalized_name, drafted_pct, fpts. ``snapshot``: the run's
    input players (name, team, position, salary, ownership.total, isOut). ``info``: normalized
    name -> {position, team, salary} from the stat-api builds.

    Returns (players, stats). Each player: name, position, team, salary, actual (ownership %),
    points (None when absent from the contest), proj (None when not projected), in_contest.
    Slate players missing from the contest become 0% owned only when the contest's ownership
    mass reaches ``min_mass``; else they are dropped and counted. Ruled-out slate players are
    never added.
    """
    info = info or {}
    strict, loose = _index(snapshot)
    used: set[int] = set()
    players: list[dict] = []
    stats = collections.Counter()
    for r in own_rows:
        key = r.get("normalized_name") or norm(r.get("player_name"))
        meta = info.get(key, {})
        cands = strict.get(key) or loose.get(norm_loose(r.get("player_name"))) or []
        hit, ambiguous = _resolve(cands, meta)
        if hit is not None and id(hit) in used:
            hit, ambiguous = None, True
        stats["contest_players"] += 1
        if hit is None:
            stats["ambiguous" if ambiguous else "unmatched"] += 1
        else:
            used.add(id(hit))
            stats["matched"] += 1
        src = hit or {}
        players.append({
            "name": r.get("player_name"), "actual": float(r.get("drafted_pct") or 0.0),
            "points": None if r.get("fpts") is None else float(r["fpts"]),
            "position": src.get("position") or meta.get("position"), "team": src.get("team") or meta.get("team"),
            "salary": src.get("salary") if src.get("salary") is not None else meta.get("salary"),
            "proj": projected_own(src) if hit else None, "in_contest": True})
    mass = ownership_mass(own_rows)
    stats["mass_pct"] = round(100 * mass, 2)
    for p in snapshot or []:
        if id(p) in used or projected_own(p) is None:
            continue
        if p.get("isOut"):
            stats["absent_out"] += 1
            continue
        if mass < min_mass:
            stats["absent_dropped"] += 1
            continue
        stats["absent_zeroed"] += 1
        players.append({"name": p.get("name"), "actual": 0.0, "points": None, "position": p.get("position"),
                        "team": p.get("team"), "salary": p.get("salary"), "proj": projected_own(p),
                        "in_contest": False})
    return players, dict(stats)


def low_owned_smashes(weeks: dict[int, list[dict]], own_max: float = LOW_OWN,
                      min_points: float = SMASH_POINTS) -> list[dict]:
    """Every contest player under ``own_max``% owned who scored ``min_points``+, best first."""
    out = [{**p, "week": w} for w, players in weeks.items() for p in players
           if p.get("in_contest", True) and p.get("points") is not None
           and p["actual"] < own_max and p["points"] >= min_points]
    return sorted(out, key=lambda p: (-p["points"], p["actual"], p["week"], str(p["name"])))


def _ranks(xs: list[float]) -> list[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return ranks


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = st.mean(xs), st.mean(ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    return None if sxx == 0 or syy == 0 else sxy / math.sqrt(sxx * syy)


def error_stats(pairs: list[dict]) -> dict:
    """n, correlation, rank correlation (ties averaged), average signed error (projected minus
    actual) and average miss (absolute) over players with a projection."""
    rows = [p for p in pairs if p.get("proj") is not None]
    xs = [p["proj"] for p in rows]
    ys = [p["actual"] for p in rows]
    if not rows:
        return {"n": 0, "r": None, "rho": None, "bias": None, "mae": None}
    return {"n": len(rows), "r": _pearson(xs, ys), "rho": _pearson(_ranks(xs), _ranks(ys)),
            "bias": st.mean(x - y for x, y in zip(xs, ys)), "mae": st.mean(abs(x - y) for x, y in zip(xs, ys))}


def relevant(pairs: list[dict], floor: float = RELEVANT_OWN) -> list[dict]:
    """Players with a projection who were projected or drafted at ``floor``%+."""
    return [p for p in pairs if p.get("proj") is not None and max(p["proj"], p["actual"]) >= floor]


def low_group(pairs: list[dict], cut: float = LOW_OWN) -> list[dict]:
    """Players the field left under ``cut``% owned, or that we projected under ``cut``%."""
    return [p for p in pairs if p.get("proj") is not None and (p["actual"] < cut or p["proj"] < cut)]


def biggest_misses(pairs: list[dict], n: int = 5, cut: float = LOW_OWN) -> tuple[list[dict], list[dict]]:
    """(projected under ``cut``% but the field went higher, field under ``cut``% but we projected
    higher), each the ``n`` largest gaps in percentage points."""
    rows = [p for p in pairs if p.get("proj") is not None]
    under = sorted((p for p in rows if p["proj"] < cut and p["actual"] > p["proj"]),
                   key=lambda p: -(p["actual"] - p["proj"]))[:n]
    over = sorted((p for p in rows if p["actual"] < cut and p["proj"] > p["actual"]),
                  key=lambda p: -(p["proj"] - p["actual"]))[:n]
    return under, over


def qb_rows(pairs: list[dict], floor: float = 1.0) -> list[dict]:
    """Quarterbacks with a projection, projected or drafted at ``floor``%+, most-drafted first."""
    rows = [p for p in pairs if p.get("position") == "QB" and p.get("proj") is not None
            and max(p["proj"], p["actual"]) >= floor]
    return sorted(rows, key=lambda p: (-p["actual"], str(p["name"])))


def popular_qb_bias(weeks: dict[int, list[dict]], top: int = 5) -> dict:
    """Average signed error on each week's ``top`` most-drafted QBs, and on the rest."""
    pop, rest = [], []
    for players in weeks.values():
        qbs = qb_rows(players, floor=0.0)
        pop += qbs[:top]
        rest += qbs[top:]
    return {"popular": error_stats(pop), "rest": error_stats(rest), "top": top}


# ── chart helpers ────────────────────────────────────────────────────────────
def _pct(v: float | None, nd: int = 1) -> str:
    """'4.3%'; a value just under the 5% line keeps two decimals so it never reads as 5.0%."""
    if v is None:
        return "n/a"
    if LOW_OWN - 0.05 <= v < LOW_OWN:
        nd = max(nd, 2)
    return f"{v:.{nd}f}%"


def _money(v) -> str:
    return "" if v is None else f"${float(v) / 1000:.1f}K"


def _who(p: dict) -> str:
    bits = [b for b in (p.get("position"), p.get("team")) if b]
    return " ".join(bits)


# ── 5. the season's best low-owned plays ─────────────────────────────────────
def smashes_chart(smashes: list[dict], weeks_seen: list[int], path: Path, top: int = 20) -> bool:
    if not smashes:
        return False
    rows = smashes[:top]
    n = len(rows)
    height = 0.27 * n + 2.4
    fig = plt.figure(figsize=(9.6, height), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    top_frac = 1 - 1.05 / height
    ax = fig.add_axes((0.02, 0.02, 0.72, top_frac - 0.02))
    ax.set_facecolor(SURFACE)
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.6, n + 0.6)
    cols = [(0.0, "left", "#"), (0.045, "left", "Player"), (0.33, "left", "Pos / team"), (0.53, "right", "Week"),
            (0.64, "right", "Salary"), (0.78, "right", "Ownership"), (0.87, "right", "Points")]
    for x, ha, head in cols:
        ax.text(x, n + 0.15, head, ha=ha, va="bottom", fontsize=7.5, color=INK_2, fontweight="bold")
    ax.plot([0, 1], [n - 0.05, n - 0.05], color=GRID, linewidth=0.8)
    pmax = max(p["points"] for p in rows)
    for i, p in enumerate(rows):
        y = n - 1 - i + 0.45
        if i % 2 == 0:
            ax.add_patch(plt.Rectangle((0, y - 0.45), 1, 0.9, color="#f4f3ef", linewidth=0, zorder=0))
        # A thin bar under the points column, scaled to the best score.
        ax.add_patch(plt.Rectangle((0.885, y - 0.26), 0.11 * p["points"] / pmax, 0.52,
                                   color=GOOD, alpha=0.55, linewidth=0, zorder=1))
        colour = POS_COLOUR.get(p.get("position"), INK)
        vals = [str(i + 1), p["name"], _who(p), str(p["week"]), _money(p.get("salary")), _pct(p["actual"]),
                f"{p['points']:.1f}"]
        for (x, ha, _), v in zip(cols, vals):
            ax.text(x, y, v, ha=ha, va="center", fontsize=7.6, zorder=2,
                    color=colour if v == vals[2] else INK, fontweight="bold" if v in (vals[1], vals[6]) else "normal")
    # Per-week count.
    ax2 = fig.add_axes((0.84, 0.08, 0.14, top_frac - 0.14))
    _axes(ax2, ygrid=False)
    counts = collections.Counter(p["week"] for p in smashes)
    ws = sorted(weeks_seen)
    ys = list(range(len(ws)))[::-1]
    ax2.barh(ys, [counts.get(w, 0) for w in ws], color=GOOD, alpha=0.85, height=0.6)
    for y, w in zip(ys, ws):
        ax2.text(counts.get(w, 0) + 0.15, y, str(counts.get(w, 0)), va="center", fontsize=7.5, color=INK)
    ax2.set_yticks(ys, [f"Week {w}" for w in ws], fontsize=7.5, color=INK)
    ax2.set_xlim(0, max(counts.values()) * 1.25 + 1)
    ax2.set_title("How many each week", fontsize=8, color=INK_2, loc="left")
    ax2.xaxis.set_major_locator(plt.MaxNLocator(nbins=4, integer=True))
    total = len(smashes)
    shown = f"the top {n} of {total}" if total > n else f"all {total}"
    sub = (f"DraftKings Millionaire, weeks {ws[0]}-{ws[-1]}: every player in under {LOW_OWN:.0f}% of lineups who scored "
           f"{SMASH_POINTS:.0f}+ DK points. Showing {shown}.\n{OWN_NOTE} "
           "Hindsight list: picking these in advance is the hard part.")
    return _finish(fig, path, "The season's best low-owned plays", sub, height)


# ── 6. our low-owned projections against the field ──────────────────────────
_TICKS = [0, 0.5, 1, 2, 5, 10, 20, 50]


def projection_chart(weeks: dict[int, list[dict]], smashes: list[dict], path: Path,
                     notes: list[str] | None = None) -> bool:
    pairs = [{**p, "week": w} for w, ps in weeks.items() for p in relevant(ps)]
    if len(pairs) < 10:
        return False
    all_s = error_stats(pairs)
    low_s = error_stats(low_group(pairs))
    under, over = biggest_misses(pairs)
    height = 9.5
    fig = plt.figure(figsize=(9.6, height), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    grid = fig.add_gridspec(2, 2, height_ratios=[3.0, 1.15], width_ratios=[1, 1], hspace=0.2, wspace=0.12)
    ax = fig.add_subplot(grid[0, :])
    _axes(ax)
    for scale in (ax.set_xscale, ax.set_yscale):
        scale("symlog", linthresh=1, linscale=0.6)
    top = max(max(p["proj"], p["actual"]) for p in pairs) * 1.25
    ax.set_xlim(0, top)
    ax.set_ylim(0, top)
    ticks = [t for t in _TICKS if t <= top]
    fmt = plt.FuncFormatter(lambda v, _: f"{v:g}%")
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    ax.xaxis.set_major_formatter(fmt)
    ax.yaxis.set_major_formatter(fmt)
    ax.minorticks_off()
    ax.add_patch(plt.Rectangle((0, 0), LOW_OWN, top, color="#eef6f2", linewidth=0, zorder=0))
    ax.add_patch(plt.Rectangle((0, 0), top, LOW_OWN, color="#eef6f2", linewidth=0, zorder=0))
    ax.plot([0, top], [0, top], color=INK_2, linewidth=0.8, linestyle="--", zorder=1)
    ax.text(top * 0.6, top * 0.8, "projected = actual", fontsize=7, color=INK_2, rotation=0, ha="right")
    for pos, colour in list(POS_COLOUR.items()) + [(None, NEUTRAL)]:
        pts = [p for p in pairs if p.get("position") == pos] if pos else [p for p in pairs
                                                                           if p.get("position") not in POS_COLOUR]
        if pts:
            ax.scatter([p["proj"] for p in pts], [p["actual"] for p in pts], s=16, color=colour, alpha=0.7,
                       edgecolor=SURFACE, linewidth=0.4, zorder=3, label=pos or "other")
    marked = [(p, BAD) for p in under] + [(p, MISS_HIGH) for p in over]
    for i, (p, colour) in enumerate(marked, 1):
        ax.scatter([p["proj"]], [p["actual"]], s=60, facecolor="none", edgecolor=colour, linewidth=1.2, zorder=4)
    ax.set_xlabel("Projected ownership (our estimate before lock)", color=INK_2, fontsize=8)
    ax.set_ylabel("Actual ownership in the Millionaire", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=7, frameon=False, ncol=5)

    def stat_line(label, s):
        if not s["n"]:
            return f"{label}: none"
        return (f"{label} ({s['n']} players): correlation {s['r']:.2f}, rank correlation {s['rho']:.2f}, "
                f"average miss {s['mae']:.1f} pts, average error {s['bias']:+.1f} pts")

    # Lower left: how close, and the biggest misses.
    axl = fig.add_subplot(grid[1, 0])
    axl.axis("off")
    lines = [("How close, in percentage points of ownership", INK, "bold")]
    for wk in sorted(weeks):
        s = error_stats(relevant(weeks[wk]))
        if s["n"]:
            lines.append((f"Week {wk}: {s['n']} players, average miss {s['mae']:.1f}, error {s['bias']:+.1f}, "
                          f"rank corr. {s['rho']:.2f}", INK_2, "normal"))
    lines.append(("Red rings: we projected under 5%, the field went much higher", BAD, "bold"))
    lines += [(f"  {i}. {p['name']} (wk {p['week']}): {_pct(p['proj'])} projected, {_pct(p['actual'])} actual",
               INK, "normal") for i, p in enumerate(under, 1)]
    lines.append(("Gold rings: the field stayed under 5%, we projected much higher", MISS_HIGH, "bold"))
    lines += [(f"  {i}. {p['name']} (wk {p['week']}): {_pct(p['proj'])} projected, {_pct(p['actual'])} actual",
               INK, "normal") for i, p in enumerate(over, len(under) + 1)]
    step = 1 / max(len(lines), 15)
    for i, (txt, colour, weight) in enumerate(lines):
        axl.text(0, 1 - i * step, txt, transform=axl.transAxes, fontsize=6.9, color=colour, fontweight=weight,
                 va="top")
    # Lower right: the low-owned smashes from chart 5, with what we projected.
    axr = fig.add_subplot(grid[1, 1])
    axr.axis("off")
    proj_weeks = set(weeks)
    hits = [s for s in smashes if s["week"] in proj_weeks]
    by_key = {(w, norm(p["name"])): p for w, ps in weeks.items() for p in ps}
    rlines = [(f"Low-owned smashes (under {LOW_OWN:.0f}%, {SMASH_POINTS:.0f}+ pts), weeks "
               f"{min(proj_weeks)}-{max(proj_weeks)}: what we projected", INK, "bold")]
    for s in hits[:12]:
        m = by_key.get((s["week"], norm(s["name"])))
        proj = m.get("proj") if m else None
        rlines.append((f"{s['name']} (wk {s['week']}, {s['points']:.1f} pts): projected {_pct(proj)}, "
                       f"actual {_pct(s['actual'])}", INK, "normal"))
    if len(hits) > 12:
        rlines.append((f"  ... and {len(hits) - 12} more", INK_2, "normal"))
    under_proj = [s for s in hits if (by_key.get((s["week"], norm(s["name"]))) or {}).get("proj") is not None]
    if under_proj:
        below = sum(by_key[(s["week"], norm(s["name"]))]["proj"] < LOW_OWN for s in under_proj)
        rlines.append((f"We projected {below} of these {len(under_proj)} under {LOW_OWN:.0f}% as well: we saw the "
                       "low ownership", INK_2, "normal"))
        rlines.append(("coming, which is not the same as seeing the score.", INK_2, "normal"))
    for i, (txt, colour, weight) in enumerate(rlines):
        axr.text(0, 1 - i * step, txt, transform=axr.transAxes, fontsize=6.9, color=colour, fontweight=weight,
                 va="top")
    wk = sorted(weeks)
    sub = (f"DraftKings Millionaire, weeks {wk[0]}-{wk[-1]} (the weeks we recorded projected ownership). Each dot is a "
           f"player projected or drafted at {RELEVANT_OWN}%+; both axes stretch the low end.\n{OWN_NOTE} {PROJ_NOTE} "
           f"Shaded: under {LOW_OWN:.0f}% on either side.\n{stat_line('All players', all_s)}.\n"
           f"{stat_line('Low-owned, under 5% projected or actual', low_s)}.")
    foot = ["Miss = how far off, in percentage points of ownership; error = projected minus actual, so it shows the "
            "lean."] + list(notes or [])
    fig.text(0.01, 0.008, "\n".join(foot), fontsize=6.8, color=INK_2, va="bottom", linespacing=1.4)
    top = 1 - (0.5 + 0.15 * (sub.count("\n") + 1)) / height
    _heading(fig, "How well did we see the low-owned plays coming?", sub)
    fig.tight_layout(rect=(0, (0.1 + 0.14 * len(foot)) / height, 1, top))
    _place_labels(ax, [(p["proj"], p["actual"], str(i)) for i, (p, _) in enumerate(marked, 1)],
                  [(p["proj"], p["actual"]) for p in pairs])
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return True


# ── 7. quarterbacks ──────────────────────────────────────────────────────────
def qb_chart(weeks: dict[int, list[dict]], path: Path) -> bool:
    per = {w: qb_rows(ps) for w, ps in sorted(weeks.items())}
    per = {w: r for w, r in per.items() if r}
    if not per:
        return False
    nmax = max(len(r) for r in per.values())
    height = 0.25 * nmax + 2.6
    fig, axes = _fig(height, ncols=len(per), sharex=True)
    axes = list(axes) if len(per) > 1 else [axes]
    xmax = max(max(p["proj"], p["actual"]) for r in per.values() for p in r) * 1.12
    for ax, (w, rows) in zip(axes, per.items()):
        _axes(ax, ygrid=False)
        ys = [nmax - 1 - i for i in range(len(rows))]
        for y, p in zip(ys, rows):
            colour = BAD if p["proj"] > p["actual"] else SERIES[0]
            ax.plot([p["proj"], p["actual"]], [y, y], color=colour, alpha=0.45, linewidth=2.2, zorder=2)
            ax.scatter([p["proj"]], [y], s=26, facecolor=SURFACE, edgecolor=INK, linewidth=1.0, zorder=3)
            ax.scatter([p["actual"]], [y], s=26, color=INK, zorder=4)
        ax.set_yticks(ys, [f"{_short(p['name'], 14)} {p.get('team') or ''}" for p in rows], fontsize=6.8, color=INK)
        ax.set_ylim(-0.7, nmax - 0.3)
        ax.set_xlim(0, xmax)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
        s = error_stats(rows)
        ax.set_title(f"Week {w}: average miss {s['mae']:.1f} points", fontsize=8, color=INK, loc="left")
    for ax in axes:
        ax.set_xlabel("Ownership", color=INK_2, fontsize=7.5)
    allq = error_stats([p for r in per.values() for p in r])
    pb = popular_qb_bias(weeks)
    pop, rest = pb["popular"], pb["rest"]
    lean = ("over-projected" if pop["bias"] > 0 else "under-projected") if pop["n"] else "n/a"
    sub = (f"DraftKings Millionaire quarterbacks, weeks {min(per)}-{max(per)}, projected or drafted at 1%+, most-drafted "
           f"first. Hollow dot: our projection; filled dot: actual.\nRed line: we projected higher than the field went; "
           f"blue: lower.\n{OWN_NOTE} {PROJ_NOTE}\n"
           f"All {allq['n']} QB-weeks: average miss {allq['mae']:.1f} percentage points, rank correlation "
           f"{allq['rho']:.2f}. Each week's {pb['top']} most-drafted QBs: average error {pop['bias']:+.1f} (we {lean} "
           f"them); the rest {rest['bias']:+.1f}.\nError = projected minus actual. Three weeks is a small sample.")
    return _finish(fig, path, "Quarterbacks: our ownership estimate against the field", sub, height)


# ── loaders (SELECT only) ────────────────────────────────────────────────────
def load_own_rows(db, contest_id: str) -> list[dict]:
    return [dict(r) for r in db.execute(
        "SELECT player_name, normalized_name, drafted_pct, fpts FROM nfl_dfs_field_ownership WHERE contest_id = %s",
        (contest_id,))]


def load_pregame_snapshot(db, upload_id: str | None) -> tuple[list[dict], dict]:
    """The latest non-research run on the slate created before its first kickoff: (snapshot, info)."""
    if not upload_id:
        return [], {"run": None, "reason": "contest not linked to a slate"}
    runs = db.execute(
        """SELECT run_id, created_at, input_snapshot FROM nfl_dfs_optimizer_runs
           WHERE upload_id = %s AND NOT (settings ? 'researchStudy') AND status IN ('complete', 'partial')
           ORDER BY created_at DESC""", (upload_id,))
    kick = None
    for r in runs:
        kick = first_kickoff(_plain(r["input_snapshot"]) or [])
        if kick:
            break
    for r in runs:
        if kick and r["created_at"] >= kick:
            continue
        snap = _plain(r["input_snapshot"]) or []
        return snap, {"run": str(r["run_id"]), "created_at": r["created_at"], "kickoff": kick,
                      "sources": sorted({(p.get("ownership") or {}).get("source") for p in snap
                                         if projected_own(p) is not None} - {None})}
    return [], {"run": None, "reason": "no pregame run"}


def build_charts(out_dir: Path, db=None, season: int = SEASON) -> dict[str, bool]:
    if db is None:
        from config import load_config
        from db.database import DatabaseManager
        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out_dir = Path(out_dir)
    made: dict[str, bool] = {}
    all_weeks: dict[int, list[dict]] = {}
    proj_weeks: dict[int, list[dict]] = {}
    report: list[str] = []
    week_stats: dict[int, dict] = {}
    no_proj: list[int] = []
    for c in load_millionaires(db, season):
        up = db.execute("SELECT slate_upload_id FROM nfl_dfs_field_contests WHERE contest_id = %s",
                        (c["contest_id"],))
        upload = up[0]["slate_upload_id"] if up else None
        own = load_own_rows(db, c["contest_id"])
        info = load_build_points(db, c["contest_id"])
        snap, run = load_pregame_snapshot(db, upload)
        players, stats = match_week(own, snap, info)
        all_weeks[c["week"]] = [p for p in players if p["in_contest"]]
        week_stats[c["week"]] = stats
        has_proj = any(p.get("proj") is not None for p in players)
        report.append(f"week {c['week']}: contest {c['contest_id']} mass {stats.get('mass_pct')}%, run "
                      f"{run.get('run')} ({run.get('created_at')}, kickoff {run.get('kickoff')}), sources "
                      f"{run.get('sources')}, {stats}")
        if has_proj:
            proj_weeks[c["week"]] = players
        else:
            no_proj.append(c["week"])
    for line in report:
        print(line)
    smashes = low_owned_smashes(all_weeks)
    made["nfl_take_low_owned_smashes.png"] = smashes_chart(smashes, sorted(all_weeks),
                                                           out_dir / "nfl_take_low_owned_smashes.png")
    notes = []
    if no_proj:
        notes.append(f"Weeks {', '.join(map(str, no_proj))} are left out: the builds saved for them recorded no "
                     "projected ownership.")
    dropped = sorted(w for w in proj_weeks if week_stats[w].get("absent_dropped"))
    if dropped:
        notes.append(f"Weeks {', '.join(map(str, dropped))}: the stored ownership covers under {100 * MIN_MASS:g}% of "
                     "the field's picks, so slate players missing from it are left out, not counted as 0%.")
    made["nfl_take_low_owned_vs_projection.png"] = projection_chart(
        proj_weeks, smashes, out_dir / "nfl_take_low_owned_vs_projection.png", notes)
    made["nfl_take_qb_ownership.png"] = qb_chart(proj_weeks, out_dir / "nfl_take_qb_ownership.png")
    return made


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--season", type=int, default=SEASON)
    args = ap.parse_args(argv)
    made = build_charts(Path(args.out_dir), season=args.season)
    for name, ok in made.items():
        print(f"{Path(args.out_dir) / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
