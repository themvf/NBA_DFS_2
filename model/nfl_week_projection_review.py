"""One week's projections against results, for one position (a reader chart).

For every player at the position, "our projection" is what the DFS page showed
in the latest saved optimizer build for his game made BEFORE that game's
kickoff (research builds and system slates excluded): the projection, and the
model's P10 to P90 range. The result is his exact DraftKings score from
`nfl_dfs_player_week_results`. Players we had ruled out but who played are
listed separately, never counted as a zero projection.

    python -m model.nfl_week_projection_review --week 6 --position QB --out-dir DIR
"""

from __future__ import annotations

import argparse
import re
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, WIDTH_IN, _axes, _fig, _finish  # noqa: E402

SEASON = 2026
EASTERN = ZoneInfo("America/New_York")
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
_GAME_INFO = re.compile(r"(\d{2}/\d{2}/\d{4}) (\d{1,2}:\d{2}[AP]M) ET")


def kickoff_from_info(info: str | None) -> datetime | None:
    """'ARI@LAR 10/18/2026 04:05PM ET' -> aware datetime (Eastern), or None."""
    m = _GAME_INFO.search(info or "")
    if not m:
        return None
    return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%m/%d/%Y %I:%M%p").replace(tzinfo=EASTERN)


def pick_projections(runs: list[dict], position: str, window: tuple[datetime, datetime] | None = None) -> dict[int, dict]:
    """Per ffPlayerId, the player's row from the latest run created before his own kickoff.

    ``runs``: {created_at, run_id, players: [snapshot rows]}. ``window``: the
    week's first and last kickoff; a row whose game falls outside it belongs to
    another week (a bye-week player can only appear from an earlier week's build).
    """
    best: dict[int, tuple[datetime, dict]] = {}
    for run in runs:
        for p in run["players"]:
            if p.get("position") != position or p.get("ffPlayerId") is None:
                continue
            kick = kickoff_from_info(p.get("gameInfo"))
            if kick is None or run["created_at"] >= kick:
                continue
            if window and not (window[0] - timedelta(hours=1) <= kick <= window[1] + timedelta(hours=1)):
                continue
            pid = int(p["ffPlayerId"])
            if pid not in best or run["created_at"] > best[pid][0]:
                best[pid] = (run["created_at"], p | {"run_id": run["run_id"], "run_at": run["created_at"],
                                                      "kickoff": kick})
    return {pid: row for pid, (_, row) in best.items()}


def review_rows(projections: dict[int, dict], results: dict[int, dict],
                finished_teams: set[str] | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    """(projected players with a result, players who played although we had them out or at zero,
    projected players with no scored result yet).

    A projected player with no stat line whose team's game has results did not play: DraftKings
    paid him 0, so he is a row at 0 marked `dnp`, not a pending player."""
    finished_teams = finished_teams or set()
    rows, surprises, pending = [], [], []
    for pid, p in projections.items():
        r = results.get(pid)
        proj = float(p.get("ourProj") or 0)
        out = bool(p.get("isOut")) or proj <= 0
        if r is None and not out and p.get("team") in finished_teams:
            r = {"points": 0.0, "dnp": True}
        if r is None:
            if not out:
                pending.append({"name": p["name"], "team": p.get("team")})
            continue
        if out:
            if r["points"] is not None and abs(r["points"]) >= 1:
                surprises.append({"name": p["name"], "team": p.get("team"), "points": r["points"]})
            continue
        anchor = p.get("marketAnchor") or {}
        rows.append({"name": p["name"], "team": p.get("team"), "opp": p.get("opponent"), "proj": proj,
                     "p10": _num(p.get("floor")), "p90": _num(p.get("ceiling")), "actual": r["points"],
                     "salary": p.get("salary"), "anchored": bool(anchor), "model_proj": model_projection(p),
                     "dnp": bool(r.get("dnp"))})
    return rows, surprises, pending


def model_projection(p: dict) -> float | None:
    """Our model's own projection: the number before any betting-line anchor (stored as ``before.mean``)."""
    anchor = p.get("marketAnchor") or {}
    if not anchor:
        return _num(p.get("ourProj"))
    before = anchor.get("before")
    return _num(before.get("mean") if isinstance(before, dict) else before)


def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _ranks(xs: list[float]) -> list[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2
        i = j + 1
    return ranks


def summary(rows: list[dict]) -> dict:
    err = [r["actual"] - r["proj"] for r in rows]
    inside = [r for r in rows if r["p10"] is not None and r["p90"] is not None]
    rx, ry = _ranks([r["proj"] for r in rows]), _ranks([r["actual"] for r in rows])
    rho = None
    if len(rows) >= 3 and statistics.pstdev(rx) and statistics.pstdev(ry):
        rho = statistics.correlation(rx, ry)
    return {"n": len(rows), "mae": statistics.mean(abs(e) for e in err) if err else None,
            "bias": statistics.mean(err) if err else None, "rho": rho,
            "inside": sum(1 for r in inside if r["p10"] <= r["actual"] <= r["p90"]), "ranged": len(inside),
            "above": sum(1 for r in inside if r["actual"] > r["p90"]), "below": sum(1 for r in inside if r["actual"] < r["p10"])}


POSITION_NAMES = {"QB": "quarterbacks", "RB": "running backs", "WR": "wide receivers", "TE": "tight ends"}
# A deep position has many backups we had out who scored a point or two; name only the ones that mattered.
SURPRISE_MIN_POINTS = 8.0
SURPRISE_MAX_NAMES = 6


def top_by_projection(rows: list[dict], pending: list[dict], projections: dict, top: int | None):
    """Keep the `top` highest projections, counting players whose results are not loaded yet."""
    if not top:
        return rows, pending
    by_name = projections_by_name(projections)
    pool = sorted([(r["proj"], "row", r) for r in rows]
                  + [(by_name.get((p["name"], p["team"]), 0.0), "pending", p) for p in pending],
                  key=lambda t: -t[0])[:top]
    return [x for _, k, x in pool if k == "row"], [x for _, k, x in pool if k == "pending"]


def projections_by_name(projections: dict) -> dict:
    return {(p.get("name"), p.get("team")): float(p.get("ourProj") or 0.0) for p in projections.values()}


def _name_list(names: list[str], limit: int = 6) -> str:
    """A subtitle line that fits the page: the first `limit` names, then a count."""
    if len(names) <= limit:
        return ", ".join(names)
    return ", ".join(names[:limit]) + f" and {len(names) - limit} more"


def review_chart(rows: list[dict], surprises: list[dict], week: int, position: str, path: Path,
                 pending: list[dict] | None = None, top: int | None = None) -> bool:
    if not rows:
        return False
    rows = sorted(rows, key=lambda r: -r["proj"])
    if top and position != "QB":
        surprises = sorted([x for x in surprises if x["points"] >= SURPRISE_MIN_POINTS],
                           key=lambda x: -x["points"])[:SURPRISE_MAX_NAMES]
    s = summary(rows)
    height = 0.36 * len(rows) + 3.4
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = list(range(len(rows)))[::-1]
    hi = max(max(r["actual"], r["proj"], r["p90"] or 0) for r in rows)
    lo = min(min(r["actual"], r["proj"], r["p10"] or 0) for r in rows)
    for y, r in zip(ys, rows):
        if r["p10"] is not None and r["p90"] is not None:
            ax.plot([r["p10"], r["p90"]], [y, y], color=GRID, linewidth=7, solid_capstyle="round", zorder=1)
        colour = GOOD if r["actual"] >= r["proj"] else BAD
        ax.plot([r["proj"], r["actual"]], [y, y], color=colour, linewidth=1.8, zorder=2)
        ax.scatter([r["proj"]], [y], s=46, facecolor="white", edgecolor=INK, linewidth=1.4, zorder=3,
                   marker="D" if r["anchored"] else "o")
        ax.scatter([r["actual"]], [y], s=46, color=colour, zorder=4)
        ax.text(hi + 2, y, f"{r['proj']:.1f} → {r['actual']:.1f}  ({r['actual'] - r['proj']:+.1f})"
                + ("  did not play" if r.get("dnp") else ""),
                fontsize=7, color=INK, va="center")
    ax.set_yticks(ys, [f"{r['name']} {r['team']} v {r['opp']}" for r in rows], fontsize=7.5, color=INK)
    ax.set_xlim(min(lo - 2, -2), hi + 16)
    ax.axvline(0, color=INK_2, linewidth=0.6)
    ax.set_xlabel("DraftKings points", fontsize=8, color=INK_2)
    if any(not r["anchored"] for r in rows):
        ax.scatter([], [], s=46, facecolor="white", edgecolor=INK, label="Our projection (model)")
    if any(r["anchored"] for r in rows):
        ax.scatter([], [], s=46, facecolor="white", edgecolor=INK, marker="D",
                   label="Our projection (set from his betting lines)")
    ax.scatter([], [], s=46, color=GOOD, label="Actual, above projection")
    ax.scatter([], [], s=46, color=BAD, label="Actual, below projection")
    ax.plot([], [], color=GRID, linewidth=7, label="Our P10 to P90 range")
    ax.legend(fontsize=7, frameon=False, loc="upper center", bbox_to_anchor=(0.45, -0.035 * 30 / max(len(rows), 10)),
              ncol=3)
    best = max(rows, key=lambda r: r["actual"] - r["proj"])
    worst = min(rows, key=lambda r: r["actual"] - r["proj"])
    rho = f", rank correlation {s['rho']:.2f}" if s["rho"] is not None else ""
    who = (f"the top {top} {position}s by our projection" if top else f"every {position} we projected")
    sub = (f"Week {week}: {who}, from our last saved build before his game's kickoff, sorted by projection. "
           f"{s['n']} players.\nAverage miss {s['mae']:.1f} points, average actual minus projection {s['bias']:+.1f}{rho}. "
           f"{s['inside']} of {s['ranged']} finished inside our P10-P90 range ({s['above']} above it, {s['below']} below).\n"
           f"Biggest beat: {best['name']} ({best['actual'] - best['proj']:+.1f}); biggest miss: {worst['name']} "
           f"({worst['actual'] - worst['proj']:+.1f}). P10/P90: the score we expected him to beat 90% / 10% of the time.")
    if surprises:
        sub += ("\nWe had him out, he scored 8+: " if top and position != "QB" else "\nPlayed although we had him out: ")
        sub += ", ".join(f"{x['name']} {x['team']} ({x['points']:.1f})" for x in surprises)
    dnp = [r for r in rows if r.get("dnp")]
    if dnp:
        sub += ("\nDid not play, counted at 0 as DraftKings scored him: "
                + _name_list([f"{r['name']} {r['team']}" for r in dnp]))
    if pending:
        sub += "\nNot scored yet (results not loaded), left out: " + _name_list([f"{x['name']} {x['team']}" for x in pending])
    group = POSITION_NAMES.get(position, f"{position}s")
    title = (f"Week {week} {group} (top {top} by our projection): projections against the results" if top
             else f"Week {week} {group}: our projections against the results")
    return _finish(fig, path, title, sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load(db, season: int, week: int, position: str) -> tuple[dict, dict, set]:
    window = db.execute("SELECT MIN(kickoff) k0, MAX(kickoff) k1 FROM nfl_season_games WHERE season = %s AND week = %s "
                        "AND game_type = 'REG'", (season, week))[0]
    if window["k0"] is None:
        return {}, {}, set()
    runs = [dict(r) for r in db.execute(
        """SELECT o.run_id, o.created_at,
                  (SELECT jsonb_agg(e) FROM jsonb_array_elements(o.input_snapshot) e WHERE e->>'position' = %s) AS players
           FROM nfl_dfs_optimizer_runs o JOIN nfl_dfs_slate_uploads u ON u.upload_id = o.upload_id
           WHERE u.origin = 'upload' AND NOT (o.settings ? 'researchStudy')
             AND jsonb_typeof(o.input_snapshot) = 'array'
             AND o.created_at BETWEEN %s AND %s""",
        (position, window["k0"] - timedelta(days=7), window["k1"]))]
    runs = [r | {"players": r["players"] or []} for r in runs]
    results = {int(r["player_id"]): {"points": float(r["actual_dk_fpts"])} for r in db.execute(
        """SELECT DISTINCT ON (player_id) player_id, actual_dk_fpts FROM nfl_dfs_player_week_results
           WHERE season = %s AND week = %s AND position = %s AND scoring_status = 'exact'
           ORDER BY player_id, computed_at DESC""", (season, week, position))}
    finished = {r["team"] for r in db.execute(
        "SELECT DISTINCT team FROM nfl_dfs_player_week_results WHERE season = %s AND week = %s "
        "AND scoring_status = 'exact'", (season, week))}
    return pick_projections(runs, position, (window["k0"], window["k1"])), results, finished


def build_chart(out_dir: Path, week: int, position: str = "QB", season: int = SEASON, db=None,
                top: int | None = None) -> tuple[bool, Path]:
    if db is None:
        from config import load_config
        from db.database import DatabaseManager

        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    projections, results, finished = load(db, season, week, position)
    rows, surprises, pending = review_rows(projections, results, finished)
    rows, pending = top_by_projection(rows, pending, projections, top)
    suffix = f"_top{top}" if top else ""
    path = Path(out_dir) / f"nfl_take_week{week}_{position.lower()}{suffix}_projections.png"
    return review_chart(rows, surprises, week, position, path, pending, top), path


def week_misses(projections: dict, results: dict, finished: set, top: int) -> list[float]:
    """Actual minus projection for the `top` highest projections that have a result."""
    rows, _, pending = review_rows(projections, results, finished)
    rows, _ = top_by_projection(rows, pending, projections, top)
    return [r["actual"] - r["proj"] for r in rows]


def _mean_ci(xs: list[float]) -> tuple[float, float]:
    """Mean and the half-width of a normal 95% interval (0 below two values)."""
    m = statistics.mean(xs)
    return m, (1.96 * statistics.stdev(xs) / len(xs) ** 0.5) if len(xs) > 1 else 0.0


def bias_chart(misses: dict[str, dict[int, list[float]]], top: int, path: Path) -> bool:
    """One panel per position: average actual minus projection by week, plus all weeks pooled."""
    positions = [p for p in misses if any(misses[p].values())]
    if not positions:
        return False
    height = 7.6
    fig, axes = plt.subplots(2, 2, figsize=(WIDTH_IN, height), dpi=DPI, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    summary = []
    for ax, pos in zip(axes.flat, positions):
        _axes(ax, xgrid=False)
        weeks = sorted(w for w, xs in misses[pos].items() if xs)
        pooled = [x for w in weeks for x in misses[pos][w]]
        labels = [f"Wk {w}" for w in weeks] + ["All"]
        series = [misses[pos][w] for w in weeks] + [pooled]
        for i, xs in enumerate(series):
            m, ci = _mean_ci(xs)
            colour = GOOD if m >= 0 else BAD
            ax.bar(i, m, width=0.68, color=colour, alpha=1.0 if i == len(weeks) else 0.75,
                   edgecolor=INK if i == len(weeks) else "none", linewidth=1.0)
            ax.errorbar(i, m, yerr=ci, color=INK_2, linewidth=0.9, capsize=2.5)
            over = sum(1 for x in xs if x > 0)
            ax.text(i, m + (ci + 0.4 if m >= 0 else -ci - 0.4), f"{m:+.1f}", ha="center",
                    va="bottom" if m >= 0 else "top", fontsize=7.5, color=INK, fontweight="bold")
            ax.text(i, -0.03, f"{over}/{len(xs)}", ha="center", va="top", fontsize=6.3, color=INK_2,
                    transform=ax.get_xaxis_transform())
        ax.axhline(0, color=INK_2, linewidth=0.8)
        ax.axvline(len(weeks) - 0.5, color=GRID, linewidth=0.8, linestyle="--")
        ax.set_xticks(range(len(labels)), labels, fontsize=7.5)
        ax.tick_params(axis="x", pad=12)
        ax.set_title(POSITION_NAMES.get(pos, pos).capitalize(), fontsize=9.5, color=INK, loc="left",
                     fontweight="bold")
        m, _ = _mean_ci(pooled)
        summary.append(f"{pos} {m:+.1f}")
    for ax in axes.flat[len(positions):]:
        ax.set_visible(False)
    for ax in axes[:, 0]:
        ax.set_ylabel("Actual minus projection (DK pts)", fontsize=7.5, color=INK_2)
    fig.tight_layout(h_pad=2.2)
    sub = (f"Each week, the {top} players we projected highest at each position, from our last saved build before "
           f"his game's kickoff. Bar: their average DraftKings\nscore minus our projection (above zero = they beat "
           f"it), with a 95% interval. Under each bar: how many of them beat their projection. A projected\nplayer "
           f"who did not play counts as 0, as DraftKings scored him. All weeks pooled: " + ", ".join(summary) + ".")
    return _finish(fig, path, f"Did the top {top} at each position beat our projections? Weeks "
                   f"{min(w for p in positions for w, xs in misses[p].items() if xs)}-"
                   f"{max(w for p in positions for w, xs in misses[p].items() if xs)}", sub, height)


def build_bias_chart(out_dir: Path, weeks: list[int], top: int = 25, season: int = SEASON,
                     positions: tuple[str, ...] = ("QB", "RB", "WR", "TE"), db=None) -> tuple[bool, Path]:
    if db is None:
        from config import load_config
        from db.database import DatabaseManager

        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    misses: dict[str, dict[int, list[float]]] = {p: {} for p in positions}
    for w in weeks:
        for pos in positions:
            projections, results, finished = load(db, season, w, pos)
            misses[pos][w] = week_misses(projections, results, finished, top) if projections else []
    path = Path(out_dir) / f"nfl_take_projection_bias_top{top}.png"
    return bias_chart(misses, top, path), path


def line_projections(db, season: int, week: int) -> dict[tuple[str, str], float]:
    """(normalized name, position) -> DK points implied by the player's closing DraftKings-first prop lines.

    Read exactly as the live market anchor reads them (``nfl-prop-implied-v1`` with
    ``CALIBRATION_V1``); a player missing a required line has no line projection.
    A QB with no rushing line reads as 0 rushing here (the research reading).
    """
    from model import nfl_prop_implied as pim
    from research.nfl_prop_implied import load as load_lines

    out = {}
    for (_, key), d in load_lines(db, season, [week]).items():
        s = pim.stat_line(d["position"], d["lines"], calibration=pim.CALIBRATION_V1)
        if s is not None:
            out[(key, d["position"])] = pim.dk_points(d["position"], s)
    return out


def paired_misses(projections: dict, results: dict, finished: set, lines: dict, position: str,
                  top: int) -> list[tuple[float, float]]:
    """(actual - line projection, actual - model projection) for the `top` highest MODEL projections
    among players who have both a line projection and a result (0 if he did not play in a finished game)."""
    from model.nfl_dfs_dk_pool_match import normalize_name

    pairs = []
    for pid, p in projections.items():
        if p.get("isOut") or float(p.get("ourProj") or 0) <= 0:
            continue
        model = model_projection(p)
        line = lines.get((normalize_name(p.get("name") or ""), position))
        r = results.get(pid)
        actual = r["points"] if r else (0.0 if p.get("team") in finished else None)
        if model is None or line is None or actual is None:
            continue
        pairs.append((model, actual - line, actual - model))
    pairs.sort(key=lambda t: -t[0])
    return [(a, b) for _, a, b in pairs[:top]]


def line_vs_model_chart(pairs: dict[str, dict[int, list[tuple[float, float]]]], top: int, path: Path) -> bool:
    """One panel per position: average actual minus projection, betting lines beside our model, by week."""
    positions = [p for p in pairs if any(pairs[p].values())]
    if not positions:
        return False
    height = 7.8
    colours = {"line": SERIES[0], "model": SERIES[1]}
    fig, axes = plt.subplots(2, 2, figsize=(WIDTH_IN, height), dpi=DPI, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    summary = []
    weeks_seen = sorted({w for p in positions for w, xs in pairs[p].items() if xs})
    for ax, pos in zip(axes.flat, positions):
        _axes(ax, xgrid=False)
        weeks = sorted(w for w, xs in pairs[pos].items() if xs)
        pooled = [x for w in weeks for x in pairs[pos][w]]
        series = [pairs[pos][w] for w in weeks] + [pooled]
        for i, xs in enumerate(series):
            for j, (key, idx) in enumerate((("line", 0), ("model", 1))):
                vals = [x[idx] for x in xs]
                m, ci = _mean_ci(vals)
                x = i + (j - 0.5) * 0.36
                ax.bar(x, m, width=0.34, color=colours[key], edgecolor=INK if i == len(weeks) else "none",
                       linewidth=0.9)
                ax.errorbar(x, m, yerr=ci, color=INK_2, linewidth=0.7, capsize=1.8)
                ax.text(x, m + (0.3 if m >= 0 else -0.3), f"{m:+.1f}", ha="center",
                        va="bottom" if m >= 0 else "top", fontsize=6.0, color=INK)
            ax.text(i, -0.015, f"n={len(xs)}", ha="center", va="top", fontsize=6.0, color=INK_2,
                    transform=ax.get_xaxis_transform())
        ax.axhline(0, color=INK_2, linewidth=0.8)
        ax.axvline(len(weeks) - 0.5, color=GRID, linewidth=0.8, linestyle="--")
        ax.set_xticks(range(len(weeks) + 1), [f"Wk {w}" for w in weeks] + ["All"], fontsize=7.5)
        ax.tick_params(axis="x", pad=13)
        ax.set_title(POSITION_NAMES.get(pos, pos).capitalize(), fontsize=9.5, color=INK, loc="left",
                     fontweight="bold")
        lm = statistics.mean(x[0] for x in pooled)
        mm = statistics.mean(x[1] for x in pooled)
        lmae = statistics.mean(abs(x[0]) for x in pooled)
        mmae = statistics.mean(abs(x[1]) for x in pooled)
        summary.append(f"{pos} lines {lm:+.1f} / model {mm:+.1f} (average miss {lmae:.1f} vs {mmae:.1f})")
    for ax in axes.flat[len(positions):]:
        ax.set_visible(False)
    for ax in axes[:, 0]:
        ax.set_ylabel("Actual minus projection (DK pts)", fontsize=7.5, color=INK_2)
    from matplotlib.patches import Patch

    fig.legend(handles=[Patch(color=colours["line"], label="Betting-line projection"),
                        Patch(color=colours["model"], label="Our model's projection")],
               loc="lower center", ncol=2, fontsize=7.5, frameon=False, bbox_to_anchor=(0.5, -0.005))
    fig.tight_layout(h_pad=2.2, rect=(0, 0.045, 1, 1))
    sub = (f"Each week, the {top} players our model projected highest at each position among those with a full set "
           f"of closing DraftKings prop lines, so both bars cover the same\nplayers. Line projection: the lines read "
           f"as DK points the way our market anchor reads them. Model projection: our own number before any line "
           f"anchor. Above zero = they beat it.\nA player who did not play counts as 0. All weeks: "
           + "; ".join(summary[:2]) + ";\n" + "; ".join(summary[2:]) + ".")
    return _finish(fig, path, f"Betting lines vs our model: did the top {top} beat each projection? "
                   f"Weeks {weeks_seen[0]}-{weeks_seen[-1]}", sub, height)


def build_line_vs_model_chart(out_dir: Path, weeks: list[int], top: int = 25, season: int = SEASON,
                              positions: tuple[str, ...] = ("QB", "RB", "WR", "TE"), db=None) -> tuple[bool, Path]:
    if db is None:
        from config import load_config
        from db.database import DatabaseManager

        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    pairs: dict[str, dict[int, list[tuple[float, float]]]] = {p: {} for p in positions}
    for w in weeks:
        lines = line_projections(db, season, w)
        for pos in positions:
            projections, results, finished = load(db, season, w, pos)
            pairs[pos][w] = paired_misses(projections, results, finished, lines, pos, top) if projections else []
    path = Path(out_dir) / f"nfl_take_lines_vs_model_top{top}.png"
    return line_vs_model_chart(pairs, top, path), path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--week", type=int, help="one week's chart for --position")
    ap.add_argument("--bias-weeks", help="e.g. 1-6: average actual minus projection by week, all positions")
    ap.add_argument("--position", default="QB")
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--top", type=int, default=None, help="keep only the N highest projections")
    ap.add_argument("--lines-vs-model", action="store_true",
                    help="with --bias-weeks: betting-line projections beside our model's")
    args = ap.parse_args(argv)
    if args.bias_weeks:
        lo, _, hi = args.bias_weeks.partition("-")
        build = build_line_vs_model_chart if args.lines_vs_model else build_bias_chart
        ok, path = build(Path(args.out_dir), list(range(int(lo), int(hi or lo) + 1)), args.top or 25, args.season)
        print(f"{path}: {'written' if ok else 'skipped, no data'}")
        return 0
    if args.week is None:
        ap.error("--week or --bias-weeks is required")
    ok, path = build_chart(Path(args.out_dir), args.week, args.position, args.season, top=args.top)
    print(f"{path}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
