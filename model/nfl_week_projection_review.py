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

from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES  # noqa: E402
from model.story_charts import BAD, GOOD, _axes, _fig, _finish  # noqa: E402

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


def review_rows(projections: dict[int, dict], results: dict[int, dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """(projected players with a result, players who played although we had them out or at zero,
    projected players with no scored result yet)."""
    rows, surprises, pending = [], [], []
    for pid, p in projections.items():
        r = results.get(pid)
        proj = float(p.get("ourProj") or 0)
        out = bool(p.get("isOut")) or proj <= 0
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
                     "salary": p.get("salary"), "anchored": bool(anchor), "model_proj": _num(anchor.get("before"))})
    return rows, surprises, pending


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


def review_chart(rows: list[dict], surprises: list[dict], week: int, position: str, path: Path,
                 pending: list[dict] | None = None) -> bool:
    if not rows:
        return False
    rows = sorted(rows, key=lambda r: -r["proj"])
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
        ax.text(hi + 2, y, f"{r['proj']:.1f} → {r['actual']:.1f}  ({r['actual'] - r['proj']:+.1f})",
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
    sub = (f"Week {week}: every {position} we projected, from our last saved build before his game's kickoff, sorted by projection. "
           f"{s['n']} players.\nAverage miss {s['mae']:.1f} points, average actual minus projection {s['bias']:+.1f}{rho}. "
           f"{s['inside']} of {s['ranged']} finished inside our P10-P90 range ({s['above']} above it, {s['below']} below).\n"
           f"Biggest beat: {best['name']} ({best['actual'] - best['proj']:+.1f}); biggest miss: {worst['name']} "
           f"({worst['actual'] - worst['proj']:+.1f}). P10/P90: the score we expected him to beat 90% / 10% of the time.")
    if surprises:
        sub += "\nPlayed although we had him out: " + ", ".join(f"{x['name']} {x['team']} ({x['points']:.1f})" for x in surprises)
    if pending:
        sub += "\nNot scored yet (results not loaded), left out: " + ", ".join(f"{x['name']} {x['team']}" for x in pending)
    return _finish(fig, path, f"Week {week} quarterbacks: our projections against the results" if position == "QB"
                   else f"Week {week} {position}s: our projections against the results", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load(db, season: int, week: int, position: str) -> tuple[dict, dict]:
    window = db.execute("SELECT MIN(kickoff) k0, MAX(kickoff) k1 FROM nfl_season_games WHERE season = %s AND week = %s "
                        "AND game_type = 'REG'", (season, week))[0]
    if window["k0"] is None:
        return {}, {}
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
    return pick_projections(runs, position, (window["k0"], window["k1"])), results


def build_chart(out_dir: Path, week: int, position: str = "QB", season: int = SEASON, db=None) -> tuple[bool, Path]:
    if db is None:
        from config import load_config
        from db.database import DatabaseManager

        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    projections, results = load(db, season, week, position)
    rows, surprises, pending = review_rows(projections, results)
    path = Path(out_dir) / f"nfl_take_week{week}_{position.lower()}_projections.png"
    return review_chart(rows, surprises, week, position, path, pending), path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--position", default="QB")
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    ok, path = build_chart(Path(args.out_dir), args.week, args.position, args.season)
    print(f"{path}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
