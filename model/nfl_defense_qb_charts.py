"""Reader charts: how defenses affect opposing quarterbacks.

Descriptive, season to date. Nothing here claims a betting or DFS edge.

  nfl_take_def_qb_points.png       each defense's QB points allowed, against what those QBs score elsewhere
  nfl_take_def_qb_stability.png    does a defense's effect in the first half of the weeks repeat in the second?
  nfl_take_def_pressure.png        pressure (sacks + QB hits per dropback) against EPA allowed per dropback
  nfl_take_qb_when_hit.png         which quarterbacks fall off most on dropbacks where they are hit
  nfl_take_def_qb_profile.png      what each defense allows: yards per attempt, TD and INT rates, sack rate

Sources (SELECT only, plus one nflverse download):

* ``ff_player_week_stats``           nflverse weekly QB rows: attempts, team, opponent
* ``nfl_dfs_player_week_results``     exact DraftKings points per player-week
* nflverse play-by-play              dropbacks, sacks, QB hits, EPA (``model.nfl_market_scenarios.pbp_url``)

A team's quarterback in a game is the one with the most pass attempts. "What he scores elsewhere" is his
average DraftKings points in his OTHER starts this season (at least ``MIN_OTHER`` of them); a start without
that is left out of the adjusted numbers. A QB hit here means the play-by-play's ``qb_hit`` or a sack: it is
contact, not every pressure (the free feed has no pressure flag).

    python -m model.nfl_defense_qb_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import json
import statistics as st
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
MIN_OTHER = 2          # other starts a QB needs for an expectation
MIN_DROPBACKS = 120    # dropbacks a QB needs for the hit chart
MIN_HIT = 25           # dropbacks with a hit a QB needs for the hit chart
PBP_COLS = ("game_id", "week", "season_type", "posteam", "defteam", "qb_dropback", "qb_hit", "sack", "epa",
            "passer_player_name", "passer_player_id", "pass_attempt", "interception", "pass_touchdown",
            "passing_yards", "two_point_attempt", "qb_scramble")
TEAM_ALIAS = {"LA": "LAR", "WSH": "WAS", "JAC": "JAX", "AZ": "ARI"}


def _team(t):
    return TEAM_ALIAS.get(t, t) if t else t


# ── pure computations ────────────────────────────────────────────────────────
def starters(qb_rows: list[dict]) -> dict[tuple[int, str], dict]:
    """(week, team) -> the QB row with the most pass attempts."""
    out: dict[tuple[int, str], dict] = {}
    for r in qb_rows:
        k = (r["week"], r["team"])
        if k not in out or (r["attempts"] or 0) > (out[k]["attempts"] or 0):
            out[k] = r
    return out


def defense_effects(starts: list[dict], min_other: int = MIN_OTHER) -> list[dict]:
    """Each start with ``expected`` = the QB's mean DK points in his other starts (None below ``min_other``)."""
    by_qb = collections.defaultdict(list)
    for s in starts:
        by_qb[s["player_id"]].append(s)
    out = []
    for s in starts:
        others = [o["points"] for o in by_qb[s["player_id"]] if o is not s]
        out.append(s | {"expected": st.mean(others) if len(others) >= min_other else None})
    return out


def by_defense(starts: list[dict], weeks: set[int] | None = None) -> dict[str, dict]:
    """defense -> games, raw mean points allowed, mean over expected (adjusted starts only)."""
    acc = collections.defaultdict(lambda: {"raw": [], "over": []})
    for s in starts:
        if weeks is not None and s["week"] not in weeks:
            continue
        a = acc[s["defense"]]
        a["raw"].append(s["points"])
        if s["expected"] is not None:
            a["over"].append(s["points"] - s["expected"])
    return {d: {"games": len(a["raw"]), "raw": st.mean(a["raw"]),
                "over": st.mean(a["over"]) if a["over"] else None, "adjusted_games": len(a["over"])}
            for d, a in acc.items() if a["raw"]}


def pearson(xs, ys):
    if len(xs) < 3:
        return None
    mx, my = st.mean(xs), st.mean(ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    return sxy / (sxx * syy) ** 0.5 if sxx > 0 and syy > 0 else None


def dropbacks(plays: list[dict]) -> list[dict]:
    """Regular-season dropbacks with an EPA, two-point tries excluded; ``hit`` = QB hit or sack."""
    out = []
    for p in plays:
        if p.get("season_type") != "REG" or not _flag(p.get("qb_dropback")) or _flag(p.get("two_point_attempt")):
            continue
        epa = _num(p.get("epa"))
        if epa is None or not p.get("defteam"):
            continue
        out.append({"week": int(p["week"]), "game": p["game_id"], "offense": _team(p["posteam"]),
                    "defense": _team(p["defteam"]), "epa": epa, "sack": _flag(p.get("sack")),
                    "hit": _flag(p.get("qb_hit")) or _flag(p.get("sack")),
                    "attempt": _flag(p.get("pass_attempt")) and not _flag(p.get("sack")),
                    "td": _flag(p.get("pass_touchdown")), "int": _flag(p.get("interception")),
                    "yards": _num(p.get("passing_yards")) or 0.0,
                    "passer": p.get("passer_player_name"), "passer_id": p.get("passer_player_id")})
    return out


def defense_profile(dbs: list[dict]) -> dict[str, dict]:
    """defense -> dropbacks, hit rate, sack rate, EPA per dropback, yards per attempt, TD and INT rate per attempt."""
    acc = collections.defaultdict(list)
    for d in dbs:
        acc[d["defense"]].append(d)
    out = {}
    for team, ds in acc.items():
        atts = [d for d in ds if d["attempt"]]
        out[team] = {"dropbacks": len(ds), "hit_rate": sum(d["hit"] for d in ds) / len(ds),
                     "sack_rate": sum(d["sack"] for d in ds) / len(ds), "epa": st.mean(d["epa"] for d in ds),
                     "ypa": (sum(d["yards"] for d in atts) / len(atts)) if atts else None,
                     "td_rate": (sum(d["td"] for d in atts) / len(atts)) if atts else None,
                     "int_rate": (sum(d["int"] for d in atts) / len(atts)) if atts else None}
    return out


def qb_hit_splits(dbs: list[dict], min_dropbacks: int = MIN_DROPBACKS, min_hit: int = MIN_HIT) -> list[dict]:
    """Per passer: EPA per dropback with and without a hit (sacks count as hit dropbacks)."""
    acc = collections.defaultdict(list)
    for d in dbs:
        if d["passer_id"]:
            acc[(d["passer_id"], d["passer"], d["offense"])].append(d)
    out = []
    for (pid, name, team), ds in acc.items():
        hit = [d["epa"] for d in ds if d["hit"]]
        clean = [d["epa"] for d in ds if not d["hit"]]
        if len(ds) >= min_dropbacks and len(hit) >= min_hit and clean:
            out.append({"name": name, "team": team, "dropbacks": len(ds), "hit_n": len(hit),
                        "hit_rate": len(hit) / len(ds), "clean": st.mean(clean), "hit": st.mean(hit),
                        "drop": st.mean(clean) - st.mean(hit)})
    # A passer can appear under two teams after a trade; keep his biggest sample.
    best = {}
    for r in out:
        if r["name"] not in best or r["dropbacks"] > best[r["name"]]["dropbacks"]:
            best[r["name"]] = r
    return sorted(best.values(), key=lambda r: -r["drop"])


def _flag(v) -> bool:
    try:
        return float(v) == 1.0
    except (TypeError, ValueError):
        return False


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


# ── charts ───────────────────────────────────────────────────────────────────
def points_chart(defs: dict[str, dict], path: Path, weeks: str) -> bool:
    rows = sorted((d | {"team": t} for t, d in defs.items() if d["over"] is not None), key=lambda r: r["over"])
    if not rows:
        return False
    height = 0.27 * len(rows) + 3.0
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    ys = range(len(rows))
    ax.barh(list(ys), [r["over"] for r in rows], color=[BAD if r["over"] > 0 else GOOD for r in rows],
            height=0.68, alpha=0.85)
    ax.axvline(0, color=INK_2, linewidth=0.8)
    lim = max(abs(r["over"]) for r in rows) * 1.25
    for y, r in zip(ys, rows):
        ax.text(r["over"] + (0.25 if r["over"] >= 0 else -0.25), y, f"{r['over']:+.1f}", va="center",
                ha="left" if r["over"] >= 0 else "right", fontsize=6.6, color=INK)
        ax.text(lim * 1.02, y, f"{r['raw']:.1f} avg  ({r['adjusted_games']}/{r['games']} g)", va="center",
                fontsize=6.4, color=INK_2)
    ax.set_yticks(list(ys), [r["team"] for r in rows], fontsize=7.2)
    ax.set_xlim(-lim, lim * 1.55)
    ax.set_xlabel("QB DraftKings points vs what those QBs score in their other starts", fontsize=7.5, color=INK_2)
    fig.tight_layout()
    sub = (f"{weeks}. Bar: the average of (opposing starting QB's DraftKings points minus his average in his other "
           f"starts this season). Red: QBs scored more than usual\nagainst this defense; green: less. Right: raw "
           f"average QB points allowed, and games adjusted / games played (a QB needs {MIN_OTHER}+ other starts). "
           f"Five or six games a team,\nso one big or bad game moves a bar a lot.")
    return _finish(fig, path, "Which defenses held quarterbacks below their usual scoring?", sub, height)


def stability_chart(first: dict[str, dict], second: dict[str, dict], path: Path, labels: tuple[str, str]) -> bool:
    teams = [t for t in first if t in second and first[t]["over"] is not None and second[t]["over"] is not None]
    if len(teams) < 5:
        return False
    xs, ys = [first[t]["over"] for t in teams], [second[t]["over"] for t in teams]
    r = pearson(xs, ys)
    height = 7.0
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax)
    ax.axhline(0, color=INK_2, linewidth=0.7)
    ax.axvline(0, color=INK_2, linewidth=0.7)
    lim = max(map(abs, xs + ys)) * 1.15
    ax.plot([-lim, lim], [-lim, lim], color=GRID, linewidth=1, linestyle="--")
    ax.scatter(xs, ys, s=34, color=SERIES[0], alpha=0.8, edgecolor="none")
    for t, x, y in zip(teams, xs, ys):
        ax.annotate(t, (x, y), fontsize=6.6, color=INK, xytext=(3, 3), textcoords="offset points")
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-lim, lim)
    ax.set_xlabel(f"{labels[0]}: QB points vs usual allowed", fontsize=7.5, color=INK_2)
    ax.set_ylabel(f"{labels[1]}: QB points vs usual allowed", fontsize=7.5, color=INK_2)
    fig.tight_layout()
    rs = f"{r:+.2f}" if r is not None else "n/a"
    sub = (f"Each defense's QB points allowed above or below those QBs' usual, {labels[0].lower()} (across) against "
           f"{labels[1].lower()} (up). Dashed: the same in both halves.\nCorrelation {rs} across {len(teams)} "
           f"defenses: near 0 means a defense's early numbers said little about its later ones; near 1, they "
           f"repeated.\nAbout three games a half, so a lot of this is noise either way.")
    return _finish(fig, path, "Does a defense's effect on quarterbacks repeat?", sub, height)


def pressure_chart(profile: dict[str, dict], path: Path, weeks: str) -> bool:
    teams = sorted(profile)
    if len(teams) < 5:
        return False
    xs = [profile[t]["hit_rate"] for t in teams]
    ys = [profile[t]["epa"] for t in teams]
    r = pearson(xs, ys)
    height = 7.0
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax)
    mx, my = st.mean(xs), st.mean(ys)
    ax.axvline(mx, color=INK_2, linewidth=0.7, linestyle="--")
    ax.axhline(my, color=INK_2, linewidth=0.7, linestyle="--")
    ax.scatter(xs, ys, s=38, color=[GOOD if y < my else BAD for y in ys], alpha=0.85, edgecolor="none")
    for t, x, y in zip(teams, xs, ys):
        ax.annotate(t, (x, y), fontsize=6.6, color=INK, xytext=(3, 3), textcoords="offset points")
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlabel("Dropbacks with a sack or QB hit", fontsize=7.5, color=INK_2)
    ax.set_ylabel("EPA allowed per dropback (better defense is higher)", fontsize=7.5, color=INK_2)
    fig.tight_layout()
    rs = f"{r:+.2f}" if r is not None else "n/a"
    sub = (f"Every defense, {weeks}, from nflverse play-by-play. Across: share of opponents' dropbacks that ended in "
           f"a sack or a QB hit. Up: expected points\nadded per dropback by the opposing offense, flipped so the "
           f"stingiest defenses sit at the top. Dashed: league averages. Correlation {rs}.\nA hit is contact, not "
           f"every pressure: the free play-by-play has no pressure flag.")
    return _finish(fig, path, "Do defenses that hit the quarterback more allow less?", sub, height)


def qb_hit_chart(splits: list[dict], path: Path, weeks: str, n: int = 32) -> bool:
    rows = splits[:n]
    if not rows:
        return False
    height = 0.27 * len(rows) + 3.0
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    rows = rows[::-1]
    for y, r in enumerate(rows):
        ax.plot([r["hit"], r["clean"]], [y, y], color=GRID, linewidth=3, solid_capstyle="round", zorder=1)
        ax.scatter([r["clean"]], [y], s=34, color=GOOD, zorder=3)
        ax.scatter([r["hit"]], [y], s=34, color=BAD, zorder=3)
        ax.text(max(r["clean"], r["hit"]) + 0.05, y, f"-{r['drop']:.2f}  ({r['hit_rate']:.0%} hit, "
                f"{r['dropbacks']} db)", va="center", fontsize=6.4, color=INK_2)
    ax.axvline(0, color=INK_2, linewidth=0.8)
    ax.set_yticks(range(len(rows)), [f"{r['name']} {r['team']}" for r in rows], fontsize=7.2)
    lo = min(r["hit"] for r in rows) - 0.1
    hi = max(r["clean"] for r in rows) + 0.75
    ax.set_xlim(lo, hi)
    ax.set_xlabel("EPA per dropback", fontsize=7.5, color=INK_2)
    ax.scatter([], [], color=GOOD, label="No hit or sack")
    ax.scatter([], [], color=BAD, label="Hit or sacked")
    ax.legend(fontsize=7, frameon=False, loc="lower right")
    fig.tight_layout()
    sub = (f"{weeks}, quarterbacks with {MIN_DROPBACKS}+ dropbacks and {MIN_HIT}+ where they were hit or sacked, "
           f"sorted by how far they fell. Green: expected points added per\ndropback when not hit; red: when hit "
           f"or sacked (sacks are always costly, so every QB falls). Text: the gap, how often he was hit, and his "
           f"dropbacks.")
    return _finish(fig, path, "Which quarterbacks fall off most when they get hit?", sub, height)


def profile_chart(profile: dict[str, dict], path: Path, weeks: str) -> bool:
    cols = (("ypa", "Yards per attempt", 1, "{:.1f}"), ("td_rate", "TD rate", 1, "{:.1%}"),
            ("int_rate", "INT rate", -1, "{:.1%}"), ("sack_rate", "Sack rate", -1, "{:.1%}"),
            ("epa", "EPA / dropback", 1, "{:+.2f}"))
    teams = sorted(profile, key=lambda t: profile[t]["epa"])
    if not teams:
        return False
    stats = {}
    for key, *_ in cols:
        vals = [profile[t][key] for t in teams if profile[t][key] is not None]
        stats[key] = (st.mean(vals), st.pstdev(vals) or 1.0)
    height = 0.26 * len(teams) + 2.8
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    from matplotlib.colors import LinearSegmentedColormap

    cmap = LinearSegmentedColormap.from_list("defqb", [GOOD, "#f4f2ec", BAD])
    for y, t in enumerate(teams):
        for x, (key, _, sign, fmt) in enumerate(cols):
            v = profile[t][key]
            if v is None:
                continue
            m, sd = stats[key]
            z = max(-2.0, min(2.0, sign * (v - m) / sd))   # positive = good for the QB
            ax.add_patch(plt.Rectangle((x, y), 1, 1, color=cmap((z + 2) / 4), linewidth=0))
            ax.text(x + 0.5, y + 0.5, fmt.format(v), ha="center", va="center", fontsize=6.6, color=INK)
    ax.set_xlim(0, len(cols))
    ax.set_ylim(len(teams), 0)
    ax.set_xticks([i + 0.5 for i in range(len(cols))], [c[1] for c in cols], fontsize=7.4)
    ax.xaxis.tick_top()
    ax.set_yticks([i + 0.5 for i in range(len(teams))], teams, fontsize=7.2)
    for side in ax.spines.values():
        side.set_visible(False)
    ax.tick_params(length=0, colors=INK_2)
    fig.tight_layout()
    sub = (f"What opposing quarterbacks did against each defense, {weeks}, from nflverse play-by-play. Rates are per "
           f"pass attempt (sacks per dropback). Red: good for the QB,\ngreen: good for the defense, relative to the "
           f"other defenses. Sorted by EPA allowed per dropback, stingiest at the top. Raw numbers, not adjusted for "
           f"which\nquarterbacks each defense happened to face.")
    return _finish(fig, path, "What each defense gives up to quarterbacks", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager
    return DatabaseManager(load_config().database_url, initialize_schema=False)


def load_starts(db, season: int = SEASON) -> list[dict]:
    """One row per team-game: its starting QB (most attempts), the defense, his exact DraftKings points."""
    qb_rows = []
    for r in db.execute(
            """SELECT s.player_id, f.canonical_name, s.week, s.team, s.source_row FROM ff_player_week_stats s
               JOIN ff_players f ON f.id = s.player_id
               WHERE s.season = %s AND s.week <= 18 AND s.source_row->>'position' = 'QB'""", (season,)):
        row = r["source_row"] if isinstance(r["source_row"], dict) else json.loads(r["source_row"] or "{}")
        qb_rows.append({"player_id": r["player_id"], "name": r["canonical_name"], "week": r["week"],
                        "team": _team(r["team"] or row.get("team")), "defense": _team(row.get("opponent_team")),
                        "attempts": float(row.get("attempts") or 0)})
    points = {(r["player_id"], r["week"]): float(r["actual_dk_fpts"]) for r in db.execute(
        """SELECT DISTINCT ON (player_id, week) player_id, week, actual_dk_fpts FROM nfl_dfs_player_week_results
           WHERE season = %s AND scoring_status = 'exact' ORDER BY player_id, week, computed_at DESC""", (season,))}
    starts = []
    for s in starters(qb_rows).values():
        p = points.get((s["player_id"], s["week"]))
        if p is not None and s["defense"] and s["attempts"] > 0:
            starts.append(s | {"points": p})
    return starts


def load_pbp(season: int = SEASON) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    last = None
    for attempt in range(3):
        try:
            frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in PBP_COLS, compression="gzip",
                                low_memory=False)
            missing = set(PBP_COLS) - set(frame.columns)
            if missing:
                raise RuntimeError(f"play-by-play is missing columns: {sorted(missing)}")
            return frame.to_dict("records")
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse play-by-play {season} download failed: {last}")


def build_all(out_dir: Path, season: int = SEASON, db=None) -> list[tuple[str, bool]]:
    db = db or _db()
    out_dir = Path(out_dir)
    starts = defense_effects(load_starts(db, season))
    ws = sorted({s["week"] for s in starts})
    weeks = f"Weeks {ws[0]}-{ws[-1]}" if ws else "No weeks"
    mid = ws[len(ws) // 2 - 1] if ws else 0
    first_w, second_w = {w for w in ws if w <= mid}, {w for w in ws if w > mid}
    labels = (f"Weeks {min(first_w)}-{max(first_w)}", f"Weeks {min(second_w)}-{max(second_w)}") if first_w and second_w \
        else ("First half", "Second half")
    dbs = dropbacks(load_pbp(season))
    profile = defense_profile(dbs)
    pweeks = sorted({d["week"] for d in dbs})
    pw = f"Weeks {pweeks[0]}-{pweeks[-1]}" if pweeks else "No weeks"
    jobs = [("nfl_take_def_qb_points.png", lambda p: points_chart(by_defense(starts), p, weeks)),
            ("nfl_take_def_qb_stability.png",
             lambda p: stability_chart(by_defense(starts, first_w), by_defense(starts, second_w), p, labels)),
            ("nfl_take_def_pressure.png", lambda p: pressure_chart(profile, p, pw)),
            ("nfl_take_qb_when_hit.png", lambda p: qb_hit_chart(qb_hit_splits(dbs), p, pw)),
            ("nfl_take_def_qb_profile.png", lambda p: profile_chart(profile, p, pw))]
    return [(name, fn(out_dir / name)) for name, fn in jobs]


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
