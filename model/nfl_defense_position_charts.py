"""Reader charts: how defenses affect running backs, wide receivers and tight ends.

The skill-position companion to ``model.nfl_defense_qb_charts``. Descriptive, season to date; nothing
here claims a betting or DFS edge.

  nfl_take_def_pos_points.png      per position: each defense's points allowed against those players' usual
  nfl_take_def_pos_stability.png   per position: does a defense's early effect repeat later?
  nfl_take_def_rb_profile.png      what each defense allows running backs
  nfl_take_def_wr_profile.png      what each defense allows wide receivers
  nfl_take_def_te_profile.png      what each defense allows tight ends
  nfl_take_def_yards_share.png     where each defense gets beaten: receiving yards allowed to WR, TE and RB

Sources (SELECT only, plus one nflverse download):

* ``nfl_dfs_player_week_results``  exact DraftKings points per player-week, with position, team and opponent
* ``ff_players``                    GSIS id -> position, to label play-by-play rushers and receivers
* nflverse play-by-play            carries, targets, yards, success, touchdowns (``pbp_url``)

"Usual" is a player's average DraftKings points in his OTHER games this season (at least ``MIN_OTHER``).
A defense's game at a position sums every opposing player at that position who has a usual, actual minus
usual; a player without one (too few other games) counts in the raw total only. Positions are the roster
positions in ``ff_players``, so a receiver listed as a TE is a TE here.

    python -m model.nfl_defense_position_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import statistics as st
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.nfl_defense_qb_charts import _flag, _num, _team, pearson  # noqa: E402
from model.story_charts import BAD, GOOD, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
POSITIONS = ("RB", "WR", "TE")
NAMES = {"RB": "Running backs", "WR": "Wide receivers", "TE": "Tight ends"}
MIN_OTHER = 2
PBP_COLS = ("game_id", "week", "season_type", "posteam", "defteam", "play_type", "rush_attempt", "pass_attempt",
            "sack", "complete_pass", "yards_gained", "success", "rush_touchdown", "pass_touchdown",
            "rusher_player_id", "receiver_player_id", "two_point_attempt")


# ── pure computations ────────────────────────────────────────────────────────
def with_usual(rows: list[dict], min_other: int = MIN_OTHER) -> list[dict]:
    """Each player-game with ``usual`` = his mean points in his other games (None below ``min_other``)."""
    by = collections.defaultdict(list)
    for r in rows:
        by[r["player_id"]].append(r)
    out = []
    for r in rows:
        others = [o["points"] for o in by[r["player_id"]] if o is not r]
        out.append(r | {"usual": st.mean(others) if len(others) >= min_other else None})
    return out


def defense_games(rows: list[dict], position: str) -> list[dict]:
    """(defense, week) at one position: raw points allowed and the sum of actual minus usual."""
    acc = collections.defaultdict(lambda: {"raw": 0.0, "over": 0.0, "adjusted": 0})
    for r in rows:
        if r["position"] != position:
            continue
        a = acc[(r["defense"], r["week"])]
        a["raw"] += r["points"]
        if r["usual"] is not None:
            a["over"] += r["points"] - r["usual"]
            a["adjusted"] += 1
    return [{"defense": d, "week": w, **a} for (d, w), a in acc.items()]


def by_defense(games: list[dict], weeks: set[int] | None = None) -> dict[str, dict]:
    acc = collections.defaultdict(list)
    for g in games:
        if weeks is None or g["week"] in weeks:
            acc[g["defense"]].append(g)
    return {d: {"games": len(gs), "raw": st.mean(g["raw"] for g in gs), "over": st.mean(g["over"] for g in gs)}
            for d, gs in acc.items() if gs}


def plays(pbp: list[dict], positions: dict[str, str]) -> list[dict]:
    """Regular-season rushes and targets with the ball carrier's position (unknown ids dropped)."""
    out = []
    for p in pbp:
        if p.get("season_type") != "REG" or _flag(p.get("two_point_attempt")) or not p.get("defteam"):
            continue
        if _flag(p.get("rush_attempt")) and p.get("rusher_player_id"):
            kind, pid = "rush", p["rusher_player_id"]
        elif _flag(p.get("pass_attempt")) and not _flag(p.get("sack")) and p.get("receiver_player_id"):
            kind, pid = "target", p["receiver_player_id"]
        else:
            continue
        pos = positions.get(pid)
        if pos not in POSITIONS:
            continue
        out.append({"game": p["game_id"], "week": int(p["week"]), "defense": _team(p["defteam"]), "kind": kind, "position": pos,
                    "yards": _num(p.get("yards_gained")) or 0.0, "success": _flag(p.get("success")),
                    "catch": _flag(p.get("complete_pass")),
                    "td": _flag(p.get("rush_touchdown")) if kind == "rush" else _flag(p.get("pass_touchdown"))})
    return out


def position_profile(ps: list[dict], position: str, raw_points: dict[str, float]) -> dict[str, dict]:
    """defense -> per-game volume and per-play efficiency allowed to one position."""
    acc = collections.defaultdict(list)
    games = collections.defaultdict(set)
    for p in ps:
        games[p["defense"]].add(p["game"])
        if p["position"] == position:
            acc[p["defense"]].append(p)
    out = {}
    for d, xs in acc.items():
        n = len(games[d])
        rush = [x for x in xs if x["kind"] == "rush"]
        tgt = [x for x in xs if x["kind"] == "target"]
        row = {"games": n, "dk": raw_points.get(d),
               "targets_pg": len(tgt) / n, "ypt": (sum(x["yards"] for x in tgt) / len(tgt)) if tgt else None,
               "catch_rate": (sum(x["catch"] for x in tgt) / len(tgt)) if tgt else None,
               "rec_td_pg": sum(x["td"] for x in tgt) / n}
        if position == "RB":
            row |= {"carries_pg": len(rush) / n, "ypc": (sum(x["yards"] for x in rush) / len(rush)) if rush else None,
                    "success": (sum(x["success"] for x in rush) / len(rush)) if rush else None,
                    "rush_td_pg": sum(x["td"] for x in rush) / n}
        out[d] = row
    return out


def yards_share(ps: list[dict]) -> dict[str, dict]:
    """defense -> receiving yards allowed (completions) by position, and each position's share."""
    acc = collections.defaultdict(lambda: collections.Counter())
    for p in ps:
        if p["kind"] == "target" and p["catch"]:
            acc[p["defense"]][p["position"]] += p["yards"]
    out = {}
    for d, c in acc.items():
        total = sum(c.values())
        if total > 0:
            out[d] = {"total": total, **{pos: c[pos] / total for pos in POSITIONS}}
    return out


# ── charts ───────────────────────────────────────────────────────────────────
def points_chart(defs: dict[str, dict[str, dict]], path: Path, weeks: str) -> bool:
    if not any(defs.values()):
        return False
    n = max(len(v) for v in defs.values())
    height = 0.25 * n + 3.2
    fig, axes = plt.subplots(1, len(POSITIONS), figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    for ax, pos in zip(axes, POSITIONS):
        rows = sorted(({"team": t, **v} for t, v in defs[pos].items()), key=lambda r: r["over"])
        _axes(ax, ygrid=False)
        if not rows:
            ax.set_visible(False)
            continue
        ys = list(range(len(rows)))
        ax.barh(ys, [r["over"] for r in rows], color=[BAD if r["over"] > 0 else GOOD for r in rows], height=0.7,
                alpha=0.85)
        lim = max(abs(r["over"]) for r in rows) * 1.45
        for y, r in zip(ys, rows):
            ax.text(r["over"] + (0.2 if r["over"] >= 0 else -0.2), y, f"{r['over']:+.1f}", va="center",
                    ha="left" if r["over"] >= 0 else "right", fontsize=6.0, color=INK)
        ax.axvline(0, color=INK_2, linewidth=0.8)
        ax.set_yticks(ys, [r["team"] for r in rows], fontsize=6.6)
        ax.set_xlim(-lim, lim)
        ax.set_title(NAMES[pos], fontsize=9.5, color=INK, loc="left", fontweight="bold")
        ax.tick_params(axis="x", labelsize=6.6)
    fig.tight_layout(w_pad=1.4)
    sub = (f"{weeks}. For each defense and game, the opposing players at the position: their DraftKings points minus "
           f"their average in their other games, added up; the bar is\nthe average per game. Red: that position "
           f"scored more than usual against this defense; green: less. Players with fewer than {MIN_OTHER} other "
           f"games are left out of the\nadjustment. Five or six games a team, so one game moves a bar a lot. Each "
           f"panel is sorted on its own.")
    return _finish(fig, path, "Which defenses held running backs, receivers and tight ends below their usual?",
                   sub, height)


def stability_chart(first: dict[str, dict], second: dict[str, dict], path: Path, labels: tuple[str, str]) -> bool:
    height = 4.9
    fig, axes = plt.subplots(1, len(POSITIONS), figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    notes, drew = [], False
    for ax, pos in zip(axes, POSITIONS):
        _axes(ax)
        a, b = first.get(pos, {}), second.get(pos, {})
        teams = [t for t in a if t in b]
        if len(teams) < 5:
            ax.set_visible(False)
            continue
        xs, ys = [a[t]["over"] for t in teams], [b[t]["over"] for t in teams]
        r = pearson(xs, ys)
        lim = max(map(abs, xs + ys)) * 1.15
        ax.plot([-lim, lim], [-lim, lim], color=GRID, linewidth=1, linestyle="--")
        ax.axhline(0, color=INK_2, linewidth=0.6)
        ax.axvline(0, color=INK_2, linewidth=0.6)
        ax.scatter(xs, ys, s=18, color=SERIES[0], alpha=0.8, edgecolor="none")
        for t, x, y in zip(teams, xs, ys):
            ax.annotate(t, (x, y), fontsize=5.4, color=INK, xytext=(2, 2), textcoords="offset points")
        ax.set_xlim(-lim, lim)
        ax.set_ylim(-lim, lim)
        rs = f"{r:+.2f}" if r is not None else "n/a"
        ax.set_title(f"{NAMES[pos]}  (r {rs})", fontsize=9, color=INK, loc="left", fontweight="bold")
        ax.set_xlabel(labels[0], fontsize=7, color=INK_2)
        ax.set_ylabel(labels[1], fontsize=7, color=INK_2)
        ax.tick_params(labelsize=6.4)
        notes.append(f"{NAMES[pos].lower()} {rs}")
        drew = True
    if not drew:
        plt.close(fig)
        return False
    fig.tight_layout(w_pad=1.2)
    sub = (f"Each defense's points allowed against usual at the position, {labels[0].lower()} (across) against "
           f"{labels[1].lower()} (up). Dashed: the same in both halves.\nCorrelations: " + ", ".join(notes) +
           ".\nNear 0: the early numbers said little about the later ones (for quarterbacks it was +0.48). "
           "About three games a half, so this is mostly noise either way.")
    return _finish(fig, path, "Does a defense's effect on each position repeat?", sub, height)


PROFILE_COLS = {
    "RB": (("carries_pg", "Carries / g", "{:.1f}"), ("ypc", "Yards / carry", "{:.1f}"),
           ("success", "Run success", "{:.0%}"), ("rush_td_pg", "Rush TD / g", "{:.2f}"),
           ("targets_pg", "Targets / g", "{:.1f}"), ("dk", "DK pts / g", "{:.1f}")),
    "WR": (("targets_pg", "Targets / g", "{:.1f}"), ("catch_rate", "Catch rate", "{:.0%}"),
           ("ypt", "Yards / target", "{:.1f}"), ("rec_td_pg", "TD / g", "{:.2f}"), ("dk", "DK pts / g", "{:.1f}")),
    "TE": (("targets_pg", "Targets / g", "{:.1f}"), ("catch_rate", "Catch rate", "{:.0%}"),
           ("ypt", "Yards / target", "{:.1f}"), ("rec_td_pg", "TD / g", "{:.2f}"), ("dk", "DK pts / g", "{:.1f}")),
}


def profile_chart(profile: dict[str, dict], position: str, path: Path, weeks: str) -> bool:
    cols = PROFILE_COLS[position]
    teams = sorted((t for t in profile if profile[t].get("dk") is not None), key=lambda t: profile[t]["dk"])
    if not teams:
        return False
    stats = {}
    for key, *_ in cols:
        vals = [profile[t][key] for t in teams if profile[t].get(key) is not None]
        stats[key] = (st.mean(vals), st.pstdev(vals) or 1.0) if vals else (0.0, 1.0)
    height = 0.26 * len(teams) + 2.8
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    from matplotlib.colors import LinearSegmentedColormap

    cmap = LinearSegmentedColormap.from_list("defpos", [GOOD, "#f4f2ec", BAD])
    for y, t in enumerate(teams):
        for x, (key, _, fmt) in enumerate(cols):
            v = profile[t].get(key)
            if v is None:
                continue
            m, sd = stats[key]
            z = max(-2.0, min(2.0, (v - m) / sd))
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
    what = ("carries and targets to running backs" if position == "RB"
            else f"targets to {NAMES[position].lower()}")
    sub = (f"What opposing {NAMES[position].lower()} did against each defense, {weeks}: {what} from nflverse "
           f"play-by-play, DraftKings points from our results table.\nRed: more than other defenses allowed (good "
           f"for the offense); green: less. Sorted by DraftKings points allowed per game, fewest at the top. Raw, "
           f"not adjusted\nfor which players each defense faced. Positions are roster positions.")
    return _finish(fig, path, f"What each defense gives up to {NAMES[position].lower()}", sub, height)


def share_chart(shares: dict[str, dict], path: Path, weeks: str) -> bool:
    teams = sorted(shares, key=lambda t: shares[t]["WR"])
    if not teams:
        return False
    height = 0.26 * len(teams) + 3.0
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    colours = {"WR": SERIES[0], "TE": SERIES[1], "RB": SERIES[2]}
    league = {pos: sum(shares[t][pos] * shares[t]["total"] for t in teams) / sum(shares[t]["total"] for t in teams)
              for pos in POSITIONS}
    for y, t in enumerate(teams):
        left = 0.0
        for pos in ("WR", "TE", "RB"):
            v = shares[t][pos]
            ax.barh(y, v, left=left, color=colours[pos], height=0.72, label=pos if y == 0 else None)
            if v >= 0.07:
                ax.text(left + v / 2, y, f"{v:.0%}", ha="center", va="center", fontsize=6.0, color="white")
            left += v
        ax.text(1.01, y, f"{shares[t]['total']:.0f} yds", va="center", fontsize=6.2, color=INK_2)
    ax.axvline(league["WR"], color=INK, linewidth=0.9, linestyle="--")
    ax.axvline(league["WR"] + league["TE"], color=INK, linewidth=0.9, linestyle=":")
    ax.set_yticks(range(len(teams)), teams, fontsize=7.2)
    ax.invert_yaxis()
    ax.set_xlim(0, 1.12)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}" if v <= 1 else ""))
    ax.legend(fontsize=7, frameon=False, ncol=3, loc="lower center", bbox_to_anchor=(0.45, -0.07 * 30 / len(teams)))
    fig.tight_layout()
    sub = (f"Receiving yards each defense allowed, {weeks}, split by who caught them (nflverse play-by-play, roster "
           f"positions). Sorted by the share that went to wide receivers,\nsmallest at the top: the defenses at the "
           f"top gave up more of their yardage to tight ends and backs. League share: WR {league['WR']:.0%}, "
           f"TE {league['TE']:.0%}, RB {league['RB']:.0%}\n(dashed and dotted lines). Right: total receiving "
           f"yards allowed.")
    return _finish(fig, path, "Where each defense gets beaten through the air", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager
    return DatabaseManager(load_config().database_url, initialize_schema=False)


def load_player_games(db, season: int = SEASON) -> list[dict]:
    return [{"player_id": r["player_id"], "week": r["week"], "position": r["position"], "team": _team(r["team"]),
             "defense": _team(r["opponent"]), "points": float(r["actual_dk_fpts"])}
            for r in db.execute(
                """SELECT DISTINCT ON (player_id, week) player_id, week, position, team, opponent, actual_dk_fpts
                   FROM nfl_dfs_player_week_results
                   WHERE season = %s AND scoring_status = 'exact' AND position = ANY(%s) AND opponent IS NOT NULL
                   ORDER BY player_id, week, computed_at DESC""", (season, list(POSITIONS)))]


def load_positions(db, season: int = SEASON) -> dict[str, str]:
    return {r["gsis_id"]: r["position"] for r in db.execute(
        "SELECT gsis_id, position FROM ff_players WHERE season = %s AND gsis_id IS NOT NULL", (season,))}


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
    rows = with_usual(load_player_games(db, season))
    ws = sorted({r["week"] for r in rows})
    weeks = f"Weeks {ws[0]}-{ws[-1]}" if ws else "No weeks"
    mid = ws[len(ws) // 2 - 1] if ws else 0
    first_w, second_w = {w for w in ws if w <= mid}, {w for w in ws if w > mid}
    labels = (f"Weeks {min(first_w)}-{max(first_w)}", f"Weeks {min(second_w)}-{max(second_w)}") \
        if first_w and second_w else ("First half", "Second half")
    games = {pos: defense_games(rows, pos) for pos in POSITIONS}
    full = {pos: by_defense(games[pos]) for pos in POSITIONS}
    ps = [x for x in plays(load_pbp(season), load_positions(db, season)) if x["week"] in set(ws)]
    jobs = [("nfl_take_def_pos_points.png", lambda p: points_chart(full, p, weeks)),
            ("nfl_take_def_pos_stability.png",
             lambda p: stability_chart({pos: by_defense(games[pos], first_w) for pos in POSITIONS},
                                       {pos: by_defense(games[pos], second_w) for pos in POSITIONS}, p, labels))]
    for pos in POSITIONS:
        raw = {d: v["raw"] for d, v in full[pos].items()}
        jobs.append((f"nfl_take_def_{pos.lower()}_profile.png",
                     lambda p, pos=pos, raw=raw: profile_chart(position_profile(ps, pos, raw), pos, p, weeks)))
    jobs.append(("nfl_take_def_yards_share.png", lambda p: share_chart(yards_share(ps), p, weeks)))
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
