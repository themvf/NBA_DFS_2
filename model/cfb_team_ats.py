"""Team against-the-spread standings and conference report cards (`cfb-team-ats-v1`).

DESCRIPTIVE, season to date. Uses the same closing lines as the market
breakdown (`cfb_pattern_measures.load_base_rate_games`), regular season only.
Every game where a team is FBS and the game has a closing spread counts for
that team; an FCS opponent counts for the FBS team but never gets a row of its
own. Spread and total are graded at -110: a win pays +100/110, a loss -1, a
push 0. Nothing here is a trigger, a recommendation or evidence of an edge.

    python -m model.cfb_team_ats [--out-dir artifacts/cfb_pattern_watch/charts]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

TEAM_ATS_VERSION = "cfb-team-ats-v1"
WIN_UNITS = 100 / 110
DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "charts"


# ── grading ──────────────────────────────────────────────────────────────────
def _grade(margin: float) -> str:
    return "W" if margin > 0 else "L" if margin < 0 else "P"


def _units(result: str) -> float:
    return WIN_UNITS if result == "W" else -1.0 if result == "L" else 0.0


def _pct(w: int, l: int) -> float | None:
    return None if w + l == 0 else round(100 * w / (w + l), 1)


def _is_fbs(cls) -> bool:
    return (cls or "").lower() == "fbs"


def team_games(games: list[dict]) -> list[dict]:
    """One row per (FBS team, game) with a closing spread, regular season, from that team's side."""
    out = []
    for g in games:
        if g.get("season_type", "regular") != "regular" or g.get("home_spread") is None:
            continue
        if g.get("home_score") is None or g.get("away_score") is None:
            continue
        neutral = bool(g.get("neutral_site"))
        total = g.get("vegas_total")
        pts = g["home_score"] + g["away_score"]
        ou = None if total is None else ("O" if pts > total else "U" if pts < total else "P")
        for side in ("home", "away"):
            me, opp = ("h", "a") if side == "home" else ("a", "h")
            if not _is_fbs(g.get(f"{me}class")):
                continue
            score = g["home_score"] if side == "home" else g["away_score"]
            opp_score = g["away_score"] if side == "home" else g["home_score"]
            line = float(g["home_spread"]) if side == "home" else -float(g["home_spread"])
            margin = score - opp_score
            conf_game = g.get("conference_game")
            if conf_game is None:
                conf_game = g.get(f"{me}conf") is not None and g.get(f"{me}conf") == g.get(f"{opp}conf")
            out.append({
                "team_id": g.get(f"{side}_team_id"), "team": g.get(f"{me}name"), "conference": g.get(f"{me}conf"),
                "opponent_fcs": not _is_fbs(g.get(f"{opp}class")),
                "venue": "Neutral" if neutral else ("Home" if side == "home" else "Away"),
                "line": line, "margin": margin, "won": margin > 0,
                "ats": _grade(margin + line), "cover_margin": margin + line,
                "ou": ou, "conference_game": bool(conf_game),
            })
    return out


class _Tally:
    def __init__(self):
        self.w = self.l = self.p = 0
        self.units = 0.0

    def add(self, result: str) -> None:
        if result == "W":
            self.w += 1
        elif result == "L":
            self.l += 1
        else:
            self.p += 1
        self.units += _units(result)

    @property
    def n(self) -> int:
        return self.w + self.l + self.p

    def record(self) -> str:
        return f"{self.w}-{self.l}-{self.p}"

    def pct(self) -> float | None:
        return _pct(self.w, self.l)


def _ats(rows: list[dict]) -> _Tally:
    t = _Tally()
    for r in rows:
        t.add(r["ats"])
    return t


def _ou(rows: list[dict]) -> _Tally:
    t = _Tally()
    for r in rows:
        if r["ou"] is not None:
            t.add({"O": "W", "U": "L", "P": "P"}[r["ou"]])
    return t


def _split(rows: list[dict], prefix: str) -> dict:
    t = _ats(rows)
    return {f"{prefix} ATS": t.record(), f"{prefix} covered (%)": t.pct(), f"{prefix} games": t.n}


# ── 1. team standings ────────────────────────────────────────────────────────
def team_rows(games: list[dict]) -> list[dict]:
    """One row per FBS team, sorted by cover % then ATS units (teams with no decided game last)."""
    by_team: dict = defaultdict(list)
    for r in team_games(games):
        by_team[r["team_id"] if r["team_id"] is not None else r["team"]].append(r)
    out = []
    for rows in by_team.values():
        ats, ou = _ats(rows), _ou(rows)
        wins = sum(r["won"] for r in rows)
        out.append({
            "Team": rows[0]["team"], "Conference": rows[0]["conference"], "Games": len(rows),
            "Vs FCS": sum(r["opponent_fcs"] for r in rows),
            "SU record": f"{wins}-{len(rows) - wins}",
            "ATS record": ats.record(), "Covered (%)": ats.pct(), "ATS units": round(ats.units, 2),
            "Avg cover margin": round(sum(r["cover_margin"] for r in rows) / len(rows), 1),
            "Over-under record": ou.record(), "Overs (%)": ou.pct(),
            **_split([r for r in rows if r["line"] < 0], "Favorite"),
            **_split([r for r in rows if r["line"] > 0], "Underdog"),
            **_split([r for r in rows if r["venue"] == "Home"], "Home"),
            **_split([r for r in rows if r["venue"] == "Away"], "Away"),
        })
    out.sort(key=lambda r: (r["Covered (%)"] is None, -(r["Covered (%)"] or 0), -r["ATS units"], r["Team"] or ""))
    return out


# ── 2. conference report cards ───────────────────────────────────────────────
def conference_rows(games: list[dict]) -> list[dict]:
    """One row per FBS conference, built from its teams' team-game rows.

    A conference game puts one team on each side of the same line, so it adds
    one cover and one non-cover (or two pushes) to that conference: the
    in-conference ATS record always sums to about 50% by construction. The
    informative number is the NON-CONFERENCE split, where the conference's
    teams face outsiders. Over % is per team-game, so an in-conference game is
    counted twice.
    """
    by_conf: dict = defaultdict(list)
    for r in team_games(games):
        if r["conference"]:
            by_conf[r["conference"]].append(r)
    out = []
    for conf, rows in by_conf.items():
        ats, ou = _ats(rows), _ou(rows)
        wins = sum(r["won"] for r in rows)
        nonconf = [r for r in rows if not r["conference_game"]]
        nc = _ats(nonconf)
        nc_wins = sum(r["won"] for r in nonconf)
        out.append({
            "Conference": conf, "Teams": len({r["team_id"] for r in rows}), "Team-games": len(rows),
            "SU record": f"{wins}-{len(rows) - wins}",
            "ATS record": ats.record(), "Covered (%)": ats.pct(), "Overs (%)": ou.pct(),
            "Non-conference games": nc.n, "Non-conference SU": f"{nc_wins}-{len(nonconf) - nc_wins}",
            "Non-conference ATS": nc.record(), "Non-conference covered (%)": nc.pct(),
            "Non-conference ATS units": round(nc.units, 2),
            "Non-conference overs (%)": _ou(nonconf).pct(),
            **_split([r for r in rows if r["venue"] == "Away" and r["line"] < 0], "Road favorite"),
            **_split([r for r in rows if r["venue"] == "Home" and r["line"] > 0], "Home underdog"),
        })
    out.sort(key=lambda r: (r["Non-conference covered (%)"] is None, -(r["Non-conference covered (%)"] or 0), r["Conference"]))
    return out


def build_tables(db) -> dict[str, list[dict]]:
    from model import cfb_pattern_measures as measures

    games = measures.load_base_rate_games(db)
    return {"team_ats": team_rows(games), "conference_report": conference_rows(games)}


# ── charts ───────────────────────────────────────────────────────────────────
def team_ats_chart(rows: list[dict], path: Path) -> bool:
    from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, _figure, _save, _style

    rows = [r for r in rows if r["Covered (%)"] is not None]
    if not rows:
        return False
    top, bottom = rows[:10], rows[-10:] if len(rows) > 10 else []
    bottom = [r for r in bottom if r not in top]
    picked = top + bottom
    fig, ax = _figure(0.28 * len(picked) + 1.4)
    ys = list(range(len(picked)))[::-1]
    colors = [SERIES[2] if r in top else SERIES[1] for r in picked]
    ax.barh(ys, [r["Covered (%)"] for r in picked], color=colors, height=0.6)
    ax.axvline(50, color=INK_2, linewidth=0.8, linestyle="--")
    for y, r in zip(ys, picked):
        ax.text(r["Covered (%)"] + 1, y, f"{r['Covered (%)']:.0f}%  {r['ATS record']}  ({r['ATS units']:+.2f}u)",
                va="center", ha="left", fontsize=7, color=INK)
    ax.set_yticks(ys, [f"{r['Team']} ({r['Conference'] or '-'})" for r in picked], fontsize=7, color=INK)
    ax.set_xlim(0, 125)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.set_xlabel("covered, % of decided games", color=INK_2, fontsize=8)
    n_games = sum(r["Games"] for r in rows)
    _style(ax, "Best and worst teams against the spread",
           f"Season to date: top and bottom 10 of {len(rows)} FBS teams, {n_games} team-games at the close.")
    _save(fig, path)
    return True


def conference_chart(rows: list[dict], path: Path) -> bool:
    from model.cfb_pattern_charts import INK, INK_2, SERIES, _figure, _save, _style

    rows = [r for r in rows if r["Non-conference covered (%)"] is not None]
    if not rows:
        return False
    xs = list(range(len(rows)))
    cover = [r["Non-conference covered (%)"] for r in rows]
    over = [r["Non-conference overs (%)"] or 0.0 for r in rows]
    fig, ax = _figure(4.2)
    w = 0.38
    ax.bar([x - w / 2 for x in xs], cover, width=w, color=SERIES[0], label="Covered")
    ax.bar([x + w / 2 for x in xs], over, width=w, color=SERIES[1], label="Went over")
    ax.axhline(50, color=INK_2, linewidth=0.8, linestyle="--")
    for x, c, o in zip(xs, cover, over):
        ax.annotate(f"{c:.0f}", (x - w / 2, c), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=6.5, color=INK)
        ax.annotate(f"{o:.0f}", (x + w / 2, o), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=6.5, color=INK)
    ax.set_xticks(xs, [f"{r['Conference']}\n{r['Non-conference games']} g" for r in rows], fontsize=6.5, color=INK,
                  rotation=30, ha="right")
    ax.set_ylim(0, 105)
    ax.set_ylabel("percent of non-conference games", color=INK_2, fontsize=8)
    ax.legend(loc="upper right", fontsize=7.5, frameon=False, labelcolor=INK)
    n = sum(r["Non-conference games"] for r in rows)
    _style(ax, "Conferences against outsiders",
           f"Non-conference games only, season to date ({n} team-games). In-conference games cancel out to 50%.")
    _save(fig, path)
    return True


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    t = build_tables(db)
    out = Path(args.out_dir)
    for name, ok in {
        "team_ats.png": team_ats_chart(t["team_ats"], out / "team_ats.png"),
        "conference_ats.png": conference_chart(t["conference_report"], out / "conference_ats.png"),
    }.items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    for r in t["team_ats"][:5]:
        print(r)
    for r in t["conference_report"]:
        print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
