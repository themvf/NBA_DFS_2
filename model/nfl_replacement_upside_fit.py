"""Fit the 'gets the job' chance behind `nfl-replacement-upside-v1`.

DESCRIPTIVE, not a blind test: 2020-2025 (true inactive status) and 2014-2018
(activity proxy) have both been looked at by earlier absence studies. The
chances this writes are stated priors for a DISPLAY-ONLY feature
(web/src/lib/nfl-dfs/replacement-upside.ts), to be graded forward on 2026.

Model, per player behind a starter who sat after playing the team's last game:
    with chance (1 - pi) he plays one of his own recent games,
    with chance pi       he plays one of the starter's recent games.
pi is chosen per role (lead / other, by recent volume) to minimise 90th-
percentile pinball loss on 2020-2025, ties to the smaller pi, then rechecked
on 2014-2018. A role keeps a non-zero pi only if it improves pinball loss in
BOTH periods.

Usage:
    python -m model.nfl_replacement_upside_fit
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from model import nfl_absence_reallocation as v1
from model import nfl_rb_first_game_blind as proxy
from model import nfl_wr_breakout_blind as wr

VERSION = "nfl-replacement-upside-v1"
FAMILIES = {
    "RB": dict(unit="carries", star_pos={"RB", "FB"}, rec_pos={"RB", "FB"}, star_min=10.0),
    "TE": dict(unit="targets", star_pos={"TE"}, rec_pos={"TE"}, star_min=4.0),
    "WR": dict(unit="targets", star_pos={"WR"}, rec_pos={"WR", "TE"}, star_min=7.0),
}
GRID = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7)
FIT_WARMUP, FIT_SEASONS = (2019,), (2020, 2021, 2022, 2023, 2024, 2025)
RECHECK_WARMUP, RECHECK_SEASONS = (2013,), (2014, 2015, 2016, 2017, 2018)


def _history(window, pid):
    return [wr.dk_points(g, pid) for g in window if g.status.get(pid) == "ACT"]


def events(teams, seasons, fam: str) -> list[dict]:
    f = FAMILIES[fam]
    spec = v1.POOLS[f["unit"]]
    out = []
    for games in teams.values():
        states = v1.team_states(games, spec)
        for n, g in enumerate(games):
            if g.season not in seasons or not g.has_stats or n == 0:
                continue
            base = states[n].baselines
            window = [x for x in reversed(games[max(0, n - v1.WINDOW_GAMES):n]) if x.has_stats]
            stars = [p for p, s in g.status.items() if s in v1.ABSENT_STATUSES and p in base
                     and g.position.get(p) in f["star_pos"] and base[p].base >= f["star_min"]
                     and games[n - 1].status.get(p) == "ACT"]
            if not stars:
                continue
            star = max(stars, key=lambda p: base[p].base)
            recs = sorted((p for p, b in base.items() if g.status.get(p) == "ACT"
                           and g.position.get(p) in f["rec_pos"] and b.base > 0), key=lambda p: -base[p].base)
            star_hist = _history(window, star)
            wr_seen = False
            for i, p in enumerate(recs):
                own = _history(window, p)
                if len(own) < 2 or len(star_hist) < 2:
                    continue
                if fam == "WR":
                    pos = g.position.get(p)
                    role = "TE" if pos == "TE" else ("lead" if not wr_seen else "other")
                    wr_seen = wr_seen or pos == "WR"
                else:
                    role = "lead" if i == 0 else "other"
                out.append(dict(role=role, own=own, star=star_hist, y=wr.dk_points(g, p)))
    return out


def _weighted_quantile(values, weights, p):
    order = np.argsort(values)
    v, w = np.asarray(values, float)[order], np.asarray(weights, float)[order]
    return float(v[np.searchsorted(np.cumsum(w) / w.sum(), p)])


def p90(row, pi):
    own, star = row["own"], row["star"]
    if pi == 0:
        return float(np.percentile(own, 90))
    weights = [(1 - pi) / len(own)] * len(own) + [pi / len(star)] * len(star)
    return _weighted_quantile(own + star, weights, 0.9)


def score(rows, pi) -> dict:
    y = np.array([r["y"] for r in rows])
    q = np.array([p90(r, pi) for r in rows])
    d = y - q
    return {"pinball_p90": float(np.mean(np.maximum(0.9 * d, -0.1 * d))), "miss_rate": float(np.mean(y > q))}


def fit(fit_events, recheck_events) -> dict:
    out = {}
    for role in sorted({r["role"] for r in fit_events}):
        a = [r for r in fit_events if r["role"] == role]
        b = [r for r in recheck_events if r["role"] == role]
        grid = {f"{pi:g}": score(a, pi) for pi in GRID}
        best = min(GRID, key=lambda pi: (round(grid[f"{pi:g}"]["pinball_p90"], 3), pi))
        base_b, best_b = (score(b, 0.0), score(b, best)) if b else (None, None)
        holds = best > 0 and best_b is not None and best_b["pinball_p90"] < base_b["pinball_p90"]
        out[role] = {"n_fit": len(a), "n_recheck": len(b), "fitted_pi": best, "fit": grid,
                     "recheck_baseline": base_b, "recheck_at_fitted": best_b,
                     "shipped_pi": best if holds else 0.0}
    return out


def main() -> None:
    cache = v1.DEFAULT_CACHE
    fit_teams, *_ = proxy.load(FIT_WARMUP + FIT_SEASONS, cache, snaps=False)
    t, played, snapless, digests, _ = proxy.load(RECHECK_WARMUP + RECHECK_SEASONS, cache, snaps=True)
    recheck_teams = proxy.with_proxy(t, played, snapless)
    result = {"version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
              "note": "Descriptive; both periods previously examined. Stated priors for a display-only feature.",
              "families": {fam: fit(events(fit_teams, FIT_SEASONS, fam), events(recheck_teams, RECHECK_SEASONS, fam))
                           for fam in FAMILIES}}
    path = Path("artifacts/nfl_replacement_upside_v1_fit.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=1) + "\n")
    for fam, roles in result["families"].items():
        for role, r in roles.items():
            base, at = r["fit"]["0"], r["fit"][f"{r['fitted_pi']:g}"]
            print(f"{fam} {role:5s} n={r['n_fit']:3d}/{r['n_recheck']:3d} fitted {r['fitted_pi']:.1f} shipped {r['shipped_pi']:.1f} "
                  f"| fit pinball {base['pinball_p90']:.2f}->{at['pinball_p90']:.2f} "
                  f"| recheck {r['recheck_baseline']['pinball_p90']:.2f}->{r['recheck_at_fitted']['pinball_p90']:.2f}")


if __name__ == "__main__":
    main()
