"""RB-room absence reallocation, graded on points.

Study `nfl-rb-absence-reallocation-v2`, registered in
docs/nfl-rb-absence-reallocation-study.md before its confirmation seasons
were read.

v1 (`model/nfl_absence_reallocation.py`) handed a missing running back's
leftover carries to the remaining backs. It predicted carries better but not
fantasy points by MAE. Discovery diagnostics showed two reasons: when a back
sits, the next back also inherits receiving work (+0.41 targets, +2.7 yards
for the lead recipient), which v1 never moved; and MAE rewards the median of a
skewed points distribution, so it barely registers a correction to the mean
that the app's expected-points projection actually needs. v2 runs the same
fixed pie twice, once for carries and once for the RB room's targets, credits
each back with his own per-carry and per-target points, and is graded on
squared error with MAE as a non-inferiority guard.

Usage:
    python -m model.nfl_rb_absence_reallocation study --discovery-only
    python -m model.nfl_rb_absence_reallocation study
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from model import nfl_absence_reallocation as v1

STUDY_ID = "nfl-rb-absence-reallocation-v2"

CARRIES = v1.POOLS["carries"]
RB_TARGETS = v1.PoolSpec("rb_targets", "targets", frozenset({"RB", "FB"}), frozenset({"RB", "FB"}),
                         frozenset(), 0.0, budget_only=frozenset({"RB", "FB"}))

# nflverse carries gameday-inactive status only from 2019, so every quantity a
# confirmation event depends on (window, reserve residuals and their windows)
# must come from 2019 or later: 2019 is warm-up, 2020-2021 are graded.
DISCOVERY_SEASONS = (2022, 2023, 2024, 2025)
DISCOVERY_WARMUP = (2021,)
CONFIRMATION_SEASONS = (2020, 2021)
CONFIRMATION_WARMUP = (2019,)

PHI_CARRIES_GRID = (0.5, 1.0)
PHI_TARGETS_GRID = (0.0, 0.5, 1.0)

GATE_MIN_EVENTS = 200
GATE_MIN_ROWS = 480


def build_events(teams, seasons):
    """One event per team game with a material RB absence; rows are the active backs."""
    events = []
    for team, games in sorted(teams.items()):
        carry_states = v1.team_states(games, CARRIES)
        target_states = v1.team_states(games, RB_TARGETS)
        for n, game in enumerate(games):
            if game.season not in seasons or not game.has_stats:
                continue
            cs, ts = carry_states[n], target_states[n]
            carry_donors = v1.donors_in(game, cs.baselines, CARRIES)
            if not any(cs.baselines[d].base >= CARRIES.material for d in carry_donors):
                continue
            c_reserve, t_reserve = v1.reserve_for(carry_states, n), v1.reserve_for(target_states, n)
            if None in (cs.budget, ts.budget, c_reserve, t_reserve):
                continue
            backs = sorted(pid for pid, b in cs.baselines.items()
                           if game.status.get(pid) == "ACT" and game.position.get(pid, "") in CARRIES.recipients
                           and b.base > 0)
            if not backs:
                continue
            target_donors = v1.donors_in(game, ts.baselines, RB_TARGETS)
            rows = []
            for pid in backs:
                c, t = cs.baselines[pid], ts.baselines.get(pid)
                rows.append({"id": pid, "group": "RB", "carries_base": c.base,
                             "targets_base": t.base if t else 0.0,
                             "points_base": c.points,
                             "rush_ppu": c.points_per_unit,
                             "rec_ppu": t.points_per_unit if t else 0.0,
                             "actual_carries": v1._stat(game, pid, "carries"),
                             "actual_targets": v1._stat(game, pid, "targets"),
                             "actual_points": v1._stat(game, pid, "fantasy_points_ppr")})
            events.append({
                "season": game.season, "week": game.week, "team": team,
                "carries": {"budget": cs.budget, "reserve": c_reserve,
                            "active": v1.active_sum(game, cs.baselines, CARRIES),
                            "donors": [{"id": d, "group": "RB", "base": cs.baselines[d].base} for d in sorted(carry_donors)]},
                "targets": {"budget": ts.budget, "reserve": t_reserve,
                            "active": v1.active_sum(game, ts.baselines, RB_TARGETS),
                            "donors": [{"id": d, "group": "RB", "base": ts.baselines[d].base} for d in sorted(target_donors)]},
                "rows": rows,
            })
    return events


def _gains(pool: dict, recipients: list[dict], phi: float) -> dict:
    if phi <= 0 or not pool["donors"] or not recipients:
        return {r["id"]: 0.0 for r in recipients}
    return v1.allocate_fixed_pie(recipients, pool["donors"], pool["budget"], pool["active"], pool["reserve"], phi, 0.0)["gains"]


def predict(event: dict, phi_carries: float | None, phi_targets: float) -> dict:
    """phi_carries=None is BASE: every back keeps his own baseline."""
    rows = event["rows"]
    if phi_carries is None:
        gc = {r["id"]: 0.0 for r in rows}
        gt = dict(gc)
    else:
        gc = _gains(event["carries"], [{"id": r["id"], "group": "RB", "base": r["carries_base"]} for r in rows], phi_carries)
        target_recipients = [{"id": r["id"], "group": "RB", "base": r["targets_base"]} for r in rows if r["targets_base"] > 0]
        gt = {r["id"]: 0.0 for r in rows} | _gains(event["targets"], target_recipients, phi_targets)
    carries = np.array([r["carries_base"] + gc[r["id"]] for r in rows])
    targets = np.array([r["targets_base"] + gt[r["id"]] for r in rows])
    points = np.array([r["points_base"] + gc[r["id"]] * r["rush_ppu"] + gt[r["id"]] * r["rec_ppu"] for r in rows])
    return {"carries": carries, "targets": targets, "points": points}


ACTUAL = {"carries": "actual_carries", "targets": "actual_targets", "points": "actual_points"}


def errors(event: dict, config) -> dict:
    pred = predict(event, *config)
    return {k: np.array([r[ACTUAL[k]] for r in event["rows"]]) - pred[k] for k in pred}


def mae(events, config, key="points") -> float:
    e = np.concatenate([errors(ev, config)[key] for ev in events])
    return float(np.abs(e).mean())


def mse(events, config, key="points") -> float:
    e = np.concatenate([errors(ev, config)[key] for ev in events])
    return float((e ** 2).mean())


def paired(events, a, b, key) -> dict:
    """Paired a - b on squared and absolute error, event-clustered bootstrap."""
    sq, ab, counts, bias = [], [], [], []
    for ev in events:
        ea, eb = errors(ev, a)[key], errors(ev, b)[key]
        sq.append(float((ea ** 2 - eb ** 2).sum()))
        ab.append(float((np.abs(ea) - np.abs(eb)).sum()))
        counts.append(len(ea))
        bias.append(float(ea.sum()))
    sq, ab, counts = np.array(sq), np.array(ab), np.array(counts, dtype=float)
    rng = np.random.default_rng(v1.BOOTSTRAP_SEED)
    idx = rng.integers(0, len(events), size=(v1.BOOTSTRAP_DRAWS, len(events)))
    denom = counts[idx].sum(axis=1)
    sq_ci = np.percentile(sq[idx].sum(axis=1) / denom, [2.5, 97.5])
    ab_ci = np.percentile(ab[idx].sum(axis=1) / denom, [2.5, 97.5])
    return {"mse_delta": float(sq.sum() / counts.sum()), "mse_ci": [float(sq_ci[0]), float(sq_ci[1])],
            "mae_delta": float(ab.sum() / counts.sum()), "mae_ci": [float(ab_ci[0]), float(ab_ci[1])],
            "events": len(events), "rows": int(counts.sum()),
            "bias_actual_minus_pred": float(sum(bias) / counts.sum())}


BASE = (None, 0.0)
CONFIGS = [(pc, pt) for pc in PHI_CARRIES_GRID for pt in PHI_TARGETS_GRID]


def key(config) -> str:
    return "BASE" if config[0] is None else f"phi_carries={config[0]:g},phi_targets={config[1]:g}"


def select(discovery) -> dict:
    """Lowest discovery points squared error; ties to the smaller phi_targets, then phi_carries."""
    scores = {key(c): round(mse(discovery, c), 3) for c in CONFIGS}
    best = sorted(CONFIGS, key=lambda c: (scores[key(c)], c[1], c[0]))[0]
    return {"phi_carries": best[0], "phi_targets": best[1], "points_mse": scores,
            "points_mae": {key(c): round(mae(discovery, c), 4) for c in CONFIGS},
            "base_points_mse": round(mse(discovery, BASE), 3), "base_points_mae": round(mae(discovery, BASE), 4),
            "events": len(discovery), "rows": sum(len(e["rows"]) for e in discovery)}


GATE_MAE_MARGIN = 0.15


def verdict(points: dict, carries: dict, targets: dict | None) -> dict:
    g1 = points["mse_ci"][1] < 0
    g2 = points["mae_ci"][1] < GATE_MAE_MARGIN
    g3 = carries["mse_delta"] < 0 and (targets is None or targets["mse_delta"] < 0)
    g4 = points["events"] >= GATE_MIN_EVENTS and points["rows"] >= GATE_MIN_ROWS
    status = "INSUFFICIENT" if not g4 else ("PROMOTE" if g1 and g2 and g3 else "NOT_PROMOTED")
    return {"G1_points_squared_error": g1, "G2_points_mae_noninferior": g2, "G3_mechanism": g3,
            "G4_sample": g4, "verdict": status}


def grade(events, frozen) -> dict:
    points = paired(events, frozen, BASE, "points")
    carries = paired(events, frozen, BASE, "carries")
    targets = paired(events, frozen, BASE, "targets") if frozen[1] > 0 else None
    by_season = {}
    for season in sorted({e["season"] for e in events}):
        sub = [e for e in events if e["season"] == season]
        by_season[str(season)] = paired(sub, frozen, BASE, "points")
    return {"points": points, "carries": carries, "targets": targets, "by_season": by_season,
            "carries_only_v1_points": paired(events, (frozen[0], 0.0), BASE, "points"),
            "gate": verdict(points, carries, targets)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("study")
    s.add_argument("--cache", type=Path, default=v1.DEFAULT_CACHE)
    s.add_argument("--out", type=Path, default=Path("artifacts/nfl_rb_absence_reallocation_v2.json"))
    s.add_argument("--discovery-only", action="store_true",
                   help="discovery grid only; never reads the confirmation seasons")
    args = parser.parse_args()

    teams, digests = v1.load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, args.cache)
    discovery = build_events(teams, DISCOVERY_SEASONS)
    selection = select(discovery)
    frozen = (selection["phi_carries"], selection["phi_targets"])
    if args.discovery_only:
        detail = {key(c): {k: round(mae(discovery, c, k), 4) for k in ACTUAL} for c in [BASE] + CONFIGS}
        print(json.dumps({"selection": selection, "discovery_mae_by_unit": detail,
                          "frozen_vs_base": paired(discovery, frozen, BASE, "points")}, indent=1))
        return

    live_teams, live_digests = v1.load((2025, 2026), args.cache)
    live = build_events(live_teams, (2026,))
    conf_teams, conf_digests = v1.load(CONFIRMATION_WARMUP + CONFIRMATION_SEASONS, args.cache)
    confirmation = build_events(conf_teams, CONFIRMATION_SEASONS)
    result = {"study": STUDY_ID, "registered": "docs/nfl-rb-absence-reallocation-study.md",
              "generated_at": datetime.now(timezone.utc).isoformat(),
              "sources": {**digests, **conf_digests, **live_digests},
              "selection": selection, "frozen": {"phi_carries": frozen[0], "phi_targets": frozen[1]},
              "confirmation": grade(confirmation, frozen),
              "descriptive_2026": ({"events": len(live), "rows": sum(len(e["rows"]) for e in live),
                                    "points_mse": {"BASE": mse(live, BASE), "FROZEN": mse(live, frozen)},
                                    "points_mae": {"BASE": mae(live, BASE), "FROZEN": mae(live, frozen)}}
                                   if live else {"events": 0})}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=1) + "\n")
    c = result["confirmation"]
    print(f"frozen {frozen}: points MSE {c['points']['mse_delta']:+.3f} {c['points']['mse_ci']}  "
          f"MAE {c['points']['mae_delta']:+.4f} {c['points']['mae_ci']}  -> {c['gate']['verdict']}")


if __name__ == "__main__":
    main()
