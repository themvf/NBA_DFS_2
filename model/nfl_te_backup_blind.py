"""Starting TE out: does the backup TE take over, the way backup RBs did?

Study `nfl-te-backup-blind-v1`, registered in docs/nfl-te-backup-blind-study.md
before any 2014-2018 outcome in its population was computed.

The RB blind test (`nfl-rb-first-game-blind-v1`) found that in the first game a
starting back misses, handing the leftover work to the back who takes over
beats his baseline. This asks the same question of tight ends: in the first
game a starting TE (4+ targets a game) misses, does a fixed pie over the TE
room's targets improve the points projection of the TE who takes over (the
lead remaining TE)? (H1, frozen before discovery.) And does that TE reach 12+
DraftKings points more often than his own recent games predict? (H2, added
after the discovery run and disclosed as such.)

The TE room is its own pie: the team budget counts only tight ends' targets,
and only tight ends inherit. Everything else is v1's walk-forward machinery
(8-game window, half-life 4, >= 2 active games, full-strength reserve, 4x cap).

Usage:
    python -m model.nfl_te_backup_blind discovery   # 2020-2025, already-seen seasons
    python -m model.nfl_te_backup_blind validate    # activity proxy vs true status, 2019-2025
    python -m model.nfl_te_backup_blind blind       # 2014-2018 structure only, no outcomes
    python -m model.nfl_te_backup_blind unmask      # once, after the registration is pushed
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from model import nfl_absence_reallocation as v1
from model import nfl_rb_first_game_blind as proxy
from model import nfl_wr_breakout_blind as wr

STUDY_ID = "nfl-te-backup-blind-v1"

# Frozen before any outcome was computed.
STARTER_MIN_TARGETS = 4.0       # a starting TE: 4+ targets a game over the window
TE_ROOM = v1.PoolSpec("te_targets", "targets", frozenset({"TE"}), frozenset({"TE"}),
                      frozenset(), STARTER_MIN_TARGETS, budget_only=frozenset({"TE"}))
PHI_GRID = (0.5, 1.0)
BOOM_DK = 12.0                  # TE big game: the frozen fantasy-board TE spike line (p85)

DISCOVERY_WARMUP = (2019,)
DISCOVERY_SEASONS = (2020, 2021, 2022, 2023, 2024, 2025)
BLIND_WARMUP = proxy.BLIND_WARMUP
BLIND_SEASONS = proxy.BLIND_SEASONS

GATE_MIN_EVENTS = 70          # was 80 before the blind count (75) was seen; see the study doc
PROXY_MIN_RECALL = 0.90
PROXY_MIN_PRECISION = 0.90

# Set from the discovery run before registration (see the study doc).
FROZEN_PHI: float | None = 0.5
MAE_MARGIN: float | None = 0.50     # same non-inferiority margin as the RB blind test

# Second hypothesis, added after discovery and disclosed as such: the lead
# remaining TE's 12+ DK rate beats his history by more than a TE2's does in
# full-strength games. Discovery 2020-2025: +8.14 pp [+2.29, +14.63].
BIG_GAME_DISCOVERY_EFFECT = 0.0814
BIG_GAME_MIN_SHARE_OF_DISCOVERY = 0.5


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def team_events(teams, seasons) -> list[dict]:
    """Starting-TE absences (first game or under way) and full-strength control games."""
    out = []
    for team, games in sorted(teams.items()):
        states = v1.team_states(games, TE_ROOM)
        for n, game in enumerate(games):
            if game.season not in seasons or not game.has_stats:
                continue
            st = states[n]
            base = st.baselines
            donors = v1.donors_in(game, base, TE_ROOM)
            starters = [d for d in donors if base[d].base >= STARTER_MIN_TARGETS]
            tes = sorted((pid for pid, b in base.items()
                          if game.status.get(pid) == "ACT" and game.position.get(pid) == "TE" and b.base > 0),
                         key=lambda p: (-base[p].base, p))
            if not tes:
                continue
            window = [g for g in reversed(games[max(0, n - v1.WINDOW_GAMES):n]) if g.has_stats]
            rates = wr.boom_rates(window, (BOOM_DK,))
            rows = [{"id": pid, "targets_base": base[pid].base, "points_base": base[pid].points,
                     "ppu": base[pid].points_per_unit,
                     "p_boom": rates.get(pid, {BOOM_DK: 0.0})[BOOM_DK],
                     "actual_targets": v1._stat(game, pid, "targets"),
                     "actual_points": v1._stat(game, pid, "fantasy_points_ppr"),
                     "dk": wr.dk_points(game, pid)} for pid in tes]
            prev = games[n - 1] if n else None
            star_wr_out = False
            if starters:
                wr_base = v1.player_baselines(window, v1.POOLS["targets"])
                star_wr_out = any(game.position.get(pid) == "WR" and s in v1.ABSENT_STATUSES
                                  and pid in wr_base and wr_base[pid].base >= wr.STAR_MIN_TARGETS
                                  for pid, s in game.status.items())
            if starters:
                reserve = v1.reserve_for(states, n)
                if st.budget is None or reserve is None:
                    continue
                kind = "absence"
            elif not donors:
                reserve = None
                kind = "control"
            else:
                continue
            out.append({
                "season": game.season, "week": game.week, "team": team, "kind": kind,
                "first_game": bool(starters) and any(prev is not None and prev.status.get(d) == "ACT" for d in starters),
                "star_wr_out": star_wr_out,
                "starters": [{"id": d, "name": game.name.get(d, d), "targets": base[d].base} for d in sorted(starters)],
                "pie": None if kind == "control" else {
                    "budget": st.budget, "reserve": reserve, "active": v1.active_sum(game, base, TE_ROOM),
                    "donors": [{"id": d, "group": "TE", "base": base[d].base} for d in sorted(donors)]},
                "rows": rows,
            })
    return out


def split(events):
    absence = [e for e in events if e["kind"] == "absence"]
    return ([e for e in absence if e["first_game"]], [e for e in absence if not e["first_game"]],
            [e for e in events if e["kind"] == "control"])


# ---------------------------------------------------------------------------
# Prediction and scoring (lead remaining TE; the pie is split across the room)
# ---------------------------------------------------------------------------

def predict(event: dict, phi: float | None) -> dict:
    """phi=None is BASE: every TE keeps his own baseline."""
    rows = event["rows"]
    gains = {r["id"]: 0.0 for r in rows}
    if phi is not None and event["pie"]:
        p = event["pie"]
        recips = [{"id": r["id"], "group": "TE", "base": r["targets_base"]} for r in rows]
        gains = v1.allocate_fixed_pie(recips, p["donors"], p["budget"], p["active"], p["reserve"], phi, 0.0)["gains"]
    return {"targets": np.array([r["targets_base"] + gains[r["id"]] for r in rows]),
            "points": np.array([r["points_base"] + gains[r["id"]] * r["ppu"] for r in rows])}


ACTUAL = {"targets": "actual_targets", "points": "actual_points"}


def errors(event, phi, which="lead"):
    pred = predict(event, phi)
    out = {k: np.array([r[ACTUAL[k]] for r in event["rows"]]) - pred[k] for k in pred}
    if which == "lead":
        return {k: v[:1] for k, v in out.items()}
    return {k: v[1:] for k, v in out.items()}


def paired(events, phi_a, phi_b, key="points", which="lead", seed_offset=0) -> dict:
    """a - b on squared and absolute error, event bootstrap."""
    sq, ab, cnt, ba, bb = [], [], [], [], []
    for ev in events:
        ea, eb = errors(ev, phi_a, which)[key], errors(ev, phi_b, which)[key]
        sq.append(float((ea ** 2 - eb ** 2).sum()))
        ab.append(float((np.abs(ea) - np.abs(eb)).sum()))
        cnt.append(len(ea))
        ba.append(float(ea.sum()))
        bb.append(float(eb.sum()))
    sq, ab, cnt = np.array(sq), np.array(ab), np.array(cnt, dtype=float)
    n = cnt.sum()
    if n == 0:
        return {"events": len(events), "rows": 0}
    rng = np.random.default_rng(v1.BOOTSTRAP_SEED + seed_offset)
    idx = rng.integers(0, len(events), size=(v1.BOOTSTRAP_DRAWS, len(events)))
    den = cnt[idx].sum(axis=1)
    den[den == 0] = np.nan
    sq_ci = np.nanpercentile(sq[idx].sum(axis=1) / den, [2.5, 97.5])
    ab_ci = np.nanpercentile(ab[idx].sum(axis=1) / den, [2.5, 97.5])
    ea_all = np.concatenate([errors(ev, phi_a, which)[key] for ev in events])
    eb_all = np.concatenate([errors(ev, phi_b, which)[key] for ev in events])
    return {"events": len(events), "rows": int(n),
            "mse_a": float((ea_all ** 2).mean()), "mse_b": float((eb_all ** 2).mean()),
            "mae_a": float(np.abs(ea_all).mean()), "mae_b": float(np.abs(eb_all).mean()),
            "bias_a": float(sum(ba) / n), "bias_b": float(sum(bb) / n),
            "mse_delta": float(sq.sum() / n), "mse_ci": [float(sq_ci[0]), float(sq_ci[1])],
            "mae_delta": float(ab.sum() / n), "mae_ci": [float(ab_ci[0]), float(ab_ci[1])]}


def select(first) -> dict:
    """Lowest lead-TE points squared error on discovery; ties to the smaller phi."""
    scores = {f"{phi:g}": paired(first, phi, None)["mse_a"] for phi in PHI_GRID}
    best = sorted(PHI_GRID, key=lambda phi: (round(scores[f"{phi:g}"], 3), phi))[0]
    base = paired(first, None, None)
    return {"phi": best, "points_mse": scores, "base_points_mse": base["mse_a"],
            "points_mae": {f"{phi:g}": paired(first, phi, None)["mae_a"] for phi in PHI_GRID},
            "base_points_mae": base["mae_a"], "events": len(first)}


def verdict(points: dict, targets: dict, mae_margin: float) -> dict:
    g1 = points["mse_ci"][1] < 0
    g2 = points["mae_ci"][1] < mae_margin
    g3 = targets["mse_delta"] < 0
    g4 = points["events"] >= GATE_MIN_EVENTS
    status = "INSUFFICIENT" if not g4 else ("PROMOTE" if g1 and g2 and g3 else "NOT_PROMOTED")
    return {"G1_points_squared_error": g1, "G2_points_mae_noninferior": g2, "G3_targets": g3,
            "G4_sample": g4, "verdict": status}


def big_game_verdict(big: dict) -> dict:
    b1 = big.get("ci", [0.0])[0] > 0
    b2 = big.get("effect", 0.0) >= BIG_GAME_MIN_SHARE_OF_DISCOVERY * BIG_GAME_DISCOVERY_EFFECT
    b3 = big.get("rows", 0) >= GATE_MIN_EVENTS
    status = "INSUFFICIENT" if not b3 else ("PROMOTE" if b1 and b2 else "NOT_PROMOTED")
    return {"B1_ci_above_zero": b1, "B2_half_of_discovery": b2, "B3_sample": b3, "verdict": status}


def big_game(first, control, seed_offset=0) -> dict:
    """Secondary: lead remaining TE's 12+ DK rate vs history, minus the same gap
    for the second TE in full-strength games (the same kind of player, not promoted)."""
    def gaps(events, pos):
        rows = [ev["rows"][pos] for ev in events if len(ev["rows"]) > pos]
        y = np.array([r["dk"] >= BOOM_DK for r in rows], dtype=float)
        p = np.array([r["p_boom"] for r in rows])
        return y, p
    ya, pa = gaps(first, 0)
    yc, pc = gaps(control, 1)
    if not len(ya) or not len(yc):
        return {"rows": len(ya)}
    rng = np.random.default_rng(v1.BOOTSTRAP_SEED + 50 + seed_offset)
    da, dc = ya - pa, yc - pc
    draws = (da[rng.integers(0, len(da), (v1.BOOTSTRAP_DRAWS, len(da)))].mean(axis=1)
             - dc[rng.integers(0, len(dc), (v1.BOOTSTRAP_DRAWS, len(dc)))].mean(axis=1))
    ci = np.percentile(draws, [2.5, 97.5])
    return {"rows": len(ya), "control_rows": len(yc), "boom_rate": float(ya.mean()),
            "expected_rate": float(pa.mean()), "control_boom_rate": float(yc.mean()),
            "control_expected_rate": float(pc.mean()),
            "effect": float(da.mean() - dc.mean()), "ci": [float(ci[0]), float(ci[1])]}


def describe(events, phi) -> dict:
    """Reported alongside the gate; none of it gates."""
    first, under_way, control = split(events)
    return {
        "big_game_12_lead_te_under_way": big_game(under_way, control, seed_offset=1),
        "big_game_12_lead_te_star_wr_also_out": big_game([e for e in first if e["star_wr_out"]], control, seed_offset=2),
        "other_tes_first_game": paired(first, phi, None, which="others"),
        "under_way_lead_te": paired(under_way, phi, None),
        "star_wr_also_out": paired([e for e in first if e["star_wr_out"]], phi, None),
        "by_season": {str(s): paired([e for e in first if e["season"] == s], phi, None)
                      for s in sorted({e["season"] for e in first})},
    }


def structure(events, excluded: int = 0) -> dict:
    first, under_way, control = split(events)
    return {"first_game_events": len(first), "under_way_events": len(under_way),
            "control_games": len(control), "excluded_already_graded": excluded,
            "star_wr_also_out": sum(e["star_wr_out"] for e in first),
            "lead_te_single_te_room": sum(len(e["rows"]) == 1 for e in first),
            "first_game_by_season": {str(s): sum(e["season"] == s for e in first)
                                     for s in sorted({e["season"] for e in events})}}


# ---------------------------------------------------------------------------
# Blind data: drop team-games whose TE rows the WR breakout unmask already graded
# ---------------------------------------------------------------------------

def blind_events(cache: Path):
    teams, played, snapless, digests, quality = proxy.load(BLIND_WARMUP + BLIND_SEASONS, cache, snaps=True)
    proxied = proxy.with_proxy(teams, played, snapless)
    graded = {(g["team"], g["season"], g["week"]) for g in wr.team_games(proxied, BLIND_SEASONS)
              if g["kind"] == "absence"}
    events = team_events(proxied, BLIND_SEASONS)
    keep = [e for e in events if e["kind"] == "control" or (e["team"], e["season"], e["week"]) not in graded]
    excluded = sum(1 for e in events if e["kind"] == "absence" and e["first_game"]
                   and (e["team"], e["season"], e["week"]) in graded)
    return keep, excluded, digests, quality


# ---------------------------------------------------------------------------
# Proxy validation (2019-2025, true status available)
# ---------------------------------------------------------------------------

def validate(cache: Path, phi: float) -> dict:
    teams, played, snapless, digests, quality = proxy.load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, cache, snaps=True)
    t_first, _, t_control = split(team_events(teams, DISCOVERY_SEASONS))
    p_first, _, p_control = split(team_events(proxy.with_proxy(teams, played, snapless), DISCOVERY_SEASONS))
    t_keys = {(e["team"], e["season"], e["week"]): e["rows"][0]["id"] for e in t_first}
    p_keys = {(e["team"], e["season"], e["week"]): e["rows"][0]["id"] for e in p_first}
    same = sum(1 for k, lead in t_keys.items() if p_keys.get(k) == lead)
    recall = same / len(t_keys) if t_keys else 0.0
    precision = same / len(p_keys) if p_keys else 0.0
    tp, pp = paired(t_first, phi, None), paired(p_first, phi, None)
    tb, pb = big_game(t_first, t_control), big_game(p_first, p_control)
    checks = {"V1_recall": recall >= PROXY_MIN_RECALL, "V2_precision": precision >= PROXY_MIN_PRECISION,
              "V3_proxy_replicates_points": tp["mse_ci"][0] <= pp["mse_delta"] <= tp["mse_ci"][1],
              "V4_proxy_replicates_big_game": tb["ci"][0] <= pb["effect"] <= tb["ci"][1]}
    return {"sources": digests, "true_first_game_events": len(t_keys), "proxy_first_game_events": len(p_keys),
            "same_event_same_lead": same, "recall": recall, "precision": precision,
            "true_status_points": tp, "proxy_status_points": pp,
            "true_status_big_game": tb, "proxy_status_big_game": pb,
            "checks": checks, "passed": all(checks.values())}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _frozen() -> float:
    if FROZEN_PHI is None or MAE_MARGIN is None:
        raise SystemExit("FROZEN_PHI and MAE_MARGIN must be set from the registered discovery run first")
    return FROZEN_PHI


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("command", choices=["discovery", "validate", "blind", "unmask"])
    parser.add_argument("--cache", type=Path, default=v1.DEFAULT_CACHE)
    args = parser.parse_args()
    base = {"study": STUDY_ID, "mode": args.command, "generated_at": datetime.now(timezone.utc).isoformat()}

    if args.command == "discovery":
        teams, _, _, digests, quality = proxy.load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, args.cache, snaps=False)
        events = team_events(teams, DISCOVERY_SEASONS)
        first, _, _ = split(events)
        sel = select(first)
        _, _, control = split(events)
        result = {**base, "sources": digests, "structure": structure(events), "selection": sel,
                  "points": paired(first, sel["phi"], None), "targets": paired(first, sel["phi"], None, "targets"),
                  "big_game": big_game(first, control), "described": describe(events, sel["phi"])}
    elif args.command == "validate":
        result = {**base, **validate(args.cache, _frozen())}
    elif args.command == "blind":
        events, excluded, digests, quality = blind_events(args.cache)
        result = {**base, "sources": digests, "data_quality": quality, "structure": structure(events, excluded)}
    else:
        phi = _frozen()
        check = validate(args.cache, phi)
        if not check["passed"]:
            raise SystemExit(f"proxy validation failed, study VOID: {check['checks']}")
        events, excluded, digests, quality = blind_events(args.cache)
        first, _, _ = split(events)
        _, _, control = split(events)
        points, targets = paired(first, phi, None), paired(first, phi, None, "targets")
        big = big_game(first, control)
        result = {**base, "registered": "docs/nfl-te-backup-blind-study.md", "sources": digests,
                  "frozen": {"phi": phi, "mae_margin": MAE_MARGIN,
                             "big_game_discovery_effect": BIG_GAME_DISCOVERY_EFFECT},
                  "proxy_validation": check["checks"], "structure": structure(events, excluded),
                  "H1_points": points, "H1_targets": targets, "H1_gate": verdict(points, targets, MAE_MARGIN),
                  "H2_big_game": big, "H2_gate": big_game_verdict(big), "described": describe(events, phi)}

    out = Path(f"artifacts/nfl_te_backup_blind_v1_{args.command}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=float) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("sources", "described")}, indent=1, default=float))


if __name__ == "__main__":
    main()
