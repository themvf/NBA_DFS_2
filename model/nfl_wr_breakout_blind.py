"""Star WR out: do the remaining pass catchers have big games more often?

Study `nfl-wr-breakout-blind-v1`, registered in
docs/nfl-wr-breakout-blind-study.md before any 2014-2018 wide receiver or
tight end outcome was computed.

Earlier absence studies graded the MEAN projection, and for targets the mean
barely moves: the leftover targets are split across many players. This study
asks a different question, about the TAIL. When a star receiver sits, does
each remaining WR/TE have a big game (20+ DraftKings points) more often than
his own recent games say? A GPP needs a player who breaks out, and it does not
matter much which of them it turns out to be.

For each pass catcher the expected chance of a big game is the recency-weighted
share of his recent active games that reached the threshold (8-game window,
half-life 4, the same window every absence study uses). The primary measure is
the gap between how often they actually boomed and how often that history
predicted, in the first game of a star's absence, minus the same gap in
full-strength games (the control removes any general bias in the estimate).

Usage:
    python -m model.nfl_wr_breakout_blind discovery   # 2020-2025, already-seen seasons
    python -m model.nfl_wr_breakout_blind validate    # activity proxy vs true status, 2019-2025
    python -m model.nfl_wr_breakout_blind blind       # 2014-2018 structure only, no outcomes
    python -m model.nfl_wr_breakout_blind unmask      # once, after the registration is pushed
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from model import nfl_absence_reallocation as v1
from model import nfl_rb_first_game_blind as proxy

STUDY_ID = "nfl-wr-breakout-blind-v1"

TARGETS = v1.POOLS["targets"]
CARRIES = v1.POOLS["carries"]

# Frozen before any outcome was computed.
STAR_POSITIONS = frozenset({"WR"})
STAR_MIN_TARGETS = 7.0          # a star: 7+ targets a game over the window
MATERIAL_TARGETS = 3.0          # control games have no pass catcher this size missing
RECIPIENT_POSITIONS = frozenset({"WR", "TE"})
BOOM_DK = 20.0                  # primary big-game threshold
SECONDARY_BOOM_DK = 25.0
THRESHOLDS = (BOOM_DK, SECONDARY_BOOM_DK)

DISCOVERY_WARMUP = (2019,)
DISCOVERY_SEASONS = (2020, 2021, 2022, 2023, 2024, 2025)
BLIND_WARMUP = proxy.BLIND_WARMUP
BLIND_SEASONS = proxy.BLIND_SEASONS

# Lowered from 150/400 before registration, after the discovery run showed
# ~25 first-game events a season and the blind seasons hold 86 (a count of
# who played, not an outcome). 150 would have guaranteed INSUFFICIENT; at 80+
# events the expected CI half-width (~1.8pp) is below the discovery effect.
GATE_MIN_EVENTS = 80
GATE_MIN_ROWS = 500
GATE_MIN_SHARE_OF_DISCOVERY = 0.5
PROXY_MIN_RECALL = 0.90
PROXY_MIN_PRECISION = 0.90

# Set from the discovery run before registration (see the study doc):
# first-game control-adjusted effect on 2020-2025, +2.81pp [+1.47, +4.20].
DISCOVERY_EFFECT: float | None = 0.0281


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def dk_points(game: v1.TeamGame, pid: str) -> float:
    """PPR points plus DraftKings' 100-yard receiving and rushing bonuses."""
    s = game.stats.get(pid)
    if not s:
        return 0.0
    bonus = 3.0 * (s["receiving_yards"] >= 100) + 3.0 * (s["rushing_yards"] >= 100)
    return s["fantasy_points_ppr"] + bonus


def boom_rates(window: list[v1.TeamGame], thresholds=THRESHOLDS) -> dict:
    """Recency-weighted share of each player's active games at or above each threshold.

    window is most-recent-first; only games the player was active in count.
    """
    weights = v1.recency_weights(len(window))
    acc = defaultdict(lambda: [0.0, np.zeros(len(thresholds))])
    for k, game in enumerate(window):
        for pid, status in game.status.items():
            if status != "ACT":
                continue
            pts = dk_points(game, pid)
            a = acc[pid]
            a[0] += weights[k]
            a[1] = a[1] + weights[k] * np.array([pts >= t for t in thresholds], dtype=float)
    return {pid: {t: float(hits[i] / w) for i, t in enumerate(thresholds)} for pid, (w, hits) in acc.items()}


def pass_share(window: list[v1.TeamGame]) -> float | None:
    if not window:
        return None
    weights = v1.recency_weights(len(window))
    tg = np.array([v1.team_total(g, TARGETS) for g in window])
    cr = np.array([v1.team_total(g, CARRIES) for g in window])
    denom = float(np.dot(weights, tg + cr))
    return float(np.dot(weights, tg)) / denom if denom > 0 else None


def team_games(teams, seasons) -> list[dict]:
    """Every graded team game: star-absence games and full-strength control games."""
    out = []
    for team, games in sorted(teams.items()):
        states = v1.team_states(games, TARGETS)
        for n, game in enumerate(games):
            if game.season not in seasons or not game.has_stats:
                continue
            base = states[n].baselines
            absent = [pid for pid, s in game.status.items()
                      if s in v1.ABSENT_STATUSES and pid in base]
            stars = [pid for pid in absent
                     if game.position.get(pid) in STAR_POSITIONS and base[pid].base >= STAR_MIN_TARGETS]
            material = [pid for pid in absent
                        if game.position.get(pid, "") in TARGETS.donors and base[pid].base >= MATERIAL_TARGETS]
            if stars:
                kind = "absence"
            elif not material:
                kind = "control"
            else:
                continue
            window = [g for g in reversed(games[max(0, n - v1.WINDOW_GAMES):n]) if g.has_stats]
            rates = boom_rates(window)
            recips = sorted(pid for pid in base
                            if game.status.get(pid) == "ACT" and game.position.get(pid) in RECIPIENT_POSITIONS)
            if not recips:
                continue
            wrs = [pid for pid in recips if game.position[pid] == "WR"]
            lead = max(wrs, key=lambda p: (base[p].base, p)) if wrs else None
            prev = games[n - 1] if n else None
            out.append({
                "season": game.season, "week": game.week, "team": team, "kind": kind,
                "first_game": bool(stars) and any(prev is not None and prev.status.get(s) == "ACT" for s in stars),
                "stars": [{"id": s, "name": game.name.get(s, s), "targets": base[s].base} for s in sorted(stars)],
                "pass_share": pass_share(window),
                "rows": [{"id": pid, "position": game.position[pid], "targets_base": base[pid].base,
                          "role": ("TE" if game.position[pid] == "TE" else ("lead_WR" if pid == lead else "other_WR")),
                          "p": rates.get(pid, {t: 0.0 for t in THRESHOLDS}), "dk": dk_points(game, pid)}
                         for pid in recips],
            })
    return out


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _event_sums(events, threshold, role=None):
    """Per event: sum of (boomed - expected), sum boomed, sum expected, row count."""
    gap, hit, exp, cnt = [], [], [], []
    for ev in events:
        rows = [r for r in ev["rows"] if role is None or r["role"] == role]
        y = np.array([r["dk"] >= threshold for r in rows], dtype=float)
        p = np.array([r["p"][threshold] for r in rows], dtype=float)
        gap.append(float((y - p).sum()))
        hit.append(float(y.sum()))
        exp.append(float(p.sum()))
        cnt.append(len(rows))
    return np.array(gap), np.array(hit), np.array(exp), np.array(cnt, dtype=float)


def _boot(sums, counts, seed_offset=0):
    rng = np.random.default_rng(v1.BOOTSTRAP_SEED + seed_offset)
    idx = rng.integers(0, len(sums), size=(v1.BOOTSTRAP_DRAWS, len(sums)))
    denom = counts[idx].sum(axis=1)
    denom[denom == 0] = np.nan
    return sums[idx].sum(axis=1) / denom


def gap_vs_control(absence, control, threshold=BOOM_DK, role=None) -> dict:
    """Actual minus expected boom rate in absence games, minus the same gap in control games."""
    ga, ha, ea, ca = _event_sums(absence, threshold, role)
    gc, hc, ec, cc = _event_sums(control, threshold, role)
    if ca.sum() == 0 or cc.sum() == 0:
        return {"events": len(absence), "rows": int(ca.sum())}
    gap_a, gap_c = ga.sum() / ca.sum(), gc.sum() / cc.sum()
    draws = _boot(ga, ca, 0) - _boot(gc, cc, 1)
    ci = np.nanpercentile(draws, [2.5, 97.5])
    return {"events": len(absence), "rows": int(ca.sum()),
            "control_events": len(control), "control_rows": int(cc.sum()),
            "boom_rate": float(ha.sum() / ca.sum()), "expected_rate": float(ea.sum() / ca.sum()),
            "control_boom_rate": float(hc.sum() / cc.sum()), "control_expected_rate": float(ec.sum() / cc.sum()),
            "gap": float(gap_a), "control_gap": float(gap_c),
            "effect": float(gap_a - gap_c), "ci": [float(ci[0]), float(ci[1])]}


def any_boom(absence, control, threshold=BOOM_DK) -> dict:
    """Team level: did at least one remaining WR/TE boom, versus 1 - prod(1 - p)?"""
    def per_event(events):
        y = np.array([float(any(r["dk"] >= threshold for r in ev["rows"])) for ev in events])
        e = np.array([1.0 - float(np.prod([1.0 - r["p"][threshold] for r in ev["rows"]])) for ev in events])
        return y, e
    ya, ea = per_event(absence)
    yc, ec = per_event(control)
    ones_a, ones_c = np.ones(len(ya)), np.ones(len(yc))
    draws = _boot(ya - ea, ones_a, 2) - _boot(yc - ec, ones_c, 3)
    ci = np.nanpercentile(draws, [2.5, 97.5])
    return {"events": len(ya), "any_boom_rate": float(ya.mean()), "expected": float(ea.mean()),
            "control_any_boom_rate": float(yc.mean()), "control_expected": float(ec.mean()),
            "effect": float((ya - ea).mean() - (yc - ec).mean()), "ci": [float(ci[0]), float(ci[1])]}


def split(games):
    absence = [g for g in games if g["kind"] == "absence"]
    return ([g for g in absence if g["first_game"]], [g for g in absence if not g["first_game"]],
            [g for g in games if g["kind"] == "control"])


def describe(games) -> dict:
    """Everything reported alongside the gate; none of it gates."""
    first, under_way, control = split(games)
    shares = [g["pass_share"] for g in first if g["pass_share"] is not None]
    cut = float(np.median(shares)) if shares else None
    return {
        "first_game_25_points": gap_vs_control(first, control, SECONDARY_BOOM_DK),
        "first_game_any_boom": any_boom(first, control),
        "first_game_by_role": {role: gap_vs_control(first, control, role=role)
                               for role in ("lead_WR", "other_WR", "TE")},
        "pass_heavy_cut": cut,
        "first_game_pass_heavy": gap_vs_control([g for g in first if cut is not None and (g["pass_share"] or 0) >= cut], control),
        "first_game_pass_light": gap_vs_control([g for g in first if cut is not None and (g["pass_share"] or 0) < cut], control),
        "under_way": gap_vs_control(under_way, control),
        "by_season": {str(s): gap_vs_control([g for g in first if g["season"] == s], control)
                      for s in sorted({g["season"] for g in first})},
    }


def structure(games) -> dict:
    """Counts that depend on who played, never on anyone's points."""
    first, under_way, control = split(games)
    return {"first_game_events": len(first), "first_game_rows": sum(len(g["rows"]) for g in first),
            "under_way_events": len(under_way), "control_games": len(control),
            "control_rows": sum(len(g["rows"]) for g in control),
            "first_game_by_season": {str(s): sum(g["season"] == s for g in first)
                                     for s in sorted({g["season"] for g in games})}}


def verdict(primary: dict, discovery_effect: float) -> dict:
    g1 = primary["ci"][0] > 0
    g2 = primary["effect"] >= GATE_MIN_SHARE_OF_DISCOVERY * discovery_effect
    g3 = primary["events"] >= GATE_MIN_EVENTS and primary["rows"] >= GATE_MIN_ROWS
    status = "INSUFFICIENT" if not g3 else ("PROMOTE" if g1 and g2 else "NOT_PROMOTED")
    return {"G1_breakout_rate_above_history": g1, "G2_holds_half_of_discovery": g2,
            "G3_sample": g3, "verdict": status}


# ---------------------------------------------------------------------------
# Proxy validation (2019-2025, true status available)
# ---------------------------------------------------------------------------

def validate(cache: Path) -> dict:
    teams, played, snapless, digests, quality = proxy.load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, cache, snaps=True)
    true_games = team_games(teams, DISCOVERY_SEASONS)
    proxy_games = team_games(proxy.with_proxy(teams, played, snapless), DISCOVERY_SEASONS)
    tf, _, tc = split(true_games)
    pf, _, pc = split(proxy_games)
    t_keys = {(g["team"], g["season"], g["week"]) for g in tf}
    p_keys = {(g["team"], g["season"], g["week"]) for g in pf}
    same = len(t_keys & p_keys)
    recall = same / len(t_keys) if t_keys else 0.0
    precision = same / len(p_keys) if p_keys else 0.0
    t_primary, p_primary = gap_vs_control(tf, tc), gap_vs_control(pf, pc)
    checks = {"V1_recall": recall >= PROXY_MIN_RECALL, "V2_precision": precision >= PROXY_MIN_PRECISION,
              "V3_proxy_replicates": t_primary["ci"][0] <= p_primary["effect"] <= t_primary["ci"][1]}
    return {"sources": digests, "data_quality": quality,
            "true_first_game_events": len(t_keys), "proxy_first_game_events": len(p_keys),
            "same_events": same, "recall": recall, "precision": precision,
            "true_status_primary": t_primary, "proxy_status_primary": p_primary,
            "checks": checks, "passed": all(checks.values())}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("command", choices=["discovery", "validate", "blind", "unmask"])
    parser.add_argument("--cache", type=Path, default=v1.DEFAULT_CACHE)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).isoformat()
    base = {"study": STUDY_ID, "mode": args.command, "generated_at": stamp}

    if args.command == "discovery":
        teams, _, _, digests, quality = proxy.load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, args.cache, snaps=False)
        games = team_games(teams, DISCOVERY_SEASONS)
        first, _, control = split(games)
        result = {**base, "sources": digests, "data_quality": quality, "structure": structure(games),
                  "primary": gap_vs_control(first, control), "described": describe(games)}
    elif args.command == "validate":
        result = {**base, **validate(args.cache)}
    elif args.command == "blind":
        teams, played, snapless, digests, quality = proxy.load(BLIND_WARMUP + BLIND_SEASONS, args.cache, snaps=True)
        games = team_games(proxy.with_proxy(teams, played, snapless), BLIND_SEASONS)
        result = {**base, "sources": digests, "data_quality": quality, "structure": structure(games)}
    else:
        if DISCOVERY_EFFECT is None:
            raise SystemExit("DISCOVERY_EFFECT must be set from the registered discovery run first")
        check = validate(args.cache)
        if not check["passed"]:
            raise SystemExit(f"proxy validation failed, study VOID: {check['checks']}")
        teams, played, snapless, digests, quality = proxy.load(BLIND_WARMUP + BLIND_SEASONS, args.cache, snaps=True)
        games = team_games(proxy.with_proxy(teams, played, snapless), BLIND_SEASONS)
        first, _, control = split(games)
        primary = gap_vs_control(first, control)
        result = {**base, "registered": "docs/nfl-wr-breakout-blind-study.md", "sources": digests,
                  "data_quality": quality, "discovery_effect": DISCOVERY_EFFECT,
                  "proxy_validation": check["checks"], "structure": structure(games),
                  "primary": primary, "gate": verdict(primary, DISCOVERY_EFFECT), "described": describe(games)}

    out = args.out or Path(f"artifacts/nfl_wr_breakout_blind_v1_{args.command}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=float) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("sources", "described")}, indent=1, default=float))


if __name__ == "__main__":
    main()
