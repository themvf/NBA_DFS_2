"""Point-only role-opportunity challenger (`nba-role-opportunity-challenger-v1`), NOT activated.

Section C of docs/nba-points-opportunity-reliability-spec.md. Pure arithmetic
on an original frozen forecast: blend the model's per-minute FGA and FTA, and
its three-point share, 50/50 with the detector's recent window, keep the
model's minutes and its frozen conversion rates. No probabilities, no P90 and
no 30+/40+ odds are produced; the 50/50 weight is a registration assumption,
not a fit, and a different weight is a new version.

Why it cannot run prospectively yet (both are recorded on every result):
  - the registered base is the ORIGINAL v3 forecast, which is produced only by
    the local v3 job (its model file is not in the repository), and
  - eligibility needs verified decision-time team identity from a timestamped
    official roster observation joined by player id; none is collected. With
    no roster evidence every player is skipped `team_identity_unverified`,
    while the workload-conflict warning still stands.

The formula is applied to every eligible player, not only flagged ones.
"""
from __future__ import annotations

from datetime import datetime
import math
from typing import Any

from model.nba_points_ladder import team_key
from research.nba_points_workload_conflicts import WINDOW_MAX, WINDOW_MIN, season_of, _order

VERSION = "nba-role-opportunity-challenger-v1"
WEIGHT = 0.5
ARITHMETIC_TOLERANCE = 0.01
REGISTERED_BASE = "v3"
ACTIVATION_BLOCKERS = (
    "original v3 forecasts are produced only by the local v3 job (model file not in the repository)",
    "the roster capture (ingest/nba_rosters.py) runs in the cloud, but must be read at the ORIGINAL v3 freeze, which runs locally",
    "no protocol registered under artifacts/nba-role-reliability/ before an original v3 freeze",
)


def _num(x: Any) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def base_points(fc: dict[str, Any], rates: dict[str, Any]) -> float:
    return 2 * (fc["fga"] - fc["fg3a"]) * rates["two"] + 3 * fc["fg3a"] * rates["three"] + fc["fta"] * rates["free"]


def recent_window(history: list[dict[str, Any]], slate_date: str) -> tuple[list[dict[str, Any]], list[str]]:
    """The detector's 3-5 game window; an inconsistent row (positive attempts, zero minutes) is excluded with a receipt, never replaced."""
    season = season_of(slate_date)
    rows = [h for h in _order([h for h in history if h.get("played", True)])
            if season_of(h["game_date"]) == season and str(h["game_date"]) < slate_date]
    window = rows[-WINDOW_MAX:]
    errors = []
    kept = []
    for h in window:
        att = [_num(h.get(k)) for k in ("fga", "fg3a", "fta")]
        m = _num(h.get("minutes"))
        if any(a is None or a < 0 for a in att) or m is None or m < 0:
            errors.append(f"invalid_row:{h.get('source_game_id')}")
            continue
        if m == 0 and sum(att) > 0:
            errors.append(f"attempts_with_zero_minutes:{h.get('source_game_id')}")
            continue
        kept.append(h)
    return kept, errors


def challenge(c: dict[str, Any], slate_date: str, *, base_model: str, freeze_at: str,
              roster: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    out = {"version": VERSION, "base_model": base_model, "player_key": c.get("player_key"), "player": c.get("player"),
           "registered_base": base_model == REGISTERED_BASE, "probabilities": "not_produced"}
    fc = {k: _num((c.get("opportunity_forecasts") or {}).get(k)) for k in ("minutes", "fga", "fg3a", "fta")}
    rates = {k: _num((c.get("opportunity_conversion_rates") or {}).get(k)) for k in ("two", "three", "free")}
    stored = _num((c.get("predictions") or {}).get("opportunity_points"))
    if stored is None or any(v is None for v in fc.values()) or any(v is None for v in rates.values()):
        return {**out, "status": "skipped", "reason": "original_not_modeled"}
    if any(not 0 <= r <= 1 for r in rates.values()) or fc["minutes"] <= 0 or fc["fga"] <= 0 or not 0 <= fc["fg3a"] <= fc["fga"] or fc["fta"] < 0:
        return {**out, "status": "skipped", "reason": "invalid_base_inputs"}
    if abs(base_points(fc, rates) - stored) > ARITHMETIC_TOLERANCE:
        return {**out, "status": "skipped", "reason": "base_arithmetic_mismatch", "stored": stored, "recomputed": base_points(fc, rates)}
    window, errors = recent_window(c.get("history") or [], slate_date)
    if len(window) < WINDOW_MIN:
        return {**out, "status": "skipped", "reason": "insufficient_history", "data_errors": errors}
    M = sum(float(h["minutes"]) for h in window)
    A = sum(float(h["fga"]) for h in window)
    T = sum(float(h["fg3a"]) for h in window)
    L = sum(float(h["fta"]) for h in window)
    if M <= 0 or A <= 0:
        return {**out, "status": "skipped", "reason": "nonpositive_recent_minutes_or_attempts", "data_errors": errors}
    teams = {h.get("team") for h in window}
    entry = (roster or {}).get(c.get("player_key"))
    if not entry:
        team_state = "team_identity_unverified"
    elif datetime.fromisoformat(entry["observed_at"]) > datetime.fromisoformat(freeze_at):
        team_state = "roster_observed_after_freeze"
    elif len({team_key(x) for x in teams}) != 1 or team_key(entry.get("team")) not in {team_key(x) for x in teams}:
        team_state = "team_changed"
    else:
        team_state = "verified"
    M0, A0, T0, L0 = fc["minutes"], fc["fga"], fc["fg3a"], fc["fta"]
    fga = M0 * (WEIGHT * A0 / M0 + (1 - WEIGHT) * A / M)
    fta = M0 * (WEIGHT * L0 / M0 + (1 - WEIGHT) * L / M)
    share = WEIGHT * T0 / A0 + (1 - WEIGHT) * T / A
    fg3a = fga * share
    if not (0 <= share <= 1 and 0 <= fg3a <= fga):
        return {**out, "status": "skipped", "reason": "invalid_candidate"}
    cand = {"minutes": M0, "fga": fga, "fg3a": fg3a, "fta": fta}
    calc = {**out, "original": {**fc, "points": stored}, "candidate": {**cand, "points": base_points(cand, rates)},
            "recent_window": {"games": len(window), "minutes": M, "fga": A, "fg3a": T, "fta": L},
            "recent_rate_baseline": {"fga": M0 * A / M, "fg3a": M0 * T / M, "fta": M0 * L / M},
            "data_errors": errors, "team_identity": team_state}
    if team_state != "verified":
        return {**calc, "status": "skipped", "reason": team_state}
    return {**calc, "status": "eligible" if base_model == REGISTERED_BASE else "eligible_unregistered_base"}


def challenge_snapshot(snapshot: dict[str, Any], *, base_model: str, roster: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    freeze_at = str(snapshot.get("created_at"))
    rows = [challenge(c, str(snapshot["game_date"]), base_model=base_model, freeze_at=freeze_at, roster=roster)
            for c in snapshot.get("candidates") or []]
    counts: dict[str, int] = {}
    for r in rows:
        k = r["status"] if r["status"] != "skipped" else "skipped:" + r["reason"]
        counts[k] = counts.get(k, 0) + 1
    return {"version": VERSION, "base_model": base_model, "game_date": snapshot["game_date"], "weight": WEIGHT,
            "activated": False, "activation_blockers": list(ACTIVATION_BLOCKERS), "counts": counts, "players": rows}
