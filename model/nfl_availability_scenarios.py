"""Availability-conditioned opportunity shares, separate from production.

Condition workload, not fantasy points or efficiency. Historical absences are
training labels from completed games; current absence evidence must be frozen
before kickoff. Unknown participants retain an explicit unallocated bucket.
Conditional limited/inactive states have no invented probability weights.
"""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from math import isfinite

VERSION = "nfl-availability-workload-v1"
PRIOR_GAMES = 8
MIN_HISTORY = 6
MIN_MATCHES = 3
ROLES = {"RB1", "RB_COMMITTEE", "WR_STARTER", "WR_ROTATION", "TE1", "TE_ROTATION"}
STATES = {"ACTIVE_CONFIRMED", "EXPECTED_ACTIVE", "QUESTIONABLE", "DOUBTFUL", "OUT_CONFIRMED",
          "CONFLICT", "STALE", "UNKNOWN"}


def stamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Evidence times require explicit timezones")
    return result.astimezone(timezone.utc)


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and value >= 0


def evidence_index(forecasts, evidence, decision_at):
    decision = stamp(decision_at)
    forecast_keys=[(f['game_id'],f['team']) for f in forecasts]
    if len(forecast_keys)!=len(set(forecast_keys)) or any(len({p['identity'] for p in f['players']})!=len(f['players']) for f in forecasts):
        raise ValueError('Duplicate forecast team/game or player identity')
    teams = {(f["game_id"], f["team"]): {p["identity"] for p in f["players"]} for f in forecasts}
    result = {}
    for row in evidence:
        key = (row["game_id"], row["team"], row["identity"])
        if key in result:
            raise ValueError("Duplicate availability decision")
        if row["identity"] not in teams.get(key[:2], set()):
            raise ValueError("Availability identity/team/game does not match the forecast")
        if row["state"] not in STATES or not row.get("observation_ids"):
            raise ValueError("A resolved state with observation provenance is required")
        if not stamp(row["available_at"]) <= decision < stamp(row["kickoff"]):
            raise ValueError("Availability evidence violates the pregame cutoff")
        if decision - stamp(row["available_at"]) > timedelta(hours=72):
            raise ValueError("Availability evidence is stale")
        result[key] = row
    return result


def condition_workloads(forecasts, evidence, history, decision_at, *, workload_factors=None):
    """Estimate conditional target/carry shares from earlier same-team games.

    Matching histories require every current donor to have been inactive; no
    PBP observation from the target game enters this computation. With multiple
    absences, independent boosts are never summed. Unsupported shares are left
    in the residual rather than handed to an arbitrary reserve player.
    """
    result = deepcopy(forecasts)
    decisions = evidence_index(result, evidence, decision_at)
    cutoff = stamp(decision_at)
    factors = workload_factors or {}
    if any(not number(v) or v > 1 for v in factors.values()):
        raise ValueError("Conditional workload factors must be between zero and one")
    known_ids = {p["identity"] for f in result for p in f["players"]}
    if set(factors) - known_ids:
        raise ValueError("Conditional workload identity is not in this slate")
    seen = set()
    for row in history:
        key = (row["game_id"], row["team"], row["identity"], row["field"])
        if key in seen:
            raise ValueError("Duplicate historical opportunity row")
        seen.add(key)
        if row["field"] not in ("targets", "carries") or not number(row["opportunities"]) or not number(row["team_budget"]) or row["opportunities"] > row["team_budget"]:
            raise ValueError("Historical opportunities do not reconcile to the team budget")
        if stamp(row["available_at"]) >= cutoff or any(row["game_id"] == f["game_id"] for f in result):
            raise ValueError("Current/future outcomes cannot be workload features")
        if not isinstance(row.get("inactive_ids"), list) or row.get('absence_coverage_complete') is not True or not row.get('source_snapshot_id'):
            raise ValueError("Historical absence coverage is missing")
    audit = []
    for forecast in result:
        game, team = forecast["game_id"], forecast["team"]
        current = {p["identity"]: decisions.get((game, team, p["identity"])) for p in forecast["players"]}
        inactive = {key for key, row in current.items() if row and row["state"] == "OUT_CONFIRMED"}
        zero = inactive | {key for key, value in factors.items() if value == 0 and key in current}
        for field in ("targets", "carries"):
            budget = forecast["budgets"].get(field)
            if not budget or not number(budget.get("mean")):
                raise ValueError("A finite team opportunity budget is required")
            candidates = [p for p in forecast["players"] if field in p.get("components", {})]
            shares = {p["identity"]: p["components"][field].get("share") for p in candidates}
            if any(not number(s) or s > 1 for s in shares.values()) or sum(shares.values()) > 1 + 1e-8:
                raise ValueError("Baseline shares exceed the team opportunity budget")
            donors = {p['identity'] for p in candidates if p['identity'] in zero and shares[p['identity']] > 0
                      and (field == 'targets' or p['position'] == 'RB')}
            # An ambiguous active role cannot justify allocating missing work.
            coverage_ok = all(current[p["identity"]] and current[p["identity"]]["state"] in
                              {"ACTIVE_CONFIRMED", "EXPECTED_ACTIVE", "QUESTIONABLE", "DOUBTFUL", "OUT_CONFIRMED"}
                              for p in candidates)
            adjustments = []
            proposed = dict(shares)
            for player in candidates:
                key = player["identity"]
                row = current[key]
                if key in zero:
                    proposed[key] = 0.0
                    adjustments.append({"identity": key, "status": "conditional_inactive", "matches": 0})
                    continue
                past = [r for r in history if r["identity"] == key and r["team"] == team and r["field"] == field and r["team_budget"] > 0]
                matched = [r for r in past if donors and donors.issubset(set(r["inactive_ids"]))]
                recipient = row and row.get("expected_role") in ROLES and (field == "targets" or player["position"] == "RB")
                status = "baseline_retained"
                if coverage_ok and recipient and len(past) >= MIN_HISTORY and len(matched) >= MIN_MATCHES:
                    matching_share = sum(r["opportunities"] / r["team_budget"] for r in matched) / len(matched)
                    weight = len(matched) / (len(matched) + PRIOR_GAMES)
                    proposed[key] = weight * matching_share + (1 - weight) * shares[key]
                    status = "same_team_absence_conditioned"
                factor = factors.get(key, 1)
                proposed[key] *= factor
                adjustments.append({"identity": key, "status": status, "history_games": len(past),
                                    "matches": len(matched), "workload_factor": factor})
            # Preserve the unknown baseline role bucket, not merely a <=100% sum.
            maximum_known = sum(shares.values())
            fixed = {p['identity'] for p in candidates if field == 'carries' and p['position'] != 'RB'}
            fixed_total = sum(proposed[key] for key in fixed)
            total = sum(value for key, value in proposed.items() if key not in fixed)
            scale = min(1, max(0, maximum_known-fixed_total) / total) if total > 0 else 1
            for player in candidates:
                key = player["identity"]
                component = player["components"][field]
                share = proposed[key] * (1 if key in fixed else scale)
                component.update(share=share, mean=budget["mean"] * share)
            known = fixed_total + total * scale
            budget.update(allocated_share=known, unallocated_share=max(0, 1 - known))
            audit.append({"game_id": game, "team": team, "field": field, "donors": sorted(donors),
                          "role_coverage_complete": coverage_ok, "baseline_allocated_share": maximum_known,
                          "allocated_share": known, "unallocated_share": max(0, 1 - known),
                          "normalization": scale, "players": adjustments})
        # OUT players must not retain attempts, rushing or rare-event simulation.
        for player in forecast["players"]:
            if player["identity"] in zero:
                player["components"] = {}
            elif player["identity"] in factors and "attempts" in player.get("components", {}):
                component = player["components"]["attempts"]
                component["share"] *= factors[player["identity"]]
                component["mean"] = forecast["budgets"]["attempts"]["mean"] * component["share"]
        if forecast["budgets"].get("attempts"):
            known = sum(p.get("components", {}).get("attempts", {}).get("share", 0) for p in forecast["players"])
            if known > 1 + 1e-8:
                raise ValueError("Passing attempts exceed the team budget")
            forecast["budgets"]["attempts"].update(allocated_share=known,unallocated_share=max(0,1-known))
    return {"version": VERSION, "forecasts": result, "audit": audit,
            "probability": None, "authority": "shadow_only", "production_changed": False,
            "efficiency_policy": "Recipient efficiency is unchanged; only opportunity shares are conditioned."}


def workload_scenarios(forecasts, evidence, history, decision_at, *, limited_factors=None):
    """One-at-a-time sensitivities plus simultaneous inactive/limited stress.

    50% is a declared sensitivity, not a medical prediction. Normal workload
    remains conditional even when official evidence establishes ACTIVE.
    """
    decisions = evidence_index(forecasts, evidence, decision_at)
    uncertain = sorted(key[2] for key, row in decisions.items() if row["state"] in {"QUESTIONABLE", "DOUBTFUL"})
    learned=limited_factors or {}
    if set(learned)-set(uncertain) or any(not number(v) or v>=1 for v in learned.values()):
        raise ValueError('Limited workload factors require this slate\'s uncertain identities and values below one')
    if len(uncertain) != len(set(uncertain)):
        raise ValueError("A workload identity occurs in multiple games")
    result = [{"id": "normal_if_active", **condition_workloads(forecasts, evidence, history, decision_at)}]
    for identity in uncertain:
        for label, factor in (("limited", learned.get(identity,.5)), ("inactive", 0)):
            result.append({"id": f"{identity}:{label}", **condition_workloads(forecasts, evidence, history, decision_at,
                           workload_factors={identity: factor}), 'limited_factor_source':'learned_shadow' if identity in learned else 'declared_sensitivity'})
    if len(uncertain) > 1:
        for label, factor in (("limited", .5), ("inactive", 0)):
            result.append({"id": f"all_uncertain:{label}", **condition_workloads(forecasts, evidence, history, decision_at,
                           workload_factors={key:learned.get(key,factor) if label=='limited' else 0 for key in uncertain})})
    return result
