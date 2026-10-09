"""Shared workload DFS development candidate, based on pinned coherent v5.

Historical paired game budgets supply cross-team dependence. The existing
team simulator allocates opportunities and conditional offense. One event
ledger supplies both offensive and DST scoring. This changes marginals and
is NEVER a production-approved adapter or a claim of calibrated dependence.
"""
from __future__ import annotations
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
import math
from pathlib import Path
import numpy as np

from model.nfl_context_engine import stable_digest
from model.nfl_shared_dfs_efficiency import CONFIG, simulate_team
from model.nfl_dfs_historical import draftkings_points, BOOM_THRESHOLDS
from ingest.nfl_dfs_results import _score_dst

VERSION = "nfl-shared-matchup-research-v1"
OFFENSE_MAP = {"passing_yards": "passYds", "passing_tds": "passTds", "passing_interceptions": "interceptions",
               "rushing_yards": "rushYds", "rushing_tds": "rushTds", "receiving_yards": "recYds", "receiving_tds": "recTds",
               "receptions": "receptions", "fumbles_lost_total": "fumblesLost", "special_teams_tds": "returnTds", "fumble_recovery_tds": "offensiveFumbleRecoveryTds"}
REQUIRED_BLOCK_FIELDS = ("attempts", "carries", "targets", "sacks_suffered", "fumbles_lost_total", "passing_tds", "rushing_tds", "def_tds",
    "special_teams_tds", "pat_made", "passing_2pt_conversions", "rushing_2pt_conversions", "def_2pt_made", "def_safeties",
    "def_fg_blocks", "def_pat_blocks", "def_punt_blocks", "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49", "fg_made_50_59", "fg_made_60_")


def condition_starting_qbs(forecasts, depth_rows, decision_at):
    """Explicit normal-starter state, never an estimated availability mixture.

    Current fresh unique depth-one evidence sets passing attempts to the starter;
    backup rushing is left unallocated, not silently donated to another player.
    Missing/ambiguous evidence preserves the historical allocation and is audited.
    """
    decision = datetime.fromisoformat(decision_at.replace("Z", "+00:00"))
    audit = []
    for forecast in forecasts:
        players = [p for p in forecast["players"] if p["position"] == "QB"]
        current = []
        for row in depth_rows:
            captured = row.get("fetched_at")
            captured = datetime.fromisoformat(str(captured).replace("Z", "+00:00")) if captured else None
            if row["team"] == forecast["team"] and row.get("depth_order") == 1 and captured and 0 <= (decision - captured).total_seconds() <= 86400:
                current.append(row["identity"])
        eligible = [p for p in players if p["identity"] in current]
        if len(current) != 1 or len(eligible) != 1:
            audit.append({"team": forecast["team"], "state": "historical_allocation_unresolved_current_starter"})
            continue
        starter = eligible[0]
        for player in players:
            component = player["components"].setdefault("attempts", {})
            component.update(share=float(player is starter), raw_share=float(player is starter),
                             mean=forecast["budgets"]["attempts"]["mean"] * float(player is starter),
                             normalization="conditional_current_starter", games=component.get("games", 0))
            if player is not starter and "carries" in player["components"]:
                player["components"]["carries"].update(share=0.0, mean=0.0, normalization="conditional_backup_unallocated")
        forecast["budgets"]["attempts"].update(allocated_share=1.0, unallocated_share=0.0)
        audit.append({"team": forecast["team"], "state": "conditional_normal_starter", "identity": starter["identity"],
                      "availability_probability": None, "backup_carries": "unallocated"})
    return audit


def integer_allocate(total, weights):
    """Deterministic integer conservation with explicit final unallocated bucket."""
    total = int(total)
    if total < 0 or any(not math.isfinite(float(w)) or w < 0 for w in weights):
        raise ValueError("invalid integer allocation")
    if not weights:
        raise ValueError("allocation needs an explicit unallocated bucket")
    if sum(weights) <= 0:
        return [0] * (len(weights) - 1) + [total]
    exact = np.array(weights, dtype=float) / sum(weights) * total
    counts = np.floor(exact).astype(int)
    for index in sorted(range(len(weights)), key=lambda i: (-(exact[i] - counts[i]), i))[:total - int(counts.sum())]:
        counts[index] += 1
    return counts.tolist()


def eligible_blocks(team_rows, cutoff):
    grouped = defaultdict(list)
    rejected = []
    for row in team_rows:
        if (int(row["season"]), int(row["week"])) >= cutoff:
            continue
        stats = row.get("stats") or {}
        if not row.get("game_id") or any(type(stats.get(k)) not in (float, int) or not math.isfinite(stats[k]) or stats[k] < 0 or int(stats[k]) != stats[k] for k in REQUIRED_BLOCK_FIELDS):
            rejected.append(row.get("game_id"))
            continue
        grouped[row["game_id"]].append(row)
    blocks = []
    for game_id, rows in sorted(grouped.items()):
        if len(rows) == 2 and rows[0]["team"] == rows[1]["opponent"] and rows[1]["team"] == rows[0]["opponent"]:
            blocks.append(sorted(rows, key=lambda row: row["team"]))
        else:
            rejected.append(game_id)
    if len(blocks) < 100:
        raise ValueError(f"paired historical game coverage too small: {len(blocks)}")
    return blocks, sorted({str(v) for v in rejected})


def _offense_stats(stats):
    mapped = {target: int(round(stats.get(source, 0))) for source, target in OFFENSE_MAP.items()}
    mapped["twoPointConversions"] = int(sum(stats.get(k, 0) for k in ("passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions")))
    return mapped


def _event_allocate(players, count, field, weight_field):
    ids = sorted(players)
    weights = [max(0, players[i].get(weight_field, 0)) for i in ids] + [0]
    counts = integer_allocate(count, weights)
    for identity, number in zip(ids, counts):
        players[identity][field] = number
    return counts[-1]


def build_coherent_banks(*, slate, forecasts, history, team_rows, identities, baseline_means,
                         source_manifest, decision_at, seed=20260927, draws=400, rate_factors=None, kicker_roles=None, role_dispersion_evidence=None,
                         replacement_roles=None, replacement_evidence=None, joint_exit_fit=None, joint_gain_profiles=None):
    if slate.get("format") not in ("classic", "showdown"):
        raise ValueError("unsupported slate format")
    if slate["format"] == "showdown" and len({f["game_id"] for f in forecasts}) != 1:
        raise ValueError("Showdown requires exactly one paired game")
    role_concentrations = {}
    if joint_exit_fit:
        from model.nfl_longest_touchdown import timestamp
        if not joint_exit_fit.get('exit_fit') or not joint_exit_fit.get('role_excluded_game_ids') or timestamp(joint_exit_fit['training_cutoff']) > timestamp(decision_at):
            raise ValueError('Complete exits require prior fitted conditional non-exit roles')
    if replacement_roles:
        from model.nfl_longest_touchdown import timestamp
        evidence = replacement_evidence or {}
        if not evidence.get('source_ref') or not evidence.get('description') or not evidence.get('captured_at') or timestamp(evidence['captured_at']) > timestamp(decision_at):
            raise ValueError('Replacement roles require timestamped scenario evidence')
        forecasts = deepcopy(forecasts)
        if set(replacement_roles) - {f['team'] for f in forecasts}:
            raise ValueError('Replacement role team outside supplied games')
        for forecast in forecasts:
            for action, shares in replacement_roles.get(forecast['team'], {}).items():
                if action not in ('carries', 'targets', 'attempts'):
                    raise ValueError('Unsupported replacement action')
                eligible = [p for p in forecast['players'] if action in p.get('components', {})]
                if set(shares) != {p['identity'] for p in eligible} | {'OTHER'} or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in shares.values()) or abs(sum(shares.values()) - 1) > 1e-8:
                    raise ValueError('Replacement must cover complete field including OTHER and conserve shares')
                for player in eligible:
                    player['components'][action].update(share=shares[player['identity']],
                        normalization='documented_replacement_scenario')
    if role_dispersion_evidence is not None:
        from model.nfl_longest_touchdown import timestamp
        evidence = role_dispersion_evidence
        if not evidence.get('source_ref') or not evidence.get('training_decision_at') or timestamp(evidence['training_decision_at']) > timestamp(decision_at):
            raise ValueError('Invalid role dispersion evidence boundary')
        values = evidence.get('actions', {})
        if set(values) != {'carries', 'targets'}:
            raise ValueError('Role evidence must cover carries and targets')
        for action, report in values.items():
            k = report.get('concentration')
            if type(k) not in (int, float) or not math.isfinite(k) or k <= 0 or not report.get('method'):
                raise ValueError('Invalid fitted role concentration')
            role_concentrations[action] = k
    candidate_version = VERSION + '-shared-roles' if role_dispersion_evidence is not None else VERSION
    if joint_exit_fit:
        candidate_version += '-exits'
    if joint_gain_profiles is not None:
        candidate_version += '-empirical-gains'
    kicker_roles = kicker_roles or {}
    for team, identity in kicker_roles.items():
        matching = [p for p in slate["players"] if p["dkPlayerId"] == identities.get(identity) and p["position"] == "K" and p.get("teamAbbrev") == team]
        if len(matching) != 1 or slate["format"] != "showdown":
            raise ValueError("Kicker allocation requires one exact Showdown roster identity per supplied role")
    if draws < 100 or draws > 3000:
        raise ValueError("research banks support 100--3000 draws")
    cutoff = min((int(f["season"]), int(f["week"])) for f in forecasts)
    blocks, rejected = eligible_blocks(team_rows, cutoff)
    all_stats = [row["stats"] for block in blocks for row in block]
    budget_means = {key: float(np.mean([r[key] for r in all_stats])) for key in ("attempts", "carries", "targets")}
    total_tds = sum(r["passing_tds"] + r["rushing_tds"] + r["def_tds"] + r["special_teams_tds"] for r in all_stats)
    conversion_rates = np.array([sum(r[key] for r in all_stats) / total_tds for key in ("pat_made", "passing_2pt_conversions", "rushing_2pt_conversions", "def_2pt_made")])
    if total_tds <= 0 or sum(conversion_rates) > 1 + 1e-9:
        raise ValueError("historical conversion denominator does not reconcile")
    conversion_rates = np.append(conversion_rates, max(0, 1 - sum(conversion_rates)))
    by_player, by_position = defaultdict(list), defaultdict(list)
    for row in history:
        if (int(row["season"]), int(row["week"])) < cutoff:
            by_player[str(row["identity"])].append(row)
            by_position[row["position"]].append(row)
    grouped = defaultdict(list)
    for forecast in forecasts:
        grouped[str(forecast["game_id"])].append(forecast)
    if any(len(rows) != 2 for rows in grouped.values()):
        raise ValueError("every target game needs both team forecasts")
    manifests = {"version": VERSION, "sources": source_manifest, "draws": draws, "seed": seed,
                 "history_games": len(blocks), "rejected_game_ids": rejected, "budget_means": budget_means,
                 "conversion_rates": conversion_rates.tolist(), "rate_factors": rate_factors or {},
                 "research_registration": "local-shared-game-development-not-registered-holdout", "kicker_roles": kicker_roles, "authority": "shadow_only"}
    if role_dispersion_evidence is not None:
        manifests['version'] = candidate_version
        manifests['role_dispersion_evidence'] = role_dispersion_evidence
        manifests['research_registration'] = 'local-shared-roles-development-not-registered-holdout'
    if replacement_roles:
        manifests['replacement_roles'] = replacement_roles
        manifests['replacement_evidence'] = replacement_evidence
    if joint_exit_fit:
        manifests['joint_exit_fit_sha256'] = joint_exit_fit['fit_sha256']
    if joint_gain_profiles is not None:
        manifests['joint_gain_profiles_sha256'] = stable_digest(joint_gain_profiles)
    manifests['implementation_hashes'] = {name: sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in ('nfl_shared_matchup_scenarios.py', 'nfl_shared_dfs_efficiency.py', 'nfl_role_dispersion.py')}
    snapshot = stable_digest({"manifest": manifests, "slate": slate, "forecasts": forecasts, "identities": identities, "decision": decision_at})
    banks, diagnostics = [], []
    common_supported = None
    for stream_index, stream in enumerate(("selection", "evaluation")):
        stream_seed = int(seed) ^ (0 if stream_index == 0 else 0xA5A5A5A5)
        output = [{"id": f"{snapshot}:{stream}:{i}", "weight": 1, "stats": {}} for i in range(draws)]
        ledgers = [[] for _ in range(draws)]
        model_sums, modeled_ids = defaultdict(list), set()
        for game_id, game_forecasts in sorted(grouped.items()):
            game_forecasts = sorted(game_forecasts, key=lambda f: f["team"])
            game_seed = int(sha256(f"{stream_seed}:{game_id}".encode()).hexdigest()[:16], 16)
            rng = np.random.default_rng(game_seed)
            samples = []
            for _ in range(draws):
                selected = blocks[int(rng.integers(len(blocks)))]
                samples.append(selected if rng.integers(2) == 0 else selected[::-1])
            team_draws = []
            exit_diagnostics = []
            for side, forecast in enumerate(game_forecasts):
                budget = []
                for block in samples:
                    budget.append({key: max(0, int(round(block[side]["stats"][key] * forecast["budgets"][key]["mean"] / budget_means[key]))) for key in budget_means})
                allocator = None
                if joint_exit_fit:
                    from model.nfl_full_exit_allocation import exit_allocator
                    allocator, exit_report = exit_allocator(forecast, joint_exit_fit, draws, game_seed ^ side, decision_at)
                    exit_diagnostics.append(exit_report)
                _, coherence = simulate_team(forecast, by_player, by_position, {**CONFIG, "seed": stream_seed, "draws": draws},
                                             retain_draws=True, budget_draws=budget, rate_factors=rate_factors, strict_game_ledger=True, role_concentrations=role_concentrations,
                                             opportunity_allocator=allocator, joint_gain_profiles=joint_gain_profiles)
                team_draws.append(coherence["retained_draws"])
            for index in range(draws):
                events = []
                for side, forecast in enumerate(game_forecasts):
                    draw = deepcopy(team_draws[side][index])
                    players = draw["players"]
                    sample = samples[index][side]["stats"]
                    qb_ids = sorted(p["identity"] for p in forecast["players"] if p["position"] == "QB" and p["identity"] in players)
                    for p in players.values():
                        for key in ("passing_yards", "rushing_yards"):
                            p[key] = int(round(p.get(key, 0)))
                        for key in ("passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"):
                            p[key] = 0
                    pass_yards = sum(players[p].get("passing_yards", 0) for p in qb_ids)
                    receivers = sorted(p for p in players if "receiving_yards" in players[p])
                    if joint_gain_profiles is None:
                        yard_parts = integer_allocate(pass_yards, [max(0, players[p].get("receiving_yards", 0)) for p in receivers] + [max(0, draw["unallocated"]["receiving_yards"])])
                        for identity, value in zip(receivers, yard_parts):
                            players[identity]["receiving_yards"] = value
                        draw["unallocated"]["receiving_yards"] = yard_parts[-1]
                    unknown_fumbles = _event_allocate(players, sample["fumbles_lost_total"], "fumbles_lost_total", "fumbles_lost_total")
                    unknown_returns = _event_allocate(players, sample["special_teams_tds"], "special_teams_tds", "special_teams_tds")
                    touchdowns = sum(players[p].get("passing_tds", 0) for p in qb_ids) + sum(p.get("rushing_tds", 0) + p.get("fumble_recovery_tds", 0) for p in players.values())
                    events.append({"team": forecast["team"], "players": players, "qb_ids": qb_ids, "opportunities": draw["opportunities"],
                                   "midgame_exit_allocation": exit_diagnostics[side] if joint_exit_fit and index == 0 else None,
                                   "participant_opportunities": draw["participant_opportunities"], "supplied_budgets": draw["supplied_budgets"], "budget_reconciliation": draw["budget_reconciliation"],
                                   "unallocated": {**draw["unallocated"], "fumbles_lost": unknown_fumbles, "return_tds": unknown_returns},
                                   "passing_yards": pass_yards, "offensive_tds": int(touchdowns), "interceptions_thrown": sum(players[p].get("passing_interceptions", 0) for p in qb_ids),
                                   "fumbles_lost": sample["fumbles_lost_total"], "sacks_suffered": sample["sacks_suffered"],
                                   "special_teams_tds": sample["special_teams_tds"], "safeties": sample["def_safeties"],
                                   "blocked_kicks": sum(sample[k] for k in ("def_fg_blocks", "def_pat_blocks", "def_punt_blocks")),
                                   "empirical_defensive_tds": sample["def_tds"], "fg_short": sum(sample[k] for k in ("fg_made_0_19", "fg_made_20_29", "fg_made_30_39")),
                                   "fg_medium": sample["fg_made_40_49"], "fg_long": sample["fg_made_50_59"] + sample["fg_made_60_"],
                                   "historical_block": samples[index][side]["game_id"]})
                for side, event in enumerate(events):
                    opposing = events[1 - side]
                    event["defensive_tds"] = min(event["empirical_defensive_tds"], opposing["interceptions_thrown"] + opposing["fumbles_lost"] + event["blocked_kicks"])
                    total_td = event["offensive_tds"] + event["defensive_tds"] + event["special_teams_tds"]
                    xp, pass_two, rush_two, defended_two, failed = map(int, rng.multinomial(total_td, conversion_rates))
                    event.update(pat_made=xp, passing_two=pass_two, rushing_two=rush_two, conversion_return_allowed=defended_two, failed_conversion=failed)
                    players = event["players"]
                    event["unallocated"]["passing_two"] = _event_allocate(players, pass_two, "passing_2pt_conversions", "passing_tds")
                    event["unallocated"]["receiving_two"] = _event_allocate(players, pass_two, "receiving_2pt_conversions", "receptions")
                    event["unallocated"]["rushing_two"] = _event_allocate(players, rush_two, "rushing_2pt_conversions", "rushing_tds")
                for side, event in enumerate(events):
                    opposing = events[1 - side]
                    event["two_point_returns"] = opposing["conversion_return_allowed"]
                    event["final_points"] = int(6 * (event["offensive_tds"] + event["defensive_tds"] + event["special_teams_tds"]) + event["pat_made"]
                        + 2 * (event["passing_two"] + event["rushing_two"] + event["safeties"] + event["two_point_returns"]) + 3 * (event["fg_short"] + event["fg_medium"] + event["fg_long"]))
                for side, event in enumerate(events):
                    opposing = events[1 - side]
                    kicker_identity = kicker_roles.get(event["team"])
                    kicker_id = identities.get(kicker_identity)
                    kicking = {"extraPointsMade": event["pat_made"], "fgMade0to39": event["fg_short"],
                               "fgMade40to49": event["fg_medium"], "fgMade50Plus": event["fg_long"]}
                    event["kicker_allocation"] = {"identity": kicker_identity, "dkPlayerId": kicker_id,
                        "status": "conditional_allocated" if kicker_id is not None else "unallocated_role", "events": kicking}
                    if kicker_id is not None:
                        output[index]["stats"][str(kicker_id)] = kicking
                        model_sums[str(kicker_id)].append(draftkings_points("K", {"pat_made": event["pat_made"],
                            "fg_made_0_19": event["fg_short"], "fg_made_40_49": event["fg_medium"], "fg_made_50_59": event["fg_long"]}))
                        modeled_ids.add(int(kicker_id))
                    for identity, stats in event["players"].items():
                        dk_id = identities.get(identity)
                        if dk_id is None:
                            continue
                        output[index]["stats"][str(dk_id)] = _offense_stats(stats)
                        position = next(p["position"] for p in game_forecasts[side]["players"] if p["identity"] == identity)
                        model_sums[str(dk_id)].append(draftkings_points(position, stats))
                        modeled_ids.add(int(dk_id))
                    dst_id = identities.get("DST:" + event["team"])
                    if dst_id is not None:
                        raw = {"def_sacks": opposing["sacks_suffered"], "def_interceptions": opposing["interceptions_thrown"],
                               "fumble_recovery_opp": opposing["fumbles_lost"], "def_safeties": event["safeties"], "def_tds": event["defensive_tds"],
                               "special_teams_tds": event["special_teams_tds"], "def_fg_blocks": event["blocked_kicks"], "def_pat_blocks": 0, "def_punt_blocks": 0,
                               "def_2pt_made": event["two_point_returns"]}
                        dst = _score_dst({"raw_team_stats": raw}, {"opponent_final_points": opposing["final_points"], "opponent_raw_team_stats": {
                            "def_tds": opposing["defensive_tds"], "def_safeties": opposing["safeties"], "def_2pt_made": opposing["two_point_returns"]}})
                        components = dst.evidence["scoring_components"]
                        output[index]["stats"][str(dst_id)] = {"sacks": int(raw["def_sacks"]), "dstInterceptions": int(raw["def_interceptions"]), "fumbleRecoveries": int(raw["fumble_recovery_opp"]),
                            "safeties": int(raw["def_safeties"]), "blockedKicks": int(event["blocked_kicks"]), "dstTds": int(event["defensive_tds"] + event["special_teams_tds"]),
                            "twoPointReturns": int(event["two_point_returns"]), "pointsAllowed": int(components["dk_points_allowed"])}
                        model_sums[str(dst_id)].append(dst.actual_dk_fpts)
                        modeled_ids.add(int(dst_id))
                    event["dropbacks"] = event["opportunities"]["attempts"] + event["sacks_suffered"]
                    event["kicks_blocked_against"] = opposing["blocked_kicks"]
                    counts = event["opportunities"]
                    participants = event["participant_opportunities"]
                    if not 0 <= counts["completions"] <= counts["targets"] <= counts["attempts"]:
                        raise AssertionError("completion/target/attempt budget mismatch")
                    for field in ("attempts", "carries", "targets"):
                        if sum(row.get(field, 0) for row in participants.values()) + event["unallocated"][field] != counts[field]:
                            raise AssertionError(f"{field} participant reconciliation failed")
                    if sum(row.get("completions", 0) for row in participants.values()) != counts["completions"]:
                        raise AssertionError("QB completion accounting failed")
                    if sum(p.get("receptions", 0) for p in event["players"].values()) + event["unallocated"]["receptions"] != counts["completions"]:
                        raise AssertionError("reception accounting failed")
                    if event["unallocated"]["receptions"] > event["unallocated"]["targets"] or any(p.get("receptions", 0) > participants[key].get("targets", 0) for key, p in event["players"].items()):
                        raise AssertionError("receptions exceeded eligible targets")
                    # Core football identities must hold before a single bank is delivered.
                    if event["passing_yards"] != sum(p.get("receiving_yards", 0) for p in event["players"].values()) + event["unallocated"]["receiving_yards"]:
                        raise AssertionError("passing/receiving yard ledger mismatch")
                    if sum(event["players"][p].get("passing_tds", 0) for p in event["qb_ids"]) != sum(p.get("receiving_tds", 0) for p in event["players"].values()) + event["unallocated"]["receiving_tds"]:
                        raise AssertionError("passing/receiving touchdown ledger mismatch")
                ledgers[index].append({"game_id": game_id, "teams": events})
        supported = modeled_ids & {p["dkPlayerId"] for p in slate["players"]}
        common_supported = supported if common_supported is None else common_supported & supported
        diagnostics.append({"stream": stream, "player_marginals": [{"dkPlayerId": int(key), "researchMean": float(np.mean(values)),
                            "researchP10": float(np.quantile(values, .1)), "researchP25": float(np.quantile(values, .25)), "researchP50": float(np.quantile(values, .5)),
                            "researchP75": float(np.quantile(values, .75)), "researchP90": float(np.quantile(values, .9)),
                            "boomThreshold": BOOM_THRESHOLDS[next(p["position"] for p in slate["players"] if p["dkPlayerId"] == int(key))],
                            "boomProbability": float(np.mean(np.array(values) >= BOOM_THRESHOLDS[next(p["position"] for p in slate["players"] if p["dkPlayerId"] == int(key))])),
                            "baselineMean": baseline_means.get(key), "delta": float(np.mean(values)) - baseline_means[key] if baseline_means.get(key) is not None else None} for key, values in sorted(model_sums.items())],
                            "event_ledgers": ledgers})
        banks.append({"schemaVersion": 1, "runId": stable_digest({"snapshot": snapshot, "stream": stream, "seed": stream_seed}), "modelVersion": candidate_version,
                      "snapshotId": snapshot, "decisionAt": decision_at, "inputsCapturedAt": source_manifest.get("captured_at", decision_at), "source": "model", "sampling": "iid", "seed": stream_seed,
                      "streamId": f"{snapshot}:{stream}", "scenarios": output})
    supported_slate = {**slate, "players": [p for p in slate["players"] if p["dkPlayerId"] in common_supported]}
    for bank in banks:
        for draw in bank["scenarios"]:
            draw["stats"] = {key: value for key, value in draw["stats"].items() if int(key) in common_supported}
            if len(draw["stats"]) != len(common_supported):
                raise AssertionError("unsupported zero-fill would be required")
    return {"version": candidate_version, "authority": "shadow_only", "productionChanged": False, "status": "coherent_research_unqualified",
            "slate": supported_slate, "selection": banks[0], "evaluation": banks[1], "manifest": manifests, "diagnostics": diagnostics,
            "coverage": {"inputPlayers": len(slate["players"]), "modeledPlayers": len(common_supported), "excludedPlayerIds": sorted({p["dkPlayerId"] for p in slate["players"]} - common_supported)},
            "limitations": ["Separately named coherent research model; player and DST marginals change and are audited, not presented as preserved.",
                "Paired historical-game budgets provide empirical cross-team dependence; this generator has not passed forward calibration or portfolio promotion.",
                "Current-source historical reconstruction and current-roster assumptions are research-only. Unknown allocations remain explicit.",
                "Sacks add to modeled attempts to define dropbacks; explicit scramble decomposition and sequential possession simulation are not supplied.",
                "Kicker events are allocated only to explicit unique Showdown role identities; ambiguous roles remain unallocated and those K players are excluded. Captain salary and 1.5 scoring are applied by the canonical lineup scorer, never by the underlying player draws."]}
