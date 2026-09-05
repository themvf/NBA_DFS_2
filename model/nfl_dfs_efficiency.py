"""Conditional NFL efficiency and exact DraftKings scoring research candidate."""
from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
from math import isfinite, sqrt
from typing import Any, Mapping, Sequence

import numpy as np

from model.nfl_dfs_historical import BOOM_THRESHOLDS, draftkings_points


VERSION = "nfl-dfs-efficiency-v3"
CONFIG = {
    "half_life_games": 8.0,
    "max_player_games": 34,
    "max_prior_rows": 1000,
    "draws": 1000,
    "seed": 20260904,
}

RATE_DEFS = {
    "completion_rate": {"label": "Completion rate", "numerator": "completions", "denominator": "attempts", "positions": ("QB",), "prior_opportunities": 100.0, "probability": True},
    "passing_yards_per_completion": {"label": "Passing yards / completion", "numerator": "passing_yards", "denominator": "completions", "positions": ("QB",), "prior_opportunities": 80.0},
    "passing_td_rate": {"label": "Passing TD / completion", "numerator": "passing_tds", "denominator": "completions", "positions": ("QB",), "prior_opportunities": 200.0, "probability": True},
    "interception_rate": {"label": "Interception / attempt", "numerator": "passing_interceptions", "denominator": "attempts", "positions": ("QB",), "prior_opportunities": 200.0, "probability": True},
    "rushing_yards_per_carry": {"label": "Rushing yards / carry", "numerator": "rushing_yards", "denominator": "carries", "positions": ("QB", "RB", "WR", "TE"), "prior_opportunities": 50.0},
    "rushing_td_rate": {"label": "Rushing TD / carry", "numerator": "rushing_tds", "denominator": "carries", "positions": ("QB", "RB", "WR", "TE"), "prior_opportunities": 150.0, "probability": True},
    "catch_rate": {"label": "Catch rate", "numerator": "receptions", "denominator": "targets", "positions": ("RB", "WR", "TE"), "prior_opportunities": 100.0, "probability": True},
    "receiving_yards_per_reception": {"label": "Receiving yards / reception", "numerator": "receiving_yards", "denominator": "receptions", "positions": ("RB", "WR", "TE"), "prior_opportunities": 80.0},
    "receiving_td_rate": {"label": "Receiving TD / reception", "numerator": "receiving_tds", "denominator": "receptions", "positions": ("RB", "WR", "TE"), "prior_opportunities": 200.0, "probability": True},
}

RARE_FIELDS = {
    "passing_2pt_conversions": ("QB",),
    "rushing_2pt_conversions": ("QB", "RB", "WR", "TE"),
    "receiving_2pt_conversions": ("RB", "WR", "TE"),
    "special_teams_tds": ("QB", "RB", "WR", "TE"),
    "fumble_recovery_tds": ("QB", "RB", "WR", "TE"),
    "fumbles_lost_total": ("QB", "RB", "WR", "TE"),
}

CORE_BACKTEST_RATES = tuple(RATE_DEFS)
DST_COMPONENTS = (
    "sacks", "interceptions", "fumble_recoveries", "safeties", "defensive_tds",
    "special_teams_return_tds", "blocked_kicks", "two_point_returns", "points_allowed_fpts",
)


def number(row: Mapping[str, Any], key: str, *, nonnegative: bool = True) -> float | None:
    value = (row.get("stats") or {}).get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    if not isfinite(result) or (nonnegative and result < 0):
        return None
    return result


def denominator(row: Mapping[str, Any], definition: str | tuple[str, ...] | None) -> float | None:
    if definition is None:
        return 1.0
    keys = (definition,) if isinstance(definition, str) else definition
    values = [number(row, key) for key in keys]
    return None if any(value is None for value in values) else float(sum(values))


def recency_weights(length: int, half_life: float) -> np.ndarray:
    ages = np.arange(length - 1, -1, -1, dtype=float)
    return np.power(0.5, ages / half_life)


def rate_totals(rows: Sequence[Mapping[str, Any]], spec: Mapping[str, Any], config: Mapping[str, Any] = CONFIG) -> tuple[float, float, int]:
    valid: list[tuple[float, float]] = []
    for row in sorted(rows, key=lambda item: (item["season"], item["week"])):
        numerator = number(row, str(spec["numerator"]), nonnegative=not str(spec["numerator"]).endswith("yards"))
        opportunities = denominator(row, spec["denominator"])
        if numerator is None or opportunities is None or opportunities <= 0:
            continue
        if spec.get("probability") and numerator > opportunities:
            continue
        valid.append((numerator, opportunities))
    valid = valid[-int(config["max_player_games"]):]
    if not valid:
        return 0.0, 0.0, 0
    weights = recency_weights(len(valid), float(config["half_life_games"]))
    return float(sum(w * item[0] for w, item in zip(weights, valid))), float(sum(w * item[1] for w, item in zip(weights, valid))), len(valid)


def estimate_rate(
    own_rows: Sequence[Mapping[str, Any]],
    prior_rows: Sequence[Mapping[str, Any]],
    rate_name: str,
    config: Mapping[str, Any] = CONFIG,
) -> dict[str, Any] | None:
    spec = RATE_DEFS[rate_name]
    own_num, own_den, own_games = rate_totals(own_rows, spec, config)
    prior_num, prior_den, prior_games = rate_totals(prior_rows[-int(config["max_prior_rows"]):], spec, {**config, "max_player_games": config["max_prior_rows"]})
    if prior_den <= 0:
        return None
    prior_rate = prior_num / prior_den
    prior_opportunities = float(spec["prior_opportunities"])
    mean = (own_num + prior_rate * prior_opportunities) / (own_den + prior_opportunities)
    if spec.get("probability"):
        mean = min(1.0, max(0.0, mean))
    return {
        "label": spec["label"],
        "mean": mean,
        "player_rate": own_num / own_den if own_den > 0 else None,
        "position_prior": prior_rate,
        "player_opportunities": own_den,
        "prior_equivalent_opportunities": prior_opportunities,
        "games": own_games,
        "prior_rows": prior_games,
        "numerator": spec["numerator"],
        "denominator": spec["denominator"],
    }


def per_game_mean(own_rows: Sequence[Mapping[str, Any]], prior_rows: Sequence[Mapping[str, Any]], field: str, prior_games: float = 12.0, config: Mapping[str, Any] = CONFIG) -> float:
    def total(rows: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
        rows = sorted(rows, key=lambda item: (item["season"], item["week"]))[-int(config["max_player_games"]):]
        values = [(number(row, field), row) for row in rows]
        values = [(value, row) for value, row in values if value is not None]
        if not values:
            return 0.0, 0.0
        weights = recency_weights(len(values), float(config["half_life_games"]))
        return float(sum(weight * value for weight, (value, _) in zip(weights, values))), float(weights.sum())

    own_total, own_weight = total(own_rows)
    prior_total, prior_weight = total(prior_rows[-int(config["max_prior_rows"]):])
    prior = prior_total / prior_weight if prior_weight else 0.0
    return (own_total + prior * prior_games) / (own_weight + prior_games)


def scoring_contributions(stats: Mapping[str, float]) -> dict[str, float]:
    passing_yards = float(stats.get("passing_yards", 0.0))
    rushing_yards = float(stats.get("rushing_yards", 0.0))
    receiving_yards = float(stats.get("receiving_yards", 0.0))
    return {
        "passing_yards": passing_yards / 25.0,
        "passing_tds": 4.0 * float(stats.get("passing_tds", 0.0)),
        "interceptions": -float(stats.get("passing_interceptions", 0.0)),
        "rushing_yards": rushing_yards / 10.0,
        "rushing_tds": 6.0 * float(stats.get("rushing_tds", 0.0)),
        "receiving_yards": receiving_yards / 10.0,
        "receiving_tds": 6.0 * float(stats.get("receiving_tds", 0.0)),
        "receptions": float(stats.get("receptions", 0.0)),
        "two_point_conversions": 2.0 * sum(float(stats.get(key, 0.0)) for key in ("passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions")),
        "misc_tds": 6.0 * sum(float(stats.get(key, 0.0)) for key in ("special_teams_tds", "fumble_recovery_tds")),
        "fumbles_lost": -float(stats.get("fumbles_lost_total", 0.0)),
        "yardage_bonuses": 3.0 * sum((passing_yards >= 300, rushing_yards >= 100, receiving_yards >= 100)),
    }


def dst_contributions(stats: Mapping[str, Any]) -> dict[str, float]:
    return {
        "sacks": float(stats.get("sacks", 0.0)),
        "interceptions": 2.0 * float(stats.get("interceptions", 0.0)),
        "fumble_recoveries": 2.0 * float(stats.get("fumble_recoveries", 0.0)),
        "safeties": 2.0 * float(stats.get("safeties", 0.0)),
        "defensive_tds": 6.0 * float(stats.get("defensive_tds", 0.0)),
        "special_teams_return_tds": 6.0 * float(stats.get("special_teams_return_tds", 0.0)),
        "blocked_kicks": 2.0 * float(stats.get("blocked_kicks", 0.0)),
        "two_point_returns": 2.0 * float(stats.get("two_point_returns", 0.0)),
        "points_allowed": float(stats.get("points_allowed_fpts", 0.0)),
    }


def _weighted(values: Sequence[float], half_life: float = CONFIG["half_life_games"]) -> float | None:
    if not values:
        return None
    weights = recency_weights(len(values), half_life)
    return float(np.dot(np.asarray(values, dtype=float), weights) / weights.sum())


def simulate_dst(team: str, opponent: str, own_rows: Sequence[Mapping[str, Any]], prior_rows: Sequence[Mapping[str, Any]], config: Mapping[str, Any] = CONFIG) -> dict[str, Any] | None:
    own = [row for row in sorted(own_rows, key=lambda item: (item["season"], item["week"])) if number(row, "fantasy_points", nonnegative=False) is not None][-int(config["max_player_games"]):]
    peers = [row for row in sorted(prior_rows, key=lambda item: (item["season"], item["week"])) if row.get("team") != team and number(row, "fantasy_points", nonnegative=False) is not None][-int(config["max_prior_rows"]):]
    opponent_rows = [row for row in peers if row.get("opponent") == opponent][-int(config["max_player_games"]):]
    if not peers:
        return None
    own_weights = recency_weights(len(own), float(config["half_life_games"])) if own else np.array([])
    if len(own_weights):
        own_weights = own_weights / own_weights.sum()
    opponent_weights = recency_weights(len(opponent_rows), float(config["half_life_games"])) if opponent_rows else np.array([])
    if len(opponent_weights):
        opponent_weights = opponent_weights / opponent_weights.sum()
    source_weights = np.asarray([len(own), len(opponent_rows), 4.0], dtype=float)
    source_weights /= source_weights.sum()
    seed = int(sha256(f"{config['seed']}:DST:{team}:{opponent}".encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(seed)
    scores: list[float] = []
    stat_sums: defaultdict[str, float] = defaultdict(float)
    contribution_sums: defaultdict[str, float] = defaultdict(float)
    for _ in range(int(config["draws"])):
        source = int(rng.choice(3, p=source_weights))
        if source == 0 and own:
            selected = own[int(rng.choice(len(own), p=own_weights))]
        elif source == 1 and opponent_rows:
            selected = opponent_rows[int(rng.choice(len(opponent_rows), p=opponent_weights))]
        else:
            selected = peers[int(rng.integers(0, len(peers)))]
        stats = selected["stats"]
        contributions = dst_contributions(stats)
        score = sum(contributions.values())
        source_score = number(selected, "fantasy_points", nonnegative=False)
        if source_score is None or abs(score - source_score) > 1e-8:
            raise AssertionError("DST component bridge does not reconcile")
        scores.append(score)
        for key in DST_COMPONENTS:
            stat_sums[key] += float(stats.get(key, 0.0))
        for key, value in contributions.items():
            contribution_sums[key] += value
    draws = int(config["draws"])
    array = np.asarray(scores, dtype=float)
    return {
        "identity": f"DST:{team}", "name": f"{team} DST", "position": "DST",
        "status": "separate_dst_research", "history_games": len(own), "rates": {},
        "opponent_context": {"opponent": opponent, "opponent_allowed_games": len(opponent_rows),
                             "defense_games": len(own), "league_prior_equivalent_games": 4},
        "stat_means": {key: value / draws for key, value in sorted(stat_sums.items())},
        "scoring_contributions": {key: value / draws for key, value in sorted(contribution_sums.items())},
        "mean_fpts": float(array.mean()), "p10_fpts": float(np.quantile(array, .10)),
        "median_fpts": float(np.quantile(array, .50)), "p90_fpts": float(np.quantile(array, .90)),
        "boom_threshold": BOOM_THRESHOLDS["DST"], "boom_rate": float(np.mean(array >= BOOM_THRESHOLDS["DST"])),
        "draws": draws, "seed": seed, "coherence_scope": "separate_dst_whole_game_resample",
    }


def _count(rng: np.random.Generator, mean: float | None) -> int:
    return int(rng.poisson(max(0.0, mean or 0.0)))


def _yards(rng: np.random.Generator, opportunities: int, rate: float, spread: float) -> float:
    if opportunities <= 0:
        return 0.0
    return float(rng.normal(rate * opportunities, spread * sqrt(opportunities)))


def _allocate_count(rng: np.random.Generator, total: int, shares: Sequence[float]) -> tuple[list[int], int]:
    """Allocate a team count to known players while retaining the unknown-role bucket."""
    clean = [max(0.0, float(share)) for share in shares]
    known = sum(clean)
    if known > 1.0:
        clean = [share / known for share in clean]
        known = 1.0
    result = rng.multinomial(max(0, total), clean + [max(0.0, 1.0 - known)])
    return [int(value) for value in result[:-1]], int(result[-1])


def _allocate_with_capacity(
    rng: np.random.Generator,
    total: int,
    capacities: Sequence[int],
    weights: Sequence[float],
    unknown_capacity: int,
) -> tuple[list[int], int]:
    """Allocate integer outcomes without letting a receiver exceed sampled targets."""
    remaining = [max(0, int(value)) for value in capacities]
    unknown_remaining = max(0, int(unknown_capacity))
    allocated = [0] * len(remaining)
    unknown = 0
    for _ in range(max(0, int(total))):
        choices = [max(0.0, float(weight)) * capacity for weight, capacity in zip(weights, remaining)]
        choices.append(float(unknown_remaining))
        denominator_value = sum(choices)
        if denominator_value <= 0:
            unknown += 1
            continue
        selected = int(rng.choice(len(choices), p=np.asarray(choices) / denominator_value))
        if selected == len(remaining):
            unknown += 1
            unknown_remaining -= 1
        else:
            allocated[selected] += 1
            remaining[selected] -= 1
    return allocated, unknown


def simulate_team(
    forecast: Mapping[str, Any],
    by_player: Mapping[str, Sequence[Mapping[str, Any]]],
    by_position: Mapping[str, Sequence[Mapping[str, Any]]],
    config: Mapping[str, Any] = CONFIG,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Simulate one shared offensive game state and return player-level distributions."""
    prepared = []
    for player in sorted(forecast["players"], key=lambda item: (str(item.get("position")), str(item.get("identity") or item.get("name")))):
        position = str(player["position"])
        if position not in ("QB", "RB", "WR", "TE") or not player.get("components"):
            continue
        identity = str(player.get("identity"))
        peers = [row for row in by_position[position] if str(row.get("identity")) != identity]
        rates = {name: estimate_rate(by_player.get(identity, ()), peers, name, config)
                 for name, spec in RATE_DEFS.items() if position in spec["positions"]}
        if any(rate is None for rate in rates.values()):
            continue
        rare = {field: per_game_mean(by_player.get(identity, ()), peers, field, config=config)
                for field, positions in RARE_FIELDS.items() if position in positions}
        prepared.append({"player": player, "position": position, "rates": rates, "rare": rare})

    seed_key = f"{config['seed']}:{forecast['game_id']}:{forecast['team']}:coupled"
    seed = int(sha256(seed_key.encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(seed)
    draws = int(config["draws"])
    states = [{"scores": [], "stats": defaultdict(float), "contributions": defaultdict(float)} for _ in prepared]
    maximum_mismatch = defaultdict(float)
    unallocated_sums = defaultdict(float)

    qbs = [index for index, item in enumerate(prepared) if item["position"] == "QB"]
    rushers = [index for index, item in enumerate(prepared) if "carries" in item["player"]["components"]]
    receivers = [index for index, item in enumerate(prepared) if "targets" in item["player"]["components"]]
    budgets = forecast["budgets"]

    for _ in range(draws):
        attempts = _count(rng, (budgets.get("attempts") or {}).get("mean"))
        carries = _count(rng, (budgets.get("carries") or {}).get("mean"))
        qb_attempts, unknown_attempts = _allocate_count(rng, attempts, [prepared[index]["player"]["components"]["attempts"]["share"] for index in qbs])
        rushing_counts, unknown_carries = _allocate_count(rng, carries, [prepared[index]["player"]["components"]["carries"]["share"] for index in rushers])

        draw_stats: list[dict[str, float]] = [dict() for _ in prepared]
        completions = passing_yards = passing_tds = 0
        for index, attempts_for_qb in zip(qbs, qb_attempts):
            rates = prepared[index]["rates"]
            completed = int(rng.binomial(attempts_for_qb, rates["completion_rate"]["mean"]))
            yards = max(0.0, _yards(rng, completed, rates["passing_yards_per_completion"]["mean"], 5.0))
            touchdowns = int(rng.binomial(completed, rates["passing_td_rate"]["mean"]))
            draw_stats[index].update({"passing_yards": yards, "passing_tds": touchdowns,
                                      "passing_interceptions": int(rng.binomial(attempts_for_qb, rates["interception_rate"]["mean"]))})
            completions += completed
            passing_yards += yards
            passing_tds += touchdowns

        targets = min(attempts, max(completions, _count(rng, (budgets.get("targets") or {}).get("mean"))))
        target_counts, unknown_targets = _allocate_count(rng, targets, [prepared[index]["player"]["components"]["targets"]["share"] for index in receivers])
        catch_weights = [prepared[index]["rates"]["catch_rate"]["mean"] for index in receivers]
        reception_counts, unknown_receptions = _allocate_with_capacity(rng, completions, target_counts, catch_weights, unknown_targets + unknown_attempts)

        receiving_weights = [count * prepared[index]["rates"]["receiving_yards_per_reception"]["mean"] for index, count in zip(receivers, reception_counts)]
        unknown_yard_weight = unknown_receptions * (_weighted([rate["rates"]["receiving_yards_per_reception"]["position_prior"] for rate in prepared if rate["position"] in ("RB", "WR", "TE")]) or 10.0)
        yard_denominator = sum(receiving_weights) + unknown_yard_weight
        known_receiving_yards = []
        for weight in receiving_weights:
            known_receiving_yards.append(passing_yards * weight / yard_denominator if yard_denominator > 0 else 0.0)
        unknown_receiving_yards = passing_yards - sum(known_receiving_yards)
        td_weights = [prepared[index]["rates"]["receiving_td_rate"]["mean"] for index in receivers]
        receiving_tds, unknown_receiving_tds = _allocate_with_capacity(rng, passing_tds, reception_counts, td_weights, unknown_receptions)

        for index, carry_count in zip(rushers, rushing_counts):
            rates = prepared[index]["rates"]
            draw_stats[index].update({"rushing_yards": max(0.0, _yards(rng, carry_count, rates["rushing_yards_per_carry"]["mean"], 3.5)),
                                      "rushing_tds": int(rng.binomial(carry_count, rates["rushing_td_rate"]["mean"]))})
        for index, receptions, yards, touchdowns in zip(receivers, reception_counts, known_receiving_yards, receiving_tds):
            draw_stats[index].update({"receptions": receptions, "receiving_yards": yards, "receiving_tds": touchdowns})

        for index, item in enumerate(prepared):
            for field, mean in item["rare"].items():
                draw_stats[index][field] = int(rng.poisson(max(0.0, mean)))
            score = draftkings_points(item["position"], draw_stats[index])
            contributions = scoring_contributions(draw_stats[index])
            if abs(score - sum(contributions.values())) > 1e-8:
                raise AssertionError("DraftKings component bridge does not reconcile")
            states[index]["scores"].append(score)
            for key, value in draw_stats[index].items():
                states[index]["stats"][key] += float(value)
            for key, value in contributions.items():
                states[index]["contributions"][key] += value

        maximum_mismatch["opportunity_budget"] = max(maximum_mismatch["opportunity_budget"],
            abs(attempts - sum(qb_attempts) - unknown_attempts), abs(carries - sum(rushing_counts) - unknown_carries),
            abs(targets - sum(target_counts) - unknown_targets))
        maximum_mismatch["completions"] = max(maximum_mismatch["completions"], abs(completions - sum(reception_counts) - unknown_receptions))
        maximum_mismatch["passing_receiving_yards"] = max(maximum_mismatch["passing_receiving_yards"], abs(passing_yards - sum(known_receiving_yards) - unknown_receiving_yards))
        maximum_mismatch["passing_receiving_tds"] = max(maximum_mismatch["passing_receiving_tds"], abs(passing_tds - sum(receiving_tds) - unknown_receiving_tds))
        for key, value in (("attempts", unknown_attempts), ("carries", unknown_carries), ("targets", unknown_targets),
                           ("receptions", unknown_receptions), ("receiving_yards", unknown_receiving_yards), ("receiving_tds", unknown_receiving_tds)):
            unallocated_sums[key] += value

    output = []
    for item, state in zip(prepared, states):
        array = np.asarray(state["scores"], dtype=float)
        output.append({"identity": item["player"].get("identity"), "name": item["player"].get("name"), "position": item["position"],
            "status": "role_unresolved_research", "history_games": max((rate["games"] for rate in item["rates"].values()), default=0),
            "rates": item["rates"], "stat_means": {key: value / draws for key, value in sorted(state["stats"].items())},
            "scoring_contributions": {key: value / draws for key, value in sorted(state["contributions"].items())},
            "mean_fpts": float(array.mean()), "p10_fpts": float(np.quantile(array, .10)), "median_fpts": float(np.quantile(array, .50)),
            "p90_fpts": float(np.quantile(array, .90)), "boom_threshold": BOOM_THRESHOLDS[item["position"]],
            "boom_rate": float(np.mean(array >= BOOM_THRESHOLDS[item["position"]])), "draws": draws, "seed": seed,
            "coherence_scope": "team_coupled_offense"})
    return output, {"scope": "team_coupled_offense", "draws": draws, "seed": seed,
                    "max_absolute_mismatch": dict(sorted(maximum_mismatch.items())),
                    "mean_unallocated": {key: value / draws for key, value in sorted(unallocated_sums.items())}}


def simulate_player(
    player: Mapping[str, Any],
    own_rows: Sequence[Mapping[str, Any]],
    prior_rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any] = CONFIG,
) -> dict[str, Any] | None:
    position = str(player["position"])
    if position not in ("QB", "RB", "WR", "TE") or not player.get("components"):
        return None
    identity = str(player.get("identity"))
    peers = [row for row in prior_rows if str(row.get("identity")) != identity]
    rates = {
        name: estimate_rate(own_rows, peers, name, config)
        for name, spec in RATE_DEFS.items()
        if position in spec["positions"]
    }
    if any(rate is None for rate in rates.values()):
        return None
    rare = {field: per_game_mean(own_rows, peers, field, config=config) for field, positions in RARE_FIELDS.items() if position in positions}
    seed_key = f"{config['seed']}:{player.get('identity')}:{player.get('name')}"
    seed = int(sha256(seed_key.encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(seed)
    draws = int(config["draws"])
    scores: list[float] = []
    stat_sums: defaultdict[str, float] = defaultdict(float)
    contribution_sums: defaultdict[str, float] = defaultdict(float)
    components = player["components"]

    for _ in range(draws):
        stats: dict[str, float] = {}
        attempts = _count(rng, components.get("attempts", {}).get("mean"))
        carries = _count(rng, components.get("carries", {}).get("mean"))
        targets = _count(rng, components.get("targets", {}).get("mean"))
        if position == "QB":
            completions = int(rng.binomial(attempts, rates["completion_rate"]["mean"]))
            stats.update({
                "passing_yards": _yards(rng, completions, rates["passing_yards_per_completion"]["mean"], 5.0),
                "passing_tds": int(rng.binomial(completions, rates["passing_td_rate"]["mean"])),
                "passing_interceptions": int(rng.binomial(attempts, rates["interception_rate"]["mean"])),
            })
        if carries:
            stats.update({
                "rushing_yards": _yards(rng, carries, rates["rushing_yards_per_carry"]["mean"], 3.5),
                "rushing_tds": int(rng.binomial(carries, rates["rushing_td_rate"]["mean"])),
            })
        if position in ("RB", "WR", "TE"):
            receptions = int(rng.binomial(targets, rates["catch_rate"]["mean"]))
            stats.update({
                "receptions": receptions,
                "receiving_yards": _yards(rng, receptions, rates["receiving_yards_per_reception"]["mean"], 6.0),
                "receiving_tds": int(rng.binomial(receptions, rates["receiving_td_rate"]["mean"])),
            })
        for field, mean in rare.items():
            stats[field] = int(rng.poisson(max(0.0, mean)))
        score = draftkings_points(position, stats)
        contributions = scoring_contributions(stats)
        if abs(score - sum(contributions.values())) > 1e-8:
            raise AssertionError("DraftKings component bridge does not reconcile")
        scores.append(score)
        for key, value in stats.items():
            stat_sums[key] += float(value)
        for key, value in contributions.items():
            contribution_sums[key] += value

    array = np.asarray(scores, dtype=float)
    return {
        "identity": player.get("identity"),
        "name": player.get("name"),
        "position": position,
        "status": "role_unresolved_research",
        "history_games": max((rate["games"] for rate in rates.values()), default=0),
        "rates": rates,
        "stat_means": {key: value / draws for key, value in sorted(stat_sums.items())},
        "scoring_contributions": {key: value / draws for key, value in sorted(contribution_sums.items())},
        "mean_fpts": float(array.mean()),
        "p10_fpts": float(np.quantile(array, 0.10)),
        "median_fpts": float(np.quantile(array, 0.50)),
        "p90_fpts": float(np.quantile(array, 0.90)),
        "boom_threshold": BOOM_THRESHOLDS[position],
        "boom_rate": float(np.mean(array >= BOOM_THRESHOLDS[position])),
        "draws": draws,
        "seed": seed,
        "coherence_scope": "within_player_only",
    }


def build(workload_report: Mapping[str, Any], history: Sequence[Mapping[str, Any]], config: Mapping[str, Any] = CONFIG) -> list[dict[str, Any]]:
    output = []
    for forecast in workload_report["forecasts"]:
        cutoff = (int(forecast["season"]), int(forecast["week"]))
        by_player: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        by_position: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in sorted(history, key=lambda item: (item["season"], item["week"])):
            if (int(row["season"]), int(row["week"])) >= cutoff:
                continue
            by_player[str(row["identity"])].append(row)
            by_position[str(row["position"])].append(row)
        players, team_coherence = simulate_team(forecast, by_player, by_position, config)
        dst = simulate_dst(forecast["team"], forecast["opponent"], by_player[f"DST:{forecast['team']}"], by_position["DST"], config)
        if dst:
            players.append(dst)
        output.append({
            "game_id": forecast["game_id"], "team": forecast["team"], "opponent": forecast["opponent"],
            "kickoff": forecast["kickoff"], "players": players, "team_coherence": team_coherence,
        })
    return output


def backtest(history: Sequence[Mapping[str, Any]], start: tuple[int, int] = (2024, 1), config: Mapping[str, Any] = CONFIG) -> list[dict[str, Any]]:
    groups: defaultdict[tuple[int, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in history:
        groups[(int(row["season"]), int(row["week"]))].append(row)
    prior_by_position: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    prior_dst_by_opponent: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    own_by_player: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    output: list[dict[str, Any]] = []
    for chronological_key in sorted(groups):
        current = groups[chronological_key]
        prior_rates: dict[tuple[str, str], float] = {}
        for position, prior_rows in prior_by_position.items():
            for rate_name in CORE_BACKTEST_RATES:
                spec = RATE_DEFS[rate_name]
                if position not in spec["positions"]:
                    continue
                prior_num, prior_den, _ = rate_totals(
                    prior_rows[-int(config["max_prior_rows"]):],
                    spec,
                    {**config, "max_player_games": config["max_prior_rows"]},
                )
                if prior_den > 0:
                    prior_rates[(position, rate_name)] = prior_num / prior_den
        if chronological_key >= start:
            for row in current:
                position = str(row["position"])
                own = own_by_player[str(row["identity"])]
                if position == "DST":
                    actual = number(row, "fantasy_points", nonnegative=False)
                    own_values = [value for prior in own if (value := number(prior, "fantasy_points", nonnegative=False)) is not None][-int(config["max_player_games"]):]
                    league_values = [value for prior in prior_by_position[position] if (value := number(prior, "fantasy_points", nonnegative=False)) is not None][-int(config["max_prior_rows"]):]
                    opponent_values = [value for prior in prior_dst_by_opponent[str(row.get("opponent"))] if prior.get("team") != row.get("team") and (value := number(prior, "fantasy_points", nonnegative=False)) is not None][-int(config["max_player_games"]):]
                    baseline, league, opponent_allowed = _weighted(own_values), _weighted(league_values), _weighted(opponent_values)
                    if actual is not None and baseline is not None and league is not None:
                        numerator_value = len(own_values) * baseline + 4.0 * league
                        denominator_value = len(own_values) + 4.0
                        if opponent_allowed is not None:
                            numerator_value += len(opponent_values) * opponent_allowed
                            denominator_value += len(opponent_values)
                        output.append({"season": chronological_key[0], "week": chronological_key[1], "identity": row["identity"],
                            "position": position, "rate": "dst_dk_points", "actual": actual,
                            "candidate": numerator_value / denominator_value, "baseline": baseline})
                    continue
                for rate_name in CORE_BACKTEST_RATES:
                    spec = RATE_DEFS[rate_name]
                    if position not in spec["positions"]:
                        continue
                    actual = number(row, str(spec["numerator"]), nonnegative=not str(spec["numerator"]).endswith("yards"))
                    opportunities = denominator(row, spec["denominator"])
                    own_num, own_den, _ = rate_totals(own, spec, config)
                    prior_rate = prior_rates.get((position, rate_name))
                    if actual is None or opportunities is None or opportunities <= 0 or prior_rate is None or own_den <= 0:
                        continue
                    prior_opportunities = float(spec["prior_opportunities"])
                    candidate_rate = (own_num + prior_rate * prior_opportunities) / (own_den + prior_opportunities)
                    if spec.get("probability"):
                        candidate_rate = min(1.0, max(0.0, candidate_rate))
                    output.append({
                        "season": chronological_key[0], "week": chronological_key[1], "identity": row["identity"],
                        "position": position, "rate": rate_name, "actual": actual,
                        "candidate": candidate_rate * opportunities,
                        "baseline": (own_num / own_den) * opportunities,
                    })
        for row in current:
            prior_by_position[str(row["position"])].append(row)
            own_by_player[str(row["identity"])].append(row)
            if str(row["position"]) == "DST":
                prior_dst_by_opponent[str(row.get("opponent"))].append(row)
    return output


def metrics(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for rate_name in CORE_BACKTEST_RATES + ("dst_dk_points",):
        sample = [row for row in rows if row["rate"] == rate_name]
        definition = RATE_DEFS.get(rate_name)
        result.append({
            "rate": rate_name,
            "label": definition["label"] if definition else "DST DraftKings points",
            "n": len(sample),
            "candidate_mae": float(np.mean([abs(row["candidate"] - row["actual"]) for row in sample])) if sample else None,
            "baseline_mae": float(np.mean([abs(row["baseline"] - row["actual"]) for row in sample])) if sample else None,
            "candidate_bias_actual_minus_projected": float(np.mean([row["actual"] - row["candidate"] for row in sample])) if sample else None,
            "unit": definition["numerator"] if definition else "fantasy_points",
        })
    return result
