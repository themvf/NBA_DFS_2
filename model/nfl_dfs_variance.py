"""Research-only player variance shrinkage using prior out-of-sample errors.

Means are unchanged. This empirical residual approximation is not a new
football component simulator or production candidate activation.
"""
from collections import defaultdict
import numpy as np

VERSION = "nfl-dfs-player-variance-v1"
STRENGTHS = (4., 12., 24.)


def shrink_variance(errors, prior_variance, strength, half_life=6.):
    if strength <= 0 or half_life <= 0 or prior_variance < 0:
        raise ValueError("Invalid shrinkage configuration")
    x = np.asarray(errors[-34:], dtype=float)
    if not np.isfinite(x).all():
        raise ValueError("Non-finite residual history")
    if len(x) < 2:
        return {"variance": prior_variance, "effective_n": float(len(x)), "weight": 0., "prior_variance": prior_variance}
    w = .5 ** (np.arange(len(x)-1, -1, -1) / half_life)
    w /= w.sum()
    effective = float(1 / np.sum(w*w))
    mean = float(np.dot(w, x))
    variance = float(np.dot(w, (x-mean)**2) / (1 - np.sum(w*w)))
    weight = effective / (effective + strength)
    return {"variance": weight*variance + (1-weight)*prior_variance,
            "player_variance": variance, "prior_variance": prior_variance,
            "effective_n": effective, "weight": weight}


def interval_score(actual, lo, hi, alpha=.2):
    if hi < lo:
        raise ValueError("Inverted interval")
    return hi-lo + 2/alpha * (max(lo-actual, 0) + max(actual-hi, 0))


def walk_forward(samples, strength):
    weeks = defaultdict(list)
    for s in samples:
        if s.get("model") == "baseline":
            weeks[(s["season"], s["week"])].append(s)
    positional, individual = defaultdict(list), defaultdict(list)
    predictions = []
    for cutoff, targets in sorted(weeks.items()):
        for s in sorted(targets, key=lambda x: x["sample_key"]):
            history = positional[s["position"]][-4000:]
            if len(history) < 100:
                continue
            centered = np.array(history)-np.mean(history)
            prior_variance = float(np.var(centered))
            evidence = shrink_variance(individual[s["sample_key"].split(":", 2)[-1]], prior_variance, strength)
            # Centering preserves the baseline mean; empirical shape retains
            # asymmetry. Endpoints are not rounded to manufacture calibration.
            scale = np.sqrt(evidence["variance"] / prior_variance) if prior_variance > 0 else 1.
            draws = float(s["baseline"]) + centered*scale
            lo, hi = np.quantile(draws, [.1, .9])
            predictions.append({"sample_key": s["sample_key"], "season": s["season"], "week": s["week"],
                "position": s["position"], "game_id": s["game_id"], "mean": s["baseline"],
                "p10": float(lo), "p90": float(hi), "actual": s["actual"],
                "boom_probability": float(np.mean(draws >= s["boom_threshold"])),
                "boom_threshold": s["boom_threshold"], "baseline_p10": s["p10"], "baseline_p90": s["p90"],
                "baseline_boom_probability": s["boom_probability"], "strength": strength,
                "prior_errors": len(history), "cutoff_exclusive": list(cutoff), **evidence})
        # No player can see another player's outcome from the same week.
        for s in sorted(targets, key=lambda x: x["sample_key"]):
            error = float(s["actual"])-float(s["baseline"])
            positional[s["position"]].append(error)
            individual[s["sample_key"].split(":", 2)[-1]].append(error)
    return predictions


def distribution_metrics(rows, baseline=False):
    if not rows:
        return {"n": 0}
    prefix = "baseline_" if baseline else ""
    return {"n": len(rows),
        "mae": float(np.mean([abs(r["actual"]-r["mean"]) for r in rows])),
        "interval_score_80": float(np.mean([interval_score(r["actual"], r[prefix+"p10"], r[prefix+"p90"]) for r in rows])),
        "coverage_80": float(np.mean([r[prefix+"p10"] <= r["actual"] <= r[prefix+"p90"] for r in rows])),
        "mean_width": float(np.mean([r[prefix+"p90"]-r[prefix+"p10"] for r in rows])),
        "boom_brier": float(np.mean([(r[prefix+"boom_probability"] - (r["actual"]>=r["boom_threshold"]))**2 for r in rows]))}


def study(samples):
    trials = {strength: walk_forward(samples, strength) for strength in STRENGTHS}
    positions = sorted({r["position"] for r in samples})
    report, saved = {}, []
    for position in positions:
        scores = []
        for strength, predictions in trials.items():
            validation = [r for r in predictions if r["season"] == 2024 and r["position"] == position]
            if validation:
                scores.append((distribution_metrics(validation)["interval_score_80"], strength))
        if not scores:
            report[position] = {"status": "insufficient_validation_history"}
            continue
        _, selected = min(scores)
        rows = [r for r in trials[selected] if r["position"] == position and r["season"] in (2024,2025)]
        report[position] = {"selected_strength": selected,
            "validation_trials": [{"strength": s, "interval_score_80": score} for score, s in scores],
            "seasons": {str(y): {"candidate": distribution_metrics([r for r in rows if r["season"]==y]),
                                  "baseline": distribution_metrics([r for r in rows if r["season"]==y], True)} for y in (2024,2025)}}
        saved.extend(rows)
    return {"version": VERSION, "positions": report, "production_promotion": False, "shadow_activation": False,
            "split": "2023 residual warm-up; 2024 selects strength; 2025 retrospective diagnostic, previously inspected",
            "limits": ["recorded-stat cohort, not DK slate", "historical publication latency unavailable",
                       "player/position shrinkage; role/injury model not yet integrated",
                       "empirical residual ranges, not new component-stat simulations",
                       "2026 forward validation required before activation"]}, saved
