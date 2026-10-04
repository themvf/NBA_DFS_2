"""Auditable NFL ownership estimates from strictly earlier contest evidence.

Names and IDs join evidence only. Model features describe salary, position and
pregame projection/value ranks so a player can inherit evidence from analogous
players. No fantasy results, winning lineups or current contest ownership enter
the feature matrix. This estimates player marginals, not a simulated field.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path

import numpy as np

VERSION = "nfl-ownership-v1"
POSITIONS = ("QB", "RB", "WR", "TE", "DST", "K")
FEATURES = ("salary_k", "projection", "dk_average", "projection_missing", "dk_average_missing",
            "salary_rank", "projection_rank", "value_rank", "log_position_pool",
            "backup_role", *[f"position_{p}" for p in POSITIONS])
RECIPE = {"features": list(FEATURES), "ridge_alpha": 0.05, "target_probability_floor": 0.0001,
          "contest_weight": "equal; each contest's labeled players share its weight",
          "flex_prior": {"RB": .35, "WR": .55, "TE": .10}, "flex_prior_contests": 3,
          "status": "experimental_uncalibrated", "salary_limit": 50000}


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def code_digest(path):
    """Canonical text hash survives Git's Windows LF/CRLF checkout conversion."""
    return sha256(Path(path).read_text(encoding="utf-8").encode("utf-8")).hexdigest()


def stamp(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Timestamps must include a timezone")
    return dt.astimezone(timezone.utc)


def finite(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)


def seal(value):
    return {**value, "artifact_digest": digest(value)}


def verify(value):
    if value.get("artifact_digest") != digest({k: v for k, v in value.items() if k != "artifact_digest"}):
        raise ValueError("Artifact digest mismatch")
    return value


def write_artifact(directory, prefix, value):
    """Content-addressed, immutable and idempotent; never overwrite other bytes."""
    verify(value)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}-{value['artifact_digest']}.json"
    content = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise ValueError("Refusing to overwrite an existing artifact")
    else:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    return path


def validate_snapshot(snapshot):
    if "artifact_digest" in snapshot:
        verify(snapshot)
    if snapshot["format"] not in ("classic", "showdown"):
        raise ValueError("Unsupported contest format")
    if stamp(snapshot["captured_at"]) >= stamp(snapshot["lock_at"]):
        raise ValueError("Features must be frozen before slate lock")
    if not snapshot.get("slate_id") or not snapshot.get("source_digest"):
        raise ValueError("Snapshot requires slate identity and source digest")
    players = snapshot["players"]
    if not players or len({p["player_id"] for p in players}) != len(players):
        raise ValueError("Empty pool or duplicate player identity")
    for p in players:
        if p["position"] not in POSITIONS or snapshot["format"] == "classic" and p["position"] == "K":
            raise ValueError("Unsupported position")
        if not finite(p["salary"]) or p["salary"] <= 0 or not isinstance(p["is_out"], bool):
            raise ValueError("Invalid salary or availability")
        for key in ("projection", "dk_average"):
            if p.get(key) is not None and not finite(p[key]):
                raise ValueError(f"Invalid {key}")
    return snapshot


def _rank(values):
    if len(values) == 1:
        return np.array([.5])
    return np.array([(sum(v < x for v in values) + (sum(v == x for v in values) - 1) / 2)
                     / (len(values) - 1) for x in values])


def feature_rows(snapshot):
    validate_snapshot(snapshot)
    rows = [p for p in sorted(snapshot["players"], key=lambda p: str(p["player_id"])) if not p["is_out"]]
    ranks = {}
    for pos in POSITIONS:
        group = [p for p in rows if p["position"] == pos]
        salary = [p["salary"] for p in group]
        projection = [p.get("projection") or 0 for p in group]
        value = [v / (s / 1000) for v, s in zip(projection, salary)]
        for p, a, b, c in zip(group, _rank(salary), _rank(projection), _rank(value)):
            ranks[p["player_id"]] = [float(a), float(b), float(c), math.log1p(len(group))]
    matrix = []
    for p in rows:
        matrix.append([p["salary"] / 1000, p.get("projection") or 0, p.get("dk_average") or 0,
                       float(p.get("projection") is None), float(p.get("dk_average") is None),
                       *ranks[p["player_id"]], float("backup" in (p.get("role") or "").lower()),
                       *[float(p["position"] == pos) for pos in POSITIONS]])
    return rows, np.array(matrix, dtype=float).reshape((-1, len(FEATURES)))


def validate_contest(contest):
    s = validate_snapshot(contest["snapshot"])
    if stamp(contest["labels_available_at"]) < stamp(s["lock_at"]):
        raise ValueError("Historical ownership labels cannot precede lock")
    slots = {"OVERALL"} if s["format"] == "classic" else {"CPT", "FLEX"}
    ids = {p["player_id"] for p in s["players"]}
    keys = set()
    if not contest["labels"]:
        raise ValueError("Contest has no ownership labels")
    if s["format"] == "classic":
        flex = contest["flex_allocation"]
        if set(flex) != {"RB", "WR", "TE"} or any(not finite(v) or not 0 <= v <= 1 for v in flex.values()) or abs(sum(flex.values()) - 1) > 1e-6:
            raise ValueError("Invalid Classic FLEX allocation")
    for label in contest["labels"]:
        key = (label["player_id"], label["slot"])
        if key in keys or key[0] not in ids or key[1] not in slots:
            raise ValueError("Duplicate, unmatched or invalid ownership label")
        keys.add(key)
        if not finite(label["ownership_pct"]) or not 0 <= label["ownership_pct"] <= 100:
            raise ValueError("Ownership must be between 0 and 100")
    return contest


def _fit_group(contests, slot):
    xs, ys, weights, references = [], [], [], []
    for c in contests:
        rows, matrix = feature_rows(c["snapshot"])
        labels = {r["player_id"]: r["ownership_pct"] for r in c["labels"] if r["slot"] == slot}
        usable = [(p, x) for p, x in zip(rows, matrix) if p["player_id"] in labels]
        for p, x in usable:
            xs.append(x)
            ys.append(math.log(labels[p["player_id"]] / 100 + RECIPE["target_probability_floor"]))
            weights.append(1 / max(1, len(usable)))
            references.append({"contest_id": c["contest_id"], "player_id": p["player_id"],
                               "slot": slot, "ownership_pct": labels[p["player_id"]]})
    if not xs:
        return None
    x, y, w = np.array(xs), np.array(ys), np.array(weights)
    w /= w.sum()
    mean = np.sum(x * w[:, None], axis=0)
    scale = np.sqrt(np.sum((x - mean) ** 2 * w[:, None], axis=0))
    scale[scale < 1e-8] = 1
    z = np.column_stack([np.ones(len(x)), (x - mean) / scale])
    penalty = np.eye(z.shape[1]) * RECIPE["ridge_alpha"]
    penalty[0, 0] = 0
    coef = np.linalg.solve(z.T @ (z * w[:, None]) + penalty, z.T @ (y * w))
    return {"mean": mean.tolist(), "scale": scale.tolist(), "coefficients": coef[1:].tolist(),
            "intercept": float(coef[0]), "training_rows": len(x), "evidence": references}


def fit(contests, as_of):
    """As-of fit. Outcome timestamps govern inclusion, not just game date."""
    cutoff = stamp(as_of)
    seen, sources, accepted, excluded = set(), set(), [], []
    for c in sorted(contests, key=lambda c: c["contest_id"]):
        validate_contest(c)
        if c["contest_id"] in seen:
            raise ValueError("Duplicate contest ID")
        if c["source_digest"] in sources:
            raise ValueError("Duplicate contest source under another ID")
        seen.add(c["contest_id"])
        sources.add(c["source_digest"])
        if stamp(c["labels_available_at"]) > cutoff or stamp(c["snapshot"]["lock_at"]) >= cutoff:
            excluded.append({"contest_id": c["contest_id"], "reason": "labels_not_available_before_decision"})
        else:
            accepted.append(c)
    groups, support, budgets = {}, {}, {}
    for fmt, slots in (("classic", ("OVERALL",)), ("showdown", ("CPT", "FLEX"))):
        subset = [c for c in accepted if c["snapshot"]["format"] == fmt]
        support[fmt] = {"contests": len(subset), "slates": len({c['snapshot']['slate_id'] for c in subset})}
        for slot in slots:
            groups[f"{fmt}:{slot}"] = _fit_group(subset, slot)
        if fmt == "classic":
            prior = RECIPE["flex_prior"]
            n = RECIPE["flex_prior_contests"]
            for pos in prior:
                budgets[pos] = (n * prior[pos] + sum(c["flex_allocation"][pos] for c in subset)) / (n + len(subset))
    core = {"version": VERSION, "kind": "model", "as_of": as_of, "recipe": json.loads(json.dumps(RECIPE)),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "runtime": {"numpy": np.__version__},
            "implementation_digest": code_digest(__file__),
            "training_digest": digest(accepted), "training_contests": [c["contest_id"] for c in accepted],
            "training_slates": sorted({c["snapshot"]["slate_id"] for c in accepted}),
            "training_sources": [{"contest_id": c["contest_id"], "source_digest": c["source_digest"],
                                   "snapshot_digest": digest(c["snapshot"]), "labels_available_at": c["labels_available_at"]} for c in accepted],
            "excluded_contests": excluded, "support": support, "classic_flex_allocation": budgets,
            "groups": groups, "qualification": "experimental_uncalibrated"}
    return seal(core)


def _allocate(scores, total, caps):
    """Capped proportional allocation. Outputs percentage points."""
    scores, caps = np.asarray(scores, dtype=float), np.asarray(caps, dtype=float)
    if caps.sum() + 1e-7 < total:
        raise ValueError("Pool cannot satisfy roster ownership totals")
    result = np.zeros(len(scores))
    remaining = total
    active = caps > 1e-10
    for _ in range(len(scores) + 1):
        if remaining < 1e-9:
            return result
        share = scores[active] / scores[active].sum() * remaining
        indices = np.where(active)[0]
        full = share >= caps[active] - 1e-10
        if not full.any():
            result[active] = share
            return result
        filled = indices[full]
        result[filled] = caps[filled]
        remaining -= caps[filled].sum()
        active[filled] = False
    raise ValueError("Ownership allocation failed")


def _marginals(rows, logs, fmt, flex, salary_penalty):
    salary = np.array([p["salary"] for p in rows], dtype=float)
    scores = {slot: np.exp(np.clip(value - salary_penalty * salary / 1000 * (1.5 if slot == "CPT" else 1), -60, 30)) for slot, value in logs.items()}
    if fmt == "showdown":
        cpt = _allocate(scores["CPT"], 100, np.full(len(rows), 100.))
        flx = _allocate(scores["FLEX"], 500, 100 - cpt)
        return {"CPT": cpt, "FLEX": flx}, float(np.sum(salary * (1.5 * cpt + flx)) / 100)
    output = np.zeros(len(rows))
    count = {pos: sum(p["position"] == pos for p in rows) for pos in POSITIONS}
    base = {"QB": 1, "RB": 2, "WR": 3, "TE": 1, "DST": 1}
    if any(count[pos] < n for pos, n in base.items()):
        raise ValueError("Classic pool cannot fill mandatory positions")
    flex_positions = ("RB", "WR", "TE")
    shares = _allocate([flex[p] for p in flex_positions], 1, [min(1, count[p] - base[p]) for p in flex_positions])
    budgets = {p: 100 * (n + (shares[flex_positions.index(p)] if p in flex_positions else 0)) for p, n in base.items()}
    for pos, budget in budgets.items():
        ix = [i for i, p in enumerate(rows) if p["position"] == pos]
        output[ix] = _allocate(scores["OVERALL"][ix], budget, np.full(len(ix), 100.))
    return {"OVERALL": output}, float(np.sum(salary * output) / 100)


def forecast(model, snapshot, as_of, baseline=False):
    verify(model)
    if model["version"] != VERSION or model["recipe"] != RECIPE or model["implementation_digest"] != code_digest(__file__):
        raise ValueError("Model implementation or feature recipe changed; refit before forecasting")
    validate_snapshot(snapshot)
    if stamp(model["as_of"]) > stamp(as_of) or stamp(snapshot["captured_at"]) > stamp(as_of) or stamp(as_of) >= stamp(snapshot["lock_at"]):
        raise ValueError("Forecast timestamps violate the pregame decision cutoff")
    if snapshot["slate_id"] in model["training_slates"]:
        raise ValueError("Cannot forecast a training slate as prospective")
    rows, x = feature_rows(snapshot)
    fmt = snapshot["format"]
    slots = ("OVERALL",) if fmt == "classic" else ("CPT", "FLEX")
    logs, explanations, methods = {}, {}, {}
    for slot in slots:
        group = None if baseline else model["groups"].get(f"{fmt}:{slot}")
        methods[slot] = "historical_ridge" if group else "salary_prior_no_matching_history"
        if group:
            contributions = (x - np.array(group["mean"])) / np.array(group["scale"]) * np.array(group["coefficients"])
            logs[slot] = group["intercept"] + contributions.sum(axis=1)
            explanations[slot] = [{"units": "raw log probability before roster and salary normalization", "intercept": group["intercept"], "feature_contributions": dict(zip(FEATURES, v.tolist()))} for v in contributions]
        else:
            logs[slot] = np.log(np.array([p["salary"] / 1000 for p in rows])) * 2
            explanations[slot] = [{"salary_prior_power": 2} for _ in rows]
    flex = model["classic_flex_allocation"] if not baseline else RECIPE["flex_prior"]
    values, expected_salary = _marginals(rows, logs, fmt, flex, 0)
    penalty = 0.
    if expected_salary > 50000 + .001:
        low, high = 0., 1.
        for _ in range(12):
            trial, cost = _marginals(rows, logs, fmt, flex, high)
            if cost <= 50000:
                break
            high *= 2
        else:
            raise ValueError("Ownership marginals cannot satisfy the salary cap with this pool")
        for _ in range(55):
            middle = (low + high) / 2
            trial, cost = _marginals(rows, logs, fmt, flex, middle)
            if cost > 50000:
                low = middle
            else:
                high = middle
        penalty = high
        values, expected_salary = _marginals(rows, logs, fmt, flex, penalty)
    by_id = {p["player_id"]: i for i, p in enumerate(rows)}
    output = []
    for p in sorted(snapshot["players"], key=lambda p: str(p["player_id"])):
        i = by_id.get(p["player_id"])
        for slot in slots:
            output.append({"player_id": p["player_id"], "name": p["name"], "team": p["team"], "position": p["position"],
                           "slot": slot, "salary": p["salary"], "ownership_pct": 0. if i is None else float(values[slot][i]),
                           "method": "explicitly_out" if i is None else methods[slot],
                           "features": None if i is None else dict(zip(FEATURES, x[i].tolist())),
                           "explanation": {"reason": "Unavailable in the frozen snapshot"} if i is None else explanations[slot][i]})
    created_at = datetime.now(timezone.utc).isoformat()
    return seal({"version": VERSION, "kind": "forecast", "as_of": as_of, "created_at": created_at,
                 "timing": "pregame_frozen" if stamp(created_at) < stamp(snapshot["lock_at"]) else "retrospective_replay",
                 "slate_id": snapshot["slate_id"], "format": fmt,
                 "lock_at": snapshot["lock_at"], "snapshot_digest": digest(snapshot), "model_digest": model["artifact_digest"],
                 "status": "experimental_uncalibrated", "support": model["support"][fmt], "players": output,
                 "ownership_capability": "heuristic_uncalibrated", "validated_ownership_enabled": False,
                 "units": "percentage points; denominator is complete lineups",
                 "normalization": {"slot_totals": {s: float(v.sum()) for s, v in values.items()},
                                   "expected_salary": expected_salary, "salary_penalty": penalty, "classic_flex_allocation": flex},
                 "warnings": ["Small historical sample; prospective accuracy has not been established.",
                              "Ownership is a marginal estimate, not a duplication, payout or tournament-return model.",
                              "Availability and projection freshness are inherited from the frozen input; re-freeze after news."],
                 "input_snapshot": snapshot})


def metrics(prediction, contest):
    """Only observed labels are scored. Missing ownership is never imputed zero."""
    pred = {(r["player_id"], r["slot"]): r["ownership_pct"] for r in prediction["players"]}
    result = {}
    for slot in sorted({r["slot"] for r in contest["labels"]}):
        pairs = [(pred[(r["player_id"], slot)], r["ownership_pct"]) for r in contest["labels"] if r["slot"] == slot and (r["player_id"], slot) in pred]
        p, y = np.array(pairs).T
        buckets = []
        for lo, hi in ((0, 1), (1, 5), (5, 10), (10, 20), (20, 101)):
            ix = (p >= lo) & (p < hi)
            if ix.any():
                buckets.append({"predicted_range": [lo, hi], "n": int(ix.sum()), "predicted_mean": float(p[ix].mean()), "actual_mean": float(y[ix].mean())})
        top = min(10, len(p))
        result[slot] = {"labeled_players": len(p), "mae_pp": float(np.abs(p - y).mean()), "bias_pp": float((p - y).mean()),
                        "rmse_pp": float(np.sqrt(((p - y) ** 2).mean())),
                        "top10_overlap": len(set(np.argsort(p)[-top:]) & set(np.argsort(y)[-top:])), "calibration_buckets": buckets}
    return result


def walk_forward(contests):
    folds = []
    for c in sorted(contests, key=lambda c: (stamp(c["snapshot"]["lock_at"]), c["contest_id"])):
        cutoff = c["snapshot"]["captured_at"]
        earlier = [r for r in contests if r["snapshot"]["slate_id"] != c["snapshot"]["slate_id"]
                   and stamp(r["snapshot"]["lock_at"]) < stamp(cutoff) and stamp(r["labels_available_at"]) <= stamp(cutoff)]
        model = fit(earlier, cutoff)
        supported = model["support"][c["snapshot"]["format"]]["slates"] > 0
        prediction = forecast(model, c["snapshot"], cutoff)
        base = forecast(model, c["snapshot"], cutoff, baseline=True)
        folds.append({"contest_id": c["contest_id"], "slate_id": c["snapshot"]["slate_id"], "decision_at": cutoff,
                      "trained_contests": model["training_contests"], "model_digest": model["artifact_digest"],
                      "status": "forward_scored" if supported else "prior_only_no_earlier_same_format_history",
                      "model": metrics(prediction, c), "salary_baseline": metrics(base, c)})
    return seal({"version": VERSION, "kind": "walk_forward", "history_digest": digest(contests), "folds": folds,
                 "trained_forward_folds": sum(f["status"] == "forward_scored" for f in folds),
                 "qualification": "experimental_uncalibrated", "policy": "All contests for a slate stay together; labels must be available before the frozen decision."})
