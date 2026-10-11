"""Grade the frozen ownership accuracy gate, not merely row coverage.

Classic requires four chronological, slate-grouped holdouts. More contests on
the same slate add labels, not independent units. Showdown cannot inherit a
Classic pass. Missing field labels are never counted as zero ownership.
"""
from collections import defaultdict
import numpy as np
from model.nfl_ownership import VERSION, digest, fit, forecast, seal, stamp, validate_contest

REGISTRATION = "nfl-ownership-classic-phase2"


def ranks(values):
    return np.array([sum(v < x for v in values) + (sum(v == x for v in values) - 1) / 2 for x in values])


def grade_pairs(pairs):
    if not pairs:
        return {"maePp": None, "biasPp": None, "spearman": None, "labeledPlayers": 0}
    p, y = np.array(pairs).T
    a, b = ranks(p), ranks(y)
    rho = float(np.corrcoef(a, b)[0, 1]) if a.std() > 0 and b.std() > 0 else None
    return {"maePp": float(np.abs(p-y).mean()), "biasPp": float((p-y).mean()),
            "spearman": rho, "labeledPlayers": len(pairs)}


def qualify(contests):
    if len({c["contest_id"] for c in contests}) != len(contests):
        raise ValueError("Duplicate contest identity")
    for contest in contests:
        validate_contest(contest)
    by_slate, folds, exclusions = defaultdict(list), [], []
    for contest in sorted(contests, key=lambda c: (stamp(c["snapshot"]["lock_at"]), c["contest_id"])):
        snapshot = contest["snapshot"]
        fmt, slate_id, cutoff = snapshot["format"], snapshot["slate_id"], snapshot["captured_at"]
        earlier = [c for c in contests if c["snapshot"]["slate_id"] != slate_id
                   and stamp(c["snapshot"]["lock_at"]) < stamp(cutoff)
                   and stamp(c["labels_available_at"]) <= stamp(cutoff)]
        model = fit(earlier, cutoff)
        if model["support"][fmt]["slates"] == 0:
            exclusions.append({"contestId": contest["contest_id"], "slateId": slate_id,
                               "reason": "no earlier same-format training slate"})
            continue
        prediction = forecast(model, snapshot, cutoff)
        predictions = {(r["player_id"], r["slot"]): r["ownership_pct"] for r in prediction["players"]}
        labels = {(r["player_id"], r["slot"]): r["ownership_pct"] for r in contest["labels"]}
        active = [p for p in snapshot["players"] if not p["is_out"]]
        required = ("OVERALL",) if fmt == "classic" else ("CPT", "FLEX")
        coverage = {}
        valid = True
        for slot in required:
            covered = [p for p in active if (p["player_id"], slot) in labels]
            total = sum(max(0, p.get("projection") or 0) for p in active)
            mass = sum(max(0, p.get("projection") or 0) for p in covered) / total if total else 0
            fraction = len(covered) / len(active) if active else 0
            expected = 900 if fmt == "classic" else 100 if slot == "CPT" else 500
            measured = sum(v for (pid, s), v in labels.items() if s == slot)
            tolerance = 135 if fmt == "classic" else 15 if slot == "CPT" else 75
            coverage[slot] = {"players": fraction, "projectionMass": mass, "slotTotalPct": measured}
            valid &= fraction >= .95 and mass >= .99 and abs(measured - expected) <= tolerance
        if not valid:
            exclusions.append({"contestId": contest["contest_id"], "slateId": slate_id,
                               "reason": "label coverage/mass/slot totals failed", "coverage": coverage})
            continue
        pairs = {key: (predictions[key], value) for key, value in labels.items() if key in predictions}
        by_slate[(fmt, slate_id)].append(pairs)
        folds.append({"contestId": contest["contest_id"], "slateId": slate_id, "format": fmt,
                      "decisionAt": cutoff, "trainedContests": model["training_contests"],
                      "trainingDigest": model["training_digest"], "coverage": coverage})
    reports = {}
    for fmt in ("classic", "showdown"):
        slate_ids = sorted(key[1] for key in by_slate if key[0] == fmt)
        # Average contests within a slate before pooling player labels.
        pooled = []
        for slate_id in slate_ids:
            pairs = defaultdict(list)
            for contest_pairs in by_slate[(fmt, slate_id)]:
                for key, pair in contest_pairs.items():
                    pairs[key].append(pair)
            pooled.extend(np.mean(values, axis=0).tolist() for key, values in sorted(pairs.items()))
        metrics = grade_pairs(pooled)
        accuracy = metrics["spearman"] is not None and metrics["spearman"] >= .70 and metrics["maePp"] <= 2.0 and abs(metrics["biasPp"]) <= .5
        passed = fmt == "classic" and len(slate_ids) >= 4 and accuracy
        reasons = []
        if fmt != "classic":
            reasons.append("Showdown needs its own registered accuracy gate; Classic qualification cannot authorize Captain/Flex leverage.")
        if len(slate_ids) < 4:
            reasons.append(f"Only {len(slate_ids)} independent trained holdout slates; Classic needs four.")
        if not accuracy:
            reasons.append("Pooled accuracy does not meet Spearman >=0.70, MAE <=2.0pp and absolute bias <=0.5pp.")
        reports[fmt] = {"format": fmt, "modelVersion": VERSION, "registration": REGISTRATION,
                        "heldOutSlateIds": slate_ids, "sourceDigest": digest(contests), **metrics,
                        "status": "qualified" if passed else "withheld", "reasons": reasons,
                        "leverageEnabled": passed, "duplicationValidated": False}
    return seal({"kind": "qualification", "version": "nfl-ownership-qualification-v1", "formats": reports,
                 "folds": folds, "excluded": exclusions, "productionChanged": False,
                 "policy": "Frozen chronological fit; all contests on a slate stay together. Null labels are not zero."})
