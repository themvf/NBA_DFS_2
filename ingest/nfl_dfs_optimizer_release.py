"""Recheck saved chronological predictions for an opt-in optimizer release.

This does not refit, promote the production model, or relabel 2025 as untouched.
Both point error and interval score must improve; mean gains alone are not enough.
"""
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
from model.nfl_dfs_historical import artifact_digest
from model.nfl_dfs_variance import interval_score

ROOT = Path(__file__).resolve().parents[1]
STUDY = ROOT / "artifacts/nfl_dfs_research_36cbc63d06d706a9/8bab909112d93a5d"


def measure(rows):
    return {"n": len(rows), "mae": float(np.mean([abs(r["prediction"] - r["actual"]) for r in rows])),
            "intervalScore80": float(np.mean([interval_score(r["actual"], r["p10"], r["p90"]) for r in rows])),
            "coverage80": float(np.mean([r["p10"] <= r["actual"] <= r["p90"] for r in rows])),
            "boomBrier": float(np.mean([(r["boom_probability"] - (r["actual"] >= r["boom_threshold"])) ** 2 for r in rows]))}


def evaluate(rows, report):
    positions = {}
    for position in ["QB", "RB", "WR", "TE", "DST"]:
        seasons = {}
        for season in [2024, 2025]:
            paired = {model: sorted([r for r in rows if r["position"] == position and r["season"] == season and r["model"] == model], key=lambda r: r["sample_key"]) for model in ["baseline", "opportunity"]}
            assert [r["sample_key"] for r in paired["baseline"]] == [r["sample_key"] for r in paired["opportunity"]]
            assert len(paired["baseline"]) >= 100
            seasons[str(season)] = {m: measure(v) for m, v in paired.items()}
        qualifies = all(s["opportunity"][metric] < s["baseline"][metric] for s in seasons.values() for metric in ["mae", "intervalScore80", "boomBrier"])
        candidate = report["candidates"][f"{position}:opportunity"]
        positions[position] = {"enabledForOptIn": qualifies and candidate["status"] == "eligible_for_shadow_only", "recipeDigest": artifact_digest(candidate["recipe"]), "seasons": seasons}
    return {"version": "nfl-dfs-calibrated-opt-in-v1", "studyId": report["run_id"], "studyDigest": report["output_digest"], "positions": positions,
            "productionDefaultPromotion": False, "evidence": "2023 fit, 2024 selection, 2025 retrospective diagnostic; fresh forward validation pending",
            "rule": "Opt-in only: qualified opportunity recipe, >=100 paired rows per split, lower MAE, interval score and boom Brier in 2024 and 2025. Others retain the chosen fallback.",
            "limits": ["Not a DK slate profitability backtest", "Benchmark disables market inputs; not archived live projections", "No current injury or roster counterfactual adjustment", "Player marginals are not a joint lineup distribution", "Previously inspected 2025 is not an untouched holdout"]}


if __name__ == "__main__":
    content = (STUDY / "predictions.json.gz").read_bytes()
    result = evaluate(json.loads(gzip.decompress(content)), json.loads((STUDY / "report.json").read_text()))
    result["predictionsDigest"] = hashlib.sha256(content).hexdigest()
    path = ROOT / "web/src/lib/nfl-dfs/calibrated-release.json"
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({p: {"enabled": v["enabledForOptIn"], "2025": v["seasons"]["2025"]} for p, v in result["positions"].items()}, indent=2))
