"""Recheck saved chronological predictions for an opt-in optimizer release.

This does not refit, promote the production model, or relabel 2025 as untouched.
Both point error and interval score must improve; mean gains alone are not enough.

The study is the one the shadow job is pinned to
(`artifacts/nfl_dfs_shadow_config.json`), never a hard-coded path. Until
2026-09-29 this file named study 8bab9091 directly, so after the shadow job was
re-pinned (twice) the page kept reading a study that no longer froze any
forecasts and the calibrated source silently produced nothing for week 3.
A release now names the same study the shadow job writes, and a test asserts
the two agree.
"""
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
from model.nfl_dfs_historical import artifact_digest
from model.nfl_dfs_variance import interval_score

ROOT = Path(__file__).resolve().parents[1]
SHADOW_CONFIG = ROOT / "artifacts/nfl_dfs_shadow_config.json"
RELEASE_PATH = ROOT / "web/src/lib/nfl-dfs/calibrated-release.json"
# v2: generated from the shadow pin; records each position's study candidate
# status so the page can say why a position has no forecast.
RELEASE_VERSION = "nfl-dfs-calibrated-opt-in-v2"


def pinned_study(config_path: Path = SHADOW_CONFIG) -> tuple[Path, dict]:
    """The study directory and report the shadow job is pinned to, verified."""
    config = json.loads(config_path.read_text())
    report_path = ROOT / config["report"]
    report = json.loads(report_path.read_text())
    if report["run_id"] != config["study_run_id"] or report["output_digest"] != config["output_digest"]:
        raise ValueError("Shadow config and study report disagree; refusing to build a release")
    return report_path.parent, report


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
        shadow_candidate = candidate["status"] == "eligible_for_shadow_only"
        positions[position] = {"enabledForOptIn": qualifies and shadow_candidate, "recipeDigest": artifact_digest(candidate["recipe"]),
                               "shadowCandidate": shadow_candidate, "studyCandidateStatus": candidate["status"],
                               "releaseMetricsQualify": qualifies, "seasons": seasons}
    return {"version": RELEASE_VERSION, "studyId": report["run_id"], "studyDigest": report["output_digest"], "positions": positions,
            "generatedFrom": "artifacts/nfl_dfs_shadow_config.json",
            "productionDefaultPromotion": False, "evidence": "2023 fit, 2024 selection, 2025 retrospective diagnostic; fresh forward validation pending",
            "rule": "Opt-in only: the pinned study marks the opportunity recipe eligible_for_shadow_only, >=100 paired rows per split, lower MAE, interval score and boom Brier in 2024 and 2025. Others retain the chosen fallback.",
            "limits": ["Not a DK slate profitability backtest", "Benchmark disables market inputs; not archived live projections", "No current injury or roster counterfactual adjustment", "Player marginals are not a joint lineup distribution", "Previously inspected 2025 is not an untouched holdout"]}


def build_release(config_path: Path = SHADOW_CONFIG) -> dict:
    study, report = pinned_study(config_path)
    content = (study / "predictions.json.gz").read_bytes()
    result = evaluate(json.loads(gzip.decompress(content)), report)
    result["predictionsDigest"] = hashlib.sha256(content).hexdigest()
    return result


if __name__ == "__main__":
    result = build_release()
    RELEASE_PATH.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({p: {"enabled": v["enabledForOptIn"], "study_status": v["studyCandidateStatus"], "2025": v["seasons"]["2025"]} for p, v in result["positions"].items()}, indent=2))
