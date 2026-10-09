"""Paired forward grading for the separately registered coherent distribution."""
from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
import numpy as np

from model.nfl_dfs_context_variant_study import timestamp, finite, paired_interval
from model.nfl_matchup_study import digest, implementation_digest

VERSION = "nfl-coherent-distribution-grade-v1"
QUANTILES = ("p10","p25","median","p75","p90")


def distribution_metrics(value, actual, boom_threshold):
    if not isinstance(value,dict) or not finite(actual) or not all(finite(value.get(k)) for k in (*QUANTILES,"mean","boom_probability")):
        raise ValueError("missing exact paired quantiles or mean/boom forecast")
    q = [value[k] for k in QUANTILES]
    if q != sorted(q) or not 0 <= value["boom_probability"] <= 1:
        raise ValueError("invalid paired distribution")
    intervals = {}
    for alpha,low,high in ((.5,"p25","p75"),(.2,"p10","p90")):
        width = value[high]-value[low]
        score = width+2/alpha*(max(value[low]-actual,0)+max(actual-value[high],0))
        intervals[alpha] = score
    wis = (.5*abs(actual-value["median"])+.25*intervals[.5]+.1*intervals[.2])/2.5
    return {"wis":wis,"mae":abs(actual-value["mean"]),
            "coverage50":float(value["p25"]<=actual<=value["p75"]),
            "coverage80":float(value["p10"]<=actual<=value["p90"]),
            "width50":value["p75"]-value["p25"],"width80":value["p90"]-value["p10"],
            "boom_brier":(value["boom_probability"]-int(actual>=boom_threshold))**2}


def bootstrap_tail(rows, draws, seed):
    """Two-sided centered week bootstrap p-value for the improvement direction."""
    groups = defaultdict(list)
    for row in rows:
        groups[(row["season"],row["week"])].append(row["delta"])
    sums = np.array([sum(values) for values in groups.values()])
    sizes = np.array([len(values) for values in groups.values()])
    observed = float(sums.sum()/sizes.sum())
    indices = np.random.default_rng(seed).integers(0,len(sums),size=(draws,len(sums)))
    estimates = sums[indices].sum(axis=1)/sizes[indices].sum(axis=1)
    if observed >= 0:
        return 1.
    return min(1.,2*(1+int(np.sum(estimates-observed<=observed)))/(draws+1))


def grade_coherent(manifest, records, *, now, complete_weeks, cohort=None):
    """Never interpolate absent baseline quantiles or combine code/model pins."""
    report = {"version":VERSION,"study_id":manifest.get("study_id"),"verdict":"NO_VERDICT",
              "production_promotion":False,"registered_manifest_hash":digest(manifest),
              "grader_implementation_hash":implementation_digest(Path(__file__).read_bytes()),
              "evaluated_at":timestamp(now).isoformat(),"cohort":cohort,"rejected":{},"positions":{},"frozen_rows":0,"scored_rows":0}
    report["scoring_contract"]={"scenario":manifest.get("scoring_version"),"outcome":manifest.get("outcome_scoring_version",manifest.get("scoring_version"))}
    required = ("registered_at","evaluation_end_at","positions","seed","bootstrap_draws","draws",
                "scoring_version","implementation_hashes","model_version","forward_start","boom_thresholds","baseline_config_hash")
    missing = [key for key in required if not manifest.get(key)]
    if missing:
        report.update(reason="incomplete coherent registration",registration_errors=missing)
        return report
    positions = manifest["positions"]
    if cohort is not None:
        declaration = manifest.get("cohorts",{}).get(cohort)
        if not declaration:
            report.update(reason="unregistered coherent format cohort")
            return report
        positions = declaration["positions"]
    if manifest["bootstrap_draws"]<2000 or manifest["draws"]<100:
        report.update(reason="invalid registered simulation/bootstrap draw count")
        return report
    now = timestamp(now)
    registered,end = timestamp(manifest["registered_at"]),timestamp(manifest["evaluation_end_at"])
    first = (manifest["forward_start"]["season"],manifest["forward_start"]["first_full_week"])
    complete = {tuple(w) for w in complete_weeks}
    selected,rejected = {},Counter()
    for row in records:
        try:
            if cohort is not None and row.get("format") != cohort:
                raise ValueError("other registered format cohort")
            if row["model_version"] != manifest["model_version"] or row["registration_hash"] != digest(manifest):
                raise ValueError("different coherent registration/model")
            if row["implementation_hashes"] != manifest["implementation_hashes"] or row["draws"] != manifest["draws"]:
                raise ValueError("coherent implementation or draw configuration mismatch")
            if row.get("baseline_config_hash") != manifest["baseline_config_hash"]:
                raise ValueError("coherent baseline configuration mismatch")
            if row.get("baseline_identity_valid") is False:
                raise ValueError("coherent row/source baseline identity mismatch")
            capture,available,kickoff,published = (timestamp(row[k]) for k in ("captured_at","available_at","kickoff","published_at"))
            if not available <= capture or not registered <= capture <= published < kickoff < end or published>now:
                raise ValueError("ineligible coherent pregame provenance")
            if (row["season"],row["week"]) < first:
                raise ValueError("before first full registered forward week")
            if row["position"] not in positions or row["scoring_version"] != manifest.get("outcome_scoring_version",manifest["scoring_version"]):
                raise ValueError("position or exact scoring version differs")
            source_hash=row.get("input_manifest_hash","")
            if not isinstance(source_hash,str) or len(source_hash)!=64 or any(c not in "0123456789abcdef" for c in source_hash) or not row.get("baseline_run_id") or row.get("retrospective"):
                raise ValueError("missing frozen inputs or retrospective capture")
            key = (row["player_id"],row["game_id"])
            rank = (capture,published,str(row["forecast_id"]))
            if key not in selected or rank>selected[key][0]:
                selected[key] = rank,row
        except (KeyError,ValueError,TypeError) as exc:
            rejected[str(exc)] += 1
    pairs=[]
    for _,row in selected.values():
        try:
            if row.get("scoring_status")!="exact" or row.get("actual") is None:
                raise ValueError("unscored")
            if (row["season"],row["week"]) not in complete:
                raise ValueError("week not completely scorable")
            if not row.get("result_id") or not row.get("result_digest") or not row.get("result_at") or timestamp(row["result_at"])>now:
                raise ValueError("missing immutable available outcome revision")
            if row.get("baseline_reproduced") is not True:
                raise ValueError("exact baseline distribution was not reproduced")
            threshold=manifest["boom_thresholds"][row["position"]]
            if not isinstance(row.get("baseline"),dict) or not isinstance(row.get("candidate"),dict):
                raise ValueError("missing exact paired distribution")
            if any(row[arm].get("boom_threshold") != threshold for arm in ("baseline","candidate")):
                raise ValueError("boom event differs from coherent registration")
            baseline=distribution_metrics(row["baseline"],row["actual"],threshold)
            candidate=distribution_metrics(row["candidate"],row["actual"],threshold)
            pairs.append({**row,"base":baseline,"candidate_metrics":candidate,"delta":candidate["wis"]-baseline["wis"],
                          "mae_delta":candidate["mae"]-baseline["mae"]})
        except (KeyError,ValueError,TypeError) as exc:
            rejected[str(exc)] += 1
    evidence=[{key:row.get(key) for key in ("forecast_id","player_id","game_id","position","season","week",
        "captured_at","published_at","input_manifest_hash","baseline_run_id","result_id","result_digest","result_at")}
        for _,row in sorted(selected.values(),key=lambda item:str(item[1]["forecast_id"]))]
    report.update(frozen_rows=len(selected),frozen_paired_rows=sum(row.get("baseline_reproduced") is True for _,row in selected.values()),scored_rows=len(pairs),rejected=dict(rejected),
                  forecast_evidence=evidence,population_digest=digest(evidence))
    enough=True
    for position in positions:
        sample=[row for row in pairs if row["position"]==position]
        summary={"n":len(sample),"weeks":len({(r["season"],r["week"]) for r in sample}),"games":len({r["game_id"] for r in sample})}
        summary["floor_met"]=summary["n"]>=200 and summary["weeks"]>=8 and summary["games"]>=50
        enough= enough and summary["floor_met"]
        if sample:
            baseline_wis=sum(r["base"]["wis"] for r in sample)/len(sample)
            summary.update(baseline_wis=baseline_wis,primary=paired_interval(sample,draws=manifest["bootstrap_draws"],seed=manifest["seed"]),
                mean_mae=paired_interval(sample,"mae_delta",draws=manifest["bootstrap_draws"],seed=manifest["seed"]),
                bootstrap_p=bootstrap_tail(sample,manifest["bootstrap_draws"],manifest["seed"]),
                distribution_checks={arm:{key:sum(row[field][key] for row in sample)/len(sample) for key in
                    ("coverage50","coverage80","width50","width80","boom_brier")} for arm,field in (("baseline","base"),("candidate","candidate_metrics"))})
        report["positions"][position]=summary
    if not enough:
        report["reason"]="each registered position requires 200 paired player-games, eight complete weeks, and 50 NFL games"
        return report
    if now<end:
        report["reason"]="awaiting fixed coherent evaluation endpoint"
        return report
    ordered=sorted(report["positions"],key=lambda p:report["positions"][p]["bootstrap_p"])
    previous_rejected=True
    for rank,position in enumerate(ordered):
        summary=report["positions"][position]
        alpha=.05/(len(ordered)-rank)
        sample=[row for row in pairs if row["position"]==position]
        summary["holm_alpha"]=alpha
        summary["holm_primary"]=paired_interval(sample,draws=manifest["bootstrap_draws"],seed=manifest["seed"],alpha=alpha)
        summary["holm_reject"]=previous_rejected and summary["bootstrap_p"]<=alpha
        previous_rejected=summary["holm_reject"]
        summary["materiality_pass"]=summary["primary"]["delta"]<=-.01*summary["baseline_wis"]
        summary["passed"]=summary["holm_reject"] and summary["holm_primary"]["ci"][1]<0 and summary["materiality_pass"] and summary["mean_mae"]["ci"][1]<.1
    passed=all(s["passed"] for s in report["positions"].values())
    definite_failure=any(s["primary"]["delta"]>-.01*s["baseline_wis"] or s["mean_mae"]["ci"][0]>=.1 or s["primary"]["ci"][0]>=0 for s in report["positions"].values())
    report.update(verdict="PASS" if passed else "FAIL" if definite_failure else "NO_VERDICT",
                  reason="registered coherent distribution gate passed; production remains unchanged" if passed else
                         "coherent materiality or demonstrated harm gate failed" if definite_failure else "insufficient coherent forecast precision")
    return report
