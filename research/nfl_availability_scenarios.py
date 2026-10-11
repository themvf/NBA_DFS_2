"""Freeze conditional workload/coherent banks locally without production writes.

Input: forecasts, evidence, history (point-in-time opportunity rows), decision_at.
Optional coherent_input: the keyword inputs to build_coherent_banks, including
complete historical team event blocks and the exact salary pool. Input hashes
and unweighted conditional states are retained. Not a new pregame forecast
adapter: reconstructing a past snapshot from today's roster is prohibited.
"""
import argparse
from copy import deepcopy
import json
from pathlib import Path
from model.nfl_availability_scenarios import VERSION, workload_scenarios
from model.nfl_context_engine import stable_digest


def build_report(payload):
    risk_report=None
    factors={}
    if payload.get('workload_risk_input'):
        from research.nfl_workload_risk import build_report as risk_build_report
        from model.nfl_availability_scenarios import stamp
        if any(stamp(row['decision_at'])!=stamp(payload['decision_at']) for row in payload['workload_risk_input'].get('forecast_cases',[])):
            raise ValueError('Risk and workload decision cutoffs differ')
        risk_report=risk_build_report(payload['workload_risk_input'])
        uncertain={(str(row['identity']),str(row['game_id'])) for row in payload['evidence'] if row['state'] in ('QUESTIONABLE','DOUBTFUL')}
        seen=set()
        for row in risk_report['forecasts']:
            key=(str(row['player_id']),str(row['game_id']))
            if key not in uncertain:raise ValueError('Risk forecast does not match a current uncertain player/game')
            if key in seen:raise ValueError('Duplicate workload-risk forecast')
            seen.add(key)
            if row.get('limited_workload_factor') is not None:factors[str(row['player_id'])]=row['limited_workload_factor']
    scenarios=workload_scenarios(payload["forecasts"],payload["evidence"],payload["history"],payload["decision_at"],limited_factors=factors)
    if payload.get("coherent_input"):
        from model.nfl_matchup_scenarios import build_coherent_banks
        for scenario in scenarios:
            kwargs=deepcopy(payload["coherent_input"])
            if kwargs["decision_at"] != payload["decision_at"]:
                raise ValueError("Workload and coherent decision cutoffs differ")
            kwargs["forecasts"]=scenario["forecasts"]
            kwargs["source_manifest"]={**kwargs["source_manifest"],"availability_candidate":VERSION,
                                       "availability_input_digest":stable_digest(payload),"conditional_state":scenario["id"]}
            banks=build_coherent_banks(**kwargs)
            banks["version"]="nfl-coherent-availability-v1"
            for name in ("selection","evaluation"):
                banks[name]["modelVersion"]=banks["version"]
            banks["manifest"]["research_registration"]="nfl-availability-workload-v1"
            banks["manifest"]["authority"]="shadow_only"
            banks["limitations"].append("Unweighted conditional injury state, not a calibrated participation/workload mixture.")
            scenario["joint_banks"]=banks
    return {"version":VERSION,"input_digest":stable_digest(payload),"decision_at":payload["decision_at"],
            "production_changed":False,"optimizer_enabled":False,"scenarios":scenarios,'workload_risk':risk_report,
            "qualification":"withheld_pending_registered_forward_evidence"}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",required=True)
    parser.add_argument("--output",required=True)
    args=parser.parse_args(argv)
    payload=json.loads(Path(args.input).read_text(encoding="utf-8-sig"))
    report=build_report(payload)
    with Path(args.output).open("x",encoding="utf-8") as handle:
        json.dump(report,handle,indent=2,allow_nan=False)
    print(json.dumps({"output":args.output,"scenarios":len(report["scenarios"]),"optimizer_enabled":False}))


if __name__ == "__main__": main()
