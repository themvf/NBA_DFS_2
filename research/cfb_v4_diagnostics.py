"""Arithmetic decomposition of frozen scoring errors; no causal or refitting claim."""
from __future__ import annotations


def decompose(expected_drives,predicted_ppd,predicted_other,actual_drives,actual_offensive_points,final_points):
    if actual_drives<=0:return None
    drive_error=(actual_drives-expected_drives)*predicted_ppd
    efficiency_error=actual_offensive_points-actual_drives*predicted_ppd
    other_error=final_points-actual_offensive_points-predicted_other
    predicted=expected_drives*predicted_ppd+predicted_other
    return {"expected_drives":expected_drives,"actual_drives":actual_drives,
            "predicted_ppd":predicted_ppd,"actual_offensive_ppd":actual_offensive_points/actual_drives,
            "possession_contribution":drive_error,"efficiency_contribution":efficiency_error,
            "other_scoring_contribution":other_error,"actual_minus_predicted":final_points-predicted,
            "identity_error":drive_error+efficiency_error+other_error-(final_points-predicted),
            "interpretation":"Arithmetic attribution only; turnovers, field position and efficiency interact."}


def legacy_audit(db,audits):
    by_game={a["game_id"]:a for a in audits}
    ids=[g for g in (453,454,455,456,457,458) if g in by_game]
    if not ids:return []
    rows=db.execute("""SELECT DISTINCT ON(f.game_id) f.*,r.generated_at,r.version,
        r.report_json->'explanations'->(f.game_id::text) explanation,m.home_team_id,m.away_team_id
        FROM cfb_game_forecasts f JOIN cfb_forecast_runs r ON r.id=f.run_id JOIN cfb_matchups m ON m.id=f.game_id
        WHERE f.game_id=ANY(%s) AND r.version='cfb-score-possession-v2' AND r.generated_at<f.kickoff
        ORDER BY f.game_id,r.generated_at DESC,f.id DESC""",(ids,))
    output=[]
    for row in rows:
        a=by_game[row["game_id"]];e=row["explanation"] or {}
        result={"game_id":row["game_id"],"forecast_id":row["id"],"forecast_at":row["generated_at"],"version":row["version"],
                "source_audit_usable":a["usable"],"issues":a["issues"],"teams":{}}
        for side in ("home","away"):
            s=a["teams"][str(row[side+"_team_id"])];rate=e.get(side+"_predicted_ppd");drives=e.get(side+"_expected_drives")
            if a["usable"] and rate is not None and drives is not None:
                d=decompose(drives,rate,0.,s["source_drives"],s["regulation_offensive_points"]+s["overtime_offensive_points"],s["final_points"])
            else:d=None
            result["teams"][side]={"actual_audit":s,"decomposition":d}
        output.append(result)
    return output
