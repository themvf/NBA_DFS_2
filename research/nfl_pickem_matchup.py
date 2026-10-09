"""Fit a retrospective development residual and freeze future pick'em shadows.

2023-2025 are development data, never an executable historical backtest.
The fixed ridge-20/no-intercept model is evaluated prospectively only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from scipy.optimize import minimize
from scipy.special import expit

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_matchup_context import load_matchups
from model.nfl_context_engine import stable_digest
from model.nfl_matchup_projection import feature_vector
from model.nfl_pfr_supplement import team_code

VERSION = "pickem-matchup-residual-shadow-v1"
KEYS = ("own_pressure", "opp_pressure", "own_ybc", "opp_ybc", "own_yac", "opp_yac")
DEFINITIONS = {k: f"pickem_home_minus_away_{k}@v1" for k in KEYS}
MODEL_PATH = Path("artifacts/nfl_pickem_matchup_model_v1.json")
REGISTRATION_PATH = Path("artifacts/nfl_matchup_registrations/nfl-matchup-pickem-combined-v1.json")
DDL = """CREATE TABLE IF NOT EXISTS nfl_pickem_matchup_forecasts (
 forecast_id TEXT PRIMARY KEY, game_id TEXT NOT NULL, available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 decision_cutoff TIMESTAMPTZ NOT NULL, model_version TEXT NOT NULL, payload JSONB NOT NULL);
 CREATE INDEX IF NOT EXISTS nfl_pickem_matchup_forecasts_game ON nfl_pickem_matchup_forecasts(game_id,available_at DESC);
"""


def no_vig(home, away):
    if home is None or away is None or abs(float(home)) < 100 or abs(float(away)) < 100:
        return None
    def implied(x):
        x = float(x)
        return -x / (-x + 100) if x < 0 else 100 / (100 + x)
    h, a = implied(home), implied(away)
    return h / (h + a)


def eligible_quote(captured_at, cutoff, kickoff):
    if captured_at is None or not captured_at <= cutoff < kickoff:
        return False
    max_age = 7200 if (kickoff-cutoff).total_seconds() <= 86400 else 86400
    return (cutoff-captured_at).total_seconds() <= max_age


def paired_probabilities(probability, tie, residual):
    if not all(np.isfinite(v) for v in (probability, tie, residual)) or not 0 < probability < 1 or not 0 <= tie <= 1:
        raise ValueError("Invalid paired probability inputs")
    candidate_p = probability if residual == 0 else float(expit(np.log(probability/(1-probability))+residual))
    baseline = {"home":(1-tie)*probability,"away":(1-tie)*(1-probability),"tie":tie}
    return baseline, {"home":(1-tie)*candidate_p,"away":(1-tie)*(1-candidate_p),"tie":tie,"homeConditional":candidate_p}


def require_model_before_cutoff(model, cutoff):
    if datetime.fromisoformat(model["fittedAt"]) > cutoff or datetime.fromisoformat(model["trainedThrough"].replace("Z", "+00:00")) >= cutoff:
        raise ValueError("Model fit and training outcomes must precede the prospective decision cutoff")


def fit_offset(x, outcome, market, ridge=20.0):
    """Fixed-scale ridge logistic regression with market logit offset, no intercept."""
    x = np.asarray(x, dtype=float)
    outcome, market = np.asarray(outcome, dtype=float), np.asarray(market, dtype=float)
    if len(outcome) < 100 or x.shape != (len(outcome), len(KEYS)) or not np.isfinite(x).all():
        raise ValueError("At least 100 complete development games required")
    # Explicit unit scales, not fitted on held-out labels: pressure percentage
    # points / 10, contact yards per carry / 1.
    scales = np.asarray([10., 10., 1., 1., 1., 1.])
    design = x / scales
    offset = np.log(np.clip(market, 1e-6, 1-1e-6) / np.clip(1-market, 1e-6, 1-1e-6))
    def objective(beta):
        logits = offset + design @ beta
        loss = np.sum(np.logaddexp(0, logits) - outcome * logits) + ridge / 2 * np.sum(beta ** 2)
        gradient = design.T @ (expit(logits) - outcome) + ridge * beta
        return float(loss), gradient
    result = minimize(objective, np.zeros(len(KEYS)), jac=True, method="L-BFGS-B")
    if not result.success or not np.isfinite(result.x).all():
        raise ValueError(f"Residual fit did not converge: {result.message}")
    return {k: float(v) for k, v in zip(KEYS, result.x / scales)}


def fit_model(db, feature_path):
    raw = json.loads(feature_path.read_text(encoding="utf-8"))
    rows = raw.get("rows", raw.get("games", [])) if isinstance(raw, dict) else raw
    by_game = {}
    for r in rows:
        by_game.setdefault(r["game_id"], {})[team_code(r["team"])] = r
    games = db.execute("""SELECT g.nflverse_game_id game_id,g.season,g.week,g.kickoff,g.home_score,g.away_score,
        g.quoted_home_ml,g.quoted_away_ml,h.abbreviation home,a.abbreviation away
        FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
        JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season BETWEEN 2023 AND 2025 AND g.game_type='REG' AND g.completed=TRUE""")
    x, y, p, ids = [], [], [], []
    ties = sum(g["home_score"] == g["away_score"] for g in games if g["home_score"] is not None and g["away_score"] is not None)
    scored = sum(g["home_score"] is not None and g["away_score"] is not None for g in games)
    for g in games:
        pair = by_game.get(g["game_id"], {})
        home, away = pair.get(team_code(g["home"])), pair.get(team_code(g["away"]))
        market = no_vig(g["quoted_home_ml"], g["quoted_away_ml"])
        if not home or not away or market is None or g["home_score"] is None or g["away_score"] is None or g["home_score"] == g["away_score"]:
            continue
        if any(home.get(k) is None or away.get(k) is None for k in KEYS):
            continue
        if any(int(r["season"]) != int(g["season"]) or int(r["week"]) != int(g["week"]) for r in (home, away)):
            raise ValueError("Development feature/schedule season or week mismatch")
        values = [float(home[k])-float(away[k]) for k in KEYS]
        if not np.isfinite(values).all():
            continue
        x.append(values); y.append(int(g["home_score"] > g["away_score"])); p.append(market); ids.append(g["game_id"])
    coefficients = fit_offset(x, y, p)
    manifest = {"version": VERSION, "status": "development_fitted_shadow_only", "trainedThrough": max(g["kickoff"] for g in games).isoformat(),
        "developmentSeasons": [2023, 2024, 2025], "n": len(x), "featureKeys": list(KEYS), "coefficients": coefficients,
        "intercept": 0, "ridge": 20, "featureUnits": ["percentage_points"]*2+["yards_per_carry"]*4,
        "tieProbability": ties/scored if scored else None, "tieModel": {"version": "training-empirical-ties-v1", "ties": ties, "games": scored},
        "trainingManifest": {"availability": "retrospective_development_only", "feature_file": str(feature_path),
          "feature_digest": stable_digest(raw), "market_digest": stable_digest(games), "game_ids": ids,
          "market_source": "nfl_season_games.quoted_home_ml/quoted_away_ml; historical capture chronology unproven",
          "scope": "Strictly prior game features; revised charting/current identity mapping; all 2023-2025 outcomes used for development",
          "holdout": "none; prospective study starts only after a real pregame freeze", "productionAuthority": "none"},
        "fittedAt": datetime.now(timezone.utc).isoformat()}
    manifest["artifactId"] = stable_digest(manifest)
    manifest["definitionId"] = f"pickem_matchup_combined:{manifest['artifactId']}"
    MODEL_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def matchup_values(matchup):
    values = {}
    for team in (matchup["home"], matchup["away"]):
        pressure = feature_vector(matchup, team, "pressure")
        contact = feature_vector(matchup, team, "contact")
        if pressure is None or contact is None:
            return None
        values[team] = {**pressure, **contact}
    return {k: values[matchup["home"]][k]-values[matchup["away"]][k] for k in KEYS}


def current_quotes(db, season, week, cutoff):
    return db.execute("""SELECT g.nflverse_game_id game_id, q.home_ml,q.away_ml,q.captured_at,q.quote_id,q.quote_source
      FROM nfl_season_games g LEFT JOIN LATERAL (
        SELECT home_ml,away_ml,captured_at,quote_id,quote_source FROM (
          SELECT home_ml,away_ml,captured_at,id::text quote_id,'game_odds_history'::text quote_source FROM game_odds_history
          WHERE sport='nfl' AND matchup_id=g.matchup_id AND captured_at<=%s AND captured_at<g.kickoff
          UNION ALL SELECT g.market_home_ml,g.market_away_ml,g.market_captured_at,
            g.id::text || ':' || g.market_captured_at::text,'nfl_season_games.market'::text
          WHERE g.market_captured_at<=%s AND g.market_captured_at<g.kickoff
        ) x WHERE ABS(home_ml)>=100 AND ABS(away_ml)>=100 ORDER BY captured_at DESC,quote_source,quote_id DESC LIMIT 1
      ) q ON TRUE WHERE g.season=%s AND g.week=%s AND g.kickoff>%s""", (cutoff, cutoff, season, week, cutoff))


def freeze_forecasts(db, model, season, week, persist=False):
    cutoff = datetime.now(timezone.utc)
    require_model_before_cutoff(model, cutoff)
    registration = None
    if REGISTRATION_PATH.exists():
        registration = json.loads(REGISTRATION_PATH.read_text(encoding="utf-8"))
        study_index = json.loads(Path("docs/nfl-matchup-studies.json").read_text(encoding="utf-8"))
        studies = study_index.get("studies", [])
        if isinstance(studies, dict):
            studies = list(studies.values())
        study_entry = next((s for s in studies if s.get("study_id") == registration["study_id"]), {})
        pin_path = Path(study_entry["implementation_pin_file"]) if study_entry.get("implementation_pin_file") else REGISTRATION_PATH.with_name(REGISTRATION_PATH.stem+".implementation-pin.json")
        pin = json.loads(pin_path.read_text(encoding="utf-8")) if pin_path.exists() else {}
        actual_hashes = {path:hashlib.sha256(Path(path).read_bytes().replace(b"\r\n",b"\n")).hexdigest() for path in pin.get("hashes",{})}
        if registration["candidate_config_hash"] != model["artifactId"] or not actual_hashes or actual_hashes != pin.get("hashes"):
            raise ValueError("Registration model/code pin mismatch; create a new approved implementation pin before freezing")
        if datetime.fromisoformat(registration["registered_at"].replace("Z", "+00:00")) > cutoff:
            raise ValueError("Registration must precede the prospective freeze")
        registration = {"study_id":registration["study_id"],"baseline_config_hash":registration["baseline_config_hash"],
            "candidate_config_hash":registration["candidate_config_hash"],"implementation_hashes":actual_hashes,
            "registration_manifest_hash":pin.get("registration_manifest_hash"),"registered_at":registration["registered_at"]}
    elif persist:
        raise ValueError("Prospective registration and implementation pin are required before persistence")
    matchups = load_matchups(db, season, week, cutoff)
    quotes = {r["game_id"]: r for r in current_quotes(db, season, week, cutoff)}
    forecasts = []
    for game_id, matchup in matchups.items():
        q = quotes.get(game_id) or {}; probability = no_vig(q.get("home_ml"), q.get("away_ml"))
        features = matchup_values(matchup)
        reasons = []
        if probability is None or q.get("captured_at") is None:
            reasons.append("Missing contemporaneous two-sided market quote")
        elif not eligible_quote(q["captured_at"], cutoff, datetime.fromisoformat(matchup["kickoff"])):
            reasons.append("Market quote is stale at cutoff")
        if features is None:
            reasons.append("Complete pressure/contact matchup features unavailable")
        tie = model["tieProbability"]
        input_ = {"gameId":game_id,"decisionCutoff":cutoff.isoformat(),"kickoff":matchup["kickoff"],
          "baseline":{"homeConditional":probability,"tie":tie,"marketCapturedAt":q["captured_at"].isoformat() if q.get("captured_at") else None,
            "source":"market_ml_novig","quoteId":q.get("quote_id"),"quoteSource":q.get("quote_source"),"homeMoneyline":q.get("home_ml"),"awayMoneyline":q.get("away_ml")},
          "model":{"artifactId":model["artifactId"],"definitionId":model["definitionId"],"version":VERSION,"consumerId":"nfl-pickem",
            "useCase":"game-win","cohort":"regular-season","coefficients":{DEFINITIONS[k]:v for k,v in model["coefficients"].items()},
            "intercept":0,"trainedThrough":model["trainedThrough"],"trainingManifest":model["trainingManifest"]},
          "features":[{"definitionId":DEFINITIONS[k],"value":v,"snapshotId":matchup["manifest_hash"]+":"+k,
            "availableAt":cutoff.isoformat(),"sourceManifest":matchup} for k,v in (features or {}).items()]}
        baseline = candidate = None; residual = None
        market_eligible = probability is not None and eligible_quote(q.get("captured_at"), cutoff, datetime.fromisoformat(matchup["kickoff"]))
        if market_eligible and tie is not None:
            residual = sum(features[k]*model["coefficients"][k] for k in KEYS) if features is not None else 0.0
            baseline, candidate = paired_probabilities(probability, tie, residual)
        covered = candidate is not None and features is not None
        forecast = {"input":input_,"status":"shadow" if covered else "fallback" if candidate else "unavailable", "covered":covered,"baseline":baseline,"candidate":candidate,
          "residual":residual,"reasons":reasons+["Retrospective development fit; no forward qualification or active forecast change"],
          "tieModel":model["tieModel"],"featureManifest":matchup,"forecastBuiltAt":datetime.now(timezone.utc).isoformat(),
          **(registration or {})}
        forecast["forecastId"] = stable_digest(forecast); forecasts.append(forecast)
    if persist:
        from psycopg2.extras import Json
        db.execute(DDL)
        with db.connect() as conn:
            cur=conn.cursor()
            for key, definition_id in DEFINITIONS.items():
                cur.execute("""INSERT INTO nfl_context_definitions
                  (definition_id,context_key,version,unit,description,definition)
                  VALUES (%s,%s,'v1',%s,%s,%s) ON CONFLICT DO NOTHING""",
                  (definition_id,"pickem_home_minus_away_"+key,"percentage_points" if "pressure" in key else "yards_per_carry",
                   "Prior-four-game home-minus-away matchup residual feature",Json({"featureKey":key,"lookback":4,"population":"prior eligible games","authority":"shadow_only"})))
            cur.execute("""INSERT INTO nfl_context_definitions
              (definition_id,context_key,version,unit,description,definition)
              VALUES (%s,'pickem_matchup_combined',%s,'log_odds',%s,%s) ON CONFLICT DO NOTHING""",
              (model["definitionId"],model["artifactId"],"Fitted market-offset combined matchup residual",Json(model)))
            for f in forecasts:
                if datetime.fromisoformat(f["input"]["kickoff"])<=datetime.now(timezone.utc):
                    continue
                cur.execute("""INSERT INTO nfl_pickem_matchup_forecasts
                    (forecast_id,game_id,decision_cutoff,model_version,payload) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING available_at""",
                    (f["forecastId"],f["input"]["gameId"],cutoff,VERSION,Json(f)))
                stored = cur.fetchone()
                if stored:
                    f["available_at"] = stored["available_at"].isoformat()
    return {"version":VERSION,"season":season,"week":week,"decisionCutoff":cutoff.isoformat(),"model":model,
            "forecasts":forecasts,"productionChanged":False,"persisted":persist}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season",type=int,required=True);parser.add_argument("--week",type=int,required=True)
    parser.add_argument("--fit",action="store_true");parser.add_argument("--persist",action="store_true")
    parser.add_argument("--fit-only",action="store_true")
    parser.add_argument("--development-features",type=Path,default=Path("artifacts/nfl_matchup_development_games.json"))
    parser.add_argument("--output",type=Path,default=Path("artifacts/nfl-matchup-implementation/2026-09-27/pickem-shadow.json"))
    args=parser.parse_args();db=DatabaseManager(load_config().database_url,initialize_schema=False)
    with db.reuse_connection():
        model=fit_model(db,args.development_features) if args.fit else json.loads(MODEL_PATH.read_text(encoding="utf-8"))
        if args.fit_only:
            print(json.dumps({"development_games":model["n"],"artifact_id":model["artifactId"],"model_path":str(MODEL_PATH)}))
            return
        artifact=freeze_forecasts(db,model,args.season,args.week,args.persist)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(artifact,indent=2),encoding="utf-8")
    print(json.dumps({"development_games":model["n"],"forecasts":len(artifact["forecasts"]),
        "numerical_candidates":sum(f["candidate"] is not None for f in artifact["forecasts"]),"production_changed":False,"output":str(args.output)}))


if __name__ == "__main__":
    main()
