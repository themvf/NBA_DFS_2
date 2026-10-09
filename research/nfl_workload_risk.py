"""Fit and grade frozen Q/D workload cases; never publish live projections.

Each held-out game uses only labels available at its pregame decision. Cases
from the same game stay together. Prepared point-in-time cases are mandatory;
today's injury status must not be used to reconstruct old decisions.
"""
import argparse
import json
import math
from pathlib import Path
from model.nfl_ownership import digest, stamp
from model.nfl_workload_risk import VERSION, fit, predict, outcome


def evaluate(cases):
    # Also validates identities, timestamps and labels before any exclusion.
    fit(cases, "1900-01-01T00:00:00Z")
    records, unavailable = [], []
    for case in sorted(cases, key=lambda row: (stamp(row['decision_at']), str(row['game_id']), str(row['player_id']))):
        earlier = [r for r in cases if r['game_id'] != case['game_id']
                   and stamp(r['labels_available_at']) <= stamp(case['decision_at'])
                   and stamp(r['kickoff']) < stamp(case['decision_at'])]
        model = fit(earlier, case['decision_at'])
        prediction = predict(model, case)
        if prediction['probabilities'] is None:
            unavailable.append({'player_id':case['player_id'],'game_id':case['game_id'],'reason':'insufficient earlier training'})
            continue
        observed = outcome(case)
        group = [r for r in earlier if r['position']==case['position'] and r['designation']==case['designation']]
        if not group:
            unavailable.append({'player_id':case['player_id'],'game_id':case['game_id'],'reason':'no earlier position/designation baseline'})
            continue
        baseline = [(sum(outcome(r)==state for r in group)+1)/(len(group)+3) for state in range(3)]
        probabilities = list(prediction['probabilities'].values())
        participating = float(observed!=0)
        probability = 1-probabilities[0]
        reference = 1-baseline[0]
        records.append({'player_id':case['player_id'],'game_id':case['game_id'],'decision_at':case['decision_at'],
                        'position':case['position'],'designation':case['designation'],
                        'training_digest':model['training_digest'],'training_cases':len(earlier),'outcome':observed,
                        'probabilities':prediction['probabilities'],
                        'participation_probability':probability,'participated':participating,
                        'participation_brier':(probability-participating)**2,
                        'baseline_participation_brier':(reference-participating)**2,
                        'participation_log_loss':-math.log(max(1e-12,probability if participating else 1-probability)),
                        'baseline_participation_log_loss':-math.log(reference if participating else 1-reference),
                        'brier':sum((p-float(state==observed))**2 for state,p in enumerate(probabilities)),
                        'baseline_brier':sum((p-float(state==observed))**2 for state,p in enumerate(baseline)),
                        'log_loss':-math.log(max(1e-12,probabilities[observed])),
                        'baseline_log_loss':-math.log(baseline[observed])})
    metrics = {key:sum(r[key] for r in records)/len(records) if records else None
               for key in ('brier','baseline_brier','log_loss','baseline_log_loss','participation_brier',
                           'baseline_participation_brier','participation_log_loss','baseline_participation_log_loss')}
    bins=[]
    for bucket in range(10):
        members=[r for r in records if min(9,int(r['participation_probability']*10))==bucket]
        if members:bins.append({'bucket':bucket,'count':len(members),
                                'predicted':sum(r['participation_probability'] for r in members)/len(members),
                                'observed':sum(r['participated'] for r in members)/len(members)})
    metrics['participation_ece']=sum(b['count']*abs(b['predicted']-b['observed']) for b in bins)/len(records) if records else None
    return {'version':VERSION,'input_digest':digest(cases),'records':records,'unavailable':unavailable,
            'metrics':metrics,'participation_reliability':bins,'held_out_player_games':len(records),'held_out_games':len({r['game_id'] for r in records}),
            'qualification':'withheld','optimizer_enabled':False,
            'reasons':['Historical walk-forward results cannot replace the registered eight prospective weeks.',
                       'Participation and limited-workload calibration need separate qualification.']}


def build_report(payload):
    model=fit(payload['cases'],payload['as_of'])
    forecasts=[{'player_id':row['player_id'],'game_id':row['game_id'],**predict(model,row)} for row in payload.get('forecast_cases',[])]
    return {'version':VERSION,'input_digest':digest(payload),'model':model,'forecasts':forecasts,
            'evaluation':evaluate(payload['cases']),'production_changed':False,'optimizer_enabled':False}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args(argv)
    report=build_report(json.loads(Path(args.input).read_text(encoding='utf-8-sig')))
    with Path(args.output).open('x',encoding='utf-8') as handle:json.dump(report,handle,indent=2,allow_nan=False)
    print(json.dumps({'output':args.output,'forecast_cases':len(report['forecasts']),'qualification':'withheld','optimizer_enabled':False}))


if __name__=='__main__':main()
