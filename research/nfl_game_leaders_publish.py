"""Publish a reviewed batch and its evaluation evidence to the game-leader page."""
import argparse
from pathlib import Path

from model.nfl_game_leaders import METRICS, VERSION
from research.nfl_longest_touchdown import read, write


def publish(batch, evaluations):
    if batch['version']!=VERSION or batch['authority']!='exploratory_not_calibrated' or batch.get('market_inputs_used') is not False:
        raise ValueError('Require exploratory market-free batch')
    games=[]
    for p in batch['forecasts']:
        if p.get('recent_history_verified') is not True:
            raise ValueError('Require verified recent-game coverage before publication')
        if p.get('market_inputs_used') is not False or p['scope']!='full_game_including_overtime':
            raise ValueError('Unsupported forecast scope or market inputs')
        for metric in METRICS:
            rows=p['metrics'][metric]['players']
            if any(not 0<=r['win_share']<=1 for r in rows) or abs(sum(r['win_share'] for r in rows)-1)>1e-8:
                raise ValueError('Invalid probability accounting')
        games.append({'game':p['game'],'decision_at':p['decision_at'],
            'availability_verified':p['availability_verified'],'metrics':p['metrics'],
            'history_coverage':p['history_coverage'],
            'workload_only_training_games':len(p.get('workload_only_game_ids',[])),
            'training_games':len(p['training_game_ids']),'draws':p['settings']['draws'],
            'limits':p['limits'],'source_sha256':batch['source_sha256'],
            'role_scenarios':'See frozen request and diagnostics'})
    return {'version':VERSION,'authority':'exploratory_not_calibrated',
        'season':batch['season'],'week':batch['week'],'decision_at':batch['decision_at'],
        'games':games,'skipped':batch['skipped'],'rejected_training_games':len(batch['reconciliation_rejections']),
        'evaluation':[{'season':e['season'],'weeks':e['weeks'],'games':e['graded_games'],
            'skipped':len(e['skipped']),'summary':{m:{k:v for k,v in e['summary'][m].items() if k!='calibration'} for m in METRICS}}
            for e in evaluations],
        'evaluation_limit':'Corrected historical records and reconstructed prior-usage rosters; not archived game-day availability or validated betting probabilities.'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--batch',type=Path,required=True)
    p.add_argument('--evaluation',type=Path,action='append',default=[])
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    write(a.output,publish(read(a.batch),[read(e) for e in a.evaluation]))


if __name__=='__main__':
    main()
