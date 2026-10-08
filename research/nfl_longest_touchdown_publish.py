"""Publish a frozen exploratory forecast for the web page; no database writes.

python -m research.nfl_longest_touchdown_publish --forecast FORECAST.json --support SUPPORT.json --output web/src/data/longest-touchdown.json
The output is exclusive-create. Forecasts and their original evidence are never overwritten.
"""
import argparse
import math
from datetime import datetime, timezone
from pathlib import Path

from research.nfl_longest_touchdown import read, write


def publish(forecast, support):
    if forecast.get('outcome_scope') != 'regulation_scrimmage' or forecast.get('authority') != 'unvalidated_exploratory':
        raise ValueError('Only explicit exploratory regulation forecasts may be published')
    if support.get('decision_at') != forecast['decision_at']:
        raise ValueError('Support audit must match frozen decision time')
    if forecast.get('market_inputs_used') is not False:
        raise ValueError('This board must not use market inputs')
    total = sum(r['longest_td_win_share'] for r in forecast['players'])+forecast['no_scrimmage_td_probability']
    if not math.isfinite(total) or abs(total-1)>1e-8:
        raise ValueError('Probability accounting failed')
    current = {r['identity']:r['current_opportunities'] for r in support['support']}
    known, residual, unresolved = [], [], {p['identity']:p['name'] for p in forecast['unresolved_players']}
    for p in forecast['players']:
        if not (0 <= p['td_40_plus_probability'] <= p['td_20_plus_probability'] <= p['any_td_probability'] <= 1
                and 0 <= p['longest_td_win_share'] <= p['any_td_probability']):
            raise ValueError('Invalid nested probabilities')
        row = {'name':p['name'],'identity':p['identity'],'longestShare':p['longest_td_win_share'],
               'anyTd':p['any_td_probability'],'longTd':p['td_40_plus_probability']}
        if p['identity'].startswith('OTHER:'):
            residual.append(row)
        elif current.get(p['identity'],0)>0:
            known.append(row)
        else:
            # No observed role is unknown, never proof of a zero future chance.
            unresolved[p['identity']] = p['name']
            if p['longest_td_win_share']>0:
                residual.append({**row,'name':f"Unresolved role: {p['name']}"})
    return {'schemaVersion':1,'modelVersion':forecast['version'],'experimental':True,
        'scope':'regulation_scrimmage','decisionAt':forecast['decision_at'],
        'publishedAt':datetime.now(timezone.utc).isoformat(),'game':forecast['game'],
        'draws':forecast['settings']['draws'],'players':known,'residual':residual,
        'unresolved':sorted(unresolved.values()),'noTd':forecast['no_scrimmage_td_probability'],
        'trainingGames':forecast['training_games'],'marketInputsUsed':forecast['market_inputs_used'],
        'newcomerReserve':forecast.get('newcomer_reserve',{}),
        'provenance':{k:forecast[k] for k in ('source_sha256','request_sha256','implementation_sha256')},
        'limits':['Not calibrated; these estimates do not establish betting edges.',
            'Frozen evidence, not a live feed. Later injuries and role changes are not incorporated.',
            'Rushing and receiving TDs in regulation only. No passing credit, overtime, return or defensive TDs.',
            'Quarterback changes and replacement workloads are not separately fitted.',
            'Unexpected scorers use a pooled allowance; unresolved players do not have reliable named estimates.']}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--forecast',type=Path,required=True)
    parser.add_argument('--support',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    write(args.output,publish(read(args.forecast),read(args.support)))


if __name__=='__main__':
    main()
