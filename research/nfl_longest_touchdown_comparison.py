"""Freeze and run a reproducible development comparison, never a promotion gate.

python -m research.nfl_longest_touchdown_comparison --input SOURCE.json.gz --output REPORT.json
Both models use the same regulation scope and optional newcomer allowance.
"""
import argparse
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path

import numpy as np

from model.nfl_longest_touchdown import IMPLEMENTATION_SHA256, Settings, VERSION
from research.nfl_longest_touchdown import RESEARCH_SHA256, read, walk_forward, write


def paired_summary(left, right, *, seed=20261007):
    a = {r['game_id']:r for r in left}
    b = {r['game_id']:r for r in right}
    common = sorted(a.keys() & b.keys())
    result = {'paired_games':len(common),'left_only':sorted(a.keys()-b.keys()),
              'right_only':sorted(b.keys()-a.keys())}
    for metric in ('log_loss','brier_score'):
        delta = np.array([a[g][metric]-b[g][metric] for g in common])
        rng = np.random.default_rng(seed)
        means = [float(rng.choice(delta,len(delta),replace=True).mean()) for _ in range(1000)] if common else []
        result[metric] = {'mean_left_minus_right':float(delta.mean()) if common else None,
            'game_bootstrap_95_percent_interval':np.quantile(means,[.025,.975]).tolist() if means else None,
            'per_game': [{'game_id':g,'difference':float(d)} for g,d in zip(common,delta)]}
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--season',type=int,default=2024)
    p.add_argument('--start-week',type=int,default=5)
    p.add_argument('--end-week',type=int,default=5)
    p.add_argument('--limit',type=int,default=8)
    p.add_argument('--draws',type=int,default=300)
    args = p.parse_args()
    if args.output.exists():
        raise FileExistsError('Frozen comparison already exists')
    settings = Settings(draws=args.draws)
    registration = {'version':VERSION,'authority':'development_comparison_not_confirmation',
        'implementation_sha256':IMPLEMENTATION_SHA256,'research_sha256':RESEARCH_SHA256,
        'comparison_sha256':sha256(Path(__file__).read_bytes()).hexdigest(),
        'source_file_sha256':sha256(args.input.read_bytes()).hexdigest(),
        'season':args.season,'weeks':[args.start_week,args.end_week],'limit':args.limit,
        'settings':asdict(settings),'comparison':'v2 reserve enabled minus v2 reserve disabled; each baseline has matching reserve',
        'acceptance':'Mechanical integration evidence only; no calibration or release promotion',
        'bootstrap_draws':1000,'bootstrap_seed':20261007}
    write(args.output.with_suffix('.registration.json'),registration)
    snapshot = read(args.input)
    off = walk_forward(snapshot,args.season,args.start_week,args.end_week,settings,limit=args.limit)
    on = walk_forward(snapshot,args.season,args.start_week,args.end_week,replace(settings,newcomer_reserve=True),limit=args.limit)
    write(args.output,{**registration,'reserve_disabled':off,'reserve_enabled':on,
        'reserve_effect':paired_summary(on['grades'],off['grades']),
        'model_vs_comparable_baseline':paired_summary(on['grades'],on['baseline_grades'])})


if __name__=='__main__':
    main()
