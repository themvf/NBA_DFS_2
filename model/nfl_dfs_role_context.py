"""Prior-only WR role context experiment. Does not activate production projections."""
from collections import defaultdict
import numpy as np
from model.nfl_dfs_target_share import average

CONFIG={'team_games':8,'minimum_player_games':4,'minimum_residuals':100,'maximum_residuals':2000,'qb_window':4}


def forecast(history, teams, team, cutoff):
    prior=sorted([r for r in teams if r['team']==team and (r['season'],r['week'])<cutoff],key=lambda r:(r['season'],r['week']))[-8:]
    if len(prior)<4:return None
    keys=[(r['season'],r['week']) for r in prior]
    if len(set(keys))!=len(keys):raise ValueError('Duplicate team week')
    records={}
    for r in history:
        if r['team']!=team or (r['season'],r['week']) not in keys:continue
        key=(r['identity'],r['season'],r['week'])
        if key in records:raise ValueError('Duplicate player week')
        records[key]=r
    players={}
    budget=average([r['targets'] for r in prior])
    qbs=[r.get('primary_qb') for r in prior[-CONFIG['qb_window']:]]
    qb_state='unknown' if any(q is None for q in qbs) else 'stable' if len(set(qbs))==1 else 'changed'
    for pid in sorted({k[0] for k in records}):
        own=[records.get((pid,*key)) for key in keys]
        recorded=[r for r in own if r]
        if len(recorded)<4 or not any(own[-2:]):continue
        # Known team games with no stat record contribute zero usage, not a new injury label.
        shares=[(r['targets'] if r else 0.)/t['targets'] if t['targets'] else 0. for r,t in zip(own,prior)]
        share=average(shares)
        targets=budget*share
        base_targets=average([r['targets'] if r else 0. for r in own])
        base_fpts=average([r['fpts'] if r else 0. for r in own])
        efficiency=min(3.5,max(.5,sum(r['fpts'] for r in recorded)/max(1.,sum(r['targets'] for r in recorded))))
        players[pid]={'mean':max(0.,base_fpts+(targets-base_targets)*efficiency),'targets':targets,'share':share,'recorded_games':len(recorded),'team_games':len(prior)}
    if sum(p['share'] for p in players.values())>1+1e-9:raise ValueError('Role shares exceed team budget')
    return {'players':players,'qb_state':qb_state,'target_budget':budget,'cutoff_exclusive':list(cutoff)}


def replay(history, teams):
    weeks=defaultdict(list)
    for r in history:weeks[(r['season'],r['week'])].append(r)
    residuals=defaultdict(list);pooled=[];output=[];excluded=defaultdict(int)
    for cutoff,current in sorted(weeks.items()):
        estimates={t:forecast(history,teams,t,cutoff) for t in sorted({r['team'] for r in current})}
        pending=[]
        for r in sorted(current,key=lambda r:(r['game_id'],r['identity'])):
            if r['position']!='WR':continue
            team=estimates[r['team']];p=team['players'].get(r['identity']) if team else None
            if not p:excluded['insufficient_prior_role']+=1;continue
            tier=0 if p['targets']<4 else 1 if p['targets']<7 else 2
            group=(tier,team['qb_state'])
            source=residuals[group] if len(residuals[group])>=CONFIG['minimum_residuals'] else pooled
            if len(source)>=CONFIG['minimum_residuals']:
                draws=p['mean']+np.asarray(source[-CONFIG['maximum_residuals']:])
                pooled_draws=p['mean']+np.asarray(pooled[-CONFIG['maximum_residuals']:])
                output.append({'season':r['season'],'week':r['week'],'game_id':r['game_id'],'identity':r['identity'],'actual':r['fpts'],
                    'targets_actual':r['targets'],'targets_baseline':p['targets'],'targets_candidate':p['targets'],'qb_state':team['qb_state'],
                    'residual_source':'role_and_qb' if source is residuals[group] else 'pooled','history_cutoff':team['cutoff_exclusive'],
                    'candidate':{'mean':float(draws.mean()),'p10':float(np.quantile(draws,.1)),'p50':float(np.quantile(draws,.5)),'p90':float(np.quantile(draws,.9))},
                    'pooled_candidate':{'mean':float(pooled_draws.mean()),'p10':float(np.quantile(pooled_draws,.1)),'p90':float(np.quantile(pooled_draws,.9))},
                    'pooled_boom_probability':float(np.mean(pooled_draws>=25)),
                    'boom_probability':float(np.mean(draws>=25))})
            else:excluded['residual_warmup']+=1
            pending.append((group,r['fpts']-p['mean']))
        # Batch update after every forecast in the week; target-week results cannot affect peers.
        for group,error in pending:residuals[group].append(error);pooled.append(error)
    return output,dict(excluded)
