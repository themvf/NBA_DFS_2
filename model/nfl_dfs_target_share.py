"""Research-only team-volume / receiver-share model. No production model mutation."""
from collections import defaultdict
from math import isfinite
import numpy as np

VERSION = 'nfl-dfs-volume-share-v1'
TEAM_ALIASES = {'LA':'LAR','WAS':'WSH','AZ':'ARI','JAC':'JAX'}

def normalize_team(team):
    return TEAM_ALIASES.get(team, team)

CONFIG = {'games': 8, 'half_life': 4, 'min_player_games': 4, 'redistribution_fraction': .5, 'residual_min': 100}


def pregame_availability(member, team, now, kickoff):
    """No retrospective provider timestamp can substitute for observed pregame capture."""
    source=member.get('sleeper') or {}
    captured=member.get('fetched_at')
    if not captured or captured.tzinfo is None or now.tzinfo is None or kickoff.tzinfo is None:
        return None
    if not captured <= now < kickoff or (now-captured).total_seconds()>72*3600:
        return None
    if normalize_team(member.get('team'))!=normalize_team(team) or normalize_team(source.get('team'))!=normalize_team(team) or source.get('position')!=member.get('position'):
        return None
    status=str(source.get('injury_status') or source.get('status') or 'UNKNOWN').upper()
    out_statuses={'OUT','IR','PUP','NFI','SUSPENDED','INACTIVE'}
    return {'status':status,'captured_at':captured.isoformat(),'source':'Sleeper roster; retrieval timestamp',
            'out':status in out_statuses or str(source.get('status')).upper() in out_statuses}


def average(values):
    if not values:
        return None
    x = np.asarray(values[-CONFIG['games']:], dtype=float)
    if not np.isfinite(x).all():
        raise ValueError('Non-finite history')
    weights = .5 ** (np.arange(len(x)-1, -1, -1)/CONFIG['half_life'])
    return float(np.dot(x, weights)/weights.sum())


def allocate(shares, target_budget, out_ids=()):
    """Reserve unknown work; redistribute only half of removed known share."""
    if not isfinite(target_budget) or target_budget < 0 or any(not isfinite(v) or v < 0 for v in shares.values()):
        raise ValueError('Invalid target budget/share')
    total = sum(shares.values())
    normalized = {p: s/max(1., total) for p, s in shares.items()}
    removed = sum(s for p, s in normalized.items() if p in out_ids)
    available = sum(s for p, s in normalized.items() if p not in out_ids)
    allocated = {p: (0. if p in out_ids else s + (removed*CONFIG['redistribution_fraction']*s/available if available else 0.)) for p, s in normalized.items()}
    return {'players': {p: {'share': s, 'targets': s*target_budget} for p, s in allocated.items()},
            'unallocated_targets': max(0., 1-sum(allocated.values()))*target_budget,
            'removed_share': removed, 'redistributed_share': removed*CONFIG['redistribution_fraction'] if available else 0.}


def forecast(history, teams, team, cutoff, out_ids=()):
    prior_teams = sorted([r for r in teams if r['team']==team and (r['season'],r['week'])<cutoff], key=lambda r:(r['season'],r['week']))[-CONFIG['games']:]
    if len(prior_teams)<4:
        return None
    attempts = average([r['attempts'] for r in prior_teams])
    ratios = [r['targets']/r['attempts'] for r in prior_teams if r['attempts']>0]
    if not ratios:
        return None
    target_budget = attempts*min(1., average(ratios))
    lookup = {(r['season'],r['week']):r for r in teams if r['team']==team and (r['season'],r['week'])<cutoff}
    recent = {(r['season'],r['week']) for r in prior_teams[-4:]}
    prior = sorted([r for r in history if r['team']==team and (r['season'],r['week'])<cutoff],key=lambda r:(r['season'],r['week'],r['identity']))
    eligible = {r['identity'] for r in prior if (r['season'],r['week']) in recent}
    by_player = defaultdict(list)
    for r in prior:
        if r['identity'] in eligible:
            by_player[r['identity']].append(r)
    shares, evidence = {}, {}
    for pid, rows in by_player.items():
        own = rows[-CONFIG['games']:]
        usable = [r for r in own if lookup.get((r['season'],r['week']),{}).get('targets',0)>0]
        if not usable:
            continue
        shares[pid] = average([r['targets']/lookup[(r['season'],r['week'])]['targets'] for r in usable])
        evidence[pid] = {'name':own[-1]['name'], 'position':own[-1]['position'], 'games':len(own),
            'baseline_targets':average([r['targets'] for r in own]), 'baseline_fpts':average([r['fpts'] for r in own]),
            'fpts_per_target':min(3.5,max(.5,sum(r['fpts'] for r in own)/max(1.,sum(r['targets'] for r in own))))}
    allocation = allocate(shares,target_budget,out_ids)
    for pid, result in allocation['players'].items():
        e=evidence[pid]
        result.update(e)
        result['candidate_fpts']=0. if pid in out_ids else max(0.,e['baseline_fpts']+(result['targets']-e['baseline_targets'])*e['fpts_per_target'])
    return {'team':team,'cutoff_exclusive':list(cutoff),'attempts':attempts,'target_budget':target_budget,**allocation}


def replay(history, teams):
    weeks=defaultdict(list)
    for r in history:
        weeks[(r['season'],r['week'])].append(r)
    errors={'baseline':[],'candidate':[]}
    predictions=[]
    excluded=defaultdict(int)
    for cutoff, rows in sorted(weeks.items()):
        forecasts={team:forecast(history,teams,team,cutoff) for team in sorted({r['team'] for r in rows})}
        pending=[]
        for row in rows:
            if row['position']!='WR':
                continue
            f=forecasts[row['team']]
            p=f['players'].get(row['identity']) if f else None
            if not p or p['games']<CONFIG['min_player_games']:
                excluded['insufficient_prior_role']+=1
                continue
            means={'baseline':p['baseline_fpts'],'candidate':p['candidate_fpts']}
            if len(errors['baseline'])>=CONFIG['residual_min']:
                output={'season':row['season'],'week':row['week'],'game_id':row['game_id'],'identity':row['identity'],'actual':row['fpts'],'targets_actual':row['targets'],'targets_baseline':p['baseline_targets'],'targets_candidate':p['targets']}
                for name,mean in means.items():
                    # Same prior-only positional residual method for both means. No target outcome enters draws.
                    residuals=np.asarray(errors[name][-2000:]); residuals=residuals-residuals.mean()
                    draws=mean+residuals
                    output[name]={'mean':mean,'p10':float(np.quantile(draws,.1)),'p50':float(np.quantile(draws,.5)),'p90':float(np.quantile(draws,.9))}
                predictions.append(output)
            else:
                excluded['residual_warmup']+=1
            pending.append({name:row['fpts']-mean for name,mean in means.items()})
        for error in pending:
            for name in errors:
                errors[name].append(error[name])
    return predictions,dict(excluded)


def metrics(rows):
    result={'n':len(rows)}
    for model in ['baseline','candidate']:
        if not rows:
            result[model]=None
            continue
        result[model]={'mae':float(np.mean([abs(r['actual']-r[model]['mean']) for r in rows])),
            'interval_score80':float(np.mean([r[model]['p90']-r[model]['p10']+10*(max(r[model]['p10']-r['actual'],0)+max(r['actual']-r[model]['p90'],0)) for r in rows])),
            'coverage80':float(np.mean([r[model]['p10']<=r['actual']<=r[model]['p90'] for r in rows])),
            'targets_mae':float(np.mean([abs(r['targets_actual']-r['targets_'+model]) for r in rows]))}
    return result
