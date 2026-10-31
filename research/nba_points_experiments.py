"""Pre-registered, prospective NBA disagreement, opportunity and ceiling tests.

Immutable inputs and fixed rules. No forecast/model mutation or paid requests.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import mean, stdev

import numpy as np

from research.nba_points_forward import aware, digest, write_new
from research.nba_points_ghost import verify
from research.nba_points_v3_model import MODEL_VERSION, band, probability

PROTOCOL = 'nba-points-experiment-protocol-v1'
VERSION = 'nba-points-experiments-v1'
GRADE = 'nba-points-experiment-grade-v1'
V3 = 'nba-points-ghost-v3'
DEFAULT_PROTOCOL = Path(__file__).parent/'artifacts/nba_points_experiments_v1.json'


def now():
    return datetime.now(timezone.utc).isoformat()


def signed(value):
    value['sha256'] = digest({k:v for k,v in value.items() if k!='sha256'})
    return value


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def tail_key(prediction):
    share = prediction['fg3a']/prediction['fga'] if prediction['fga'] else 0
    return f"{band(prediction['points'])}|three_share_{'high' if share>=.4 else 'low'}|minutes_{'high' if prediction['minutes']>=30 else 'low'}"


def protocol(model, created):
    verify(model,MODEL_VERSION)
    if aware(created)<aware(model['created_at']):
        raise ValueError('registration precedes fitted model')
    groups = defaultdict(list); bands = defaultdict(list)
    for r in model['calibration_rows']:
        if not ('2026-01-01' <= r['date'] <= model['calibration_end']):
            raise ValueError('tail calibration row outside fixed calibration period')
        groups[tail_key(r['forecast'])].append(r['residuals']['points'])
        bands[band(r['forecast']['points'])].append(r['residuals']['points'])
    return signed({'version':PROTOCOL,'created_at':aware(created).isoformat(),'model_sha256':model['sha256'],
        'primary_market_line_minimum':20,'disagreement_absolute_boundaries':[2,4],
        'watchlist':{'maximum_k':5,'minimum_fga_increase':2,'minimum_minutes_increase':2,
            'ranking':'descending FGA increase, then minutes increase, then canonical player key',
            'comparators':['market_line','prior10_points','v3_points'],
            'target':'actual full-game points strictly above frozen historical P90'},
        'ceilings':{'minimum_conditional_pool':100,'three_share_boundary':.4,'minutes_boundary':30,
            'q':.9,'thresholds':[30,40],'probability_bin_edges':[0,.1,.25,.5,.75,1],
            'unstable_minutes_sd_boundary':6,'history_window':10},
        'conditional_tail_calibration':{'groups':dict(groups),'bands':dict(bands),
            'n':len(model['calibration_rows']),'period_end':model['calibration_end'],
            'method':'January residuals grouped by predicted scoring band, 3PA/FGA >= .4 and minutes >=30; <100 falls back to scoring band. No mean-model change.'},
        'review':{'completed_slate_dates':30,'resolved_alerts':100,'date_bootstrap_draws':2000,'seed':20261028,
            'promotion':'never automatic; repeated daily looks descriptive, no sequential significance claims'},
        'primary_endpoints':[
            'V3-minus-market paired MAE in positive/negative disagreement groups, frozen line >=20.',
            'Historical-P90 hit-rate difference for opportunity alerts vs equal-size market ranking in the same high-line eligible pool.',
            'Conditional-tail-minus-original-V3 P90 loss on identical high-line players.'],
        'limitations':['Research choices, not validated thresholds. Calibration season was previously inspected.',
            'Only forecasts created after registration and experiments frozen before first tip can enter prospective reports.',
            'Historical absence is not verified historical injury. No causal injury bonuses.',
            'All scoring probabilities conditional on appearance; no bet or profit claim.']})


def conditional_tail(p, spec):
    calibration = spec['conditional_tail_calibration']; key = tail_key(p)
    rs = calibration['groups'].get(key,[])
    fallback = len(rs)<spec['ceilings']['minimum_conditional_pool']
    if fallback:
        rs = calibration['bands'].get(band(p['points']),[])
    if not rs:
        raise ValueError('missing conditional tail calibration band')
    values = np.maximum(0,np.rint(p['points']+np.asarray(rs))).astype(int)
    return {'q90':float(np.quantile(values,.9)),
        'thresholds':{str(t):probability(values,t) for t in spec['ceilings']['thresholds']},
        'calibration_n':len(rs),'group_key':key,'fallback_to_scoring_band':fallback}


def bucket(delta):
    if delta<=-4:return 'at_least_4_below'
    if delta<=-2:return '2_to_4_below'
    if delta<2:return 'within_2'
    if delta<4:return '2_to_4_above'
    return 'at_least_4_above'


def freeze(forecast, source, spec, created, allow_retrospective=False):
    verify(forecast,V3);verify(source,'nba-points-ghost-v2');verify(spec,PROTOCOL)
    created=aware(created)
    if source['sha256']!=forecast['source_snapshot_sha256'] or source['game_date']!=forecast['game_date']:
        raise ValueError('forecast/source identity mismatch')
    if forecast['model_sha256']!=spec['model_sha256']:
        raise ValueError('model differs from registered experiment')
    if created<aware(forecast['created_at']) or created<aware(spec['created_at']):
        raise ValueError('experiment creation precedes inputs')
    if aware(forecast['lock_at'])!=aware(source['lock_at']):
        raise ValueError('source cutoff mismatch')
    starts=[aware(g['start_time']) for g in source.get('slate_schedule',[]) if g.get('start_time')]
    if not starts or aware(forecast['lock_at'])>min(starts):
        raise ValueError('unverified full-slate cutoff')
    prospective=(forecast['mode']=='prospective' and aware(spec['created_at'])<=aware(forecast['created_at'])<=created<min(starts))
    if not prospective and not allow_retrospective:
        raise ValueError('experiment cannot be prospectively registered after forecast or first tip')
    originals={(str(c['event_id']),c['player_key']):c for c in source['candidates']}
    if len(originals)!=len(source['candidates']):raise ValueError('duplicate source identity')
    rows=[];seen=set();excluded=Counter()
    for r in forecast['rows']:
        key=(str(r['event_id']),r['player_key'])
        if key in seen:raise ValueError('duplicate forecast identity')
        seen.add(key);c=originals.get(key)
        if c is None:raise ValueError('forecast player absent from source')
        h=c['history'][-spec['ceilings']['history_window']:]
        if len(h)<10 or c.get('historical_p90') is None:
            excluded['insufficient_history_or_reference_p90']+=1;continue
        if any(str(x['game_date'])>=source['game_date'] or not x.get('provenance') or any(aware(p['available_at'])>=aware(source['as_of']) for p in x['provenance']) for x in h):
            raise ValueError('future or missing historical provenance')
        if r['market_line']!=c['predictions']['market_line']:
            raise ValueError('market comparison is not the original frozen line')
        if str(r.get('availability',{}).get('status','')).lower()=='out':
            excluded['reported_out']+=1;continue
        p=r['prediction'];fga10=mean(x['fga'] for x in h);minutes10=mean(x['minutes'] for x in h)
        shot_share=sum(x['fg3a'] for x in h)/sum(x['fga'] for x in h) if sum(x['fga'] for x in h) else 0
        delta=p['points']-r['market_line'];samples=r['samples']
        ps={str(t):probability(samples,t) for t in spec['ceilings']['thresholds']}
        for t,prob in ps.items():
            if abs(prob-r['distribution']['thresholds'][t]['probability'])>1e-9:
                raise ValueError('frozen probability/sample disagreement')
        rows.append({'event_id':key[0],'player_key':key[1],'player':r['player'],
            'primary':r['market_line']>=spec['primary_market_line_minimum'],
            'market_line':r['market_line'],'v3_points':p['points'],'prior10_points':mean(x['points'] for x in h),
            'disagreement':delta,'disagreement_bucket':bucket(delta),
            'fga10':fga10,'minutes10':minutes10,'fga_increase':p['fga']-fga10,
            'minutes_increase':p['minutes']-minutes10,'projected_fga':p['fga'],'projected_minutes':p['minutes'],
            'historical_p90':c['historical_p90'],'v2_p90':c.get('experimental_p90'),
            'v3_p90':r['distribution']['p90'],'v3_thresholds':ps,'conditional_tail':conditional_tail(p,spec),
            'three_point_profile':'high' if shot_share>=spec['ceilings']['three_share_boundary'] else 'low',
            'minute_stability':'unstable' if stdev(x['minutes'] for x in h)>=spec['ceilings']['unstable_minutes_sd_boundary'] else 'stable',
            'availability':r['availability'].get('status','unknown')})
    primary=[r for r in rows if r['primary']];w=spec['watchlist']
    alerts=sorted([r for r in primary if r['fga_increase']>=w['minimum_fga_increase'] or r['minutes_increase']>=w['minimum_minutes_increase']],
        key=lambda r:(-r['fga_increase'],-r['minutes_increase'],r['player_key']))[:w['maximum_k']]
    identify=lambda r:[r['event_id'],r['player_key']]
    selections={'opportunity':[identify(r) for r in alerts]}
    for method in w['comparators']:
        selections[method]=[identify(r) for r in sorted(primary,key=lambda r:(-r[method],r['player_key']))[:len(alerts)]]
    return signed({'version':VERSION,'created_at':created.isoformat(),'game_date':forecast['game_date'],
        'mode':'prospective' if prospective else 'retrospective_reconstruction',
        'lock_at':forecast['lock_at'],'source_snapshot_sha256':source['sha256'],
        'forecast_sha256':forecast['sha256'],'model_sha256':forecast['model_sha256'],
        'protocol_sha256':spec['sha256'],'rows':rows,'selections':selections,
        'quoted_n':forecast['quoted_pool_n'],'modeled_n':forecast['modeled_pool_n'],'experiment_n':len(rows),
        'exclusions':dict(excluded),'watch_k':len(alerts),'scope':'Identical modeled quoted pool; watchlists use frozen market line >=20.',
        'primary_endpoints':spec['primary_endpoints'],'probabilities':'Experimental, conditional on appearing. No model or mean forecasts changed.'})


def metrics(rows):
    if not rows:return {'n':0}
    actual=lambda r:r['actual_points']
    delta=[abs(r['v3_points']-actual(r))-abs(r['market_line']-actual(r)) for r in rows]
    correct=sum((r['disagreement']>0 and actual(r)>r['market_line']) or (r['disagreement']<0 and actual(r)<r['market_line']) for r in rows)
    directional_n=sum(r['disagreement']!=0 for r in rows)
    return {'n':len(rows),'market_mae':mean(abs(r['market_line']-actual(r)) for r in rows),
        'v3_mae':mean(abs(r['v3_points']-actual(r)) for r in rows),'paired_mae_delta':mean(delta),
        'mean_actual_minus_line':mean(actual(r)-r['market_line'] for r in rows),
        'overs':sum(actual(r)>r['market_line'] for r in rows),
        'unders':sum(actual(r)<r['market_line'] for r in rows),'pushes':sum(actual(r)==r['market_line'] for r in rows),
        'over_rate':mean(actual(r)>r['market_line'] for r in rows),
        'direction_correct':correct,'nonzero_disagreement_n':directional_n,
        'direction_correct_rate':correct/directional_n if directional_n else None}


def pinball(actual,q):
    e=actual-q;return max(.9*e,-.1*e)


def ceilings(rows,spec):
    paired=[r for r in rows if all(r.get(k) is not None for k in ['historical_p90','v2_p90','v3_p90'])]
    out={'n':len(paired),'models':{}}
    for name in ['historical_p90','v2_p90','v3_p90','conditional_tail']:
        pairs=[(r['actual_points'],r['conditional_tail']['q90'] if name=='conditional_tail' else r[name]) for r in paired]
        out['models'][name]={'n':len(pairs),'exceedances':sum(a>q for a,q in pairs),
            'quantile_loss':mean(pinball(a,q) for a,q in pairs) if pairs else None}
    out['probability_audit']={}
    edges=spec['ceilings']['probability_bin_edges']
    for name in ['v3_thresholds','conditional_tail']:
        table={}
        for t in spec['ceilings']['thresholds']:
            pairs=[{'p':(r[name]['thresholds'] if name=='conditional_tail' else r[name])[str(t)],'hit':r['actual_points']>=t} for r in paired]
            bins=[]
            for lo,hi in zip(edges[:-1],edges[1:]):
                group=[r for r in pairs if lo<=r['p']<hi or (hi==1 and r['p']==1)]
                bins.append({'from':lo,'through':hi,'n':len(group),'hits':sum(r['hit'] for r in group),
                    'mean_probability':mean(r['p'] for r in group) if group else None,
                    'observed_rate':mean(r['hit'] for r in group) if group else None})
            table[str(t)]={'n':len(pairs),'hits':sum(r['hit'] for r in pairs),
                'brier':mean((r['p']-r['hit'])**2 for r in pairs) if pairs else None,'bins':bins}
        out['probability_audit'][name]=table
    return out


def grade(experiment,v3_grade,diagnostics,spec,graded_at):
    verify(experiment,VERSION);verify(spec,PROTOCOL)
    if experiment['protocol_sha256']!=spec['sha256'] or experiment['model_sha256']!=spec['model_sha256']:
        raise ValueError('experiment/protocol/model mismatch')
    if v3_grade['version']!=V3 or v3_grade['snapshot_sha256']!=experiment['forecast_sha256'] or v3_grade['model_sha256']!=experiment['model_sha256']:
        raise ValueError('grade refers to another original forecast/model')
    if v3_grade['source_snapshot_sha256']!=experiment['source_snapshot_sha256'] or diagnostics['snapshot_sha256']!=experiment['source_snapshot_sha256'] or diagnostics['game_date']!=experiment['game_date']:
        raise ValueError('diagnostics/source mismatch')
    ds={(str(r['event_id']),r['player_key']):r for r in diagnostics['rows']}
    if len(ds)!=len(diagnostics['rows']):raise ValueError('duplicate diagnostic identity')
    rows=[];unresolved=[];final_dnp=[]
    grade_actual={r['player']:r['actual'] for r in v3_grade['rows']}
    if len(grade_actual)!=len(v3_grade['rows']):raise ValueError('duplicate v3 grade player')
    for r in experiment['rows']:
        d=ds.get((r['event_id'],r['player_key']))
        if not d or d['status']!='verified':
            missing={'player':r['player'],'event_id':r['event_id'],'player_key':r['player_key'],'status':d['status'] if d else 'missing'}
            (final_dnp if d and d['status']=='dnp' else unresolved).append(missing);continue
        a=d['actual']
        if grade_actual.get(r['player'])!=a['points']:
            raise ValueError('diagnostic points differ from v3 grade')
        out={**r,'actual_points':a['points'],'actual_fga':a.get('fga'),'actual_minutes':a.get('minutes')}
        rows.append(out)
    by_key={(r['event_id'],r['player_key']):r for r in rows};watch={}
    for method,selected in experiment['selections'].items():
        resolved=[by_key[tuple(k)] for k in selected if tuple(k) in by_key]
        watch[method]={'selected_n':len(selected),'resolved_n':len(resolved),
            'ceiling_hits':sum(r['actual_points']>r['historical_p90'] for r in resolved),
            'actual_points_total':sum(r['actual_points'] for r in resolved) if len(resolved)==len(selected) else None,
            'fga_n':sum(r['actual_fga'] is not None for r in resolved),
            'fga_growth_hits':sum(r['actual_fga'] is not None and r['actual_fga']-r['fga10']>=spec['watchlist']['minimum_fga_increase'] for r in resolved),
            'minutes_n':sum(r['actual_minutes'] is not None for r in resolved),
            'minutes_growth_hits':sum(r['actual_minutes'] is not None and r['actual_minutes']-r['minutes10']>=spec['watchlist']['minimum_minutes_increase'] for r in resolved)}
    # For hit-rate comparisons, drop a rank-paired slot from BOTH lists if
    # either selected player is unresolved/DNP. Never substitute a new player.
    watch_pairs={}
    for method in spec['watchlist']['comparators']:
        pairs=[]
        for ka,kb in zip(experiment['selections']['opportunity'],experiment['selections'][method]):
            a=by_key.get(tuple(ka));b=by_key.get(tuple(kb))
            if a and b:pairs.append((a,b))
        watch_pairs[method]={'n':len(pairs),'opportunity_hits':sum(a['actual_points']>a['historical_p90'] for a,b in pairs),
            'comparator_hits':sum(b['actual_points']>b['historical_p90'] for a,b in pairs),
            'same_players_overlap':sum(ka in experiment['selections'][method] for ka in experiment['selections']['opportunity'])}
    return signed({'version':GRADE,'graded_at':aware(graded_at).isoformat(),'game_date':experiment['game_date'],
        'experiment_sha256':experiment['sha256'],'forecast_sha256':experiment['forecast_sha256'],
        'protocol_sha256':spec['sha256'],'model_sha256':experiment['model_sha256'],'mode':experiment['mode'],
        'complete':not unresolved,'rows':rows,'unresolved':unresolved,'final_dnp':final_dnp,
        'watchlists':watch,'watch_rank_paired':watch_pairs,
        'primary_disagreement':{b:metrics([r for r in rows if r['primary'] and r['disagreement_bucket']==b]) for b in ['at_least_4_below','2_to_4_below','within_2','2_to_4_above','at_least_4_above']},
        'ceilings_primary':ceilings([r for r in rows if r['primary']],spec),
        'notes':['Selected DNP/missing never replaced or assigned zero. Watchlist comparisons use equal resolved rank-paired denominators.',
            'All primary analyses use the frozen market-line >=20 cohort. Other strata are exploratory.']})


def cluster_interval(groups,spec):
    groups={d:rs for d,rs in groups.items() if rs}
    if len(groups)<5:return {'dates':len(groups),'interval_95':None,'status':'fewer_than_five_dates'}
    dates=sorted(groups);rng=np.random.default_rng(spec['review']['seed']);values=[]
    for _ in range(spec['review']['date_bootstrap_draws']):
        sample=rng.choice(dates,len(dates),replace=True)
        values.append(mean(v for d in sample for v in groups[d]))
    return {'dates':len(groups),'interval_95':np.quantile(values,[.025,.975]).tolist(),
        'status':'descriptive date-clustered bootstrap; repeated looks are not sequential tests'}


def report(grades,spec):
    verify(spec,PROTOCOL);latest={};excluded=Counter()
    for g in grades:
        verify(g,GRADE)
        if g['mode']!='prospective':excluded['retrospective']+=1;continue
        if g['protocol_sha256']!=spec['sha256'] or g['model_sha256']!=spec['model_sha256']:
            excluded['other_protocol_or_model']+=1;continue
        key=g['experiment_sha256']
        if key not in latest or aware(g['graded_at'])>aware(latest[key]['graded_at']):latest[key]=g
    # Different originals for one date/protocol must never be silently pooled.
    if len({g['game_date'] for g in latest.values()})!=len(latest):
        raise ValueError('multiple original experiment snapshots for same date')
    gs=sorted(latest.values(),key=lambda g:g['game_date']);rows=[r for g in gs for r in g['rows']]
    primary=[r for r in rows if r['primary']];disagreement={}
    for b in ['at_least_4_below','2_to_4_below','within_2','2_to_4_above','at_least_4_above']:
        rs=[r for r in primary if r['disagreement_bucket']==b];groups=defaultdict(list)
        for g in gs:
            groups[g['game_date']]=[abs(r['v3_points']-r['actual_points'])-abs(r['market_line']-r['actual_points']) for r in g['rows'] if r['primary'] and r['disagreement_bucket']==b]
        disagreement[b]={**metrics(rs),'uncertainty':cluster_interval(groups,spec)}
    watch={};alerts=0
    for method in spec['watchlist']['comparators']:
        n=sum(g['watch_rank_paired'][method]['n'] for g in gs)
        ah=sum(g['watch_rank_paired'][method]['opportunity_hits'] for g in gs)
        bh=sum(g['watch_rank_paired'][method]['comparator_hits'] for g in gs)
        groups={g['game_date']:[(g['watch_rank_paired'][method]['opportunity_hits']-g['watch_rank_paired'][method]['comparator_hits'])/g['watch_rank_paired'][method]['n']]*g['watch_rank_paired'][method]['n'] for g in gs if g['watch_rank_paired'][method]['n']}
        watch[method]={'n':n,'opportunity_hits':ah,'comparator_hits':bh,
            'opportunity_hit_rate':ah/n if n else None,'comparator_hit_rate':bh/n if n else None,
            'hit_rate_difference':(ah-bh)/n if n else None,'uncertainty':cluster_interval(groups,spec)}
    alerts=sum(g['watchlists']['opportunity']['resolved_n'] for g in gs)
    completed=sum(g['complete'] and bool(g['rows']) for g in gs)
    tail_groups={g['game_date']:[pinball(r['actual_points'],r['conditional_tail']['q90'])-pinball(r['actual_points'],r['v3_p90']) for r in g['rows'] if r['primary'] and all(r.get(k) is not None for k in ['historical_p90','v2_p90','v3_p90'])] for g in gs}
    tail_differences=[v for rs in tail_groups.values() for v in rs]
    opportunity_counts={method:{key:sum(g['watchlists'][method][key] for g in gs)
        for key in ['selected_n','resolved_n','ceiling_hits','fga_n','fga_growth_hits','minutes_n','minutes_growth_hits']}
        for method in ['opportunity',*spec['watchlist']['comparators']]}
    return signed({'version':'nba-points-experiment-report-v1','created_at':now(),'protocol_sha256':spec['sha256'],
        'model_sha256':spec['model_sha256'],'graded_slate_dates':len(gs),'completed_slate_dates':completed,
        'resolved_players':len(rows),'primary_players':len(primary),'resolved_alerts':alerts,
        'excluded_grades':dict(excluded),'unresolved':sum(len(g['unresolved']) for g in gs),
        'final_dnp':sum(len(g['final_dnp']) for g in gs),'disagreement':disagreement,'watch_rank_paired':watch,
        'watch_realized_opportunities':opportunity_counts,
        'ceilings_primary':ceilings(primary,spec),
        'primary_tail_comparison':{'n':len(tail_differences),
            'conditional_minus_original_v3_p90_loss':mean(tail_differences) if tail_differences else None,
            'uncertainty':cluster_interval(tail_groups,spec)},
        'ceilings_by_shooting_profile':{k:ceilings([r for r in primary if r['three_point_profile']==k],spec) for k in ['high','low']},
        'ceilings_by_minute_stability':{k:ceilings([r for r in primary if r['minute_stability']==k],spec) for k in ['stable','unstable']},
        'review_checkpoint_reached':completed>=spec['review']['completed_slate_dates'] and alerts>=spec['review']['resolved_alerts'],
        'status':'pending_no_prospective_grades' if not gs else 'collecting_fixed_prospective_evidence',
        'decision':'No automatic tuning, promotion or betting recommendation. Checkpoint does not establish validation.'})


def render(data):
    lines=['# NBA projection experiments','',data.get('status',data.get('mode','registered')),'',
        'Fixed prospective tests; experimental probabilities, no automatic promotion.','']
    if 'completed_slate_dates' in data:
        lines += [f"Completed prospective dates: {data['completed_slate_dates']}; resolved players: {data['resolved_players']}; primary players: {data['primary_players']}; resolved alerts: {data['resolved_alerts']}.",
            '', '| Disagreement | Players | V3 − market point error |', '|---|---:|---:|']
        for name,r in data['disagreement'].items():
            value='Pending' if r['n']==0 else f"{r['paired_mae_delta']:+.3f}"
            lines.append(f"| {name} | {r['n']} | {value} |")
        lines += ['', 'Negative error differences favor v3. Pending means no original prospective outcomes, not zero error.',
            '', 'Watchlist and conditional-tail probability audits, denominators, uncertainty and unresolved coverage are in report.json. Review checkpoint: 30 completed dates and 100 resolved alerts; no automatic promotion.']
    return '\n'.join(lines)+'\n'


def save_registration(forecast,source,spec,record_dir,research_root,created):
    """Attach fixed tests to a successful v3 recording, without editing it."""
    index=Path(research_root)/'experiments-index';index.mkdir(parents=True,exist_ok=True)
    marker=index/(forecast['game_date']+'-'+spec['sha256']+'.json')
    if marker.exists():return {'status':'preserved_original','index':str(marker)}
    experiment=freeze(forecast,source,spec,created)
    path=Path(record_dir)/'experiment.json';write_new(path,experiment)
    write_new(marker,{'date':forecast['game_date'],'protocol_sha256':spec['sha256'],'model_sha256':spec['model_sha256'],
        'experiment_sha256':experiment['sha256'],'experiment_path':str(path.resolve()),
        'forecast_sha256':forecast['sha256']})
    return {'status':'registered','experiment_sha256':experiment['sha256'],'index':str(marker),'watch_k':experiment['watch_k']}


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='action',required=True)
    q=sub.add_parser('init');q.add_argument('--model',type=Path,required=True);q.add_argument('--output',type=Path,required=True)
    for action in ['freeze','grade','report']:
        q=sub.add_parser(action);q.add_argument('--protocol',type=Path,default=DEFAULT_PROTOCOL);q.add_argument('--output-dir',type=Path,required=True)
        if action=='freeze':
            q.add_argument('--forecast',type=Path,required=True);q.add_argument('--source-snapshot',type=Path,required=True)
            q.add_argument('--allow-retrospective',action='store_true')
        if action=='grade':
            q.add_argument('--experiment',type=Path,required=True);q.add_argument('--v3-grade',type=Path,required=True);q.add_argument('--diagnostics',type=Path,required=True)
        if action=='report':q.add_argument('--research-root',type=Path,required=True)
    args=p.parse_args()
    if args.action=='init':
        spec=protocol(read(args.model),now());write_new(args.output,spec)
        print(json.dumps({'registered_protocol':str(args.output),'sha256':spec['sha256']}));return
    spec=read(args.protocol);verify(spec,PROTOCOL);args.output_dir.mkdir(parents=True,exist_ok=False)
    if args.action=='freeze':
        out=freeze(read(args.forecast),read(args.source_snapshot),spec,now(),args.allow_retrospective);filename='experiment.json'
    elif args.action=='grade':
        out=grade(read(args.experiment),read(args.v3_grade),read(args.diagnostics),spec,now());filename='experiment-grade.json'
    else:
        originals={}
        for path in (args.research_root/'experiments-index').glob('*.json'):
            index=read(path)
            if index['protocol_sha256']!=spec['sha256']:continue
            original=read(index['experiment_path']);verify(original,VERSION)
            if original['sha256']!=index['experiment_sha256'] or original['mode']!='prospective' or original['model_sha256']!=spec['model_sha256']:
                raise ValueError('invalid prospective experiment index')
            originals[original['sha256']]=original
        gs=[];unindexed=0
        for path in args.research_root.rglob('experiment-grade.json'):
            g=read(path);verify(g,GRADE)
            if g['experiment_sha256'] in originals:
                original=originals[g['experiment_sha256']]
                if any(g[k]!=original[k] for k in ['game_date','forecast_sha256','protocol_sha256','model_sha256','mode']):
                    raise ValueError('grade identity differs from registered original')
                gs.append(g)
            else:unindexed+=1
        out=report(gs,spec);out['indexed_recordings']=len(originals);out['unindexed_grade_files_excluded']=unindexed
        out=signed(out);filename='report.json'
    write_new(args.output_dir/filename,out)
    (args.output_dir/'review.md').write_text(render(out),encoding='utf-8')
    print(json.dumps({'output_dir':str(args.output_dir.resolve()),'version':out['version'],
        'status':out.get('status',out.get('mode')),'rows':len(out.get('rows',[])),
        'completed_dates':out.get('completed_slate_dates')}))


if __name__=='__main__':main()
