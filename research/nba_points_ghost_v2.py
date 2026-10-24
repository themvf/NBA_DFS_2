"""Versioned NBA shadow forecasts with merged, decision-time historical evidence.

The historical quantile and a ceiling-watch score are distinct outputs. No
watch score is exposed as a calibrated probability or added to a point forecast.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import math
import json
from statistics import mean, stdev

from model.nba_points_ladder import player_key, team_key
from research.nba_points_forward import aware, digest
from research.nba_points_ghost import verify

VERSION = 'nba-points-ghost-v2'
ARTIFACT_VERSION = 'nba-ghost-opportunity-model-v1'
MODELS = {
    'history20': 'Arithmetic points mean of up to 20 merged prior regular-season appearances; minimum ten.',
    'blend90_10': 'Market line + clip(.10*(history20-market line),-2,+2); fixed research assumption.',
    'opportunity_points': 'Separate learned FGA/3PA/FTA forecasts multiplied by shrunken prior conversion rates; experimental.',
}
STATS = ['minutes', 'points', 'fgm', 'fga', 'fg3m', 'fg3a', 'ftm', 'fta']


def finite(value):
    return value is not None and not isinstance(value, bool) and math.isfinite(float(value))


def quantile(values, q):
    values = sorted(float(v) for v in values)
    at = (len(values)-1)*q
    lo, hi = math.floor(at), math.ceil(at)
    return values[lo]+(values[hi]-values[lo])*(at-lo)


def season_day(day):
    day = date.fromisoformat(str(day))
    return day.year if day.month >= 10 else day.year-1


def valid_stats(row):
    if not all(finite(row.get(k)) and float(row[k]) >= 0 for k in STATS):
        return False
    return (row['minutes'] > 0 and row['fg3m'] <= row['fg3a'] <= row['fga']
            and row['fgm'] <= row['fga'] and row['fg3m'] <= row['fgm']
            and row['ftm'] <= row['fta']
            and abs(2*row['fgm']+row['fg3m']+row['ftm']-row['points']) < 1e-6)


def merge_history(logs, boxes, games, teams, classifications, target, cutoff):
    """Unique exact-name identities; NBA/ESPN IDs remain separate provenance.

    Source duplicates and disagreements block the affected identity. Equal game
    date, home team and away team form the cross-provider event key. Neither
    dates alone nor provider-specific IDs are used to deduplicate games.
    """
    target = str(target); cutoff = aware(cutoff)
    names = {int(t['team_id']):t['name'] for t in teams}
    game_map = {str(g['espn_event_id']):g for g in games}
    proofs = defaultdict(list)
    for proof in classifications:
        if aware(proof['captured_at']) < cutoff and proof.get('source_url'):
            proofs[str(proof['espn_event_id'])].append(proof)
    groups = defaultdict(list); blocked = defaultdict(set); audit = Counter()
    box_totals=defaultdict(lambda:defaultdict(float))
    for row in boxes:
        g=game_map.get(str(row['espn_event_id']))
        if g and row.get('played') and aware(row['fetched_at'])<cutoff and valid_stats(row):
            box_totals[str(row['espn_event_id'])][team_key(row['team'])+'_points']+=row['points']
            box_totals[str(row['espn_event_id'])][team_key(row['team'])+'_fga']+=row['fga']
    seen = set()
    def add(row):
        key = player_key(row['name'])
        if not row['source_player_id'] or row['source_player_id']=='None':
            blocked[key].add('missing_source_player_id');return
        identity = (row['source'], row['source_game_id'], row['source_player_id'])
        if identity in seen:
            blocked[key].add('duplicate_source_identity'); audit['duplicate_source_identity'] += 1
            return
        seen.add(identity)
        if not valid_stats(row):
            blocked[key].add('invalid_scoring_or_shot_accounting'); audit['invalid_stats'] += 1
            return
        groups[key].append(row)
    for raw in logs:
        if str(raw['game_date']) >= target or aware(raw['fetched_at']) >= cutoff:
            audit['log_after_boundary'] += 1; continue
        if raw['season_type'] != 'Regular Season' or not raw.get('minutes') or raw['minutes'] <= 0 or raw.get('points') is None:
            continue
        key = player_key(raw['name'])
        if raw.get('team_id') not in names or raw.get('opponent_team_id') not in names or raw.get('is_home') is None:
            blocked[key].add('missing_canonical_game_teams'); continue
        own, opponent = names[raw['team_id']], names[raw['opponent_team_id']]
        home, away = (own,opponent) if raw['is_home'] else (opponent,own)
        row = {**raw,'source':'nba_log','source_game_id':str(raw['game_id']),
               'source_player_id':str(raw['player_id']),'team':own,
               'event_key':(str(raw['game_date']),team_key(home),team_key(away)),
               'available_at':str(raw['fetched_at'])}
        add(row)
    for raw in boxes:
        g = game_map.get(str(raw['espn_event_id']))
        if not g or str(g['game_date']) >= target or aware(raw['fetched_at']) >= cutoff or aware(g['fetched_at']) >= cutoff:
            audit['box_after_boundary_or_missing_game'] += 1; continue
        if not g['completed'] or not raw.get('played'):
            continue
        evidence = proofs.get(str(raw['espn_event_id']),[])
        if not evidence or {str(e['season_type']) for e in evidence} != {'2'}:
            audit['box_regular_season_unverified'] += 1; continue
        if any(team_key(e['home_team']) != team_key(g['home_team']) or team_key(e['away_team']) != team_key(g['away_team']) or str(e['game_date']) != str(g['game_date']) for e in evidence):
            blocked[player_key(raw['player'])].add('classification_game_mismatch'); continue
        expected = g['home_team'] if raw['is_home'] else g['away_team']
        if team_key(raw['team']) != team_key(expected):
            blocked[player_key(raw['player'])].add('box_team_mismatch'); continue
        totals=box_totals[str(raw['espn_event_id'])]
        if any(not finite(g.get(side+'_score')) or abs(totals[team_key(g[side+'_team'])+'_points']-g[side+'_score'])>1e-6 for side in ['home','away']):
            audit['box_team_scores_unreconciled']+=1;continue
        row = {**raw,'name':raw['player'],'game_date':str(g['game_date']),
               'source':'espn_box','source_game_id':str(raw['espn_event_id']),
               'source_player_id':str(raw['espn_athlete_id']),
               'event_key':(str(g['game_date']),team_key(g['home_team']),team_key(g['away_team'])),
               'available_at':max(aware(raw['fetched_at']),aware(g['fetched_at'])).isoformat(),
               'team_fga':totals[team_key(raw['team'])+'_fga']}
        add(row)
    result = {}
    for key, rows in groups.items():
        if any(len({r['source_player_id'] for r in rows if r['source']==source}) > 1 for source in ['nba_log','espn_box']):
            blocked[key].add('ambiguous_cross_provider_name')
        by_event = defaultdict(list)
        for row in rows: by_event[row['event_key']].append(row)
        chosen = []
        for _,same in by_event.items():
            if len({team_key(r['team']) for r in same}) > 1:
                blocked[key].add('same_game_team_disagreement'); continue
            if len(same)>1:
                # Rounded minute values can differ; the scoring/counting stats must agree.
                if any(any(abs(float(r[k])-float(same[0][k])) > 1e-6 for k in STATS if k!='minutes') for r in same[1:]):
                    blocked[key].add('cross_provider_stats_disagreement'); continue
                audit['cross_provider_duplicates_collapsed'] += len(same)-1
            preferred = next((r for r in same if r['source']=='nba_log'),same[0])
            preferred = {**preferred,'provenance':[{'source':r['source'],'player_id':r['source_player_id'],'game_id':r['source_game_id'],'available_at':r['available_at']} for r in same]}
            chosen.append(preferred)
        if len({str(r['game_date']) for r in chosen}) != len(chosen):
            blocked[key].add('multiple_games_same_player_date')
        if key not in blocked:
            result[key] = sorted(chosen,key=lambda r:(str(r['game_date']),r['event_key']))
    return result, {k:sorted(v) for k,v in blocked.items()}, dict(audit)


def history_features(rows):
    """Same feature definitions as the registered exploratory tail replay."""
    rows = rows[-20:]
    if len(rows)<10: return None
    def avg(k,n): return mean(float(r[k]) for r in rows[-n:])
    def sd(k,n): return stdev(float(r[k]) for r in rows[-n:])
    total = lambda k:sum(float(r[k]) for r in rows)
    f = {'p5':avg('points',5),'p10':avg('points',10),'p20':avg('points',20),
         'p_std10':sd('points',10),'p_last':rows[-1]['points'],
         'reference_q90':quantile([r['points'] for r in rows],.9)}
    weights = [.75**9]+[.25*.75**k for k in range(8,-1,-1)]
    f['ewma10'] = sum(float(r['points'])*w for r,w in zip(rows[-10:],weights))
    f['scoring_trend'] = f['p5']-f['p20']
    for col in ['minutes','fga','fg3a','fta']:
        for n in ([3,10,20] if col=='minutes' else [3,10]): f[col+str(n)] = avg(col,n)
    f['points_per_min10'] = f['p10']/f['minutes10']
    f['fga_per_min10'] = f['fga10']/f['minutes10']
    f['shot_share10'] = mean(r['fga']/r['team_fga'] for r in rows[-10:]) if all(finite(r.get('team_fga')) and r['team_fga']>0 for r in rows[-10:]) else None
    f['minutes_growth'] = f['minutes3']-f['minutes20']
    f['shots_growth'] = f['fga3']-avg('fga',20)
    f['ft_growth'] = f['fta3']-avg('fta',20)
    f['fga_last'],f['fta_last'] = rows[-1]['fga'],rows[-1]['fta']
    two_den = total('fga')-total('fg3a')
    f['two_rate20'] = (total('fgm')-total('fg3m'))/two_den if two_den else None
    f['three_rate20'] = total('fg3m')/total('fg3a') if total('fg3a') else None
    f['free_rate20'] = total('ftm')/total('fta') if total('fta') else None
    f['volume_points3'] = (2*(f['fga3']-f['fg3a3'])*f['two_rate20']+3*f['fg3a3']*f['three_rate20']+f['fta3']*f['free_rate20']) if all(finite(f[k]) for k in ['two_rate20','three_rate20','free_rate20']) else None
    f['conversion_gap3'] = f['volume_points3']-avg('points',3) if finite(f['volume_points3']) else None
    return f


def linear_predict(spec, features):
    values=[]
    for i,name in enumerate(spec['features']):
        value=features.get(name)
        value=float(value) if finite(value) else spec['impute'][i]
        values.append((value-spec['mean'][i])/spec['scale'][i])
    return float(spec['intercept']+sum(x*w for x,w in zip(values,spec['coef'])))


def freeze(market, merged, blocked, audit, created, model=None, classifications=None, availability=None):
    from research.nba_points_forward import VERSION as MARKET_VERSION
    verify(market,MARKET_VERSION)
    created=aware(created); lock=aware(market['lock_at']);cutoff=min(created,lock)
    if created<aware(market['created_at']): raise ValueError('ghost creation precedes market snapshot')
    starts=[aware(g['start_time']) for g in market.get('espn_scheduled_starts',[]) if g.get('start_time')]
    if starts and lock>min(starts):raise ValueError('common lock is after first scheduled start')
    if model:
        verify(model,ARTIFACT_VERSION)
        if aware(model['created_at'])>=created or str(model['calibration_end'])>=market['game_date']:
            raise ValueError('model did not exist before this prediction or was trained on future outcomes')
    rows=[]
    for c in market['candidates']:
        key=c['player_key'];history=merged.get(key,[])[-20:]
        # Guard pure callers too; a merge built for another cutoff must not leak.
        if any(str(r['game_date'])>=market['game_date'] or any(aware(p['available_at'])>=cutoff for p in r['provenance']) for r in history):
            raise ValueError('historical inputs exceed decision boundary')
        f=history_features(history) if history else None
        hist=f['p20'] if f else None
        predictions={'market_line':c['line'],'history20':hist,'blend90_10':c['line']+max(-2,min(2,.1*(hist-c['line']))) if hist is not None else None,'opportunity_points':None}
        forecasts={};watch={'status':'insufficient_history','flag':None,'score':None,'probability':None}
        prior90=f['reference_q90'] if f else None
        p90=None
        if f and model:
            forecasts={name:max(0,linear_predict(spec,f)) for name,spec in model['opportunity_models'].items()}
            forecasts['minutes']=min(48,forecasts['minutes'])
            forecasts['fg3a']=min(forecasts['fga'],forecasts['fg3a'])
            total=lambda k:sum(float(r[k]) for r in history)
            # Fixed 20-attempt shrinkage toward development-only league rates.
            rates={
                'two':(total('fgm')-total('fg3m')+20*model['league_rates']['two'])/(total('fga')-total('fg3a')+20),
                'three':(total('fg3m')+20*model['league_rates']['three'])/(total('fg3a')+20),
                'free':(total('ftm')+20*model['league_rates']['free'])/(total('fta')+20)}
            predictions['opportunity_points']=2*(forecasts['fga']-forecasts['fg3a'])*rates['two']+3*forecasts['fg3a']*rates['three']+forecasts['fta']*rates['free']
            primary=f['p10']>=20
            p90=max(0,linear_predict(model['p90_model'],f)+model['p90_offsets'][str(primary)])
            score=linear_predict(model['watch_model'],f)
            watch={'status':'experimental','flag':score>=model['watch_score_thresholds'][str(primary)],'score':score,'probability':None,
                   'reference_points':prior90,'event':'actual points strictly above frozen historical reference P90'}
        facts=[]
        if history and history[-1]['minutes']>=30 and history[-1]['fga']>=18:facts.append('large_last_game_workload')
        if f and (f['minutes_growth']>=2 or f['shots_growth']>=2):facts.append('recent_opportunity_increase')
        statuses=list(blocked.get(key,[]))
        if not f:statuses.append('insufficient_history_for_distribution')
        current=sum(season_day(r['game_date'])==season_day(market['game_date']) for r in history)
        if history and current<10:statuses.append('opening_season_transfer_unvalidated')
        if f and f['shot_share10'] is None:statuses.append('missing_team_shot_share_imputed')
        if history and (date.fromisoformat(market['game_date'])-date.fromisoformat(str(history[-1]['game_date']))).days>30:statuses.append('stale_recent_role')
        evidence=(availability or {}).get(key)
        if evidence and (aware(evidence['captured_at'])>=cutoff or not evidence.get('source_url')):raise ValueError('availability evidence is late or missing provenance')
        if evidence and evidence.get('team') and team_key(evidence['team']) not in {team_key(c['home_team']),team_key(c['away_team'])}:
            raise ValueError('availability evidence refers to a different game team')
        if evidence and evidence.get('player') and player_key(evidence['player'])!=key:
            raise ValueError('availability evidence refers to a different player')
        if evidence and str(evidence.get('status','')).lower()=='out':
            statuses.append('reported_out');watch={**watch,'flag':None,'status':'reported_out'}
        rows.append({**c,'history':history,'history_status':'blocked' if key in blocked else ('ready' if f else 'insufficient_history'),
                     'history_count':len(history),'current_season_history_count':current,'features':f,'predictions':predictions,
                     'opportunity_forecasts':forecasts,'historical_p90':prior90,'experimental_p90':p90,
                     'market_ceiling_approximation':c.get('ceiling'),'ceiling_watch':watch,'observed_workload_flags':facts,
                     'availability':evidence or {'status':'unverified'},'quality_flags':statuses})
    result={'version':VERSION,'models':MODELS,'created_at':created.isoformat(),'game_date':market['game_date'],
            'lock_at':market['lock_at'],'as_of':cutoff.isoformat(),'market_as_of':market['as_of'],
            'mode':'prospective' if created<lock and market['mode']=='prospective' else 'retrospective_reconstruction',
            'market_snapshot_sha256':market['sha256'],'model_sha256':model['sha256'] if model else None,
            'source_audit':audit,'classification_evidence':classifications or [],'candidates':rows,
            'limitations':['Experimental only; no validated improvement over market projections.',
                           'Historical P90 and conditional experimental P90 are separate; watch targets the historical threshold.',
                           'Watch scores are rankings, not calibrated probabilities. Workload flags are observations, not bonuses.',
                           'Current injury/rotation restrictions remain unresolved without timestamped availability evidence.',
                           'Minimum ten appearances excludes rookie distribution estimates; cross-season transfer is unvalidated.']}
    result=json.loads(json.dumps(result,default=str))
    result['sha256']=digest(result)
    return result


def summarize(rows):
    result={}
    for name in MODELS:
        paired=[r for r in rows if r['status'] in ('graded','push') and finite(r.get('actual_points')) and finite(r['predictions'].get(name))]
        def errors(key):
            values=[r['predictions'][key]-r['actual_points'] for r in paired]
            return {'n':len(values),'mae':mean(abs(e) for e in values) if values else None,
                    'rmse':math.sqrt(mean(e*e for e in values)) if values else None,'bias':mean(values) if values else None}
        ghost,base=errors(name),errors('market_line')
        result[name]={'ghost':ghost,'paired_market':base,'mae_change':ghost['mae']-base['mae'] if paired else None}
    for threshold in ['historical_p90','experimental_p90','market_ceiling_approximation']:
        paired=[r for r in rows if r['status'] in ('graded','push') and finite(r.get('actual_points')) and finite(r.get(threshold))]
        losses=[max(.9*(r['actual_points']-r[threshold]),-.1*(r['actual_points']-r[threshold])) for r in paired]
        result[threshold]={'n':len(paired),'hits':sum(r['actual_points']>r[threshold] for r in paired),
                           'hit_rate':mean(r['actual_points']>r[threshold] for r in paired) if paired else None,
                           'quantile_loss':mean(losses) if losses else None}
    watch={}
    for flag in [True,False]:
        group=[r for r in rows if r['ceiling_watch']['flag'] is flag and r['status'] in ('graded','push') and finite(r.get('actual_points')) and finite(r.get('historical_p90'))]
        watch[str(flag)]={'n':len(group),'hits':sum(r['actual_points']>r['historical_p90'] for r in group),
                          'hit_rate':mean(r['actual_points']>r['historical_p90'] for r in group) if group else None}
    result['ceiling_watch']=watch
    return result


def grade(snapshot,games):
    from model.nba_points_calibration import Capture,grade_captures
    verify(snapshot,VERSION)
    if any(g.start_time and g.start_time<aware(snapshot['lock_at']) for g in games):raise ValueError('verified schedule precedes common cutoff')
    rows=[]
    for c in snapshot['candidates']:
        cap=Capture(c['event_id'],aware(c['commence_time']),aware(c['captured_at']),c['home_team'],c['away_team'],c['player'],c['main_books'])
        outcome=grade_captures(date.fromisoformat(snapshot['game_date']),[cap],{},games)[0]
        rows.append({k:c[k] for k in ['event_id','player_key','player','predictions','historical_p90','experimental_p90','market_ceiling_approximation','ceiling_watch','quality_flags']})
        rows[-1].update(status=outcome.status,actual_points=outcome.actual_points,actual_minutes=outcome.minutes)
    return {'version':VERSION,'models':MODELS,'game_date':snapshot['game_date'],'snapshot_mode':snapshot['mode'],
            'snapshot_sha256':snapshot['sha256'],'model_sha256':snapshot['model_sha256'],
            'graded_at':datetime.now(timezone.utc).isoformat(),'rows':rows,'by_status':dict(Counter(r['status'] for r in rows)),
            'all_players':summarize(rows),'market_line_at_least20':summarize([r for r in rows if r['predictions']['market_line']>=20])}


def report(grades):
    days=set();model_ids=set();rows=[];excluded=[]
    for g in grades:
        if g.get('version')!=VERSION or g.get('models')!=MODELS:raise ValueError('incompatible version')
        if g['snapshot_mode']!='prospective':excluded.append(g['game_date']);continue
        if g['game_date'] in days:raise ValueError('multiple snapshots per date; select one before inspecting outcomes')
        days.add(g['game_date']);model_ids.add(g['model_sha256']);rows.extend(g['rows'])
    if len(model_ids)>1:raise ValueError('different fitted models must not be pooled')
    return {'version':VERSION,'dates':sorted(days),'retrospective_dates_excluded':excluded,
            'by_status':dict(Counter(r['status'] for r in rows)),'all_players':summarize(rows),
            'market_line_at_least20':summarize([r for r in rows if r['predictions']['market_line']>=20]),
            'interpretation':'Descriptive paired errors and frozen-threshold hits; no validated edge implied.'}
