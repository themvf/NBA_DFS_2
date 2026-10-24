"""Audit saved pre-lock NFL Classic/Showdown value; never refit predictions."""
import gzip
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from zoneinfo import ZoneInfo

OUT = Path('artifacts/nfl-value-history-20261021')
x = json.load(gzip.open(OUT/'inputs.json.gz','rt',encoding='utf-8'))
dt = lambda v: datetime.fromisoformat(str(v).replace('Z','+00:00')) if v else None
alias = lambda t: {'WAS':'WSH','LA':'LAR','JAC':'JAX','AZ':'ARI'}.get(t,t)
canon = lambda k: '@'.join(alias(t) for t in (k or '').split('@'))
def norm(s):
    s = re.sub(r'\b(jr|sr|ii|iii|iv)\b','',s.lower())
    return re.sub('[^a-z0-9]','',s)
def number(v):
    return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)
games = {g['id']: g for g in x['games']}
uploads = {u['upload_id']:u for u in x['uploads']}
runs = {r['run_id']:r for r in x['selected_runs']}
results = {}
for r in x['results']:
    if r['scoring_status'] != 'exact' or not number(r['actual_dk_fpts']):
        continue
    key=(r['game_id'],r['player_id'])
    # v4 fixes DST components; older immutable scoring versions remain evidence only.
    quality=(r['scoring_version']=='nfl-dk-realized-v4',dt(r['computed_at']),r['id'])
    if key not in results or quality > results[key][0]:
        results[key]=(quality,r)
results={k:v[1] for k,v in results.items()}
lineups_by_run=defaultdict(list)
for l in x['lineups']:
    lineups_by_run[l['run_id']].append(l)
owners_by_contest=defaultdict(list)
for o in x['ownership']:
    owners_by_contest[o['contest_id']].append(o)

reports=[]
for group in sorted(x['groups'],key=lambda g:(g['week'],g['lock_at'],g['format'])):
    r=runs.get(group['selected_run_id'])
    upload=uploads.get(group['selected_upload_id'])
    if not upload:
        reports.append(dict(**group,status='no_pregame_forecast',rows=[]));continue
    ls=lineups_by_run[r['run_id']] if r else []
    boundary=r['created_at'] if r else upload['projection_as_of']
    ps=r['input_snapshot'] if r else []
    fallback=[]
    if not r:
        baseline={p['player_id']:p for p in x['baseline_projections'] if p['run_id']==upload['projection_run_id']}
        for p in x['players']:
            if p['upload_id']!=upload['upload_id']:
                continue
            b=baseline.get(p['ff_player_id'])
            if b and (alias(b['team'])!=alias(p['team']) or alias(b['opponent'])!=alias(p['opponent']) or b['position']!=p['position']):
                b=None
            fallback.append(dict(name=p['name'],position=p['position'],team=p['team'],opponent=p['opponent'],
                gameKey=p['game_key'],salary=p['salary'],captainSalary=p['captain_salary'],dkPlayerId=p['dk_player_id'],
                captainDkPlayerId=p['captain_dk_player_id'],ffPlayerId=p['ff_player_id'],ourProj=b['model_proj_fpts'] if b else None,
                projectionStatus=b['projection_status'] if b else 'unmatched',historyGames=b['history_games'] if b else 0,
                isOut=b['projection_status']=='out' if b else p['is_out'],baselineSource={'asOf':upload['projection_as_of']},
                projectionAudit={'final':b['model_proj_fpts'] if b else None,'source':'immutable_baseline_only'}))
        ps=fallback
    pmap={p['dkPlayerId']:p for p in ps}
    assert len(pmap)==len(ps),group['key']
    exposure=Counter(s['dkPlayerId'] for l in ls for s in l['slots'])
    captains=Counter(s['dkPlayerId'] for l in ls for s in l['slots'] if s['slot']=='CPT')
    flex=exposure-captains
    eligibility={e['dkPlayerId']:e for e in (r['settings'].get('runEvidence',{}).get('eligibility',[]) if r else [])}
    related=[c for c in x['contests'] if c['week']==group['week'] and c['format']==group['format']
        and (c['slate_upload_id'] in group['upload_ids'] or (not c['slate_upload_id'] and group['games']==['TB@DAL'] and '(TB @ DAL)' in c['contest_name']))]
    related.sort(key=lambda c:(c['file_name'].startswith('stat-api:'),c.get('entry_count') or 0),reverse=True)
    own={}
    for c in related:
        byname=defaultdict(list)
        for o in owners_by_contest[c['contest_id']]:byname[norm(o['player_name'])].append(o)
        for name,os in byname.items():
            if len(os)==1 and name not in own:own[name]=dict(os[0],field_source=c['file_name'])
    counts=Counter(norm(p['name']) for p in ps)
    game_by_key={canon(games[gid]['away']+'@'+games[gid]['home']):games[gid] for gid in group['game_ids']}
    coverage={}
    for g in game_by_key.values():
        teams={alias(z['team']) for (gid,_),z in results.items() if gid==g['id'] and z['position'] in ('QB','WR','RB','TE')}
        coverage[str(g['id'])]=dict(game=canon(g['away']+'@'+g['home']),completed=g['completed'],
            covered={alias(g['away']),alias(g['home'])}<=teams,teams=sorted(teams))
    issues=[]
    for l in ls:
        if len(l['slots'])!=(9 if group['format']=='classic' else 6):issues.append(dict(lineup=l['lineup_number'],issue='slot_count'))
        if len({s['dkPlayerId'] for s in l['slots']})!=len(l['slots']):issues.append(dict(lineup=l['lineup_number'],issue='duplicate_player'))
        salary=sum(s['salary'] for s in l['slots'])
        if salary!=l['total_salary'] or salary>50000:issues.append(dict(lineup=l['lineup_number'],issue='salary'))
        if abs(sum(s['projection'] for s in l['slots'])-l['projected_fpts'])>0.011:issues.append(dict(lineup=l['lineup_number'],issue='projection_sum'))
        for s in l['slots']:
            p=pmap.get(s['dkPlayerId'])
            if p is None:
                issues.append(dict(lineup=l['lineup_number'],issue='missing_snapshot_identity'));continue
            mult=1.5 if s['slot']=='CPT' else 1
            a=p.get('projectionAudit') or {}
            if number(a.get('final')) and abs(s['projection']-a['final']*mult)>0.011:
                issues.append(dict(lineup=l['lineup_number'],player=p['name'],issue='audit_projection_difference',slot_base=s['projection']/mult,audit=a['final']))
            expected=p.get('captainSalary') if mult==1.5 else p['salary']
            if expected is not None and expected!=s['salary']:
                issues.append(dict(lineup=l['lineup_number'],player=p['name'],issue='slot_salary_difference'))
    rows=[]
    for p in ps:
        if p['salary']>=5000:
            continue
        key=canon(p.get('gameKey'))
        g=game_by_key.get(key)
        z=results.get((g['id'],p.get('ffPlayerId'))) if g else None
        identity_ok=bool(g and alias(p['team']) in (alias(g['away']),alias(g['home'])) and alias(p.get('opponent')) in (alias(g['away']),alias(g['home'])) and alias(p['team'])!=alias(p.get('opponent')))
        if z and (alias(z['team'])!=alias(p['team']) or alias(z['opponent'])!=alias(p.get('opponent')) or z['position']!=p['position']):
            identity_ok=False
        o=own.get(norm(p['name'])) if counts[norm(p['name'])]==1 else None
        canonical=z['actual_dk_fpts'] if z and identity_ok else None
        field=o.get('fpts') if o else None
        conflict=number(canonical) and number(field) and abs(canonical-field)>0.011
        # A contest record is what the slate paid; retain source disagreements explicitly.
        actual=field if identity_ok and number(field) else canonical
        src='contest' if identity_ok and number(field) else 'canonical_exact' if canonical is not None else 'unresolved'
        a=p.get('projectionAudit') or {}
        base=a.get('final',p.get('ourProj'))
        slot_bases={round(s['projection']/(1.5 if s['slot']=='CPT' else 1),9) for l in ls for s in l['slots'] if s['dkPlayerId']==p['dkPlayerId']}
        if len(slot_bases)==1 and number(base) and abs(next(iter(slot_bases))-base)>0.011:
            base=next(iter(slot_bases))
        status=p.get('projectionStatus')
        baseline_time=(p.get('baselineSource') or {}).get('asOf')
        selected_capture=(a.get('modelSnapshot') or {}).get('capturedAt')
        late=any(dt(t)>dt(boundary) for t in (baseline_time,selected_capture) if t)
        supported=identity_ok and number(base) and status=='historical' and not p.get('isOut') and not a.get('excluded') and not late
        forecast_kind='supported' if supported else 'late_input' if late else 'role_or_out_zero' if p.get('isOut') or status=='out' else 'position_prior' if status=='position_prior' else 'unsupported'
        proj=base if supported else None
        eligible=eligibility.get(p['dkPlayerId'])
        cap=(r['settings'].get('maxExposureByPlayer',{}).get(str(p['dkPlayerId']),r['settings'].get('maxExposure')) if r else None)
        cap_count=math.floor(cap*len(ls)+1e-6) if number(cap) and ls else None
        signals=p.get('playerSignals') or []
        total=exposure[p['dkPlayerId']] if r else None
        pct=100*total/len(ls) if ls else None
        delta=actual-proj if number(actual) and number(proj) else None
        if not supported:verdict='Forecast/identity coverage review'
        elif not ls:verdict='Projection review only; no saved portfolio'
        elif delta is None or delta<=0:verdict='Did not beat projection / result unavailable'
        elif cap_count is not None and total>=cap_count and cap_count>0:verdict='Already at saved exposure cap'
        elif pct>10:verdict='Already represented'
        elif eligible and eligible.get('eligible') is False:verdict='Pool exclusion review'
        elif signals:verdict='Low exposure with frozen opportunity signals'
        else:verdict='Low exposure; pre-lock case unproven'
        components=z['scoring_evidence'].get('scoring_input',z['scoring_evidence'].get('scoring_components')) if z else None
        rows.append(dict(name=p['name'],position=p['position'],team=alias(p['team']),opponent=alias(p.get('opponent')),
            dk_player_id=p['dkPlayerId'],ff_player_id=p.get('ffPlayerId'),salary=p['salary'],captain_salary=p.get('captainSalary'),
            captain_under_5000=number(p.get('captainSalary')) and p['captainSalary']<5000,
            forecast_kind=forecast_kind,projection=proj,effective_optimizer_projection=base,projection_source=a.get('source'),
            actual=actual,actual_source=src,canonical_actual=canonical,result_id=z['id'] if z else None,
            scoring_version=z['scoring_version'] if z else None,scoring_components=components,
            delta=delta,value_per_1000=actual/(p['salary']/1000) if number(actual) and p['salary'] else None,
            captain_projection=proj*1.5 if number(proj) else None,captain_actual=actual*1.5 if number(actual) else None,
            captain_delta=delta*1.5 if number(delta) else None,entries=total,
            captain_entries=captains[p['dkPlayerId']] if r and group['format']=='showdown' else None,
            flex_entries=flex[p['dkPlayerId']] if r and group['format']=='showdown' else None,
            exposure_pct=pct,cap=cap,eligibility=eligible,verdict=verdict,signals=signals,prior_opportunity=p.get('priorOpportunity'),
            availability=p.get('availability'),history_games=p.get('historyGames'),identity_method=p.get('identityMethod'),
            field_ownership=o.get('drafted_pct') if o else None,field_by_slot=o.get('drafted_by_slot') if o else None,
            field_contest_id=o.get('contest_id') if o else None,result_conflict=conflict,baseline_as_of=baseline_time))
    scored=[p for p in rows if number(p['delta'])]
    winners=sorted([p for p in scored if p['delta']>0],key=lambda p:p['delta'],reverse=True)
    # This is a transparent reporting screen, not a calibrated exposure rule.
    notable=[p for p in winners if p['delta']>=5 and p['actual']>=10]
    gaps=sorted([p for p in rows if p['projection'] is None and (p['actual'] or 0)>=10],key=lambda p:p['actual'],reverse=True)
    report=dict(**group,status='portfolio' if r else 'baseline_only',selected_created_at=boundary,
        run_mode=r['mode'] if r else None,projection_source=r['projection_source'] if r else 'immutable_baseline_only',
        requested_lineups=r['requested_lineups'] if r else None,lineups=len(ls) if r else None,coverage=coverage,verification_issues=issues,
        contests=[{k:c[k] for k in ['contest_id','contest_name','file_name','entry_count']} for c in related],
        metrics=dict(listed_under_5000=len(rows),supported_scored=len(scored),outperformers=len(winners),notable=len(notable),
            mean_error_actual_minus_projection=mean(p['delta'] for p in scored) if scored else None,
            mae=mean(abs(p['delta']) for p in scored) if scored else None,
            missing_actual=sum(p['actual'] is None for p in rows),conflicting_actual=sum(p['result_conflict'] for p in rows)),
        rows=rows,outperformers=winners,notable=notable,coverage_gaps=gaps)
    reports.append(report)

out=dict(season=2026,weeks=[1,2,3,4,5,6],captured_at=x['captured_at'],reports=reports,
    policy={'salary':'strictly less than $5,000 FLEX/base salary; Captain purchase eligibility recorded separately',
        'exposure':'latest pre-lock user-upload build, not submitted entries; system experiment builds excluded',
        'notable_screen':'actual >=10 and actual minus supported frozen forecast >=5; descriptive thresholds',
        'unknown_results':'missing rows stay unknown, never automatically zero',
        'roles':'historical same-time dual-depth and official inactive evidence unavailable; no role promotion inferred'})
(OUT/'assessment.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
print(json.dumps({'slates':len(reports),'formats':dict(Counter(g['format'] for g in reports)),
    'lineups':sum(g.get('lineups') or 0 for g in reports),'verification_issues':sum(len(g.get('verification_issues',[])) for g in reports),
    'supported_scored':sum(g.get('metrics',{}).get('supported_scored',0) for g in reports),
    'result_conflicts':sum(g.get('metrics',{}).get('conflicting_actual',0) for g in reports)}))
for g in reports:
    print(g['week'],g['format'],g['games'] if g['format']=='showdown' else 'main','n',g.get('lineups'),
        'notable',g.get('metrics',{}).get('notable'),'TOP',[(p['name'],p['salary'],round(p['projection'],2),p['actual'],p['entries'],p['verdict']) for p in g.get('notable',[])[:4]],
        'GAPS',[(p['name'],p['actual'],p['forecast_kind']) for p in g.get('coverage_gaps',[])[:3]])
