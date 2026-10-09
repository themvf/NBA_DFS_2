"""Capture, freeze, forecast, grade and evaluate market-free single-game leaders.

python -m research.nfl_game_leaders capture --output CAPTURE.json.gz
python -m research.nfl_game_leaders forecast --input CAPTURE.json.gz --request REQUEST.json --output FORECAST.json
python -m research.nfl_game_leaders backtest --input CAPTURE.json.gz --season 2025 --start-week 5 --end-week 8 --output REPORT.json
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from hashlib import sha256
import json
import io
from pathlib import Path

import numpy as np

from model.nfl_game_leaders import METRICS, SOURCE_METRICS, VERSION, Settings, forecast, grade, prepare
from model.nfl_longest_touchdown import canonical, timestamp
from research.nfl_longest_touchdown import capture as capture_pbp, read, write


def full_box_capture(result, minimum_season, maximum_season):
    # The fantasy DB's rows are filtered through its current player universe:
    # retired/unlisted historical players are absent. Use the FULL source feed.
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    boxes, sources, unmatched = [], [], []
    index = defaultdict(list)
    for g in result['games']:
        for team,opponent in ((g['away'],g['home']),(g['home'],g['away'])):
            index[g['season'],g['week'],team,opponent].append(g)
    for season in range(minimum_season,maximum_season+1):
        url=NFLVERSE_WEEKLY_STATS_URL.format(season=season)
        response=requests.get(url,timeout=90)
        response.raise_for_status()
        fetched=datetime.now(timezone.utc).isoformat()
        frame=pd.read_csv(io.BytesIO(response.content),low_memory=False)
        sources.append({'url':url,'sha256':sha256(response.content).hexdigest(),'fetched_at':fetched,'rows':len(frame)})
        for s in json.loads(frame.to_json(orient='records')):
            if s.get('season_type')!='REG' or not s.get('player_id'):
                continue
            # All positions can produce a carry/catch (including specialists).
            if not any(s.get(k) for k in ('carries','targets',*SOURCE_METRICS)):
                continue
            team=canonical(s.get('team') or s.get('recent_team'))
            opponent=canonical(s.get('opponent_team') or s.get('opponent'))
            games=index[int(s['season']),int(s['week']),team,opponent]
            if len(games)!=1:
                unmatched.append({'season':season,'week':s['week'],'identity':s['player_id'],'team':team,'opponent':opponent})
                continue
            g=games[0]
            if s.get('game_id') and s['game_id']!=g['game_id']:
                raise ValueError('Box source game ID disagrees with canonical schedule')
            boxes.append({'game_id':g['game_id'],'identity':s['player_id'],
                'name':s.get('player_display_name') or s.get('player_name') or s['player_id'],
                'team':team,'position':s.get('position') or 'UNKNOWN','fetched_at':fetched,
                **{k:s.get(k) for k in ('carries','targets',*SOURCE_METRICS)}})
    result.update(version=VERSION,boxes=boxes,box_source='complete nflverse weekly player source, canonical schedule/GSIS',
        box_sources=sources,box_unmatched=unmatched,
        box_coverage={'rows':len(boxes),'games':len({b['game_id'] for b in boxes})})
    return result


def roster(history, game, recent_games=3):
    candidates = {}
    for team in (game['away'],game['home']):
        prior = [h for h in history if team in (h['game']['away'],h['game']['home'])][-recent_games:]
        for h in prior:
            for b in h['boxes']:
                if b['team']==team and b['carries']+b['targets']>0:
                    observed=timestamp(h['game']['kickoff'])
                    if b['identity'] not in candidates or observed>candidates[b['identity']][0]:
                        candidates[b['identity']]=(observed,{'identity':b['identity'],'name':b['name'],
                            'team':team,'position':b['position'],'status':'unresolved'})
    return [p for _,p in candidates.values()]


def expected_history(snapshot, game, decision_at, recent_games=6):
    """Require the actual latest games, never silently replace rejected games."""
    result = {}
    for team in (game['away'], game['home']):
        games = sorted((g for g in snapshot['games'] if g.get('completed')
            and team in (g['away'], g['home']) and timestamp(g['kickoff']) < timestamp(decision_at)),
            key=lambda g: (timestamp(g['kickoff']), g['game_id']))[-recent_games:]
        current = [g for g in games if g['season'] == game['season']]
        result[team] = [g['game_id'] for g in (current or games)]
    return result


def evidence_request(snapshot, game_id):
    """Freeze current dual depth, week injuries and official coverage, read only.

    Eligibility is not a target forecast. Unresolved providers are never silently
    overridden; absent game-day evidence remains explicit.
    """
    from config import load_config
    from db.database import DatabaseManager
    from research.nfl_dual_depth_audit import capture as depth_capture
    games=[g for g in snapshot['games'] if g['game_id']==game_id]
    if len(games)!=1:
        raise ValueError('Require unique canonical game')
    game=games[0]
    depth={t:depth_capture(game['season'],t) for t in (game['away'],game['home'])}
    db=DatabaseManager(load_config().database_url,initialize_schema=False)
    with db.reuse_connection():
        players=db.execute("""SELECT id,gsis_id identity,canonical_name name,team_abbrev team,position,
            metadata->'sleeper'->>'injury_status' injury_status,metadata->'sleeper'->>'status' sleeper_status,fetched_at
            FROM ff_players WHERE season=%s AND team_abbrev IN (%s,%s) AND active AND gsis_id IS NOT NULL
            AND position IN ('QB','RB','WR','TE') ORDER BY team_abbrev,id""",
            (game['season'],game['away'],game['home']))
        injuries=db.execute("""SELECT DISTINCT ON (i.player_id) i.player_id,i.normalized_status,i.observed_at,
            i.source_snapshot_id,i.raw_payload,s.request_params FROM ff_player_injury_observations i
            JOIN ff_players p ON p.id=i.player_id JOIN ff_source_snapshots s ON s.id=i.source_snapshot_id
            WHERE i.season=%s AND i.source='fantasypros' AND p.team_abbrev IN (%s,%s)
              AND s.request_params->>'week'=%s ORDER BY i.player_id,i.observed_at DESC,i.id DESC""",
            (game['season'],game['away'],game['home'],str(game['week'])))
        inactives=db.execute("SELECT * FROM nfl_official_inactive_imports WHERE season=%s AND week=%s",
            (game['season'],game['week']))
    cutoff=datetime.now(timezone.utc).isoformat()
    if timestamp(cutoff)>=timestamp(game['kickoff']):
        raise ValueError('Current evidence capture is after kickoff; do not call it pregame')
    injury_by_id={r['player_id']:r for r in injuries}
    candidates=[]
    for p in players:
        i=injury_by_id.get(p['id'])
        sleeper=(p['injury_status'] or p['sleeper_status'] or '').upper()
        fp=i['normalized_status'] if i else 'UNKNOWN'
        out=sleeper in ('OUT','IR','PUP','NFI','SUSPENDED') and fp in ('OUT','IR','PUP','NFI','SUSPENDED')
        candidates.append({'identity':p['identity'],'name':p['name'],'team':p['team'],'position':p['position'],
            'status':'out' if out else 'unresolved','sleeper_status':sleeper,'fantasypros_injury_status':fp})
    return {'game':game,'decision_at':cutoff,'players':candidates,'availability_verified':False,
        'expected_prior_game_ids':expected_history(snapshot,game,cutoff),
        'roster_evidence':'Both providers inspected; no depth-to-workload conversion. Unresolved statuses are scenario candidates, not confirmed active players.',
        'dual_depth':depth,'week_injuries':json.loads(json.dumps(injuries,default=str)),
        'official_inactive_imports':json.loads(json.dumps(inactives,default=str)),
        'official_inactive_coverage':'No imported list for this week' if not inactives else 'Imports retained; inspect game/team before claiming confirmation'}


def backtest(snapshot, season, start_week, end_week, cfg, limit=None):
    # Corrected historical labels cannot recreate actual pregame availability.
    end = max(timestamp(g['kickoff']) for g in snapshot['games'])+timedelta(days=3)
    history, rejected = prepare(snapshot,end.isoformat(),retrospective=True)
    accepted = {h['game']['game_id']:h for h in history}
    ordered = sorted((g for g in snapshot['games'] if g['season']==season and start_week<=g['week']<=end_week),
        key=lambda g:(timestamp(g['kickoff']),g['game_id']))
    if limit is not None:
        ordered = ordered[:limit]
    results, skipped, forecasts = [], [], []
    for game in ordered:
        gid = game['game_id']
        if gid not in accepted:
            skipped.append({'game_id':gid,'reason':'Target PBP/box coverage unresolved',
                'details':next((r['reasons'] for r in rejected if r['game_id']==gid),[])})
            continue
        cutoff = timestamp(game['kickoff'])-timedelta(minutes=1)
        training = [h for h in history if timestamp(h['game']['kickoff'])<cutoff]
        request = {'game':game,'decision_at':cutoff.isoformat(),'players':roster(training,game),
            'expected_prior_game_ids':expected_history(snapshot,game,cutoff.isoformat(),cfg.recent_games),
            'availability_verified':False,'roster_evidence':'reconstructed previous-three-game usage only'}
        try:
            prediction = forecast(training,request,cfg)
            outcome = grade(prediction,{**snapshot,'boxes':accepted[gid]['boxes']})
            results.append(outcome)
            forecasts.append(prediction)
            print(json.dumps({'graded':gid}),flush=True)
        except ValueError as exc:
            skipped.append({'game_id':gid,'reason':str(exc)})
    summary = {}
    for metric in METRICS:
        values = [r['metrics'][metric] for r in results]
        calibration = defaultdict(list)
        for v in values:
            for row in v['calibration_rows']:
                if not row['residual']:
                    calibration[min(9,int(row['probability']*10))].append(row)
        differences=np.array([v['top_choice_credit']-v['mean_baseline_credit'] for v in values])
        rng=np.random.default_rng(cfg.seed)
        interval=np.quantile(rng.choice(differences,(2000,len(values))).mean(axis=1),[.025,.975]).tolist() if values else None
        intervals = [row for value in values for row in value.get('interval_diagnostics', {}).values()]
        summary[metric]={'intervals': {'player_games':len(intervals),
            'coverage_80pct':float(np.mean([r['covered'] for r in intervals])) if intervals else None,
            'mean_interval_score':float(np.mean([r['interval_score'] for r in intervals])) if intervals else None}, 'games':len(values),'top_choice_minus_baseline_95pct_game_bootstrap':interval,
            **{key:float(np.mean([v[key] for v in values])) if values else None
                for key in ('brier','log_loss','top_choice_credit','mean_baseline_credit')},
            'calibration':[{'range':[i/10,(i+1)/10],'player_game_rows':len(rs),
                'mean_probability':float(np.mean([r['probability'] for r in rs])),
                'observed_credit':float(np.mean([r['observed_credit'] for r in rs]))} for i,rs in sorted(calibration.items())]}
    return {'version':VERSION,'authority':'retrospective_development_not_validated',
        'source_sha256':sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest(),
        'season':season,'weeks':[start_week,end_week],'settings':cfg.__dict__,
        'selected_games':len(ordered),'graded_games':len(results),'summary':summary,
        'grades':results,'forecasts':forecasts,'skipped':skipped,'source_reconciliation_rejections':rejected,
        'limits':['Historical roster reconstructed from prior usage; no archived inactives.',
            'Records are later corrected; temporal game exclusion does not prove source-time availability.',
            'Rows within games are dependent; calibration row counts are not independent samples.',
            'This is development evaluation, not a passed promotion gate.']}


def batch(snapshot, season, week, decision_at, cfg, requests=None):
    history,rejected=prepare(snapshot,decision_at)
    games=sorted((g for g in snapshot['games'] if g['season']==season and g['week']==week
        and timestamp(g['kickoff'])>timestamp(decision_at)),key=lambda g:(timestamp(g['kickoff']),g['game_id']))
    predictions,skipped=[],[]
    for g in games:
        if requests is not None and (g['game_id'] not in requests or requests[g['game_id']].get('capture_error')):
            skipped.append({'game_id':g['game_id'],'reason':(requests.get(g['game_id']) or {}).get('capture_error','Missing requested availability capture')})
            continue
        req=(requests or {}).get(g['game_id']) or {'game':g,'decision_at':decision_at,
            'players':roster(history,g),'availability_verified':False,
            'roster_evidence':'usage-only draft; dual-source availability and replacements unresolved'}
        if req['game']!=g or timestamp(req['decision_at'])>timestamp(decision_at):
            raise ValueError('Batch request must match canonical game and precede the batch boundary')
        try:
            # Each game's roster capture has its own boundary. Never attach the
            # later batch's training labels to an earlier decision record.
            game_history,_=prepare(snapshot,req['decision_at']) if req['decision_at']!=decision_at else (history,[])
            req = {**req, 'expected_prior_game_ids': expected_history(snapshot, g, req['decision_at'], cfg.recent_games)}
            predictions.append(forecast(game_history,req,cfg))
        except ValueError as exc:
            skipped.append({'game_id':g['game_id'],'reason':str(exc)})
    return {'version':VERSION,'authority':'exploratory_not_calibrated','season':season,'week':week,
        'decision_at':decision_at,'source_sha256':sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest(),
        'selected_games':len(games),'forecasts':predictions,'skipped':skipped,
        'reconciliation_rejections':rejected,'market_inputs_used':False,
        'refresh':'Explicit capture and batch run required. DFS slate uploads are not needed.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    subs=parser.add_subparsers(dest='command',required=True)
    c=subs.add_parser('capture')
    c.add_argument('--minimum-season',type=int,default=2023)
    c.add_argument('--maximum-season',type=int,default=2026)
    c.add_argument('--pbp-input',type=Path,help='Reuse frozen read-only PBP capture; refresh full box feeds')
    for name in ('forecast','backtest','grade','request','batch','evidence-request','week-requests'):
        p=subs.add_parser(name)
        p.add_argument('--input',type=Path,required=True)
        if name in ('forecast','backtest','batch'):
            p.add_argument('--draws',type=int,default=5000)
            p.add_argument('--seed',type=int,default=20261008)
            p.add_argument('--role-dispersion',choices=('fixed','empirical'),default='fixed')
        if name=='week-requests':
            p.add_argument('--season',type=int,required=True)
            p.add_argument('--week',type=int,required=True)
        elif name=='batch':
            p.add_argument('--season',type=int,required=True)
            p.add_argument('--week',type=int,required=True)
            p.add_argument('--decision-at',required=True)
            p.add_argument('--requests',type=Path,help='Optional game_id-to-request map with availability/scenario evidence')
        elif name=='forecast':
            p.add_argument('--request',type=Path,required=True)
            p.add_argument('--retrospective',action='store_true')
            p.add_argument('--sensitivity',action='store_true')
            p.add_argument('--export-draws',action='store_true')
        elif name=='grade':
            p.add_argument('--forecast',type=Path,required=True)
        elif name in ('request','evidence-request'):
            p.add_argument('--game',required=True)
            if name=='request':
                p.add_argument('--decision-at',required=True)
        else:
            p.add_argument('--season',type=int,required=True)
            p.add_argument('--start-week',type=int,default=5)
            p.add_argument('--end-week',type=int,default=8)
            p.add_argument('--limit',type=int)
    for p in subs.choices.values():
        p.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Frozen output already exists; choose a new path')
    if args.command=='capture':
        from config import load_config
        from db.database import DatabaseManager
        if args.pbp_input:
            result=read(args.pbp_input)
        else:
            db=DatabaseManager(load_config().database_url,initialize_schema=False)
            with db.reuse_connection():
                result=capture_pbp(db,args.minimum_season,args.maximum_season)
        result=full_box_capture(result,args.minimum_season,args.maximum_season)
        from research.nfl_game_leaders_source import capture_stat_credits, capture_box_verification
        result=capture_box_verification(capture_stat_credits(result,args.output.parent/'raw-stat-credits'))
    else:
        snapshot=read(args.input)
        if args.command=='week-requests':
            import requests as http
            import psycopg2
            upcoming=[g for g in snapshot['games'] if g['season']==args.season and g['week']==args.week
                and timestamp(g['kickoff'])>datetime.now(timezone.utc)]
            result={}
            for g in upcoming:
                try:
                    result[g['game_id']]=evidence_request(snapshot,g['game_id'])
                    print(json.dumps({'captured':g['game_id']}),flush=True)
                except (ValueError,RuntimeError,http.RequestException,psycopg2.Error) as exc:
                    result[g['game_id']]={'capture_error':str(exc)}
        elif args.command=='evidence-request':
            result=evidence_request(snapshot,args.game)
        elif args.command=='batch':
            result=batch(snapshot,args.season,args.week,args.decision_at,Settings(draws=args.draws,seed=args.seed,role_dispersion=args.role_dispersion),
                read(args.requests) if args.requests else None)
        elif args.command=='backtest':
            result=backtest(snapshot,args.season,args.start_week,args.end_week,
                Settings(draws=args.draws,seed=args.seed,role_dispersion=args.role_dispersion),args.limit)
        elif args.command=='grade':
            prediction=read(args.forecast)
            # Exact PBP/box reconciliation gates official results as well.
            history,rejected=prepare(snapshot,datetime.now(timezone.utc).isoformat(),True)
            gid=prediction['game']['game_id']
            if gid not in {h['game']['game_id'] for h in history}:
                result={'status':'needs_review','game_id':gid,'reconciliation':rejected}
            else:
                result=grade(prediction,snapshot)
        else:
            if args.command=='request':
                games=[g for g in snapshot['games'] if g['game_id']==args.game]
                if len(games)!=1:
                    raise ValueError('Require unique canonical game')
                history,rejected=prepare(snapshot,args.decision_at)
                result={'game':games[0],'decision_at':args.decision_at,'players':roster(history,games[0]),
                    'availability_verified':False,'roster_evidence':'DRAFT: usage only; attach dual depth, injuries and inactives before acting'}
            else:
                request=read(args.request)
                canonical_games=[g for g in snapshot['games'] if g['game_id']==request['game']['game_id']]
                if len(canonical_games)!=1 or any(canonical_games[0][k]!=request['game'][k]
                        for k in ('season','week','home','away')) or timestamp(canonical_games[0]['kickoff'])!=timestamp(request['game']['kickoff']):
                    raise ValueError('Forecast request must match the canonical game')
                history,rejected=prepare(snapshot,request['decision_at'],args.retrospective)
                request={**request,'expected_prior_game_ids':expected_history(snapshot,request['game'],request['decision_at'])}
                result=forecast(history,request,Settings(draws=args.draws,seed=args.seed,role_dispersion=args.role_dispersion),include_draws=args.export_draws)
                if args.sensitivity:
                    cfg=Settings(draws=args.draws,seed=args.seed)
                    result['sensitivity']=[forecast(history,request,variant) for variant in (
                        replace(cfg,surprise_slots=1),replace(cfg,surprise_slots=6),
                        replace(cfg,half_life_games=6.,efficiency_prior_events=60.))]
                    result['sensitivity_interpretation']='Explicit assumption variants, not calibrated confidence intervals'
                result.update(reconciliation_rejections=rejected,retrospective=args.retrospective,
                    source_sha256=sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest(),
                    captured_at=snapshot['captured_at'],roster_evidence=request.get('roster_evidence'))
    if args.command!='week-requests':
        result['created_at']=datetime.now(timezone.utc).isoformat()
    write(args.output,result)
    print(json.dumps({'output':str(args.output),'authority':result.get('authority'),
        'graded_games':result.get('graded_games'),'summary':result.get('summary'),'box_coverage':result.get('box_coverage')}))


if __name__=='__main__':
    main()
