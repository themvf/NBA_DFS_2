"""Local v3 research: audited replay, frozen probabilities and append-only grades.

No paid requests, production changes, v2 changes or automatic model promotion.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone, date, timedelta
import hashlib
import json
from pathlib import Path
from statistics import mean
import subprocess
import sys

from model.nba_points_ladder import team_key, player_key
from research.nba_points_forward import aware, digest, write_new
from research.nba_points_ghost import verify
from research.nba_points_ghost_v2 import VERSION as V2
from research.nba_points_v3_data import prepare, examples, features, absence_scenarios
from research.nba_points_v3_model import MODEL_VERSION, fit, predict, crps, evaluate_joint
from research.nba_points_v3_distribution import joint_draws, summarize

VERSION = 'nba-points-ghost-v3'


def source(path):
    data = path.read_bytes()
    return json.loads(data), hashlib.sha256(data).hexdigest()


def warehouse_audit():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from config import load_config
    result = {}
    with psycopg2.connect(load_config().database_url,cursor_factory=RealDictCursor) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as c:
            c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'nba_%'")
            tables = {r['table_name'] for r in c.fetchall()}
            for table in ['nba_player_game_logs','nba_team_game_logs','nba_box_score_games','nba_box_score_players']:
                if table not in tables:
                    result[table] = {'status':'missing'}; continue
                c.execute('SELECT count(*) n,min(game_date)::text first,max(game_date)::text last FROM '+table)
                result[table] = dict(c.fetchone())
            result['nba_stint_or_tracking_tables'] = sorted(t for t in tables if any(k in t for k in ['stint','tracking','substitution','play_by_play']))
            c.execute("SELECT count(*) n,min(captured_at)::text first,max(captured_at)::text last FROM prop_odds_history WHERE sport='nba' AND market='player_points'")
            result['archived_main_quotes'] = dict(c.fetchone())
    return result


def collect_inputs(day, created):
    """Read-only warehouse export and free ESPN season-type evidence."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from config import load_config
    from research.nba_ghost_runner import fetch_scoreboard, classifications_from_scoreboard
    with psycopg2.connect(load_config().database_url,cursor_factory=RealDictCursor) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as c:
            c.execute("SELECT * FROM nba_player_game_logs WHERE season_type='Regular Season' AND game_date < %s AND fetched_at < %s",(day,created))
            players = [dict(r) for r in c.fetchall()]
            c.execute("SELECT * FROM nba_team_game_logs WHERE season_type='Regular Season' AND game_date < %s AND fetched_at < %s",(day,created))
            teams = [dict(r) for r in c.fetchall()]
            c.execute('SELECT team_id,name,abbreviation FROM teams')
            names = [dict(r) for r in c.fetchall()]
            c.execute('SELECT * FROM nba_box_score_games WHERE completed AND game_date < %s AND game_date >= %s AND fetched_at < %s',(day,day-timedelta(days=45),created))
            games = [dict(r) for r in c.fetchall()]
            c.execute('''SELECT p.* FROM nba_box_score_players p JOIN nba_box_score_games g USING(espn_event_id)
                         WHERE g.completed AND g.game_date < %s AND g.game_date >= %s AND p.fetched_at < %s AND g.fetched_at < %s''',(day,day-timedelta(days=45),created,created))
            boxes = [dict(r) for r in c.fetchall()]
    proofs = []; scoreboards = []
    for d in sorted({str(g['game_date']) for g in games}):
        payload, captured = fetch_scoreboard(d)
        proofs.extend(classifications_from_scoreboard(payload,captured))
        scoreboards.append({'captured_at':captured.isoformat(),'payload':payload})
    return json.loads(json.dumps({'players':players,'teams':teams,'team_names':names,
        'recent_games':games,'recent_players':boxes,'classifications':proofs,'raw_scoreboards':scoreboards,
        'collected_at':datetime.now(timezone.utc).isoformat()},default=str))


def forecast(snapshot, model, raw, created, *, draws=5000, seed=20261028, injury_reports=None):
    verify(snapshot,V2); verify(model,MODEL_VERSION)
    created = aware(created); cutoff = aware(snapshot['as_of']); lock = aware(snapshot['lock_at'])
    if created < aware(snapshot['created_at']) or created < aware(model['created_at']):
        raise ValueError('forecast creation precedes source or model')
    if model['evaluation_end'] >= snapshot['game_date']:
        raise ValueError('model evaluated on or after target date')
    starts = [aware(g['start_time']) for g in snapshot.get('slate_schedule',[]) if g.get('start_time')]
    if not starts or lock > min(starts):
        raise ValueError('unverified full-slate first tip')
    players, contexts, names, audit = prepare(raw)
    modeled = []; excluded = []; context_ages = {}; scenarios = []
    seen = set(); seen_players = set()
    for c in snapshot['candidates']:
        key = (str(c['event_id']),c['player_key'])
        if key in seen or c['player_key'] in seen_players:
            raise ValueError('duplicate forecast identity')
        seen.add(key); seen_players.add(c['player_key'])
        history = c['history']
        for h in history:
            proofs = h.get('provenance',[])
            if not proofs or any(aware(p['available_at']) >= cutoff for p in proofs):
                raise ValueError('historical input missing provenance or observed too late')
        own = history[-1].get('team') if history else None
        game_teams = {team_key(c['home_team']),team_key(c['away_team'])}
        # Last observed team can establish a within-game side, not roster confirmation.
        if not own or team_key(own) not in game_teams:
            excluded.append({'player':c['player'],'reason':'team_identity_not_resolved_from_prior_evidence'}); continue
        home = team_key(own) == team_key(c['home_team'])
        other = c['away_team'] if home else c['home_team']
        state = dict(c.get('availability',{'status':'unverified'}))
        known_out = []
        if injury_reports is not None:
            from research.nba_points_injury_evidence import state_at
            state = state_at(injury_reports,c,snapshot['game_date'],cutoff)
            for r in state.get('same_game_reported_out',[]):
                if team_key(r['team']) == team_key(own):
                    known_out.append({**r,'player_key':player_key(r['player']),
                        'source_url':state['source_url'],'observed_at':state['observed_at']})
        if str(state.get('status','')).lower() == 'out':
            excluded.append({'player':c['player'],'reason':'reported_out'}); continue
        f, ages = features(history,snapshot['game_date'],home,own,other,contexts,cutoff)
        if f is None:
            excluded.append({'player':c['player'],'reason':'insufficient_history'}); continue
        p = predict(model,f,history)
        scenario = absence_scenarios(c,players,names,known_out,cutoff)
        scenarios.append({'player':c['player'],'known_out_teammates':known_out,'comparisons':scenario,
                          'status':'descriptive_analogue_available' if scenario else 'no_sufficient_historical_analogue'})
        context_ages[c['player']] = ages
        modeled.append({'event_id':str(c['event_id']),'player_key':c['player_key'],'player':c['player'],
            'home':home,'team':own,'market_line':c['predictions']['market_line'],
            'v2_blend':c['predictions']['blend90_10'],'v2_opportunity':c['predictions']['opportunity_points'],
            'prediction':p,'features':f,'availability':state,
            'quality_flags':['unvalidated_current_season_transfer','last_observed_team_is_not_roster_confirmation']+
                (['missing_or_stale_team_context_imputed'] if any(v is None or v>45 for v in ages.values()) or any(f.get(k) is None for k in ['team_possessions48','opponent_possessions48','opponent_allowed_fga48']) else [])})
    if modeled:
        values, sim_audit = joint_draws(model,modeled,draws,seed)
        output = summarize(model,modeled,values,sim_audit)
        by_key = {(r['event_id'],r['player_key']):i for i,r in enumerate(modeled)}
        for r in output['rows']:
            r['samples'] = values[:,by_key[(r['event_id'],r['player_key'])]].tolist()
    else:
        output = {'rows':[],'simulation':{'status':'no_eligible_modeled_candidates'},'expected_points_trio':[],'five_highest_mean_trios':[]}
    output.update(version=VERSION,created_at=created.isoformat(),game_date=snapshot['game_date'],
        as_of=cutoff.isoformat(),lock_at=lock.isoformat(),
        mode='prospective' if created<lock and snapshot['mode']=='prospective' else 'retrospective_reconstruction',
        source_snapshot_sha256=snapshot['sha256'],model_sha256=model['sha256'],source_audit=audit,
        exclusions=excluded,quoted_pool_n=len(snapshot['candidates']),modeled_pool_n=len(modeled),
        team_context_age_days=context_ages,availability_scenarios=scenarios,
        limitations=model['limitations']+['No participation probabilities or scenario weights. Unlisted is not Active.',
            'All probabilities and leader/trio shares concern this quoted modeled pool only.',
            'Original v2 snapshot and forecasts are preserved; this is a separate challenger.'])
    output['sha256'] = digest(output)
    return output


def diagnose(snapshot, diagnostics):
    if diagnostics['snapshot_sha256'] != snapshot['sha256']:
        raise ValueError('diagnostics refer to another source snapshot')
    by_key = {(str(r['event_id']),r['player_key']):r for r in diagnostics['rows']}
    rows = []; unresolved = Counter()
    for c in snapshot['candidates']:
        d = by_key.get((str(c['event_id']),c['player_key']))
        p = c['predictions'].get('opportunity_points'); opportunities = c.get('opportunity_forecasts',{})
        rates = c.get('opportunity_conversion_rates')
        if not d or d['status'] != 'verified' or p is None or not rates or not opportunities.get('minutes'):
            unresolved['missing_frozen_opportunities' if d and d['status']=='verified' else d['status'] if d else 'missing_diagnostic'] += 1; continue
        a = d['actual']
        if any(a.get(k) is None for k in ['fga','fg3a','fta','points','minutes']):
            unresolved['missing_actual_attempts'] += 1; continue
        minutes_only = p*a['minutes']/opportunities['minutes']
        actual_attempts = 2*(a['fga']-a['fg3a'])*rates['two']+3*a['fg3a']*rates['three']+a['fta']*rates['free']
        components = {'minutes':minutes_only-p,'attempt_intensity_and_mix':actual_attempts-minutes_only,
                      'conversion':a['points']-actual_attempts}
        if abs(sum(components.values())-(a['points']-p)) > 1e-7:
            raise AssertionError('decomposition does not reconcile')
        rows.append({'player':c['player'],'forecast':p,'actual':a['points'],
                     'components':components,'primary':c['predictions']['market_line']>=20})
    def summary(rs):
        return {'n':len(rs),'mae':mean(abs(r['actual']-r['forecast']) for r in rs) if rs else None,
            'mean_absolute_components':{k:mean(abs(r['components'][k]) for r in rs) if rs else None
                for k in ['minutes','attempt_intensity_and_mix','conversion']}}
    return {'snapshot_sha256':snapshot['sha256'],'game_date':snapshot['game_date'],'rows':rows,
        'all':summary(rows),'market_line_ge20':summary([r for r in rows if r['primary']]),'unresolved':dict(unresolved),
        'interpretation':'Postgame arithmetic decomposition, not causal attribution. Absolute components need not sum to MAE; components can offset.'}


def grade(frozen, diagnostics):
    verify(frozen,VERSION)
    if diagnostics['snapshot_sha256'] != frozen['source_snapshot_sha256'] or diagnostics['game_date'] != frozen['game_date']:
        raise ValueError('diagnostics do not match original source')
    source_rows = {(str(r['event_id']),r['player_key']):r for r in diagnostics['rows']}
    if len(source_rows) != len(diagnostics['rows']):
        raise ValueError('duplicate diagnostic identity')
    rows = []; missing = []
    for r in frozen['rows']:
        d = source_rows.get((r['event_id'],r['player_key']))
        if not d or d['status'] != 'verified':
            missing.append({'player':r['player'],'reason':d['status'] if d else 'missing'}); continue
        actual = d['actual']['points']; a = r['samples']; q = r['distribution']['p90']; e = actual-q
        forecasts = {'v3_opportunity':r['prediction']['points'],'v3_distribution_mean':r['distribution']['mean'],
            'market':r['market_line'],'v2_blend':r['v2_blend'],'v2_opportunity':r['v2_opportunity']}
        rows.append({'player':r['player'],'actual':actual,'forecasts':forecasts,
            'crps':crps(a,actual),'p90_loss':max(.9*e,-.1*e),'p90_hit':actual>q,
            'brier_over_market':(r['distribution']['over_market_probability']-(actual>r['market_line']))**2,
            'market_push':actual==r['market_line']})
    paired = [r for r in rows if all(v is not None for v in r['forecasts'].values())]
    mae = {k:mean(abs(r['actual']-r['forecasts'][k]) for r in paired) if paired else None
           for k in ['market','v2_blend','v2_opportunity','v3_opportunity','v3_distribution_mean']}
    actual = {r['player']:r['actual'] for r in rows}; trio = frozen['expected_points_trio']
    best = sum(sorted(actual.values(),reverse=True)[:3]) if len(actual)>=3 else None
    selected_total = sum(actual[p] for p in trio) if len(trio)==3 and all(p in actual for p in trio) else None
    capture = None
    if len(actual)>=3 and all(p in actual for p in trio):
        boundary = sorted(actual.values(),reverse=True)[2]
        above = sum(v>boundary for v in actual.values()); ties = sum(v==boundary for v in actual.values())
        capture = sum(1 if actual[p]>boundary else (3-above)/ties if actual[p]==boundary else 0 for p in trio)
    return {'version':VERSION,'snapshot_sha256':frozen['sha256'],'source_snapshot_sha256':frozen['source_snapshot_sha256'],
        'mode':frozen['mode'],'game_date':frozen['game_date'],'model_sha256':frozen['model_sha256'],
        'graded_at':datetime.now(timezone.utc).isoformat(),'resolved_n':len(rows),'paired_n':len(paired),
        'mae_same_players':mae,'rows':rows,'unresolved':missing,
        'crps':mean(r['crps'] for r in rows) if rows else None,
        'p90_loss':mean(r['p90_loss'] for r in rows) if rows else None,
        'p90_hits':sum(r['p90_hit'] for r in rows),'p90_n':len(rows),
        'mean_brier_strict_over_including_push_as_non_over':mean(r['brier_over_market'] for r in rows) if rows else None,
        'pushes':sum(r['market_push'] for r in rows),
        'trio':{'selected':trio,'actual_total':selected_total,'best_verified_total':best,
            'gap_to_best_verified':best-selected_total if best is not None and selected_total is not None else None,
            'captured_top3_slots':capture,'boundary_ties':'fractional','benchmark_provisional':bool(missing),
            'pool':'Frozen v3 modeled eligible pool only; missing/DNP not replaced or assigned zero'},
        'interpretation':'Retrospective reconstructions cannot count as prospective v3 evidence. DNP/missing are not zero or replaced.'}


def markdown(output):
    lines = ['# NBA points v3 research', '',f"Mode: {output.get('mode','retrospective historical replay')}. No validated current-season probabilities or demonstrated market edge.",'']
    if output.get('rows') and 'distribution' in output['rows'][0]:
        lines += [f"Date {output['game_date']}; source cutoff {output['as_of']}; first tip {output['lock_at']}.",
            f"Modeled {output['modeled_pool_n']} of {output['quoted_pool_n']} quoted players.",'',
            '| Player | Opportunity points | Distribution mean | P90 | P(over frozen market) | Availability |',
            '|---|---:|---:|---:|---:|---|']
        for r in output['rows'][:20]:
            d = r['distribution']
            lines.append(f"| {r['player']} | {r['prediction']['points']:.1f} | {d['mean']:.1f} | {d['p90']:.1f} | {100*d['over_market_probability']:.0f}% | {r['availability'].get('status','unknown')} |")
        lines += ['', 'Leader probabilities concern only this modeled quoted pool. All scoring estimates are conditional on appearance. Unlisted is not confirmed Active.',
            '', 'Joint draws use historical residual blocks, not a possession or rotation simulator. Simulation count does not validate accuracy.']
    else:
        lines += ['See JSON for source audit, paired evaluation, calibration denominators and unresolved coverage.']
    return '\n'.join(lines)+'\n'


def record_day(args, raw, raw_sha):
    """One explicit local recording attempt, never replacing a stored v2 trial."""
    from research.nba_ghost_runner import fetch_scoreboard
    model,_ = source(args.model); verify(model,MODEL_VERSION)
    registry = args.output_dir.parent/'prospective-index'
    marker = registry/(str(args.date)+'-'+model['sha256']+'.json')
    if marker.exists():
        original,_ = source(marker)
        return {'status':'skipped','reason':'original_v3_snapshot_preserved','original':original}
    payload, captured = fetch_scoreboard(args.date)
    write_new(args.output_dir/'scoreboard.json',{'captured_at':captured.isoformat(),'payload':payload})
    starts = [aware(e['date']) for e in payload.get('events',[])]
    if not starts or datetime.now(timezone.utc) >= min(starts):
        return {'status':'skipped','reason':'no_slate_or_first_tip_passed',
                'first_tip':min(starts).isoformat() if starts else None}
    availability_path = args.output_dir/'availability.json'
    commands = [[sys.executable,'-m','research.nba_points_injury_evidence','--date',str(args.date),
        '--archive-root',str(args.injury_archive),'--availability-output',str(availability_path)]]
    injury = subprocess.run(commands[0],capture_output=True,text=True,timeout=180)
    write_new(args.output_dir/'injury-capture.json',{'returncode':injury.returncode,'stdout':injury.stdout,'stderr':injury.stderr})
    source_path = args.output_dir/'source-snapshot.json'
    command = [sys.executable,'-m','research.nba_ghost_runner','freeze','--date',str(args.date),
               '--output',str(source_path)]
    if injury.returncode == 0 and availability_path.exists():
        command += ['--availability',str(availability_path)]
    run = subprocess.run(command,capture_output=True,text=True,timeout=300)
    receipt = {'first_tip':min(starts).isoformat(),'attempted_at':datetime.now(timezone.utc).isoformat(),
        'source_returncode':run.returncode,'source_stdout':run.stdout,'source_stderr':run.stderr,
        'injury_capture_success':injury.returncode==0,'v2_database_write':False,
        'source_input_sha256':raw_sha}
    if run.returncode or not source_path.exists():
        receipt.update(status='failed' if run.returncode else 'skipped',reason='no_new_valid_pregame_source')
        return receipt
    snapshot,_ = source(source_path)
    reports = None
    if injury.returncode == 0:
        from research.nba_points_injury_evidence import load_archive
        reports = load_archive(args.injury_archive)
    result = forecast(snapshot,model,raw,datetime.now(timezone.utc),draws=args.draws,seed=args.seed,injury_reports=reports)
    if result['mode'] != 'prospective':
        receipt.update(status='skipped',reason='cutoff_passed_during_collection'); return receipt
    result['input_bundle_sha256'] = raw_sha
    result['sha256'] = digest({k:v for k,v in result.items() if k!='sha256'})
    write_new(args.output_dir/'model.json',model);write_new(args.output_dir/'forecast.json',result)
    (args.output_dir/'forecast-review.md').write_text(markdown(result),encoding='utf-8')
    receipt.update(status='recorded',forecast_sha256=result['sha256'],modeled_n=result['modeled_pool_n'])
    registry.mkdir(parents=True,exist_ok=True)
    write_new(marker,{'date':str(args.date),'version':VERSION,'model_sha256':model['sha256'],
        'forecast_sha256':result['sha256'],'forecast_path':str((args.output_dir/'forecast.json').resolve())})
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__); sub = p.add_subparsers(dest='action',required=True)
    for action in ['collect','audit','train','evaluate','forecast','record','diagnose','grade']:
        c = sub.add_parser(action); c.add_argument('--output-dir',type=Path,required=True)
        if action == 'collect':
            c.add_argument('--date',type=date.fromisoformat,required=True)
        if action in ['audit','train','evaluate','forecast','record']:
            c.add_argument('--input',type=Path,required=True)
        if action == 'audit':
            c.add_argument('--database',action='store_true')
        if action in ['forecast','diagnose']:
            c.add_argument('--snapshot',type=Path,required=True)
        if action in ['evaluate','forecast','record']:
            c.add_argument('--model',type=Path,required=True); c.add_argument('--draws',type=int,default=5000)
            c.add_argument('--seed',type=int,default=20261028); c.add_argument('--injury-archive',type=Path)
        if action == 'record':
            c.add_argument('--date',type=date.fromisoformat,required=True)
        if action in ['diagnose','grade']:
            c.add_argument('--diagnostics',type=Path,required=True)
        if action == 'grade':
            c.add_argument('--forecast',type=Path,required=True)
    args = p.parse_args(); args.output_dir.mkdir(parents=True,exist_ok=False)
    now = datetime.now(timezone.utc).isoformat()
    if args.action in ['audit','train','evaluate','forecast','record']:
        raw, raw_sha = source(args.input)
    if args.action == 'evaluate':
        model,_ = source(args.model);verify(model,MODEL_VERSION)
        if model['source_sha256'] != raw_sha:
            raise ValueError('evaluation input differs from frozen model source bundle')
        players,ctx,names,_ = prepare(raw)
        result = evaluate_joint(model,examples(players,ctx,names),args.draws,args.seed)
        write_new(args.output_dir/'joint-evaluation.json',result)
    elif args.action == 'record':
        if args.injury_archive is None:
            p.error('record requires --injury-archive')
        try:
            result = record_day(args,raw,raw_sha)
        except Exception as exc:
            write_new(args.output_dir/'receipt.json',{'status':'failed','failure_type':type(exc).__name__,
                'reason':str(exc),'recorded_at':datetime.now(timezone.utc).isoformat()})
            raise
        write_new(args.output_dir/'receipt.json',result)
    elif args.action == 'collect':
        bundle = collect_inputs(args.date,now)
        write_new(args.output_dir/'inputs.json',bundle)
        result = {'game_date':str(args.date),'collected_at':bundle['collected_at'],
            'players':len(bundle['players']),'recent_games':len(bundle['recent_games']),
            'raw_scoreboards':len(bundle['raw_scoreboards'])}
    elif args.action in ['audit','train']:
        players, contexts, names, audit = prepare(raw)
        audit.update(source_file=str(args.input.resolve()),source_sha256=raw_sha,created_at=now)
        if args.action == 'audit' and args.database:
            audit['warehouse'] = warehouse_audit()
        write_new(args.output_dir/'audit.json',audit)
        if args.action == 'train':
            model = fit(examples(players,contexts,names),players,raw_sha,now,audit)
            write_new(args.output_dir/'model.json',model)
            write_new(args.output_dir/'evaluation.json',model['evaluation'])
        result = audit if args.action == 'audit' else {'splits':model['split_counts'],'evaluation':model['evaluation']}
    elif args.action == 'forecast':
        snapshot,_ = source(args.snapshot); model,_ = source(args.model)
        reports = None
        if args.injury_archive:
            from research.nba_points_injury_evidence import load_archive
            reports = load_archive(args.injury_archive)
        result = forecast(snapshot,model,raw,now,draws=args.draws,seed=args.seed,injury_reports=reports)
        result['input_bundle_sha256'] = raw_sha
        result['sha256'] = digest({k:v for k,v in result.items() if k != 'sha256'})
        write_new(args.output_dir/'source-snapshot.json',snapshot); write_new(args.output_dir/'model.json',model)
        write_new(args.output_dir/'forecast.json',result)
        print_result = {'mode':result['mode'],'modeled':result['modeled_pool_n'],'quoted':result['quoted_pool_n'],
                        'top3':result['expected_points_trio'],'excluded':result['exclusions']}
    elif args.action == 'diagnose':
        snapshot,_ = source(args.snapshot); verify(snapshot,V2); diagnostics,_ = source(args.diagnostics)
        result = diagnose(snapshot,diagnostics); write_new(args.output_dir/'diagnosis.json',result)
    else:
        frozen,_ = source(args.forecast); diagnostics,_ = source(args.diagnostics)
        result = grade(frozen,diagnostics); write_new(args.output_dir/'grade.json',result)
    (args.output_dir/'review.md').write_text(markdown(result),encoding='utf-8')
    if args.action != 'forecast':
        print_result = {k:v for k,v in result.items() if k not in ['rows','evaluation','daily_selection_audit']}
    print(json.dumps({'output_dir':str(args.output_dir.resolve()),'result':print_result},default=str))


if __name__ == '__main__':
    main()
