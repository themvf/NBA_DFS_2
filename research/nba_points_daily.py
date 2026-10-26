"""Create immutable daily NBA leader, trio, accuracy and diagnostic reports."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date,datetime,timezone
import json
from pathlib import Path

from model.nba_points_ladder import team_key
from research.nba_points_forward import write_new
from research.nba_leader_evaluation import identity,finite
from research.nba_points_diagnostics import assemble

LABELS={'market_line':'Market line','blend90_10':'Blend','history20':'History mean','opportunity_points':'Opportunity model'}


def chart_data(snapshot,graded,diagnostics,history=None):
    candidates={identity(c):c for c in snapshot['candidates']}
    observed={identity(r):r for r in diagnostics['rows']}
    rows=[]
    for r in graded['rows']:
        c=candidates[identity(r)];d=observed.get(identity(r),{})
        rows.append({**{k:r[k] for k in ['event_id','player_key','player','status','actual_points','predictions']},
                     'historical_p90':c.get('historical_p90'),'experimental_p90':c.get('experimental_p90'),
                     'market_ceiling_approximation':c.get('market_ceiling_approximation',c.get('ceiling')),
                     'watch':c.get('ceiling_watch',{}),'diagnostic':d})
    methods=[m for m in snapshot['models'] if any(finite(c['predictions'].get(m)) for c in snapshot['candidates'])]
    threshold_models=['historical_p90','experimental_p90','market_ceiling_approximation']
    ceilings=[]
    for key in threshold_models:
        subset=[r for r in rows if r['status'] in ('graded','push') and finite(r['actual_points']) and finite(r[key])]
        ceilings.append({'key':key,'n':len(subset),'hits':sum(r['actual_points']>r[key] for r in subset)})
    watch={}
    for flag in [True,False]:
        subset=[r for r in rows if r.get('watch',{}).get('flag') is flag and r['status'] in ('graded','push') and finite(r['actual_points']) and finite(r['historical_p90'])]
        watch[str(flag)]={'n':len(subset),'hits':sum(r['actual_points']>r['historical_p90'] for r in subset)}
    # Historical/reconstructed dates never enter a prospective trend.
    by_date={g['game_date']:g for g in (history or []) if g.get('snapshot_mode')=='prospective' and g.get('version')==graded['version'] and g.get('model_sha256')==graded.get('model_sha256')}
    if graded['snapshot_mode']=='prospective':by_date[graded['game_date']]=graded
    trend=[{'date':day,'all':g['all_players'],'high':g['market_line_at_least20']} for day,g in sorted(by_date.items())]
    return {'date':snapshot['game_date'],'version':snapshot['version'],'mode':snapshot['mode'],
            'created_at':snapshot['created_at'],'lock_at':snapshot['lock_at'],'snapshot_sha256':snapshot['sha256'],
            'methods':methods,'labels':LABELS,'rows':rows,'leaderboards':graded['leaderboards'],
            'errors':{'all':graded['all_players'],'high':graded['market_line_at_least20']},
            'ceilings':ceilings,'watch':watch,'trend':trend,'prospective_dates':len(trend),
            'status_counts':graded['by_status'],'diagnostic_coverage':diagnostics['coverage']}


def render_fragment(data):
    template=(Path(__file__).parent/'templates/nba_points_daily.html').read_text(encoding='utf-8')
    encoded=json.dumps(data,ensure_ascii=False,allow_nan=False,separators=(',',':')).replace('<','\\u003c')
    return template.replace('__DAILY_DATA__',encoded)


def markdown(data):
    lines=['# NBA scoring review — '+data['date'],'',data['version']+' · '+data['mode'],
           '',f"Frozen {data['created_at']}; first-game cutoff {data['lock_at']}.",
           '', 'Research pool: three distinct quoted players; contest eligibility and salary rules are unverified.',
           '', '## Frozen trio outcomes','',
           '| Strategy | Selected players | Forecast sum | Actual sum | Best verified sum | Gap | Top-three slots | Unresolved pool |',
           '|---|---|---:|---:|---:|---:|---:|---:|']
    fmt=lambda v:'Pending' if v is None else f'{v:.2f}'
    for method,r in data['leaderboards']['strategies'].items():
        lines.append('| '+' | '.join([LABELS.get(method,method),', '.join(c['player'] for c in r['selected']),fmt(r['selected_forecast_total']),fmt(r['selected_actual_total']),fmt(r['best_verified_trio_points']),fmt(r['gap_to_best_verified']),fmt(r['captured_top3_slots'])+'/3',str(r['unresolved_pool_count'])])+' |')
    lines+=['','Best verified totals are hindsight benchmarks among resolved eligible players. Unresolved pool members make them provisional. Selected DNP/unmatched players are never replaced or scored zero. Boundary ties receive fractional capture credit.','',
            '## Observed scoring and playing time','','| Player | Points | Regulation | Overtime | Minutes | PF | Two-point / three-point / FT points |','|---|---:|---:|---:|---:|---:|---|']
    diagnostic_rows=[r for r in data['rows'] if finite(r.get('actual_points')) and r.get('diagnostic',{}).get('status')=='verified']
    for r in sorted(diagnostic_rows,key=lambda r:-r['actual_points']):
        a=r['diagnostic']['actual'];mix=a['scoring_components']
        lines.append('| '+' | '.join([r['player'],fmt(a['points']),fmt(a['regulation_points']),fmt(a['overtime_points']),fmt(a['minutes']),fmt(a['fouls']),'/'.join(fmt(mix[k]) for k in ['two_point','three_point','free_throw']) if mix else 'Unavailable'])+' |')
    lines+=['','Overtime and foul-limit notes describe observed results. They do not establish causality or a new predictive adjustment. Full-game points, including overtime, remain the grading target.','',
            'Any opportunity/conversion decomposition uses actual attempts after the game with frozen conversion rates. It is diagnostic only and overlaps with overtime/minute effects.','',
            'Point/ceiling accuracy, coverage and model-specific paired baselines are in grade.json. Fixed reference ceilings, conditional P90 and market approximations remain distinct.','']
    return '\n'.join(lines)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--snapshot',type=Path);group.add_argument('--date',type=date.fromisoformat)
    parser.add_argument('--output-dir',required=True,type=Path);parser.add_argument('--summaries-dir',type=Path)
    parser.add_argument('--no-fetch',action='store_true');parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();args.output_dir.mkdir(parents=True,exist_ok=False)
    import psycopg2
    from psycopg2.extras import RealDictCursor,Json
    from config import load_config
    from db.database import DatabaseManager
    from model.nba_points_calibration import load_box_scores
    from research import nba_points_ghost as v1,nba_points_ghost_v2 as v2
    db=DatabaseManager(load_config().database_url,initialize_schema=False)
    history=[]
    with psycopg2.connect(db.database_url,cursor_factory=RealDictCursor) as conn:
        conn.set_session(readonly=True);db._shared_connection=conn
        try:
            if args.snapshot:snapshot=json.loads(args.snapshot.read_text(encoding='utf-8'))
            else:
                with conn.cursor() as cur:
                    cur.execute("SELECT snapshot FROM nba_ghost_snapshots WHERE game_date=%s AND version=%s AND mode='prospective'",(args.date,v2.VERSION));found=cur.fetchone()
                    if not found:raise ValueError('no genuine prospective v2 snapshot for date; retrospective substitution is forbidden')
                    snapshot=found['snapshot']
            module=v1 if snapshot['version']==v1.VERSION else v2 if snapshot['version']==v2.VERSION else None
            if module is None:raise ValueError('unsupported forecast version')
            if args.apply and module is v1:raise ValueError('v1 results stay in separate local files; --apply supports existing v2 snapshots only')
            games=load_box_scores(db,date.fromisoformat(snapshot['game_date']))
            graded=module.grade(snapshot,games)
            if module is v2:
                with conn.cursor() as cur:
                    cur.execute('''SELECT DISTINCT ON (g.snapshot_sha256) g.grade FROM nba_ghost_grades g JOIN nba_ghost_snapshots s USING(snapshot_sha256)
                                   WHERE s.mode='prospective' AND s.version=%s AND s.game_date <=%s AND s.snapshot->>'model_sha256'=%s
                                   ORDER BY g.snapshot_sha256,g.graded_at DESC,g.id DESC''',(v2.VERSION,snapshot['game_date'],snapshot['model_sha256']))
                    history=[r['grade'] for r in cur.fetchall()]
        finally:db._shared_connection=None
    wanted={(team_key(c['home_team']),team_key(c['away_team'])) for c in snapshot['candidates']}
    summaries={};missing=[]
    for game in games:
        if (team_key(game.home_team),team_key(game.away_team)) not in wanted:continue
        path=args.summaries_dir/('summary-'+game.espn_event_id+'.json') if args.summaries_dir else None
        if path and path.exists():summaries[game.espn_event_id]=json.loads(path.read_text())
        elif not args.no_fetch:missing.append(game.espn_event_id)
    def fetch(event):
        import requests
        url='https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary'
        response=requests.get(url,params={'event':event},timeout=30);response.raise_for_status();return event,response.json()
    if missing:summaries.update(dict(ThreadPoolExecutor(max_workers=4).map(fetch,missing)))
    raw_dir=args.output_dir/'summaries';raw_dir.mkdir()
    for event,payload in summaries.items():write_new(raw_dir/('summary-'+event+'.json'),payload)
    diagnostics=assemble(snapshot,graded,summaries)
    data=chart_data(snapshot,graded,diagnostics,history)
    for name,value in [('snapshot.json',snapshot),('grade.json',graded),('diagnostics.json',diagnostics),('daily.json',data)]:write_new(args.output_dir/name,value)
    with (args.output_dir/'review.html').open('x',encoding='utf-8') as f:f.write(render_fragment(data))
    with (args.output_dir/'review.md').open('x',encoding='utf-8') as f:f.write(markdown(data))
    if args.apply:
        if snapshot['version']!=v2.VERSION:raise ValueError('v1 results stay in separate local files; --apply supports existing v2 snapshots only')
        with psycopg2.connect(db.database_url) as conn:
            with conn.cursor() as cur:cur.execute('INSERT INTO nba_ghost_grades(snapshot_sha256,grade) VALUES(%s,%s)',(snapshot['sha256'],Json(graded)))
    print(json.dumps({'output_directory':str(args.output_dir),'version':snapshot['version'],'mode':snapshot['mode'],
                      'outcome_coverage':graded['by_status'],'diagnostic_coverage':diagnostics['coverage'],
                      'trios':{k:{x:v[x] for x in ['selected_actual_total','best_verified_trio_points','top3_capture_fraction','unresolved_pool_count']} for k,v in graded['leaderboards']['strategies'].items()}},indent=2))


if __name__=='__main__':main()
