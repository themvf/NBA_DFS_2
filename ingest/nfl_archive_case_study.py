"""Run the fixed Week 5 Showdown/Classic pilot against the shared Neon archive.

python -m ingest.nfl_archive_case_study --output artifacts/nfl-archive-pilot --persist
Reads archive tables without modifying them. Derived reports have their own table.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
from collections import defaultdict
from pathlib import Path

import psycopg2
import scipy
import numpy as np
from psycopg2.extras import RealDictCursor, Json

from config import load_config
from model.nfl_archive_case_study import checked_points, normalized_name, solve_lineup, team_key, scoring_position, match_game_result
from model.nfl_dfs_historical import HistoricalWeek, artifact_digest, project_player

SLATES = (134423, 134675)
VERSION = 'nfl-archive-pilot-v3-participation'


def identity_id(value):
    return int(hashlib.sha256(value.encode()).hexdigest()[:12], 16)


def query(conn, sql, params=()):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def run(conn, identity_run=None):
    resolved_results = {}
    if identity_run:
        saved = query(conn, 'SELECT report FROM nfl_player_identity_runs WHERE run_digest=%s', (identity_run,))
        if not saved:
            raise ValueError('Unknown identity reconciliation run')
        rows = query(conn, 'SELECT * FROM nfl_dk_archive_result_reconciliation WHERE run_digest=%s', (identity_run,))
        resolved_results = {(r['draft_group_id'],r['player_id']):r for r in rows}
        if not rows:
            raise ValueError('Identity run has no reconciled pilot results')
    raw = query(conn, "SELECT * FROM nfl_dk_archive_player_stats WHERE game_id LIKE '2025_%%' ORDER BY game_id,gsis_id")
    identities = defaultdict(set)
    history = []
    for row in raw:
        pos, stats = scoring_position(row['position']), row['stats']
        identities[(normalized_name(row['player_name']), pos)].add(row['gsis_id'])
        if pos not in ('QB', 'RB', 'WR', 'TE', 'K') or stats.get('season_type') != 'REG' or int(stats['week']) >= 5:
            continue
        checked_points(pos, stats)
        history.append(HistoricalWeek(identity_id(row['gsis_id']), row['gsis_id'],
                       row['player_name'], pos, 2025, int(stats['week']), row['team'],
                       row['opponent_team'], stats))
    dst = query(conn, """SELECT DISTINCT ON (season,week,team) season,week,team,opponent,
        actual_dk_fpts,input_digest,scoring_evidence,scoring_version
        FROM nfl_dfs_player_week_results WHERE season=2025 AND week<=5
        AND position='DST' AND scoring_status='exact' AND actual_dk_fpts IS NOT NULL
        ORDER BY season,week,team,computed_at DESC,id DESC""")
    dst_map = {}
    for row in dst:
        team = team_key(row['team'])
        dst_map[(row['week'], team, team_key(row['opponent']))] = row
        if row['week'] < 5:
            history.append(HistoricalWeek(identity_id('DST:'+team), None, team, 'DST',
                2025, row['week'], team, row['opponent'], {'fantasy_points': row['actual_dk_fpts']}))
    reports = []
    for draft_group_id in SLATES:
        slate = query(conn, 'SELECT * FROM nfl_dk_archive_slates WHERE draft_group_id=%s', (draft_group_id,))[0]
        entries = query(conn, 'SELECT * FROM nfl_dk_archive_salaries WHERE draft_group_id=%s ORDER BY player_id,roster_slot', (draft_group_id,))
        projections, players = {}, {}
        for entry in entries:
            pid, pos, team = entry['player_id'], entry['position'], team_key(entry['team_nflverse'])
            if pid not in players:
                evidence = {'identity_method': 'unresolved', 'actual_status': 'unresolved',
                            'reason': 'No unique game/team/position/name result; never assumed zero.'}
                points, gsis = None, None
                if pos == 'DST':
                    source = dst_map.get((entry['week'], team, team_key(entry['opponent_team'])))
                    if source:
                        points = source['actual_dk_fpts']
                        evidence = {'identity_method': 'season/week/team/opponent',
                            'actual_status': 'reconstructed_dk', 'source_digest': source['input_digest'],
                            'scoring_version': source['scoring_version'], 'components': source['scoring_evidence']}
                else:
                    matched_source, match_method = match_game_result(entry, raw)
                    ids = identities[(normalized_name(entry['player_name']), pos)]
                    gsis = next(iter(ids)) if len(ids) == 1 else None
                    if matched_source:
                        source = matched_source
                        gsis = source['gsis_id']
                        try:
                            points = checked_points(pos, source['stats'])
                            evidence = {'identity_method': match_method,
                                'source_position': source['position'], 'dk_position': pos,
                                'matched_name': source['player_name'], 'gsis_id': gsis,
                                'actual_status': 'reconstructed_dk', 'source_sha256': source['source_sha256'],
                                'stats': source['stats']}
                        except ValueError as exc:
                            evidence['reason'] = str(exc)
                reconciled = resolved_results.get((draft_group_id,pid))
                if reconciled:
                    if reconciled['game_id'] != entry['game_id']:
                        raise ValueError('Reconciled player belongs to a different game')
                    if points is not None and reconciled['actual_dk_fpts'] is not None and abs(points-reconciled['actual_dk_fpts'])>1e-6:
                        raise ValueError('Stat-line score disagrees with registry reconciliation')
                    if reconciled['actual_dk_fpts'] is not None:
                        points = reconciled['actual_dk_fpts']
                        gsis = reconciled['gsis_id']
                        evidence = {'identity_method':'permanent_registry','actual_status':reconciled['status'],
                            'registry_run_digest':identity_run,'result_digest':reconciled['result_digest'],
                            'result_evidence':reconciled['evidence']}
                projection = project_player(player_id=identity_id('DST:'+team) if pos == 'DST' else None,
                    player_gsis_id=gsis, player_name=entry['player_name'], position=pos,
                    historical_rows=history, cutoff_season=2025, cutoff_week=5, seed=202505)
                projections[pid] = projection.as_dict()
                players[pid] = {'player_id': pid, 'player_name': entry['player_name'], 'position': pos,
                    'team': team, 'gsis_id': gsis, 'actual': points, 'evidence': evidence,
                    'projection': projection.as_dict()}
            entry['actual'] = players[pid]['actual']
            entry['projection_mean'] = projections[pid]['model_proj_fpts']
            entry['player_p90'] = projections[pid]['ceiling_fpts']
            entry['scoring_multiplier'] = float(entry['scoring_multiplier'])
        optimal = solve_lineup(entries, slate['format'], 'actual')
        mean_lineup = solve_lineup(entries, slate['format'], 'projection_mean')
        p90_lineup = solve_lineup(entries, slate['format'], 'player_p90')
        unresolved = [p['player_name'] for p in players.values() if p['actual'] is None]
        eligible = [p for p in players.values() if p['actual'] is not None and p['projection']['model_proj_fpts'] is not None]
        misses = sorted([{'player': p['player_name'], 'position': p['position'],
            'projection': p['projection']['model_proj_fpts'], 'actual': p['actual'],
            'delta': round(p['actual']-p['projection']['model_proj_fpts'], 3),
            'history_games': p['projection']['history_games'],
            'baseline': p['projection']['baseline_fpts']} for p in eligible], key=lambda p: -abs(p['delta']))
        reports.append({'draft_group_id': draft_group_id, 'name': slate['name'], 'format': slate['format'],
            'starts_at': str(slate['starts_at']), 'salary_source_sha256': slate['source_sha256'],
            'game_count': len({e['game_id'] for e in entries}), 'pool_players': len(players),
            'reconciled_players': len(players)-len(unresolved), 'unresolved_players': unresolved,
            'optimum_scope': 'reconciled subset only' if unresolved else 'complete archived pool',
            'hindsight_optimal': optimal, 'mean_replay': mean_lineup, 'additive_p90_replay': p90_lineup,
            'biggest_misses': misses[:15], 'player_audits': list(players.values())})
    return {'version': VERSION, 'identity_run_digest':identity_run, 'runtime': {'scipy': scipy.__version__, 'numpy': np.__version__},
        'selection': 'Fixed 2025 Week 5: earliest full-game Showdown and Sunday main 10-game Classic; no outcome selection.',
        'projection_protocol': '2025 regular-season weeks 1-4 only; historical-v2 with seed 202505, neutral environment. Retrospective reconstruction, not archived live predictions.',
        'history_digest': artifact_digest([r.__dict__ for r in history]),
        'limitations': [
            'DK scores reconstructed from component statistics; not independently matched to official contest standings.',
            'Zeros require the supplied identity run to verify complete recorded participation and no attributed scoring events. Any remaining unknowns are excluded from hindsight optimization; consult each case coverage.',
            'This is an isolated historical-baseline diagnostic, not a replay of the fully connected production optimizer. Postgame participation settles results only; it never filters or changes pregame projections. No timestamped pre-lock injury/depth-chart/scheme overlay is available in this pilot.',
            'Historical statistics were retrieved after the season and may contain later corrections. Identity resolution can use season-wide names but predictive features stop before Week 5.',
            'P90 replay deliberately sums player P90 as a diagnostic baseline. This is not a lineup P90 or a joint-scenario model.',
            'No contest ranks, winnings, ownership, duplication, or competitor-edge conclusions can be inferred from these two cases.'
        ], 'cases': reports}


def render(report):
    esc = lambda value: html.escape(str(value))
    sections = []
    for case in report['cases']:
        cards = []
        for title, key in [('Hindsight: reconciled players', 'hindsight_optimal'),
                           ('Historical mean replay', 'mean_replay'), ('Additive player P90 diagnostic', 'additive_p90_replay')]:
            lineup = case[key]
            rows = ''.join(f"<tr><td>{esc(e['roster_slot'])}</td><td>{esc(e['player_name'])}</td><td>${e['salary']:,}</td><td>{'?' if e['actual'] is None else round(e['actual']*e['scoring_multiplier'],2)}</td></tr>" for e in lineup['players'])
            actual = 'Incomplete' if lineup['actual_points'] is None else f"{lineup['actual_points']:.2f} actual points"
            cards.append(f"<article><h3>{title}</h3><strong>{actual}</strong><p>${lineup['salary']:,} salary · legal · solver gap 0</p><table><tr><th>Slot</th><th>Player</th><th>Salary</th><th>Actual</th></tr>{rows}</table><p>Unresolved selected: {esc(', '.join(lineup['unresolved_selected']) or 'None')}</p></article>")
        bars = ''.join(f"<div class='miss'><span>{esc(m['player'])}</span><div><i style='width:{min(100,abs(m['delta'])*2)}%;background:{'#0d9488' if m['delta']>=0 else '#d97706'}'></i></div><b>{m['delta']:+.1f}</b></div>" for m in case['biggest_misses'][:8])
        sections.append(f"<section><h2>{esc(case['format'].upper())} · {case['draft_group_id']}</h2><p>{esc(case['name'])}</p><p>{case['reconciled_players']}/{case['pool_players']} players reconciled · {case['game_count']} games · {esc(case['optimum_scope'])}</p><div class='grid'>{''.join(cards)}</div><h3>Actual minus historical projection</h3>{bars}<details><summary>Unresolved players ({len(case['unresolved_players'])})</summary><p>{esc(', '.join(case['unresolved_players']))}</p></details></section>")
    limitations = ''.join('<li>'+esc(s)+'</li>' for s in report['limitations'])
    return "<!doctype html><html lang='en'><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>NFL archive pilot</title><style>body{font:16px system-ui;margin:0;background:#f1f5f9;color:#14243b}main{max-width:1450px;margin:auto;padding:32px}h1{font-size:36px}section{margin:36px 0}article{background:white;padding:20px;border:1px solid #cbd5e1;border-radius:12px}.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}table{width:100%;border-collapse:collapse;font-size:13px}td,th{text-align:left;padding:7px 3px;border-bottom:1px solid #e2e8f0}strong{font-size:23px}.miss{display:grid;grid-template-columns:180px 1fr 70px;gap:12px;margin:9px 0;max-width:850px}.miss div{background:#e2e8f0}.miss i{display:block;height:20px}details{margin-top:20px}li{margin:8px 0}@media(max-width:1000px){.grid{grid-template-columns:1fr}main{padding:16px}}</style><main><h1>2025 NFL · One Showdown, one Classic</h1><p>Week 5 diagnostic: salary-constrained hindsight versus a pre-Week-5 historical projection replay.</p><p><b>Research pilot — reconstructed scores; player coverage is reported for each slate.</b></p>" + ''.join(sections) + '<h2>What this does and does not establish</h2><ul>'+limitations+'</ul><p>Full inputs, scoring evidence and player projections: <a href="report.json">report.json</a>.</p></main></html>'


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--output', type=Path, default=Path('artifacts/nfl-archive-pilot'))
    parser.add_argument('--persist', action='store_true')
    parser.add_argument('--identity-run', help='Exact saved identity reconciliation run digest; no implicit latest lookup')
    args = parser.parse_args()
    with psycopg2.connect(load_config().database_url) as conn:
        conn.set_session(readonly=True, isolation_level='REPEATABLE READ')
        report = run(conn,args.identity_run)
    # Convert DB Decimal/datetime consistently before digesting or storing.
    report = json.loads(json.dumps(report, default=str))
    digest = artifact_digest(report)
    report['report_digest'] = digest
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output/'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    (args.output/'index.html').write_text(render(report), encoding='utf-8')
    if args.persist:
        with psycopg2.connect(load_config().database_url) as conn:
            with conn.cursor() as cur:
                cur.execute('''CREATE TABLE IF NOT EXISTS nfl_dk_archive_case_studies (
                    report_digest TEXT PRIMARY KEY, version TEXT NOT NULL,
                    report JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())''')
                cur.execute('INSERT INTO nfl_dk_archive_case_studies(report_digest,version,report) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                            (digest, VERSION, Json(report)))
    print(json.dumps({'digest': digest, 'cases': [{k:c[k] for k in ('draft_group_id','reconciled_players','pool_players','optimum_scope')} | {
        'optimal_points': c['hindsight_optimal']['actual_points'], 'salary':c['hindsight_optimal']['salary'],
        'mean_replay_actual': c['mean_replay']['actual_points']} for c in report['cases']]}, indent=2))


if __name__ == '__main__':
    main()
