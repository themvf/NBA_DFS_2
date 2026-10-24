"""SELECT-only evidence capture for Week 1 onward value review."""
import json
import gzip
import re
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import dotenv_values

OUT = Path('artifacts/nfl-value-history-20261021')
OUT.mkdir(exist_ok=True)
with psycopg2.connect(dotenv_values('web/.env.local')['DATABASE_URL'], connect_timeout=15) as conn:
    conn.set_session(readonly=True, isolation_level='REPEATABLE READ')
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        def q(sql, params=()):
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        out = {'captured_at': datetime.now(timezone.utc).isoformat()}
        out['uploads'] = q('''SELECT u.*,r.season,r.week,r.as_of_at projection_as_of,r.model_version
            FROM nfl_dfs_slate_uploads u LEFT JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
            WHERE r.season=2026 OR r.season IS NULL ORDER BY u.created_at''')
        out['runs'] = q('''SELECT r.run_id,r.upload_id,r.created_at,r.generated_lineups,r.requested_lineups,
            r.status,r.mode,r.projection_source,r.optimizer_version
            FROM nfl_dfs_optimizer_runs r JOIN nfl_dfs_slate_uploads u USING(upload_id)
            LEFT JOIN nfl_dfs_projection_runs p ON p.run_id=u.projection_run_id
            WHERE p.season=2026 OR p.season IS NULL ORDER BY r.created_at''')
        out['games'] = q('''SELECT g.id,g.season,g.week,g.kickoff,g.completed,g.nflverse_game_id,
            a.abbreviation away,h.abbreviation home FROM nfl_season_games g
            JOIN nfl_teams a ON a.team_id=g.away_team_id JOIN nfl_teams h ON h.team_id=g.home_team_id
            WHERE g.season=2026 AND g.game_type='REG' ORDER BY g.kickoff''')
        out['contests'] = q('SELECT * FROM nfl_dfs_field_contests WHERE season=2026 ORDER BY week,format,contest_id')
        out['player_columns'] = q("SELECT column_name FROM information_schema.columns WHERE table_name='nfl_dfs_slate_players' ORDER BY ordinal_position")
        out['source_inventory'] = q('''SELECT source,dataset,week,count(*) n,min(fetched_at) first,max(fetched_at) last
            FROM ff_source_snapshots WHERE season=2026 AND (dataset ILIKE '%%depth%%' OR source ILIKE '%%official%%')
            GROUP BY source,dataset,week ORDER BY source,dataset,week''')
        out['official_imports'] = q('SELECT * FROM nfl_official_inactive_imports WHERE season=2026 ORDER BY week,reviewed_at')
        out['players'] = q('''SELECT p.upload_id,p.dk_player_id,p.captain_dk_player_id,p.ff_player_id,p.name,p.position,p.team,
            p.opponent,p.game_key,p.game_info,p.salary,p.captain_salary,p.is_out,p.projection_status,p.our_proj,
            p.floor_fpts,p.ceiling_fpts,p.history_games,p.identity_method,p.created_at,p.updated_at
            FROM nfl_dfs_slate_players p JOIN nfl_dfs_slate_uploads u USING(upload_id)
            LEFT JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
            WHERE r.season=2026 OR r.season IS NULL''')
        alias = lambda t: {'WAS':'WSH','LA':'LAR','JAC':'JAX','AZ':'ARI'}.get(t,t)
        canon = lambda key: '@'.join(alias(t) for t in key.split('@'))
        grouped = {}
        for u in out['uploads']:
            if u['file_name'].startswith('system:'):
                continue
            ps = [p for p in out['players'] if p['upload_id']==u['upload_id']]
            dates = set()
            for p in ps:
                m = re.search(r'(\d{2}/\d{2}/\d{4})',p['game_info'] or '')
                if m:
                    dates.add(datetime.strptime(m[1],'%m/%d/%Y').date())
            game_keys = set(canon(k) for k in u['games'])
            gs = [g for g in out['games'] if canon(g['away']+'@'+g['home']) in game_keys
                  and (g['week']==u['week'] if u['week'] is not None else g['kickoff'].date() in dates)]
            if not gs or len(gs)!=len(game_keys):
                continue
            week = gs[0]['week']
            if week > 6 or not all(g['completed'] for g in gs):
                continue
            key = f"week-{week:02d}-{u['format']}-" + '_'.join(sorted(game_keys))
            group = grouped.setdefault(key,dict(key=key,week=week,format=u['format'],games=sorted(game_keys),
                game_ids=[g['id'] for g in gs],lock_at=min(g['kickoff'] for g in gs),upload_ids=[]))
            group['upload_ids'].append(u['upload_id'])
        out['groups'] = list(grouped.values())
        selected_ids = []
        for g in out['groups']:
            runs = [r for r in out['runs'] if r['upload_id'] in g['upload_ids'] and r['created_at']<g['lock_at']
                    and r['generated_lineups']>0]
            g['prelock_runs'] = [r['run_id'] for r in runs]
            selected = max(runs,key=lambda r:r['created_at']) if runs else None
            g['selected_run_id'] = selected['run_id'] if selected else None
            if selected:
                selected_ids.append(selected['run_id'])
                g['selected_upload_id'] = selected['upload_id']
            else:
                uploads = [u for u in out['uploads'] if u['upload_id'] in g['upload_ids'] and u['created_at']<g['lock_at']
                           and u['projection_as_of'] is not None and u['projection_as_of']<g['lock_at']]
                g['selected_upload_id'] = max(uploads,key=lambda u:u['created_at'])['upload_id'] if uploads else None
        out['selected_runs'] = q('SELECT * FROM nfl_dfs_optimizer_runs WHERE run_id=ANY(%s::uuid[])',(selected_ids,))
        out['lineups'] = q('SELECT * FROM nfl_dfs_lineups WHERE run_id=ANY(%s::uuid[])',(selected_ids,))
        out['results'] = q('SELECT * FROM nfl_dfs_player_week_results WHERE season=2026 AND week BETWEEN 1 AND 6')
        out['ownership'] = q('''SELECT o.* FROM nfl_dfs_field_ownership o JOIN nfl_dfs_field_contests c USING(contest_id)
            WHERE c.season=2026 AND c.week BETWEEN 1 AND 6''')
        fallback_ids = [u['projection_run_id'] for u in out['uploads'] if any(g['selected_run_id'] is None
            and g['selected_upload_id']==u['upload_id'] for g in out['groups']) and u['projection_run_id']]
        out['baseline_projections'] = q('SELECT * FROM nfl_dfs_player_projections WHERE run_id=ANY(%s::uuid[])',(fallback_ids,))
        out['injuries'] = q('''SELECT DISTINCT ON(i.player_id,s.week,i.source) i.*,s.week,s.dataset,s.fetched_at,s.available_at
            FROM ff_player_injury_observations i JOIN ff_source_snapshots s ON s.id=i.source_snapshot_id
            WHERE i.season=2026 AND s.week BETWEEN 1 AND 6 AND i.source IN ('sleeper','fantasypros')
            AND EXISTS (SELECT 1 FROM nfl_season_games g WHERE g.season=2026 AND g.week=s.week AND g.game_type='REG'
                AND i.observed_at < g.kickoff AND i.observed_at >= g.kickoff-interval '7 days')
            ORDER BY i.player_id,s.week,i.source,i.observed_at DESC''')
with gzip.open(OUT/'inputs.json.gz','wt',encoding='utf-8') as f:
    json.dump(out,f,default=str)
# Keep the small inventory separate from the immutable, compressed evidence.
inventory = {k:v for k,v in out.items() if k not in ['players','selected_runs','lineups','results','ownership','baseline_projections','injuries']}
(OUT/'inventory.json').write_text(json.dumps(inventory,default=str,indent=2),encoding='utf-8')
from collections import Counter
print(json.dumps({'uploads':len(out['uploads']),'by_week_format':dict(Counter(f"{u['week']} {u['format']}" for u in out['uploads'])),
    'runs':len(out['runs']),'groups':len(out['groups']),'selected_runs':len(out['selected_runs']),'lineups':len(out['lineups']),
    'contests':len(out['contests']),'official_imports':len(out['official_imports']),'depth_source_groups':len(out['source_inventory']),
    'player_columns':[r['column_name'] for r in out['player_columns']]},default=str))
