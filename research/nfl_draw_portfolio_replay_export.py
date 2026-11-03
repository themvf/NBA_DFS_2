"""Read-only export of 2026 Showdown slates for the draw-portfolio replay.

Usage: python -m research.nfl_draw_portfolio_replay_export <out.json>
Then:  node -r ./scripts/server-only-stub.cjs --import tsx ./scripts/replay-nfl-draw-portfolio.ts <out.json> [seeds]

Per Showdown slate with an imported contest score curve: the latest saved GPP
run (settings + input_snapshot), actual DK points by DraftKings player id (the
contest's own player rows, else exact weekly results), and the absent
starter's pre-availability range for every v1 absence adjustment that moved a
player up (the v2 mixture needs it). SELECTs only.
"""
import sys
import os, json, re, unicodedata, psycopg2
from dotenv import load_dotenv
load_dotenv(".env")
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.set_session(readonly=True)
cur = c.cursor()
norm = lambda s: re.sub(r'[^a-z0-9]', '', unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower())
cur.execute("""select fc.slate_upload_id, fc.contest_id, fc.contest_name, fc.entry_count, fc.week, fc.score_curve
  from nfl_dfs_field_contests fc where fc.format='showdown' and fc.season=2026 and fc.slate_upload_id is not null
  and fc.score_curve is not null order by fc.entry_count desc""")
best = {}
for up, cid, name, n, wk, curve in cur.fetchall():
    best.setdefault(up, (cid, name, n, wk, curve))
out = []
for up, (cid, name, n, wk, curve) in best.items():
    cur.execute("""select run_id, created_at, settings, input_snapshot, projection_run_id from nfl_dfs_optimizer_runs
      where upload_id=%s and mode='gpp' and generated_lineups>0 order by created_at desc limit 1""", (up,))
    r = cur.fetchone()
    if not r:
        print('no gpp run', up, name); continue
    run_id, created, settings, snap, prun = r
    cur.execute("select normalized_name, player_name, fpts, drafted_by_slot from nfl_dfs_field_ownership where contest_id=%s", (cid,))
    pts = {}
    for nn, pn, f, slot in cur.fetchall():
        if f is None: continue
        key = nn or norm(pn)
        slotname = str(slot or '').upper()
        v = float(f) / 1.5 if slotname.startswith('C') else float(f)
        pts.setdefault(key, v)
    points = {}
    missing = []
    cur.execute("select season, week from nfl_dfs_projection_runs where run_id=%s", (prun,))
    season, pweek = cur.fetchone()
    for p in snap:
        k = norm(p['name'])
        if k in pts: points[str(p['dkPlayerId'])] = pts[k]; continue
        if p.get('ffPlayerId'):
            cur.execute("select actual_dk_fpts from nfl_dfs_player_week_results where player_id=%s and season=%s and week=%s and scoring_status='exact'", (p['ffPlayerId'], season, pweek))
            r2 = cur.fetchone()
            if r2 and r2[0] is not None: points[str(p['dkPlayerId'])] = float(r2[0]); continue
        if not p.get('isOut'): missing.append(p['name'])
    starters = {}
    for p in snap:
        a = p.get('absenceCommit')
        if not a or a.get('rule') != 'own_role_games' or not (a.get('scale') or 0) > 1: continue
        for nm in a.get('from') or []:
            sp = next((q for q in snap if q['name'] == nm), None)
            if not sp or not sp.get('ffPlayerId'): continue
            cur.execute("""select source_evidence->'availability'->'pre_availability', position from nfl_dfs_player_projections
              where run_id=%s and player_id=%s""", (prun, sp['ffPlayerId']))
            row = cur.fetchone()
            pre = row[0] if row else None
            if pre and pre.get('model_proj_fpts'):
                starters[nm] = dict(mean=float(pre['model_proj_fpts']), p10=float(pre.get('floor_fpts') or 0), median=float(pre.get('median_fpts') or 0),
                                    p90=float(pre.get('ceiling_fpts') or 0), boom=pre.get('boom_rate'), position=row[1])
    out.append(dict(upload=up, contest=cid, contestName=name, entries=n, week=wk, curve=curve, runId=str(run_id),
                    createdAt=str(created), settings=settings, players=snap, points=points, missingPoints=missing, starters=starters))
    print(wk, name[:60], 'run', str(run_id)[:8], 'players', len(snap), 'pts', len(points), 'missing', missing[:6], 'starters', list(starters))
json.dump(out, open(sys.argv[1], 'w'), default=str)
print('slates', len(out))
