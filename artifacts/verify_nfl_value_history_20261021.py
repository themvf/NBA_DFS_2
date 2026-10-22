"""Independent arithmetic, timing, Captain, and prior-review checks."""
import gzip
import json
import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path.cwd()))
from model.nfl_dfs_historical import draftkings_points

ROOT=Path('artifacts/nfl-value-history-20261021')
x=json.loads((ROOT/'assessment.json').read_text(encoding='utf-8'))
raw=json.load(gzip.open(ROOT/'inputs.json.gz','rt',encoding='utf-8'))
dt=lambda t:datetime.fromisoformat(t.replace('Z','+00:00'))
assert len(x['reports'])==23
assert sum(g['format']=='classic' for g in x['reports'])==6
assert sum(g.get('lineups') or 0 for g in x['reports'])==789
component_checks=0
captain_checks=0
all_rows=0
for g in x['reports']:
    assert all(c['completed'] and c['covered'] for c in g['coverage'].values()),g['key']
    assert not g['verification_issues'],g['key']
    assert dt(g['selected_created_at'])<dt(g['lock_at'])
    eligible=[r for r in raw['runs'] if r['upload_id'] in g['upload_ids'] and r['generated_lineups']>0 and dt(r['created_at'])<dt(g['lock_at'])]
    if eligible:
        assert max(eligible,key=lambda r:dt(r['created_at']))['run_id']==g['selected_run_id']
    else:
        assert g['status']=='baseline_only' and g['lineups'] is None
    assert len({p['dk_player_id'] for p in g['rows']})==len(g['rows'])
    for p in g['rows']:
        all_rows+=1
        assert p['salary']<5000
        if p['delta'] is not None:
            assert abs(p['actual']-p['projection']-p['delta'])<1e-9
            assert p['forecast_kind']=='supported'
        else:
            assert p['projection'] is None or p['actual'] is None
        c=p['scoring_components']
        if c and p['position']!='DST' and p['canonical_actual'] is not None:
            assert abs(draftkings_points(p['position'],c)-p['canonical_actual'])<0.011,(g['key'],p['name'])
            component_checks+=1
        if g['format']=='showdown':
            if g['lineups'] is not None:
                assert p['captain_entries']+p['flex_entries']==p['entries']
                assert 0<=p['entries']<=g['lineups']
            if p['projection'] is not None:
                assert abs(p['captain_projection']-1.5*p['projection'])<1e-9
            if p['actual'] is not None:
                assert abs(p['captain_actual']-1.5*p['actual'])<1e-9
            assert p['captain_under_5000']==(p['captain_salary'] is not None and p['captain_salary']<5000)
            captain_checks+=1
    page=ROOT/'slates'/('week-'+str(g['week']).zfill(2)+'-'+g['format']+('-'+g['games'][0].replace('@','-at-') if g['format']=='showdown' else '')+'.md')
    assert page.exists()
    assert '## All supported players who beat projection' in page.read_text(encoding='utf-8')

old=json.loads(Path('artifacts/sunday-classic-value-assessment-20261020.json').read_text())
g=next(g for g in x['reports'] if g['week']==6 and g['format']=='classic')
assert g['selected_run_id']==old['run_id'] and g['lineups']==old['lineups']
oldmap={p['dk_player_id']:p for p in old['supported_outperformers']}
newmap={p['dk_player_id']:p for p in g['outperformers']}
assert set(oldmap)==set(newmap)
for pid,p in oldmap.items():
    q=newmap[pid]
    for k in ['salary','projection','actual','entries']:
        assert abs(p[k]-q[k])<1e-8,(pid,k)
conflicts=[(g['week'],p['name'],p['canonical_actual'],p['actual']) for g in x['reports'] for p in g['rows'] if p['result_conflict']]
assert set(conflicts)=={(1,'Buccaneers',8.0,9.0),(5,'Seahawks',8.0,9.0)}
evidence=json.loads((ROOT/'injury-boundaries.json').read_text(encoding='utf-8'))
boundaries={(b['slate_key'],b['player_id']):b for b in evidence['requested']}
for o in evidence['observations']:
    b=boundaries[o['slate_key'],o['player_id']]
    assert o['week']==b['week']
    assert dt(o['observed_at'])<=dt(b['cutoff']) and dt(o['fetched_at'])<=dt(b['cutoff'])
    assert o['available_at'] is None or dt(o['available_at'])<=dt(b['cutoff'])
result=dict(status='passed',slates=23,lineups=789,cheap_player_slate_rows=all_rows,
    non_dst_component_rescores=component_checks,captain_row_checks=captain_checks,
    previous_sunday_regression_players=len(newmap),injury_boundary_checks=len(evidence['observations']),
    unresolved_contest_ledger_disagreements=conflicts)
(ROOT/'verification.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
print(json.dumps(result))
