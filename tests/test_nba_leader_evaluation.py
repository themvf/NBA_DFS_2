from copy import deepcopy
import json
import re
import pytest
from research.nba_leader_evaluation import evaluate,freeze_selections,aggregate
from research.nba_points_daily import render_fragment

def fixture():
    candidates=[{'event_id':'e','player_key':str(i),'player':'Player '+str(i),'predictions':{'market_line':30-i,'model':40-i}} for i in range(5)]
    snapshot={'candidates':candidates,'models':['model']}
    rows=[{**{k:c[k] for k in ['event_id','player_key','player']},'actual_points':p,'status':'graded'} for c,p in zip(candidates,[10,20,30,40,50])]
    return snapshot,rows

def test_frozen_trio_survives_surprising_results_and_same_pool_baseline():
    s,r=fixture();s['leader_selections']=freeze_selections(s['candidates'],['market_line','model']);before=deepcopy(s)
    out=evaluate(s,r)['strategies']['model']
    assert out['selected_actual_total']==60 and out['best_verified_trio_points']==120
    assert out['gap_to_best_verified']==60 and out['top3_capture_fraction']==pytest.approx(1/3)
    assert [c['player_key'] for c in out['selected']]==['0','1','2'] and s==before
    s['candidates'][4]['predictions']['model']=None;s.pop('leader_selections')
    out=evaluate(s,r)['strategies']['model']
    assert out['pool_count']==4 and out['paired_market']['pool_count']==4 and out['best_verified_trio_points']==90

@pytest.mark.parametrize('status',['unmatched','dnp'])
def test_missing_selection_never_replaced_or_zeroed(status):
    s,r=fixture();r[0].update(status=status,actual_points=None)
    out=evaluate(s,r)['strategies']['model']
    assert out['selected_actual_total'] is None and out['gap_to_best_verified'] is None
    assert out['selected'][0]['actual_points'] is None and len(out['selected'])==3
    assert out['leaderboard_provisional']==(status!='dnp')

def test_fractional_boundary_ties_and_deterministic_forecast_ties():
    s,r=fixture()
    for c in s['candidates']:c['predictions']['model']=20
    for row,p in zip(r,[50,40,30,30,30]):row['actual_points']=p
    out=evaluate(s,r)['strategies']['model']
    assert [c['player_key'] for c in out['selected']]==['0','1','2']
    assert out['captured_top3_slots']==pytest.approx(2+1/3)
    assert out['top3_capture_fraction']==pytest.approx(7/9)

def test_ambiguous_canonical_name_and_known_out_excluded():
    s,r=fixture();s['candidates'][4]['player_key']='0';s['candidates'][3]['availability']={'status':'out'}
    policy=freeze_selections(s['candidates'],['model'])['strategies']['model']
    assert policy['eligible_count']==2 and policy['selected']==[]

def test_recorded_policy_checked_and_old_snapshot_provenance_explicit():
    s,r=fixture();assert evaluate(s,r)['selection_provenance']=='reconstructed_from_frozen_predictions'
    s['leader_selections']=freeze_selections(s['candidates'],['market_line','model'])
    assert evaluate(s,r)['selection_provenance']=='recorded_in_snapshot'
    s['candidates'][0]['predictions']['model']=0
    with pytest.raises(ValueError,match='frozen selection'):evaluate(s,r)

def test_aggregation_excludes_reconstructed_trials_and_missing_metrics():
    s,r=fixture();g={'game_date':'2026-10-24','snapshot_mode':'prospective','leaderboards':evaluate(s,r)}
    result=aggregate([g,{**g,'snapshot_mode':'retrospective'},{'snapshot_mode':'prospective','game_date':'2026-10-23'}])
    assert result['strategies']['model']['dates_evaluated']==1 and result['missing_metric_dates']==['2026-10-23']

def test_embedded_json_preserves_text_and_cannot_close_script():
    data={'player':'</script><script>alert(1)</script>'}
    output=render_fragment(data)
    assert data['player'] not in output
    embedded=re.search(r'const data\s*=\s*(.*?);',output).group(1)
    assert json.loads(embedded)==data
