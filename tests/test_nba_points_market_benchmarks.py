from copy import deepcopy
from datetime import datetime,timezone
import pytest
from research.nba_points_research_grade import BoxGame
from research.nba_points_market_benchmarks import build,choose,markdown
from research.nba_points_forward import digest

def at(hour,minute=0):return datetime(2026,10,26,hour,minute,tzinfo=timezone.utc)

def fixture():
    c={'event_id':'market','player_key':'one','player':'One','home_team':'Boston Celtics','away_team':'Miami Heat',
       'captured_at':at(12).isoformat(),'predictions':{'market_line':20.,'model':22.}}
    s={'sha256':'original','game_date':'2026-10-26','created_at':at(13).isoformat(),'lock_at':at(19).isoformat(),'models':['model'],'candidates':[c]}
    g={'snapshot_sha256':'original','rows':[{'event_id':'market','player_key':'one','status':'graded','actual_points':23.}]}
    games=[BoxGame('espn','Boston Celtics','Miami Heat',[],at(21))]
    def quote(hour,line,id):
        return {'id':id,'sport':'nba','event_id':'market','game_date':'2026-10-26','player':'One','market':'player_points',
                'home_team_name':'Boston Celtics','away_team_name':'Miami Heat','captured_at':at(hour),'commence_time':at(21,10),
                'books':{'book':{'line':line,'over':-110,'under':-110,'lastUpdate':at(hour).isoformat()}}}
    return s,g,games,quote

def test_three_boundaries_keep_original_and_save_different_late_lines():
    s,g,games,q=fixture();before=deepcopy(s)
    out=build(s,g,[q(12,20,1),q(18,21,2),q(20,24,3),q(21,99,4)],games)
    r=out['rows'][0]
    assert r['frozen_line']['line']==20 and r['contest_lock_proxy']['line']==21 and r['game_close_proxy']['line']==24
    assert r['game_close_proxy']['source_row_ids']==[3] and not r['game_close_proxy']['verified_closing_line']
    assert out['summary']['frozen_line']['over']==1 and out['summary']['game_close_proxy']['under']==1
    assert s==before and digest({k:v for k,v in out.items() if k!='sha256'})==out['sha256']

def test_official_tip_overrides_delayed_market_start_and_exact_tip_is_rejected():
    s,g,games,q=fixture();post=q(21,100,1);post['captured_at']=at(21,5)
    out=build(s,g,[post,q(21,50,2),q(20,22,3)],games)
    assert out['rows'][0]['game_close_proxy']['line']==22
    assert out['rows'][0]['game_close_proxy']['rejected']['at_or_after_cutoff']==2

def test_duplicate_disagreement_same_capture_is_not_resolved_arbitrarily():
    s,g,games,q=fixture();out=build(s,g,[q(20,22,1),q(20,24,2)],games)
    assert out['rows'][0]['game_close_proxy']['status']=='ambiguous_latest_capture'
    assert out['summary']['game_close_proxy']['n']==0
    assert out['paired_same_players']['frozen_line']['n']==0

def test_exact_duplicates_preserve_both_source_ids():
    s,g,games,q=fixture();out=build(s,g,[q(20,22,1),q(20,22,2)],games)
    assert out['rows'][0]['game_close_proxy']['source_row_ids']==[1,2]

def test_provider_future_timestamp_rejected_missing_timestamp_explicit():
    s,g,games,q=fixture();future=q(20,100,1);future['books']['book']['lastUpdate']=at(21).isoformat()
    older=q(18,22,2);older['books']['book'].pop('lastUpdate')
    out=build(s,g,[future,older],games);r=out['rows'][0]['game_close_proxy']
    assert r['line']==22 and r['book_provider_timestamps_missing']==['book']
    assert r['rejected']['provider_update_after_capture']==1 and not r['verified_closing_line']

def test_wrong_event_team_date_sport_and_alternate_market_never_join():
    s,g,games,q=fixture();rows=[]
    for key,value in [('event_id','other'),('home_team_name','Denver Nuggets'),('game_date','2026-10-25'),('sport','nfl'),('market','player_points_alternate')]:
        r=q(20,99,key);r[key]=value;rows.append(r)
    out=build(s,g,rows,games)
    assert out['rows'][0]['game_close_proxy']['line'] is None and out['summary']['game_close_proxy']['n']==0

def test_no_verified_schedule_never_falls_back_to_market_estimate():
    s,g,games,q=fixture();out=build(s,g,[q(20,22,1)],[])
    assert out['rows'][0]['game_close_proxy']['status']=='missing_verified_schedule'
    s['slate_schedule']=[{'event_id':'espn','home_team':'Boston Celtics','away_team':'Miami Heat','start_time':at(21).isoformat()}]
    assert build(s,g,[q(20,22,1)],[])['rows'][0]['game_close_proxy']['line']==22

@pytest.mark.parametrize('status',['dnp','unmatched'])
def test_dnp_unmatched_excluded_and_original_lines_not_replaced(status):
    s,g,games,q=fixture();g['rows'][0].update(status=status,actual_points=None)
    out=build(s,g,[q(20,22,1)],games)
    assert all(v['n']==0 and v['over_rate_all_graded'] is None for v in out['summary'].values())
    assert out['rows'][0]['game_close_proxy']['line']==22 and out['outcome_exclusions'][status]==1

def test_push_counts_and_model_comparison_use_identical_players():
    s,g,games,q=fixture();g['rows'][0]['actual_points']=22
    out=build(s,g,[q(20,22,1)],games)
    assert out['summary']['game_close_proxy']['push']==1
    assert out['summary']['game_close_proxy']['over_rate_excluding_pushes'] is None
    m=out['paired_model_errors']['model']['all'];assert m=={'n':1,'model_mae':0,'frozen_line_mae':2,'game_close_proxy_mae':0}
    assert 'not verified closing lines' in markdown(out)

def test_different_snapshot_grade_rejected():
    s,g,games,q=fixture();g['snapshot_sha256']='other'
    with pytest.raises(ValueError,match='different frozen'):build(s,g,[],games)

def test_near_tip_is_still_proxy_and_ambiguity_is_visible():
    s,g,games,q=fixture();r=q(20,22,1);r['captured_at']=at(20,55)
    out=build(s,g,[r],games);assert out['rows'][0]['game_close_proxy']['within_15_minutes_of_tip']
    assert not out['rows'][0]['game_close_proxy']['verified_closing_line']
    s['candidates'].append({**s['candidates'][0],'event_id':'other'})
    g['rows'].append({**g['rows'][0],'event_id':'other'})
    assert build(s,g,[r],games)['rows'][0]['game_close_proxy']['status']=='ambiguous_player'
