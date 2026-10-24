from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
import json

import pytest

from model.nba_points_calibration import BoxGame,BoxPlayer
from research.nba_points_forward import build_snapshot,digest
from research import nba_points_ghost_v2 as v2
from research.nba_ghost_runner import make_snapshot,classifications_from_scoreboard,render

LOCK=datetime(2026,10,24,22,tzinfo=timezone.utc)
CREATED=LOCK-timedelta(hours=1)


def fixture():
    raw=[dict(event_id='e',game_date=date(2026,10,24),commence_time=LOCK,captured_at=LOCK-timedelta(hours=4),
              home_team_name='Boston Celtics',away_team_name='Miami Heat',player=p,market='player_points',
              books={'dk':{'line':line,'over':-110,'under':-110}}) for p,line in [('One',20.),('Two',25.5),('Three',10.5)]]
    market=build_snapshot(raw,date(2026,10,24),LOCK,LOCK-timedelta(hours=3))
    logs=[dict(player_id=1,name='One',game_id=str(i),game_date=f'2026-04-{i+1:02d}',season_type='Regular Season',
               team_id=1,opponent_team_id=2,is_home=True,minutes=30.,points=20.,fgm=8.,fga=16.,fg3m=2.,fg3a=5.,ftm=2.,fta=3.,
               team_fga=90.,fetched_at=(LOCK-timedelta(days=1)).isoformat()) for i in range(20)]
    bundle={'logs':logs,'boxes':[],'games':[],'classifications':[],
            'teams':[{'team_id':1,'name':'Boston Celtics'},{'team_id':2,'name':'Miami Heat'}]}
    def spec(value):return {'features':['p20'],'impute':[20.],'mean':[0.],'scale':[1.],'coef':[0.],'intercept':value}
    model={'version':v2.ARTIFACT_VERSION,'created_at':(LOCK-timedelta(hours=2)).isoformat(),'calibration_end':'2026-01-31',
           'watch_model':spec(.5),'watch_score_thresholds':{'True':.25,'False':.25},
           'p90_model':spec(30),'p90_offsets':{'True':0.,'False':0.},
           'opportunity_models':{k:spec(value) for k,value in [('minutes',34),('fga',18),('fg3a',5),('fta',4)]},
           'league_rates':{'two':.5,'three':.35,'free':.75}}
    model['sha256']=digest(model)
    return market,bundle,model


def add_box(bundle,day='2026-10-22',event='box',season_type=2):
    stamp=(LOCK-timedelta(days=1)).isoformat()
    bundle['games'].append({'espn_event_id':event,'game_date':day,'home_team':'Boston Celtics','away_team':'Miami Heat',
                            'home_score':20,'away_score':20,'completed':True,'fetched_at':stamp})
    for name,athlete,home,team in [('One','espn1',True,'Boston Celtics'),('Opponent','espn2',False,'Miami Heat')]:
        bundle['boxes'].append({'espn_event_id':event,'espn_athlete_id':athlete,'player':name,'team':team,'is_home':home,'played':True,
                                'minutes':35.,'points':20.,'fgm':8.,'fga':16.,'fg3m':2.,'fg3a':5.,'ftm':2.,'fta':3.,'fetched_at':stamp})
    bundle['classifications'].append({'espn_event_id':event,'season_type':season_type,'game_date':day,'captured_at':stamp,
                                      'home_team':'Boston Celtics','away_team':'Miami Heat','source_url':'https://example.com/scoreboard'})


def merged(bundle):
    return v2.merge_history(bundle['logs'],bundle['boxes'],bundle['games'],bundle['teams'],bundle['classifications'],'2026-10-24',CREATED)


def test_current_boxes_update_history_and_preserve_original_inputs():
    market,bundle,model=fixture();old=deepcopy(market)
    add_box(bundle)
    snapshot=make_snapshot(market,bundle,model,CREATED)
    one=next(c for c in snapshot['candidates'] if c['player']=='One')
    assert one['history_count']==20 and one['current_season_history_count']==1
    assert one['history'][-1]['source']=='espn_box'
    assert one['opportunity_forecasts']['minutes']==34
    assert one['historical_p90']==20 and one['experimental_p90']==30
    assert one['ceiling_watch']['flag'] is True and one['ceiling_watch']['probability'] is None
    assert one['availability']['status']=='unverified' and market==old


def test_overlapping_game_collapses_by_teams_date_and_checks_agreement():
    _,bundle,_=fixture();add_box(bundle,day='2026-04-20')
    history,blocked,audit=merged(bundle)
    assert len(history['one'])==20 and len(history['one'][-1]['provenance'])==2
    assert audit['cross_provider_duplicates_collapsed']==1
    bundle['boxes'][0].update(points=21,ftm=3,fta=4)
    bundle['games'][0]['home_score']=21
    history,blocked,_=merged(bundle)
    assert 'one' not in history and 'cross_provider_stats_disagreement' in blocked['one']


def test_identity_collision_and_source_duplicates_block_player():
    _,bundle,_=fixture();add_box(bundle)
    duplicate={**bundle['logs'][0],'player_id':99,'game_id':'new'}
    bundle['logs'].append(duplicate)
    history,blocked,_=merged(bundle)
    assert 'one' not in history and 'ambiguous_cross_provider_name' in blocked['one']
    _,bundle,_=fixture();bundle['logs'].append(bundle['logs'][0])
    history,blocked,_=merged(bundle)
    assert 'duplicate_source_identity' in blocked['one'] and 'one' not in history


@pytest.mark.parametrize('season_type',[None,1,3])
def test_unverified_preseason_and_playoff_boxes_are_excluded(season_type):
    _,bundle,_=fixture();add_box(bundle,season_type=season_type)
    history,_,audit=merged(bundle)
    assert all(r['source']=='nba_log' for r in history['one'])
    assert audit['box_regular_season_unverified']==2


def test_late_observations_and_future_game_do_not_change_features():
    market,bundle,model=fixture();base=make_snapshot(market,bundle,model,CREATED)
    bundle['logs'].append({**bundle['logs'][0],'game_id':'future','game_date':'2026-10-24','points':1000})
    bundle['logs'].append({**bundle['logs'][0],'game_id':'late','fetched_at':CREATED.isoformat(),'points':1000})
    changed=make_snapshot(market,bundle,model,CREATED)
    assert base['candidates'][0]['features']==changed['candidates'][0]['features']
    assert base['candidates'][0]['predictions']==changed['candidates'][0]['predictions']


def test_box_total_reconciliation_and_late_classification_fail_closed():
    _,bundle,_=fixture();add_box(bundle);bundle['games'][0]['home_score']=99
    history,_,audit=merged(bundle)
    assert all(r['source']=='nba_log' for r in history['one']) and audit['box_team_scores_unreconciled']==2
    _,bundle,_=fixture();add_box(bundle);bundle['classifications'][0]['captured_at']=CREATED.isoformat()
    history,_,audit=merged(bundle)
    assert all(r['source']=='nba_log' for r in history['one']) and audit['box_regular_season_unverified']==2


def test_rookie_can_have_workload_fact_without_invented_distribution():
    market,bundle,model=fixture();bundle['logs']=[];add_box(bundle)
    snapshot=make_snapshot(market,bundle,model,CREATED)
    one=next(c for c in snapshot['candidates'] if c['player']=='One')
    assert one['historical_p90'] is None and one['experimental_p90'] is None
    assert one['ceiling_watch']['flag'] is None
    bundle['boxes'][0].update(fga=24)
    snapshot=make_snapshot(market,bundle,model,CREATED)
    one=next(c for c in snapshot['candidates'] if c['player']=='One')
    assert one['observed_workload_flags']==['large_last_game_workload']


def test_grade_distinguishes_mean_p90_and_watch_outcomes_and_counts_push():
    market,bundle,model=fixture();snapshot=make_snapshot(market,bundle,model,CREATED)
    game=BoxGame('g','Boston Celtics','Miami Heat',[BoxPlayer('One','one','Boston Celtics',True,True,20,30)],LOCK)
    grade=v2.grade(snapshot,[game]);stats=grade['all_players']
    assert stats['history20']['ghost']['n']==stats['history20']['paired_market']['n']==1
    assert stats['historical_p90']['hits']==0 and stats['historical_p90']['n']==1
    assert stats['experimental_p90']['hits']==0 and grade['by_status']['push']==1
    game.players[0].points=31
    stats=v2.grade(snapshot,[game])['all_players']
    assert stats['historical_p90']['hits']==stats['experimental_p90']['hits']==1
    game.players[0].played=False;game.players[0].points=None
    assert v2.grade(snapshot,[game])['all_players']['historical_p90']['n']==0


def test_report_timing_digests_model_versions_and_no_overwrite(tmp_path):
    from research.nba_points_forward import write_new
    market,bundle,model=fixture();snapshot=make_snapshot(market,bundle,model,CREATED)
    corrupted=deepcopy(snapshot);corrupted['candidates'][0]['historical_p90']=1000
    with pytest.raises(ValueError,match='digest'):v2.grade(corrupted,[])
    late=make_snapshot(market,bundle,model,LOCK+timedelta(minutes=1))
    assert late['mode']=='retrospective_reconstruction'
    assert v2.report([v2.grade(late,[])])['dates']==[]
    g=v2.grade(snapshot,[])
    with pytest.raises(ValueError,match='multiple snapshots'):v2.report([g,g])
    alternate={**g,'game_date':'2026-10-25','model_sha256':'different'}
    with pytest.raises(ValueError,match='different fitted'):v2.report([g,alternate])
    f=tmp_path/'snapshot.json';write_new(f,snapshot)
    with pytest.raises(FileExistsError):write_new(f,late)
    assert 'Experimental' in render(snapshot)


def test_late_model_and_availability_evidence_rejected():
    market,bundle,model=fixture();model['created_at']=CREATED.isoformat();model['sha256']=digest({k:v for k,v in model.items() if k!='sha256'})
    with pytest.raises(ValueError,match='model did not exist'):make_snapshot(market,bundle,model,CREATED)
    market,bundle,model=fixture();bundle['availability']={'one':{'captured_at':CREATED.isoformat(),'source_url':'https://example.com'}}
    with pytest.raises(ValueError,match='availability evidence'):make_snapshot(market,bundle,model,CREATED)


def test_timestamped_out_status_suppresses_watch_without_inventing_zero_points():
    market,bundle,model=fixture()
    bundle['availability']={'one':{'player':'One','team':'Boston Celtics','status':'Out',
                                  'captured_at':(CREATED-timedelta(minutes=5)).isoformat(),'source_url':'https://example.com/official-injury'}}
    snap=make_snapshot(market,bundle,model,CREATED)
    one=next(r for r in snap['candidates'] if r['player']=='One')
    assert one['ceiling_watch']['flag'] is None and one['ceiling_watch']['status']=='reported_out'
    assert one['predictions']['opportunity_points']>0 and 'Reported out' in render(snap)


def test_game_classification_uses_event_phase_not_calendar_date():
    payload={'events':[{'id':'e','date':'2026-10-24T01:00Z','season':{'type':1},'competitions':[{'competitors':[
        {'homeAway':'home','team':{'displayName':'Boston Celtics'}},{'homeAway':'away','team':{'displayName':'Miami Heat'}}]}]}]}
    proof=classifications_from_scoreboard(payload,CREATED)[0]
    assert proof['season_type']==1 and proof['game_date']=='2026-10-23'


def test_training_features_do_not_use_target_or_future_outcomes():
    from research.nba_ghost_model import frame_from_logs,FEATURES
    import numpy as np
    _,bundle,_=fixture();players=bundle['logs'];teams=[]
    for r in players:
        for team,opp in [(1,2),(2,1)]:teams.append({'game_id':r['game_id'],'game_date':r['game_date'],'team_id':team,'opponent_team_id':opp,'fga':90.})
    raw={'players':players,'teams':teams};frame=frame_from_logs(raw)
    modified=deepcopy(raw)
    for r in modified['players']:
        if r['game_date']>='2026-04-12':
            r.update(points=40.,fgm=16.,fga=30.,fg3m=4.,fg3a=12.,ftm=4.,fta=6.,minutes=40.)
    poisoned=frame_from_logs(modified)
    a=frame[frame.date=='2026-04-12'][FEATURES].to_numpy()
    b=poisoned[poisoned.date=='2026-04-12'][FEATURES].to_numpy()
    assert len(a)==1 and np.allclose(a,b,equal_nan=True)
