import pandas as pd
import pytest

from model.nfl_identity_registry import external_id, registry, valid_gsis, zero_result
from ingest.nfl_identity_registry import game_coverage

GSIS='00-0037292'


def test_namespaces_and_conflicts_never_choose_first():
    claims=[dict(namespace='draftkings.player_id',external_id='123',gsis_id=GSIS),
            dict(namespace='draftkings.player_dk_id',external_id='123',gsis_id='00-0037293')]
    assert registry(claims)[('draftkings.player_id','123')]['gsis_id']==GSIS
    claims.append(dict(claims[0],gsis_id='00-0037293'))
    assert registry(claims)[('draftkings.player_id','123')]['status']=='conflict'
    assert registry(claims)[('draftkings.player_id','123')]['gsis_id'] is None


def test_id_normalization_does_not_treat_nan_as_an_id():
    assert external_id(123.0)=='123'
    assert external_id(float('nan')) is None
    assert external_id('0') is None
    assert valid_gsis('draftable-123') is None
    assert valid_gsis(GSIS)==GSIS


def coverage():
    return dict(complete=True,participant_ids=[],event_player_ids=[])


def test_absent_row_or_inactive_status_alone_is_not_zero():
    assert zero_result(GSIS,None,roster_status='INA')['actual'] is None
    assert zero_result(GSIS,dict(coverage(),complete=False))['actual'] is None
    assert zero_result(None,coverage())['actual'] is None
    assert zero_result(GSIS,coverage(),has_stat_row=True)['actual'] is None


def test_complete_coverage_distinguishes_nonparticipant_from_zero_event_participant():
    result=zero_result(GSIS,coverage())
    assert result['actual']==0 and result['status']=='recorded_nonparticipant'
    assert not result['official_inactive']
    assert zero_result(GSIS,dict(coverage(),participant_ids=[GSIS]))['status']=='recorded_participant_no_events'
    assert zero_result(GSIS,dict(coverage(),participant_ids=[GSIS]),roster_status='INA')['status']=='source_conflict'
    assert zero_result(GSIS,dict(coverage(),event_player_ids=[GSIS]))['actual'] is None


def frames():
    offense=[f'00-{i:07d}' for i in range(100,111)]
    defense=[f'00-{i:07d}' for i in range(200,211)]
    plays=pd.DataFrame([
        dict(game_id='g',play_id=1,play_deleted=0,play_type='run',desc='Run',rusher_player_id=offense[0],total_home_score=0,total_away_score=0),
        dict(game_id='g',play_id=2,play_deleted=0,play_type='no_play',desc='Target erased by holding',receiver_player_id=GSIS,total_home_score=0,total_away_score=0),
        dict(game_id='g',play_id=3,play_deleted=0,play_type=None,desc='END GAME',total_home_score=0,total_away_score=0)])
    part=pd.DataFrame([dict(nflverse_game_id='g',play_id=1,players_on_play=';'.join(offense+defense),
        offense_players=';'.join(offense),defense_players=';'.join(defense),n_offense=11,n_defense=11)])
    return plays,part,[dict(game_id='g',home_score=0,away_score=0)]


def test_voided_target_is_retained_but_does_not_produce_points():
    result=game_coverage(*frames())['g']
    assert result['complete']
    assert GSIS in result['voided_event_player_ids']
    assert GSIS not in result['event_player_ids']
    assert zero_result(GSIS,result)['actual']==0


@pytest.mark.parametrize('problem',['missing_play','count_mismatch','unfinished','score_mismatch','scoring_no_play','conversion_no_play'])
def test_coverage_failures_block_zero(problem):
    plays,part,games=frames()
    if problem=='missing_play': part=part.iloc[:0]
    elif problem=='count_mismatch': part.loc[0,'n_offense']=12
    elif problem=='unfinished': plays=plays.iloc[:2]
    elif problem=='score_mismatch': games[0]['home_score']=7
    elif problem=='conversion_no_play': plays.loc[1,'two_point_conv_result']='success'
    else: plays.loc[1,'touchdown']=1
    result=game_coverage(plays,part,games)['g']
    assert not result['complete']
    assert zero_result(GSIS,result)['actual'] is None


def test_duplicate_plays_are_not_silently_deduplicated():
    plays,part,games=frames()
    with pytest.raises(ValueError,match='Duplicate'):
        game_coverage(pd.concat([plays,plays]),part,games)
