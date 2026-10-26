import pytest
from research.nba_points_diagnostics import parse_summary,assemble

def summary():
    def block(team,aid,name,points,fg,three,ft):
        return {'team':{'displayName':team},'statistics':[{'labels':['MIN','FG','3PT','FT','PF','PTS'],'athletes':[{'athlete':{'id':aid,'displayName':name},'stats':['35',fg,three,ft,'6',str(points)]}]}]}
    return {'header':{'id':'e','competitions':[{'id':'e','date':'2026-10-24T23:00Z','status':{'type':{'completed':True}},'competitors':[{'homeAway':side,'team':{'displayName':team},'score':str(score)} for side,team,score in [('home','Boston Celtics',5),('away','Miami Heat',2)]]}]},
            'boxscore':{'players':[block('Boston Celtics','1','One',5,'2-5','1-3','0-0'),block('Miami Heat','2','Two',2,'1-4','0-2','0-0')]},
            'plays':[{'id':str(i),'scoringPlay':True,'scoreValue':value,'period':{'number':period},'participants':[{'athlete':{'id':aid}}]} for i,(aid,value,period) in enumerate([('1',3,1),('1',2,5),('2',2,2)])]}

def test_regulation_ot_scoring_mix_and_fouls_reconcile():
    game=parse_summary(summary(),'e');p=game['players'][0]
    assert game['period_data_reconciled'] and p['regulation_points']==3 and p['overtime_points']==2
    assert p['scoring_components']=={'two_point':2,'three_point':3,'free_throw':0} and p['fouls']==6

@pytest.mark.parametrize('mutation',['missing','duplicate','partial'])
def test_bad_play_feed_never_implies_zero_overtime(mutation):
    raw=summary()
    if mutation=='missing':raw.pop('plays')
    elif mutation=='duplicate':raw['plays'].append(raw['plays'][0])
    else:raw['plays'].pop()
    game=parse_summary(raw,'e')
    assert not game['period_data_reconciled'] and all(p['overtime_points'] is None for p in game['players'])

@pytest.mark.parametrize('mutation',['event','team_score','shot_score','duplicate_player'])
def test_bad_source_identity_and_accounting_rejected(mutation):
    raw=summary()
    if mutation=='event':raw['header']['id']='other'
    if mutation=='team_score':raw['header']['competitions'][0]['competitors'][0]['score']='99'
    if mutation=='shot_score':raw['boxscore']['players'][0]['statistics'][0]['athletes'][0]['stats'][-1]='6'
    if mutation=='duplicate_player':raw['boxscore']['players'][0]['statistics'][0]['athletes']*=2
    with pytest.raises(ValueError):parse_summary(raw,'e')

def test_postgame_decomposition_uses_frozen_rates_and_sums_to_error():
    c={'event_id':'odds','player_key':'one','player':'One','home_team':'Boston Celtics','away_team':'Miami Heat','predictions':{'opportunity_points':4},'opportunity_conversion_rates':{'two':.5,'three':.3,'free':.8},'opportunity_forecasts':{'minutes':30,'fga':4,'fg3a':2,'fta':0}}
    s={'game_date':'2026-10-24','sha256':'digest','candidates':[c]};g={'rows':[{'event_id':'odds','player_key':'one','actual_points':5}]}
    result=assemble(s,g,{'e':summary()})['rows'][0]
    p=result['postgame_decomposition']
    assert p['attempt_mix_difference']+p['conversion_residual']==pytest.approx(1)
    assert result['opportunity_differences']['minutes']==5 and result['foul_limit_reached']
    c.pop('opportunity_conversion_rates');assert assemble(s,g,{'e':summary()})['rows'][0]['postgame_decomposition'] is None
    g['rows'][0]['actual_points']=6
    with pytest.raises(ValueError,match='disagree'):assemble(s,g,{'e':summary()})
