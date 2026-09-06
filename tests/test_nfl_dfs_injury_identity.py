import pytest
from model.nfl_dfs_injury_identity import reconcile,audit

P={'id':1,'canonical_name':'Mike Woods','normalized_name':'mikewoods','team_abbrev':'DEN','position':'WR','fantasypros_player_id':None,'yahoo_id':'34158'}
R={'player_id':23893,'yahoo_id':'34158','name':'Michael Woods II','team_id':'DEN','position_id':'WR'}

def test_external_identity_and_team_alias():
    assert reconcile(R,[P])['method']=='yahoo_id'
    for raw_team,canonical in [('WAS','WSH'),('JAC','JAX'),('LA','LAR'),('AZ','ARI')]:
        p={**P,'canonical_name':'Quentin Moore','team_abbrev':canonical,'yahoo_id':None}
        row={**R,'name':'Quentin Moore','team_id':raw_team,'yahoo_id':None}
        assert reconcile(row,[p])['category']=='matched'

def test_identity_guards():
    for patch,reason in [({'team_id':'FA'},'provider_nonteam'),({'position_id':'TE'},'position_conflict'),({'team_id':'BUF'},'team_conflict'),({'position_id':'CB'},'outside_skill_pool')]:
        assert reconcile({**R,**patch},[P])['category']==reason
    assert reconcile(R,[])['category']=='missing_identity'
    assert reconcile(R,[P,{**P,'id':2}])['category']=='ambiguous'
    assert reconcile(R,[{**P,'fantasypros_player_id':999}])['category']=='identifier_conflict'
    assert reconcile({**R,'yahoo_id':None},[P])['category']=='missing_identity' # no fuzzy-name promotion

def test_duplicate_and_accounting():
    with pytest.raises(ValueError): audit([R,R],[P])
    with pytest.raises(ValueError): audit([R,{**R,'player_id':4}],[P])
    rows=[R,{**R,'player_id':5,'team_id':'FA'},{**R,'player_id':6,'position_id':'CB'}]
    report=audit(rows,[P]);assert sum(report['counts'].values())==len(rows)
    assert report==audit(rows,list(reversed([P])))

def test_frozen_seventeen_gaps():
    import json
    from pathlib import Path
    fixture=json.loads((Path(__file__).parent/'fixtures/nfl_injury_week1_identity.json').read_text())
    report=audit(fixture['rows'],fixture['players'])
    assert report['counts']==fixture['expected_counts']
    remaining={r['name']:r['category'] for r in report['decisions'] if r['category'] in {'position_conflict','missing_identity'}}
    assert remaining=={'Robbie Ouzts':'position_conflict','Seth Williams':'missing_identity'}
