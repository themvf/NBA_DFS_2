from research.nba_points_daily import chart_data

def test_trend_keeps_only_prospective_same_version_same_fitted_model():
    snapshot={'candidates':[],'models':[],'game_date':'2026-10-24','version':'v2','mode':'prospective','created_at':'frozen','lock_at':'lock','sha256':'snapshot'}
    grade={'rows':[],'game_date':'2026-10-24','version':'v2','snapshot_mode':'prospective','model_sha256':'fit1','all_players':{},'market_line_at_least20':{},'by_status':{},'leaderboards':{}}
    history=[{**grade,'game_date':'2026-10-23'},{**grade,'game_date':'2026-10-22','version':'v1'},
             {**grade,'game_date':'2026-10-21','model_sha256':'fit2'},{**grade,'game_date':'2026-10-20','snapshot_mode':'retrospective_reconstruction'}]
    data=chart_data(snapshot,grade,{'rows':[],'coverage':{}},history)
    assert [r['date'] for r in data['trend']]==['2026-10-23','2026-10-24']
    assert all(r['n']==0 and r['hits']==0 for r in data['ceilings'])
    assert data['watch']['True']['n']==0
