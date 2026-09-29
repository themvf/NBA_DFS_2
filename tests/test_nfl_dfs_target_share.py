from datetime import datetime, timedelta, timezone
import pytest
from model.nfl_dfs_target_share import allocate, forecast, pregame_availability, replay


def fixture():
    teams=[];history=[]
    for week in range(1,9):
        teams.append(dict(season=2025,week=week,team='A',attempts=40.,targets=36.))
        for pid,targets in [('one',8.),('two',6.),('three',4.)]:
            history.append(dict(season=2025,week=week,team='A',identity=pid,name=pid,position='WR',targets=targets,fpts=targets*2,game_id=str(week)))
    return history,teams


def test_budget_conservation_and_reserve():
    r=allocate({'a':.3,'b':.2},30,['a'])
    assert r['players']['a']['targets']==0
    assert r['players']['b']['targets']==pytest.approx(10.5)
    assert r['unallocated_targets']==pytest.approx(19.5)
    assert sum(p['targets'] for p in r['players'].values())+r['unallocated_targets']==pytest.approx(30)
    assert allocate({'a':.8,'b':.8},40)['unallocated_targets']==pytest.approx(0)
    assert allocate({'a':1},30,['a'])['unallocated_targets']==30
    with pytest.raises(ValueError): allocate({'a':float('nan')},30)


def test_forecast_cutoff_and_team_isolation():
    history,teams=fixture()
    expected=forecast(history,teams,'A',(2025,8))
    history[-1]['fpts']=9999;teams[-1]['attempts']=900
    history.append({**history[0],'team':'B','fpts':9999})
    assert forecast(history,teams,'A',(2025,8))==expected
    assert expected['target_budget']<=expected['attempts']
    assert forecast(history,teams,'A',(2025,3)) is None
    changed=forecast(history,teams,'A',(2025,8),['one'])
    assert changed['players']['one']['targets']==0
    assert changed['players']['two']['targets']>expected['players']['two']['targets']


def test_pregame_source_checks():
    now=datetime(2026,9,6,tzinfo=timezone.utc); kickoff=now+timedelta(days=1)
    member=dict(team='A',position='WR',fetched_at=now,sleeper=dict(team='A',position='WR',injury_status='Out'))
    assert pregame_availability(member,'A',now,kickoff)['out']
    for captured in [now+timedelta(seconds=1),now-timedelta(days=4),now.replace(tzinfo=None)]:
        assert pregame_availability({**member,'fetched_at':captured},'A',now,kickoff) is None
    assert pregame_availability(member,'B',now,kickoff) is None
    assert pregame_availability(member,'A',kickoff,kickoff) is None
    assert not pregame_availability({**member,'sleeper':dict(team='A',position='WR',injury_status='Questionable')},'A',now,kickoff)['out']


def test_replay_ranges_do_not_see_same_week_actuals():
    history,teams=fixture()
    # Enough independent player errors to warm up the positional distribution.
    many=[]
    for i in range(40):
        many.extend({**r,'identity':str(i)+r['identity']} for r in history)
    before,_=replay(many,teams)
    for r in many:
        if r['week']==8: r['fpts']+=10000
    after,_=replay(many,teams)
    assert before
    assert [r['candidate'] for r in before]==[r['candidate'] for r in after]
    assert all(r['candidate']['p10']<=r['candidate']['p50']<=r['candidate']['p90'] for r in before)


def test_team_aliases_preserve_pregame_identity():
    now=datetime(2026,9,6,tzinfo=timezone.utc)
    m=dict(team='WSH',position='WR',fetched_at=now,sleeper=dict(team='WAS',position='WR',status='Out'))
    assert pregame_availability(m,'WSH',now,now+timedelta(days=1))['out']


# --- Database-backed weekly refresh (run v2) -------------------------------

from ingest.nfl_dfs_target_share import REPORT_VERSION, build_report, history_from_rows

SCORING_KEYS=('attempts','carries','passing_yards','passing_tds','passing_interceptions','rushing_yards','rushing_tds',
    'receiving_yards','receiving_tds','receptions','passing_2pt_conversions','rushing_2pt_conversions',
    'receiving_2pt_conversions','special_teams_tds','fumble_recovery_tds','fumbles_lost_total')


def _stored_rows():
    fetched=datetime(2026,9,28,tzinfo=timezone.utc)
    rows=[]
    for season,weeks in [(2025,range(14,19)),(2026,range(1,4))]:
        for week in weeks:
            game=f'{season}_{week:02d}_A_B'
            rows.append(dict(position='DST',season=season,week=week,team='A',fetched_at=fetched,
                             source_row={'raw_team_stats':{'game_id':game,'targets':36,'attempts':40}}))
            for pid,targets in [('one',8),('two',6),('three',4)]:
                stat={k:0 for k in SCORING_KEYS}
                stat.update(player_id=pid,player_display_name=pid,position='WR',team='A',game_id=game,targets=targets,receptions=targets//2,receiving_yards=targets*10)
                rows.append(dict(position='WR',season=season,week=week,team='A',fetched_at=fetched,source_row=stat))
    return rows


def test_db_history_is_strictly_before_the_target_week():
    history,teams,sources=history_from_rows(_stored_rows(),(2026,3))
    assert max((t['season'],t['week']) for t in teams)==(2026,2)
    assert all((r['season'],r['week'])<(2026,3) for r in history)
    assert {s['season']:s['latest_week'] for s in sources}=={2025:18,2026:2}
    # Team totals come from the team-week row, not a sum of whichever players are stored.
    assert teams[0]['targets']==36 and teams[0]['attempts']==40


def test_db_history_rejects_bad_team_rows_and_skips_missing_stats():
    rows=_stored_rows()
    missing=[dict(r,source_row={**r['source_row'],'receiving_tds':None}) if r['position']=='WR' and r['source_row']['player_id']=='three' else r for r in rows]
    history,_,_=history_from_rows(missing,(2026,4))
    assert 'three' not in {r['identity'] for r in history}  # missing stays missing, never scored as zero
    bad=[dict(r,source_row={'raw_team_stats':{**r['source_row']['raw_team_stats'],'targets':99}}) if r['position']=='DST' else r for r in rows]
    with pytest.raises(ValueError):
        history_from_rows(bad,(2026,4))
    with pytest.raises(ValueError):
        history_from_rows(rows+[rows[0]],(2026,4))


def test_weekly_report_names_its_run_version_and_cutoff():
    history,teams,sources=history_from_rows(_stored_rows(),(2026,4))
    now=datetime(2026,9,30,tzinfo=timezone.utc)
    games=[dict(home_team='A',away_team='B',kickoff=now+timedelta(days=4))]
    roster=[dict(identity=pid,name=pid,team='A',position='WR',fetched_at=now,sleeper=dict(team='A',position='WR',status='Active')) for pid in ['one','two','three']]
    report=build_report(history,teams,sources,[],games,roster,2026,4,now)
    assert report['report_version']==REPORT_VERSION
    assert report['history_cutoff_exclusive']==[2026,4] and report['history_through']==[2026,3]
    assert len(report['snapshot_digest'])==64
    with pytest.raises(ValueError):  # a cutoff the history already reaches is refused
        build_report(history,teams,sources,[],games,roster,2026,3,now)
