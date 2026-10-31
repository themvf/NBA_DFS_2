from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest

from tests.test_nba_points_v3 import snapshot_fixture, model_fixture
from research.nba_points_forward import digest
from research.nba_points_v3 import forecast, grade as grade_v3
from research.nba_points_experiments import (protocol, freeze, grade, report, signed,
    bucket, conditional_tail, save_registration, cluster_interval)


def fixture():
    source,raw=snapshot_fixture()
    for c in source['candidates']:
        c.update(historical_p90=13,experimental_p90=25)
    signed(source)
    model=model_fixture();model['calibration_end']='2026-01-31';signed(model)
    spec=protocol(model,'2026-10-28T10:30:00+00:00')
    future=forecast(source,model,raw,'2026-10-28T12:00:00+00:00',draws=100)
    experiment=freeze(future,source,spec,'2026-10-28T12:01:00+00:00')
    diagnostics={'snapshot_sha256':source['sha256'],'game_date':source['game_date'],'rows':[]}
    for i,r in enumerate(future['rows']):
        diagnostics['rows'].append({'event_id':r['event_id'],'player_key':r['player_key'],
            'status':'verified','actual':{'points':20+i*5,'fga':20,'minutes':38}})
    vg=grade_v3(future,diagnostics)
    return source,future,spec,experiment,diagnostics,vg


def test_rules_boundaries_and_frozen_selections():
    source,future,spec,e,_,_=fixture()
    assert [bucket(d) for d in [-4,-2,-1.99,1.99,2,4]]==[
        'at_least_4_below','2_to_4_below','within_2','within_2','2_to_4_above','at_least_4_above']
    assert e['watch_k']==4 and all(len(v)==4 for v in e['selections'].values())
    assert e['mode']=='prospective' and e['protocol_sha256']==spec['sha256']
    assert e['rows'][0]['fga10']==12 and e['rows'][0]['minutes10']==48
    assert future['sha256']==digest({k:v for k,v in future.items() if k!='sha256'})
    assert e['rows'][0]['conditional_tail']['fallback_to_scoring_band']


def test_cannot_enroll_before_registration_or_after_tip():
    source,future,spec,e,_,_=fixture()
    with pytest.raises(ValueError,match='prospectively'):
        freeze(future,source,spec,'2026-10-29T12:00:00+00:00')
    spec['created_at']='2026-10-28T12:00:30+00:00';signed(spec)
    with pytest.raises(ValueError,match='prospectively'):
        freeze(future,source,spec,'2026-10-28T12:01:00+00:00')
    r=freeze(future,source,spec,'2026-10-28T12:01:00+00:00',True)
    assert r['mode']=='retrospective_reconstruction'


def test_wrong_model_source_or_mutated_samples_fail_closed():
    source,future,spec,e,_,_=fixture()
    future['model_sha256']='other';signed(future)
    with pytest.raises(ValueError,match='model differs'):
        freeze(future,source,spec,'2026-10-28T12:01:00+00:00')
    source,future,spec,e,_,_=fixture()
    future['rows'][0]['distribution']['thresholds']['30']['probability']=.55;signed(future)
    with pytest.raises(ValueError,match='probability/sample'):
        freeze(future,source,spec,'2026-10-28T12:01:00+00:00')


def test_dnp_not_zero_and_equal_rank_paired_coverage():
    source,future,spec,e,diag,vg=fixture()
    diag['rows'][0]['status']='dnp';vg=grade_v3(future,diag)
    result=grade(e,vg,diag,spec,'2026-10-29T12:00:00+00:00')
    assert len(result['rows'])==3 and len(result['final_dnp'])==1 and result['complete']
    assert result['watchlists']['opportunity']['actual_points_total'] is None
    assert all(r['n']==3 for r in result['watch_rank_paired'].values())
    assert result['ceilings_primary']['n']==3
    for v in result['ceilings_primary']['probability_audit'].values():
        for t in v.values():assert sum(b['n'] for b in t['bins'])==t['n']


def test_missing_or_inconsistent_diagnostics_and_grade_rejected():
    _,_,spec,e,diag,vg=fixture()
    diag['rows'][0]['actual']['points']=999
    with pytest.raises(ValueError,match='points differ'):
        grade(e,vg,diag,spec,'2026-10-29T12:00:00+00:00')
    _,_,spec,e,diag,vg=fixture()
    diag['rows'].pop();r=grade(e,vg,diag,spec,'2026-10-29T12:00:00+00:00')
    assert not r['complete'] and len(r['unresolved'])==1
    diag['snapshot_sha256']='wrong'
    with pytest.raises(ValueError,match='source mismatch'):
        grade(e,vg,diag,spec,'2026-10-29T12:00:00+00:00')


def test_report_deduplicates_original_and_separates_reconstructions():
    _,_,spec,e,diag,vg=fixture();g=grade(e,vg,diag,spec,'2026-10-29T12:00:00+00:00')
    old=deepcopy(g);old['graded_at']='2026-10-29T11:00:00+00:00';signed(old)
    retro=deepcopy(g);retro['mode']='retrospective_reconstruction';signed(retro)
    out=report([old,g,retro],spec)
    assert out['completed_slate_dates']==1 and out['resolved_players']==4
    assert out['excluded_grades']['retrospective']==1 and not out['review_checkpoint_reached']
    assert out['primary_tail_comparison']['n']==4
    assert out['primary_tail_comparison']['uncertainty']['interval_95'] is None
    other=deepcopy(g);other['protocol_sha256']='different';signed(other)
    assert report([other],spec)['status']=='pending_no_prospective_grades'
    duplicate=deepcopy(g);duplicate['experiment_sha256']='another';signed(duplicate)
    with pytest.raises(ValueError,match='multiple original'):
        report([g,duplicate],spec)


def test_original_index_is_exclusive_and_reproducible(tmp_path):
    source,future,spec,_,_,_=fixture();record=tmp_path/'record';record.mkdir()
    a=save_registration(future,source,spec,record,tmp_path,'2026-10-28T12:01:00+00:00')
    content=(record/'experiment.json').read_bytes()
    b=save_registration(future,source,spec,record,tmp_path,'2026-10-28T12:02:00+00:00')
    assert a['status']=='registered' and b['status']=='preserved_original'
    assert (record/'experiment.json').read_bytes()==content
    groups={str(i):[1,2,3] for i in range(5)}
    assert cluster_interval(groups,spec)==cluster_interval(groups,spec)


def test_no_alerts_empty_pool_is_pending_not_evidence():
    source,future,spec,e,diag,vg=fixture()
    spec['watchlist']['minimum_fga_increase']=100;spec['watchlist']['minimum_minutes_increase']=100;signed(spec)
    e=freeze(future,source,spec,'2026-10-28T12:01:00+00:00')
    assert e['watch_k']==0 and all(not x for x in e['selections'].values())
    g=grade(e,vg,diag,spec,'2026-10-29T12:00:00+00:00');r=report([g],spec)
    assert r['resolved_alerts']==0 and r['watch_rank_paired']['market_line']['hit_rate_difference'] is None
    assert report([],spec)['resolved_players']==0


def test_recording_automatically_attaches_tests_without_mutating_v3(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import research.nba_points_v3 as v3
    import research.nba_points_experiments as experiments
    source,raw=snapshot_fixture()
    for c in source['candidates']:c.update(historical_p90=13,experimental_p90=25)
    signed(source)
    m=model_fixture();m['calibration_end']='2026-01-31';signed(m)
    spec=protocol(m,'2026-10-28T10:30:00+00:00')
    model_path=tmp_path/'model.json';model_path.write_text(json.dumps(m))
    protocol_path=tmp_path/'protocol.json';protocol_path.write_text(json.dumps(spec))
    monkeypatch.setattr(experiments,'DEFAULT_PROTOCOL',protocol_path)
    class FixedTime(datetime):
        @classmethod
        def now(cls,tz=None):return datetime(2026,10,28,12,0,tzinfo=timezone.utc)
    monkeypatch.setattr(v3,'datetime',FixedTime)
    monkeypatch.setattr('research.nba_ghost_runner.fetch_scoreboard',lambda d:({'events':[{'date':source['lock_at']}]},datetime(2026,10,28,11,tzinfo=timezone.utc)))
    def capture(cmd,**kwargs):
        if '--availability-output' in cmd:
            from pathlib import Path
            Path(cmd[cmd.index('--availability-output')+1]).write_text('{}')
        if '--output' in cmd:
            from pathlib import Path
            Path(cmd[cmd.index('--output')+1]).write_text(json.dumps(source))
        return SimpleNamespace(returncode=0,stdout='ok',stderr='')
    monkeypatch.setattr(v3.subprocess,'run',capture)
    out=tmp_path/'record';out.mkdir()
    args=SimpleNamespace(model=model_path,date=datetime(2026,10,28).date(),output_dir=out,
        injury_archive=tmp_path/'injuries',draws=100,seed=1)
    result=v3.record_day(args,raw,'inputsha')
    assert result['status']=='recorded' and result['experiments']['status']=='registered'
    f=json.loads((out/'forecast.json').read_text());e=json.loads((out/'experiment.json').read_text())
    assert result['forecast_sha256']==f['sha256']==e['forecast_sha256']
    assert len(list((tmp_path/'experiments-index').glob('*.json')))==1
