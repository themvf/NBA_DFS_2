from copy import deepcopy
import pytest
from research.nba_trio_math_runner import signed
from research.nba_trio_pool_report import summarize


def audit(day, gap, provisional):
    return signed({'mode':'prospective','game_date':day,'record_sha256':day,'provisional':provisional,
        'pool_gap':gap,'full_pool':{'verified':12,'eligible':13,'unresolved':{'missing':1}},
        'portfolios':{'market_baseline':{'best_entry':80,'selection_gap':3,'total_gap':gap+3,'fractional_top_three':2.5}}})


def test_complete_and_provisional_benchmarks_never_pool():
    result = summarize([audit('2026-11-04',0,False),audit('2026-11-05',8,True)],pending=1)
    assert result['complete']['mean_pool_gap'] == 0
    assert result['complete']['positive_pool_gap_dates'] == 0
    assert result['provisional']['mean_pool_gap'] == 8
    assert result['methods']['market_baseline']['complete_dates'] == 1
    assert result['methods']['market_baseline']['mean_fractional_top_three'] == 2.5
    assert result['pending_latest_audits'] == 1


def test_duplicate_originals_reconstructions_and_empty_counts():
    a=audit('2026-11-04',5,True)
    with pytest.raises(ValueError,match='duplicate'): summarize([a,a])
    b=deepcopy(a);b['mode']='retrospective_reconstruction';b=signed({k:v for k,v in b.items() if k!='sha256'})
    with pytest.raises(ValueError,match='reconstruction'): summarize([b])
    assert summarize([])['complete']['mean_pool_gap'] is None
