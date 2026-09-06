import numpy as np
import pytest
from model.nfl_dfs_workload_ranges import distribution, probability, salary_probabilities, replay, calibration


def test_salary_thresholds_are_inclusive_and_monotonic():
    draws=np.array([0.,10.,15.,20.])
    assert salary_probabilities(draws,5000)=={'2':.75,'3':.5,'4':.25}
    assert probability(draws,15)==.5
    for value in [0,-1,float('nan')]:
        with pytest.raises(ValueError):salary_probabilities(draws,value)


def test_distribution_requires_evidence():
    with pytest.raises(ValueError):distribution(10,[0]*99)
    with pytest.raises(ValueError):distribution(10,[float('nan')]*100)


def test_same_week_outcomes_never_change_forecasts():
    rows=[{'season':2023,'week':1,'identity':str(i),'game_id':str(i),'targets_baseline':8,'actual':float(i%20),'candidate':{'mean':10}} for i in range(100)]
    rows += [{'season':2023,'week':2,'identity':'a','game_id':'a','targets_baseline':8,'actual':10,'candidate':{'mean':12}}, {'season':2023,'week':2,'identity':'b','game_id':'b','targets_baseline':8,'actual':20,'candidate':{'mean':12}}]
    first,_=replay(rows)
    changed=[{**r,'actual':1000} if r['week']==2 else r for r in rows]
    second,_=replay(list(reversed(changed)))
    assert {r['identity']:r['probabilities'] for r in first}=={r['identity']:r['probabilities'] for r in second}
    assert first[0]['workload']['p10']<=first[0]['workload']['p50']<=first[0]['workload']['p90']
    assert sum(b['n'] for b in calibration(first,15)['bins'])==2
