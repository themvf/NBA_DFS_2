import pytest
from model.nfl_dfs_variance import shrink_variance, walk_forward, interval_score, study


def samples():
    return [dict(model="baseline",sample_key=f"{season}:{week}:p{i}",position="WR",season=season,week=week,
        game_id=f"{season}:{week}",baseline=10,actual=10+(-1 if i%2 else 1)*(1+i%3),p10=0,p90=20,
        boom_threshold=25,boom_probability=.1)
        for season in (2023,2024,2025) for week in range(1,4) for i in range(101)]


def test_sparse_history_uses_prior_and_effective_count_is_recency_weighted():
    assert shrink_variance([2],9,12)["variance"] == 9
    r = shrink_variance([1,-1]*10,9,12)
    assert 2 < r["effective_n"] < 20 and 0 < r["weight"] < 1


def test_equal_means_can_have_different_player_variance():
    steady = shrink_variance([-1,1]*10,9,4)
    volatile = shrink_variance([-8,8]*10,9,4)
    assert steady["variance"] < volatile["variance"]


def test_future_or_same_week_outcomes_cannot_change_intervals():
    s = samples()
    a = walk_forward(s,12)
    b = walk_forward([{**r,"actual":999} if r["season"]==2025 and r["week"]==3 else r for r in s],12)
    assert [(r["p10"],r["p90"]) for r in a] == [(r["p10"],r["p90"]) for r in b]
    assert all(r["mean"] == 10 for r in a)


def test_2025_outcomes_cannot_select_shrinkage_strength():
    s=samples()
    a,_=study(s)
    b,_=study([{**r,"actual":999} if r["season"]==2025 else r for r in s])
    assert a["positions"]["WR"]["selected_strength"] == b["positions"]["WR"]["selected_strength"]
    assert not a["production_promotion"] and not a["shadow_activation"]


def test_interval_score_penalizes_both_misses_and_unhelpful_width():
    assert interval_score(10,5,15) < interval_score(10,-100,100)
    assert interval_score(30,5,15) > interval_score(10,5,15)
    with pytest.raises(ValueError): interval_score(0,10,1)
