import pytest
from model.nfl_role_dispersion import estimate_dispersion, interval_diagnostic


def sample(volatile):
    rows = []
    for week in range(1, 13):
        boxes = []
        for team in ('A', 'B'):
            counts = (95, 5) if volatile and week % 2 else (5, 95) if volatile else (50, 50)
            boxes.extend({'identity':team+str(j), 'team':team, 'targets':n} for j,n in enumerate(counts))
        rows.append({'game':{'season':2025, 'away':'A', 'home':'B'}, 'boxes':boxes})
    return rows


def test_empirical_dispersion_responds_to_role_variability():
    stable = estimate_dispersion(sample(False), 'targets', minimum_groups=2)
    volatile = estimate_dispersion(sample(True), 'targets', minimum_groups=2)
    assert not stable['fallback'] and not volatile['fallback']
    assert volatile['concentration'] < stable['concentration']
    assert stable['game_team_observations'] == 24
    assert estimate_dispersion(sample(True), 'targets')['fallback']


def test_interval_score_penalizes_misses_not_just_width():
    assert interval_diagnostic(5, 0, 10)['covered']
    assert interval_diagnostic(20, 0, 10)['interval_score'] == 110
    with pytest.raises(ValueError):
        interval_diagnostic(5, 10, 0)
