import pytest

from research.nfl_matchup_report import no_vig, play_summary
from ingest.nfl_pfr_nflverse import NotPublished, build_supplement, KINDS, IDENTITY
import pandas as pd


def test_market_removes_vig_and_requires_two_sides():
    assert no_vig(-101, -117) == pytest.approx(.4823920412)
    assert no_vig(None, -117) is None
    assert no_vig(0, -117) is None


def test_drives_not_multiplied_and_missing_epa_not_zero():
    base = dict(game_id='g',posteam='TB',drive=1,drive_result='Touchdown',
                play_type='pass',had_sack=False,turnover_type=None,epa=None,success=None)
    rows = [base, dict(base,epa=1,success=True),dict(base,drive=2,turnover_type='interception',epa=-5,success=False)]
    result = play_summary(rows)
    assert result['all']['plays'] == 3
    assert result['all']['epa'] == -2
    assert result['all']['epa_excluding_turnover_plays'] == 1
    assert result['all']['success'] == .5
    assert result['drive_results']['Touchdown'] == 2


def test_unpublished_game_has_distinct_failure_from_bad_identity():
    game = dict(game_id='2026_01_DAL_PHI',pfr='202609130phi',season=2026,week=1,home_team='PHI',away_team='DAL')
    frames = {k: pd.DataFrame(columns=sorted(IDENTITY)) for k in KINDS}
    with pytest.raises(NotPublished):
        build_supplement(game,frames,{},'2026-09-27T00:00:00Z')
