import json
from pathlib import Path

import pytest

from model.nfl_game_leaders import Settings, forecast, prepare
from model.nfl_game_leaders_calibration import PRIOR_WEIGHT, TEMPERATURES, production_settings, temper
from tests.test_nfl_game_leaders import fixture, request

FIT = Path(__file__).resolve().parents[1] / 'artifacts/nfl-game-leaders/calibration-v1/fit.json'


def test_production_constants_equal_the_frozen_study_fit():
    fit = json.loads(FIT.read_text())
    assert fit['variant_choice']['chosen'] == 'C2' and PRIOR_WEIGHT == 2.0
    assert {m: round(t, 2) for m, t in fit['temperature'].items()} == TEMPERATURES


def test_temper_flattens_and_renormalises():
    raw = {'a': .6, 'b': .3, 'c': .1}
    out = temper(raw, 1.3)
    assert sum(out.values()) == pytest.approx(1)
    assert out['a'] < .6 and out['c'] > .1
    assert temper(raw, 1.) == raw
    with pytest.raises(ValueError):
        Settings(temperatures={'rushing_yards': 0.})


def test_forecast_keeps_raw_shares_beside_tempered_ones():
    h, _ = prepare(fixture(), '2025-10-02T00:00:00Z')
    req = request()
    req['expected_prior_game_ids'] = {t: [r['game']['game_id'] for r in h] for t in ('A', 'B')}
    raw = forecast(h, req, Settings(draws=40))
    cal = forecast(h, req, production_settings(draws=40))
    for metric, family in cal['metrics'].items():
        assert family['temperature'] == TEMPERATURES[metric]
        assert sum(r['win_share'] for r in family['players']) == pytest.approx(1)
        raw_rows = {r['identity']: r['win_share'] for r in raw['metrics'][metric]['players']}
        for r in family['players']:
            assert r['win_share_raw'] == pytest.approx(raw_rows[r['identity']])
        top = family['players'][0]
        if top['win_share_raw'] > 1 / len(family['players']):
            assert top['win_share'] <= top['win_share_raw']
    assert 'temperature' not in raw['metrics']['rushing_yards']
