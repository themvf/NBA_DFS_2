from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from research.nfl_pickem_matchup import eligible_quote, fit_offset, no_vig, paired_probabilities, require_model_before_cutoff
from research.nfl_pickem_matchup import select_model_features


def test_market_and_zero_residual_preserve_exact_baseline_and_tie():
    assert no_vig(-110, -110) == .5
    assert no_vig(None, -110) is None
    baseline, candidate = paired_probabilities(.524, .003, 0)
    assert candidate["homeConditional"] == .524
    assert baseline == {k: candidate[k] for k in baseline}
    _, shifted = paired_probabilities(.524, .003, -.15)
    assert shifted["tie"] == candidate["tie"]
    assert sum(shifted[k] for k in ("home", "away", "tie")) == pytest.approx(1)


def test_fixed_ridge_offset_no_intercept_and_no_data_means_no_fit():
    x = np.zeros((120, 6))
    coefficients = fit_offset(x, np.array([0, 1]*60), np.full(120, .5))
    assert all(v == 0 for v in coefficients.values())
    with pytest.raises(ValueError):
        fit_offset(x[:10], np.zeros(10), np.full(10, .5))


def test_independently_fitted_family_models_handle_missing_other_family():
    keys = ("own_pressure", "opp_pressure")
    assert set(fit_offset(np.zeros((120, 2)), np.array([0, 1]*60), np.full(120, .5), keys=keys)) == set(keys)
    side = {"pressure_games": 2, "pressure_pct": 25, "pressure_coverage_complete": True,
            "contact_coverage_complete": False, "rb_carries": 0}
    matchup = {"home": "TB", "away": "MIN", "teams": {t: {"offense": dict(side), "defense": dict(side)} for t in ("TB", "MIN")}}
    combined, pressure = {"artifactId": "combined"}, {"artifactId": "pressure"}
    selected, features = select_model_features(matchup, combined, {"pressure": pressure})
    assert selected is pressure and set(features) == set(keys)
    assert select_model_features(matchup, combined, {}) == (combined, None)


@pytest.mark.parametrize("contact_available,preferred", [(True, "combined"), (False, "pressure")])
def test_freeze_keeps_all_registered_arms_and_selects_one_default(monkeypatch, contact_available, preferred):
    import json
    import research.nfl_pickem_matchup as module
    now = datetime.now(timezone.utc)
    model = json.loads(module.MODEL_PATH.read_text())
    side = {"pressure_games": 2, "pressure_pct": 25, "pressure_coverage_complete": True,
            "contact_coverage_complete": contact_available, "rb_carries": 30,
            "rb_before_contact_per_carry": 1.5, "rb_after_contact_per_carry": 2.5}
    matchup = {"game_id": "fixture", "home": "TB", "away": "MIN", "manifest_hash": "fixture",
               "kickoff": (now+timedelta(hours=2)).isoformat(),
               "teams": {t: {"offense": dict(side), "defense": dict(side)} for t in ("TB", "MIN")}}
    monkeypatch.setattr(module, "registered_model", lambda *args: None)
    monkeypatch.setattr(module, "load_matchups", lambda *args: {"fixture": matchup})
    monkeypatch.setattr(module, "current_quotes", lambda *args: [{"game_id": "fixture", "home_ml": -110, "away_ml": -110, "captured_at": now}])
    forecasts = module.freeze_forecasts(None, model, 2026, 4)["forecasts"]
    assert {f["selectedFamily"] for f in forecasts} == {"combined", "pressure", "contact"}
    selected = [f for f in forecasts if f["selectedForDefault"]]
    assert len(selected) == 1 and selected[0]["selectedFamily"] == preferred and selected[0]["covered"]
    if not contact_available:
        combined = next(f for f in forecasts if f["selectedFamily"] == "combined")
        assert combined["covered"] is False and combined["candidate"] == {**combined["baseline"], "homeConditional": .5}


def test_quote_freshness_and_model_temporal_order():
    cutoff = datetime(2026, 9, 27, 15, tzinfo=timezone.utc)
    kickoff = cutoff+timedelta(hours=2)
    assert eligible_quote(cutoff-timedelta(hours=2), cutoff, kickoff)
    assert not eligible_quote(cutoff-timedelta(hours=2,seconds=1), cutoff, kickoff)
    assert not eligible_quote(cutoff+timedelta(seconds=1), cutoff, kickoff)
    assert not eligible_quote(cutoff, cutoff, cutoff)
    valid = {"fittedAt":(cutoff-timedelta(hours=1)).isoformat(),"trainedThrough":"2026-01-05T00:00:00+00:00"}
    require_model_before_cutoff(valid, cutoff)
    with pytest.raises(ValueError):
        require_model_before_cutoff({**valid,"fittedAt":(cutoff+timedelta(seconds=1)).isoformat()}, cutoff)
    with pytest.raises(ValueError):
        require_model_before_cutoff({**valid,"trainedThrough":cutoff.isoformat()}, cutoff)
