from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from research.nfl_pickem_matchup import eligible_quote, fit_offset, no_vig, paired_probabilities, require_model_before_cutoff


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
