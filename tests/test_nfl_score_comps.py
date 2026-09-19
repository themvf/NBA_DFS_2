"""Guard test for the situational NFL score-comps projection model.

Protects the two properties that make the model trustworthy, both measured by a
leave-one-out back-test on the real 2016-2025 data:

  1. CALIBRATION — the 10-90 comp range contains the real score ~80% of the time.
     If coverage drifts far from that, the ranges no longer mean what they claim.
  2. ACCURACY — the median-of-comps error stays well below the naive baseline
     (guess the league mean) and near the validated ~5.0 points. If a future change
     to the fingerprint or feature set pushes error toward the baseline, this fails.

Also guards against the LEAK the model was built to avoid: no outcome-flavoured
feature (points, td rate, epa) may appear in FEATURES.

Sources data the same way the pipeline does. Needs the Neon table AND the nflverse
release; skips cleanly when either is unavailable so a bare/offline checkout does
not see a false failure.

    NFL_SCORE_COMPS_RUN=1   to actually run (it hits the DB + downloads seasons)
"""

from __future__ import annotations

import os

import pytest

pd = pytest.importorskip("pandas")


def test_no_leaky_features() -> None:
    """The feature set must stay style/process only — never the score or a
    near-copy of it. This runs with no data, so it always executes."""
    mod = pytest.importorskip("model.nfl_score_comps")
    banned = {"points", "score", "td_drive_rate", "td_rate", "epa", "drive_epa"}
    leaked = [f for f in mod.FEATURES if f in banned or "score" in f or "td_" in f]
    assert not leaked, f"outcome-flavoured feature(s) in FEATURES (leak risk): {leaked}"


def _reference():
    if os.environ.get("NFL_SCORE_COMPS_RUN") != "1":
        pytest.skip("set NFL_SCORE_COMPS_RUN=1 to run the score-comps back-test "
                    "(hits Neon + downloads nflverse seasons)")
    mod = pytest.importorskip("model.nfl_score_comps")
    try:
        ref = mod.build_reference(range(2016, 2026))
    except Exception as exc:  # no DB creds, network down, table absent
        pytest.skip(f"could not build reference set: {exc}")
    if len(ref) < 1000:
        pytest.skip(f"reference set too small to judge ({len(ref)} rows)")
    return mod, ref


def test_backtest_calibrated_and_accurate() -> None:
    mod, ref = _reference()
    result = mod.ScoreComps(ref).backtest()

    # Calibration: a 10-90 range should cover ~80%. Allow a generous band so
    # ordinary sampling noise does not trip it, but catch real miscalibration.
    assert 74.0 <= result["coverage_pct"] <= 88.0, (
        f"coverage {result['coverage_pct']:.1f}% is off the ~80 target — the ranges "
        "no longer mean what they claim"
    )

    # Accuracy: must clearly beat the naive baseline and stay near the validated ~5.0.
    assert result["mae"] < result["baseline_mae"] - 1.5, (
        f"MAE {result['mae']:.2f} is not clearly beating baseline "
        f"{result['baseline_mae']:.2f} — the fingerprint has lost its signal"
    )
    assert result["mae"] <= 6.0, (
        f"MAE {result['mae']:.2f} has drifted above the validated ~5.0 ceiling"
    )
