"""nba-ladder-context-v1: reference scores and minutes strata, per docs/nba-points-tail-validation-spec.md checks 1-4."""
import pytest

from research import nba_ladder_context as c


def row(pts, p25=0.2, line=20.5, minutes=30.0, status="graded", date="2026-11-01", **probs):
    r = {"grade_version": "nba-points-grade-v1", "ladder_version": "nba-points-ladder-v1", "game_date": date, "event_id": "e",
         "player_key": f"p{pts}{minutes}", "status": status, "line": line, "actual_points": pts, "minutes": minutes,
         "p_over": 0.5, "p20": None, "p25": p25, "p30": None, "p35": None, "p40": None}
    r.update(probs)
    return r


def test_hand_calculated_brier_and_same_sample_constant():
    rows = [row(30, p25=0.8), row(10, p25=0.2), row(26, p25=0.5), row(5, p25=0.1)]
    ref = c.brier_reference(c.scored(rows, "25"))
    assert ref["n"] == 4 and ref["hits"] == 2 and ref["observedRate"] == 0.5
    assert ref["brier"] == pytest.approx((0.2 ** 2 + 0.2 ** 2 + 0.5 ** 2 + 0.1 ** 2) / 4)
    assert ref["fittedConstantBrier"] == pytest.approx(0.25)
    assert ref["descriptiveSkill"] == pytest.approx(1 - ref["brier"] / 0.25)


def test_all_zero_outcomes_give_null_skill_and_empty_cells_stay_empty():
    zero = c.brier_reference(c.scored([row(5, p25=0.1), row(6, p25=0.3)], "25"))
    assert zero["fittedConstantBrier"] == 0 and zero["descriptiveSkill"] is None and zero["skillNote"] == "constant_has_zero_loss"
    empty = c.brier_reference(c.scored([row(30, p25=None)], "25"))
    assert empty["n"] == 0 and empty["brier"] is None


def test_thresholds_use_their_own_cohorts_and_pushes_are_omitted_but_counted():
    rows = [row(30, p25=0.5, p40=0.1), row(10, p25=0.5), row(21, status="push", p25=0.3)]
    rep = c.build_report(rows)
    assert rep["rungs"]["25"]["sameSample"]["n"] == 2 and rep["rungs"]["40"]["sameSample"]["n"] == 1
    assert rep["rungs"]["25"]["pushRowsOmitted"] == 1


def test_minutes_strata_reconcile_and_never_infer_dnp_from_minutes():
    rows = [row(2, minutes=7.0),            # Zubac-style low-minute appearance stays graded
            row(0, minutes=28.0),           # Barnes zero points in 28 minutes
            row(8, minutes=15.0),           # exactly 15 -> 15 plus
            row(1, minutes=0.0),            # rounded-zero minutes, still played
            row(9, minutes=None),           # played, minutes unknown
            row(None, minutes=None, status="dnp"), row(None, minutes=None, status="unmatched"),
            row(None, minutes=None, status="ambiguous")]
    m = c.minutes_summary(rows)
    assert m["counts"] == {"played_under_15": 2, "played_15_plus": 2, "played_minutes_unknown": 1, "not_applicable": 3}
    assert m["reconciles"] is True and m["invalidMinutes"] == 0
    assert c.minutes_stratum(row(5, minutes=-3))[1] is True


def test_prior_only_constant_needs_100_earlier_rows_and_ignores_the_future():
    early = [row(30 if i % 4 == 0 else 5, p25=0.25, date="2026-10-20") for i in range(100)]
    late = [row(30, p25=0.3, date="2026-10-21")]
    po = c.prior_only_constant(c.scored(early + late, "25"))
    assert po["coveredRows"] == 1 and po["uncoveredRows"] == 100
    assert po["frozenValueByDate"]["2026-10-21"] == pytest.approx((25 + 1) / (100 + 2))
    assert po["prospectiveEligible"] is False
    # A changed later outcome cannot move the earlier date's frozen value.
    po2 = c.prior_only_constant(c.scored(early + [row(5, p25=0.3, date="2026-10-21")], "25"))
    assert po2["frozenValueByDate"] == po["frozenValueByDate"]


def test_mixed_reader_versions_fail_closed():
    rows = [row(30), {**row(10), "ladder_version": "nba-points-ladder-v2"}]
    with pytest.raises(ValueError, match="mixed"):
        c.build_report(rows)


def test_reliability_bins_are_fixed_and_line_bands_cover_every_row():
    pairs = c.scored([row(30, p25=0.04, line=12), row(10, p25=0.6, line=27)], "25")
    bins = c.reliability(pairs)
    assert [b["n"] for b in bins] == [1, 0, 0, 0, 0, 1, 0]
    assert sum(b["n"] for b in c.line_bands(pairs)) == 2
