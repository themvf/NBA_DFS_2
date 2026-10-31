"""Offline tests for research/nba_points_ladder_v2_eval.py (the pre-registered ladder v2 evaluator).

Synthetic rows only: nothing here reads the database or any real outcome.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from model import nba_points_calibration as cal
from research import nba_points_ladder_v2_eval as ev

TIP = datetime(2026, 11, 2, 0, 0, tzinfo=timezone.utc)
CAP = datetime(2026, 11, 1, 23, 0, tzinfo=timezone.utc)
NIGHT = date(2026, 11, 1)


def _grade_row(night=NIGHT, event="e1", key="p", line=24.5, actual=30.0, status="graded", p_over=0.5,
               at_least=None, floor=15.0, ceiling=35.0, minutes=30.0) -> cal.GradeRow:
    return cal.GradeRow(
        game_date=night, event_id=event, espn_event_id="x", player=key, player_key=key, team="T",
        commence_time=TIP, captured_at=CAP, minutes_before_tip=60.0, line=line, p_over=p_over, books=2,
        floor=floor, ceiling=ceiling, at_least=dict(at_least or {20: 0.8, 25: 0.5, 30: 0.25, 35: 0.1, 40: 0.04}),
        ladder_books=1, status=status, actual_points=actual if status in ("graded", "push") else None, minutes=minutes,
    )


def _eval_row(night, key, actual, v1_at, v2_at, line=24.5, v1_floor=15.0, v1_ceiling=35.0, v2_floor=16.0, v2_ceiling=34.0):
    a = _grade_row(night=night, key=key, actual=actual, at_least=v1_at, line=line, floor=v1_floor, ceiling=v1_ceiling)
    b = _grade_row(night=night, key=key, actual=actual, at_least=v2_at, line=line, floor=v2_floor, ceiling=v2_ceiling)
    return ev.EvalRow(game_date=night, event_id="e1", player_key=key, line=line, actual=actual, minutes=30.0, v1=a, v2=b,
                      one_sided={"v1": 3, "v2": 3}, skipped={"v1": 0, "v2": 1}, book_k={"draftkings": 1.07, "fanduel": None})


V1 = {20: 0.8, 25: 0.5, 30: 0.25, 35: 0.1, 40: 0.04}
V2 = {20: 0.82, 25: 0.5, 30: 0.23, 35: 0.08, 40: 0.03}


def _nights(n, per_night_35, start=ev.START_DATE):
    rows = []
    for i in range(n):
        night = start + timedelta(days=i)
        for j in range(per_night_35):
            rows.append(_eval_row(night, f"p{j}", actual=float(10 + j % 20), v1_at=V1, v2_at=V2))
    return rows


# ------------------------------------------------------------- pairing and integrity


def _captures_and_games():
    def books(line=24.5):
        return {"draftkings": {"line": line, "over": -110, "under": -110}, "fanduel": {"line": line, "over": -115, "under": -105}}
    ladder = {"draftkings": {"ladder": [
        {"point": 14.5, "side": "over", "price": -900}, {"point": 19.5, "side": "over", "price": -330},
        {"point": 29.5, "side": "over", "price": 190}, {"point": 34.5, "side": "over", "price": 500},
        {"point": 39.5, "side": "over", "price": 1200},
    ]}}
    caps = [cal.Capture(event_id="e1", commence_time=TIP, captured_at=CAP, home_team="Orlando Magic",
                        away_team="Atlanta Hawks", player=name, books=books()) for name in ("A", "B", "C")]
    games = [cal.BoxGame("401", "Orlando Magic", "Atlanta Hawks", [
        cal.BoxPlayer("A", "a", "Atlanta Hawks", False, True, 31.0, 34.0),
        cal.BoxPlayer("B", "b", "Atlanta Hawks", False, True, 12.0, 10.0),
        cal.BoxPlayer("C", "c", "Atlanta Hawks", False, False, None, None),
    ])]
    return caps, {("e1", "a"): ladder}, games


def test_pair_rows_keeps_graded_rows_read_both_ways():
    caps, ladders, games = _captures_and_games()
    v1 = cal.grade_captures(NIGHT, caps, ladders, games)
    v2 = cal.grade_captures(NIGHT, caps, ladders, games, devig="power")
    rows = ev.pair_rows(v1, v2, ev._counters(caps, ladders))
    assert sorted(r.player_key for r in rows) == ["a", "b"]  # c did not play
    a = next(r for r in rows if r.player_key == "a")
    # Same main line, same P(over); the over-only ladder moves the tail.
    assert a.v1.p_over == a.v2.p_over and a.v1.line == a.v2.line
    assert a.v2.at_least[35] < a.v1.at_least[35]
    assert a.one_sided == {"v1": 5, "v2": 5} and a.skipped == {"v1": 0, "v2": 0}
    assert set(a.book_k) == {"draftkings", "fanduel"} and all(k > 1 for k in a.book_k.values())


def test_pair_rows_refuses_mismatched_grades():
    caps, ladders, games = _captures_and_games()
    v1 = cal.grade_captures(NIGHT, caps, ladders, games)
    v2 = cal.grade_captures(NIGHT, caps[:2], ladders, games, devig="power")
    with pytest.raises(ValueError):
        ev.pair_rows(v1, v2)


def _stored_from(rows: list[cal.GradeRow]) -> list[dict]:
    return [{"game_date": r.game_date, "event_id": r.event_id, "player_key": r.player_key, "status": r.status,
             "actual_points": r.actual_points, "line": r.line, "p_over": r.p_over, "floor": r.floor, "ceiling": r.ceiling,
             **{f"p{n}": r.at_least.get(n) for n in (20, 25, 30, 35, 40)}} for r in rows]


def test_integrity_check_passes_on_identical_rows_and_catches_drift():
    caps, ladders, games = _captures_and_games()
    v1 = cal.grade_captures(NIGHT, caps, ladders, games)
    stored = _stored_from(v1)
    assert ev.integrity_check(v1, stored) == []

    drift = [dict(s) for s in stored]
    next(s for s in drift if s["player_key"] == "a")["p35"] += 1e-8
    assert any("p35" in m for m in ev.integrity_check(v1, drift))

    tiny = [dict(s) for s in stored]
    next(s for s in tiny if s["player_key"] == "a")["p35"] += 1e-10
    assert ev.integrity_check(v1, tiny) == []

    missing = [s for s in stored if s["player_key"] != "b"]
    assert any("no stored v1 row" in m for m in ev.integrity_check(v1, missing))

    extra = stored + [{**stored[0], "player_key": "zz"}]
    assert any("not graded on recompute" in m for m in ev.integrity_check(v1, extra))

    # A power reading is not the stored v1 reading.
    v2 = cal.grade_captures(NIGHT, caps, ladders, games, devig="power")
    assert ev.integrity_check(v2, stored) != []


# ------------------------------------------------------------- floors and the frozen population


def test_floors_not_met_withholds_the_primary_metric():
    rows = _nights(29, 40)
    status = ev.floor_status(rows, today=date(2026, 12, 1))
    assert status["nights"] == 29 and not status["nightsMet"] and status["rows35Met"]
    assert status["state"] == "not_met" and status["verdictCutoffNight"] is None
    with pytest.raises(ev.FloorsNotMet):
        ev.primary_metric(rows, status)
    report = ev.evaluate(rows, today=date(2026, 12, 1))
    assert report["primary"] == {"withheld": True, "reason": "floors not met"}
    assert "mean" not in report["primary"]
    assert report["secondaries"]["gradedRows"] == len(rows)


def test_rows_floor_counts_only_rows_with_35_under_both_readers():
    rows = _nights(40, 20)
    for r in rows[::2]:
        r.v2.at_least[35] = None
    status = ev.floor_status(rows, today=date(2026, 12, 1))
    assert status["rows35Both"] == 400 and not status["rows35Met"] and status["nightsMet"]


def test_discovery_and_post_deadline_nights_never_count():
    discovery = _nights(7, 40, start=date(2026, 10, 20))
    late = _nights(5, 40, start=ev.DEADLINE + timedelta(days=1))
    status = ev.floor_status(discovery + late, today=date(2027, 4, 20))
    assert status["nights"] == 0 and status["state"] == "inconclusive"
    assert ev.evaluate(discovery + late, today=date(2027, 4, 20))["primary"]["withheld"] is True


def test_verdict_population_is_frozen_at_the_first_night_both_floors_are_met():
    rows = _nights(30, 34)  # 30 nights x 34 = 1,020 rows: both floors met on night 30
    status = ev.floor_status(rows, today=date(2026, 12, 1))
    assert status["state"] == "met"
    assert status["verdictCutoffNight"] == (ev.START_DATE + timedelta(days=29)).isoformat()
    first = ev.primary_metric(rows, status)

    later = rows + _nights(10, 34, start=ev.START_DATE + timedelta(days=30))
    status_later = ev.floor_status(later, today=date(2026, 12, 20))
    assert status_later["verdictCutoffNight"] == status["verdictCutoffNight"]
    assert ev.primary_metric(later, status_later) == first


def test_floors_met_late_when_rows_lag_nights():
    rows = _nights(40, 30)  # nights met at 30 (900 rows); 1,000 rows first reached on night 34
    status = ev.floor_status(rows, today=date(2026, 12, 20))
    assert status["verdictCutoffNight"] == (ev.START_DATE + timedelta(days=33)).isoformat()


# ------------------------------------------------------------- the primary metric's arithmetic


def test_paired_brier_diffs_use_rungs_25_to_40_where_both_readers_have_a_value():
    r = _eval_row(NIGHT, "a", actual=33.0, v1_at={20: 0.8, 25: 0.5, 30: 0.3, 35: 0.1, 40: None},
                  v2_at={20: 0.1, 25: 0.4, 30: 0.2, 35: 0.05, 40: 0.02})
    diffs = ev.paired_brier_diffs([r])[NIGHT]
    # 25+ hit: (0.4-1)^2 - (0.5-1)^2; 30+ hit: (0.2-1)^2 - (0.3-1)^2; 35+ miss: 0.05^2 - 0.1^2; 40+ skipped; 20+ never used.
    assert diffs == pytest.approx([0.36 - 0.25, 0.64 - 0.49, 0.0025 - 0.01])


def test_night_bootstrap_is_seeded_pooled_and_resamples_nights():
    per_night = {d: [-0.01, -0.02] for d in range(10)}
    per_night[10] = [-0.005] * 8
    a = ev.night_bootstrap(per_night, resamples=2000)
    b = ev.night_bootstrap(per_night, resamples=2000)
    assert a == b and a["seed"] == ev.BOOTSTRAP_SEED
    assert a["mean"] == pytest.approx((10 * -0.03 + 8 * -0.005) / 28)
    assert a["upper"] < 0 and a["lower"] <= a["mean"] <= a["upper"] and a["pairs"] == 28 and a["nights"] == 11
    constant = ev.night_bootstrap({d: [0.0] for d in range(5)}, resamples=100)
    assert constant["lower"] == constant["upper"] == 0.0


def test_pass_needs_the_upper_bound_below_zero():
    better = _nights(30, 34)
    status = ev.floor_status(better, today=date(2026, 12, 1))
    result = ev.primary_metric(better, status)
    # Every V2 tail chance is lower and these synthetic players mostly miss the tail: v2 is better.
    assert result["verdict"] == "PASS" and result["upper"] < 0
    for r in better:  # swap the readers: now v2 is worse
        r.v1, r.v2 = r.v2, r.v1
    assert ev.primary_metric(better, status)["verdict"] == "FAIL"


# ------------------------------------------------------------- secondaries


def test_rung_table_and_coverage():
    rows = [_eval_row(NIGHT, "a", 40.0, V1, V2), _eval_row(NIGHT, "b", 10.0, V1, V2), _eval_row(NIGHT, "c", 20.0, V1, V2)]
    table = {r["rung"]: r for r in ev.rung_table(rows)}
    t40 = table["40+"]
    assert t40["v1"]["n"] == 3 and t40["v1"]["happened"] == pytest.approx(1 / 3)
    assert t40["v1"]["stated"] == pytest.approx(0.04) and t40["v2"]["stated"] == pytest.approx(0.03)
    assert t40["v1"]["brier"] == pytest.approx(((0.04 - 1) ** 2 + 2 * 0.04 ** 2) / 3)
    assert table["over the line"]["pairedN"] == 3
    cov = ev.coverage(rows)
    # v1 floor 15 / ceiling 35: 40 above, 10 below, 20 inside.
    assert cov["v1"] == {"n": 3, "below": pytest.approx(1 / 3), "inside": pytest.approx(1 / 3), "above": pytest.approx(1 / 3)}


def test_line_only_skill_matches_a_hand_computed_case():
    d1, d2 = NIGHT, NIGHT + timedelta(days=1)
    rows = [_eval_row(d1, "a", 30.0, V1, V2, line=24.5), _eval_row(d2, "b", 10.0, V1, V2, line=25.5)]
    out = ev.line_only_skill(rows, "v1", "25")
    # Band floor(24.5/3)=8 for both. Row a (hit) sees night 2: band 8 has 1 row, 0 hits, pool 0
    # -> guess 0. Row b (miss) sees night 1: band 8, 1 row 1 hit, pool 1 -> guess 1.
    assert out["lineN"] == 2
    assert out["brierLine"] == pytest.approx((1 + 1) / 2)
    assert out["brierPaired"] == pytest.approx(((0.5 - 1) ** 2 + 0.5 ** 2) / 2)
    assert out["skillVsLine"] == pytest.approx(1 - 0.25 / 1.0)
    # One night alone has no line-only guess.
    assert ev.line_only_skill(rows[:1], "v1", "25")["lineN"] == 0


def test_rung_counts_and_k_by_book():
    rows = [_eval_row(NIGHT, "a", 30.0, V1, V2), _eval_row(NIGHT, "b", 30.0, V1, V2)]
    rows[1].book_k = {"draftkings": 1.09}
    assert ev.rung_counts(rows) == {"v1": {"oneSidedRungs": 6, "skippedRungs": 0}, "v2": {"oneSidedRungs": 6, "skippedRungs": 2}}
    books = {k["book"]: k for k in ev.k_by_book(rows)}
    assert books["draftkings"]["n"] == 2 and books["draftkings"]["median"] == pytest.approx(1.08)
    assert books["fanduel"] == {"book": "fanduel", "n": 0, "unsolvable": 1, "min": None, "p10": None,
                                "median": None, "p90": None, "max": None}


# ------------------------------------------------------------- frozen verdict receipt and integration safeguards


def test_receipt_freezes_the_population_and_detects_a_later_correction():
    rows = _nights(31, 40)
    status = ev.floor_status(rows, today=date(2026, 12, 15))
    assert status["state"] == "met"
    primary = ev.primary_metric(rows, status)
    receipt = ev.build_receipt(rows, status, primary, {"f": "abc"})
    assert receipt["kind"] == "original" and receipt["cutoffNight"] == status["verdictCutoffNight"]
    assert len(receipt["population"]) == len(ev.verdict_population(rows, status))
    # Same data reproduces it, and nights after the cutoff never enter it.
    later = rows + _nights(2, 40, start=date.fromisoformat(status["verdictCutoffNight"]) + timedelta(days=1))
    assert ev.compare_to_receipt(receipt, later)["matches"] is True
    # A corrected outcome inside the frozen range is reported, never absorbed.
    changed = list(rows)
    changed[0] = _eval_row(changed[0].game_date, changed[0].player_key, actual=99.0, v1_at=V1, v2_at=V2)
    check = ev.compare_to_receipt(receipt, changed)
    assert check["matches"] is False and "amendment" in check["note"]


def test_population_digest_is_order_independent():
    rows = _nights(3, 5)
    a = ev.records_sha256(ev.population_records(rows))
    b = ev.records_sha256(ev.population_records(list(reversed(rows))))
    assert a == b


def test_grade_is_pinned_to_v1_and_code_digests_cover_the_registration():
    assert ev.LEGACY_GRADE_VERSION == "nba-points-grade-v1" == cal.GRADE_VERSION
    digests = ev.code_digests()
    assert ev.REGISTRATION in digests and "model/nba_points_ladder.py" in digests
    assert all(len(v) == 64 for v in digests.values())


def test_paired_brier_scores_both_readers_on_the_same_rows():
    rows = [_eval_row(ev.START_DATE, "a", 30.0, V1, V2), _eval_row(ev.START_DATE, "b", 10.0, {25: 0.4}, {25: None})]
    r25 = next(r for r in ev.rung_table(rows) if r["rung"] == "25+")
    assert r25["v1"]["n"] == 2 and r25["v2"]["n"] == 1 and r25["pairedN"] == 1
    assert abs(r25["pairedBrier"]["v1"] - (0.5 - 1) ** 2) < 1e-12 and abs(r25["pairedBrier"]["v2"] - (0.5 - 1) ** 2) < 1e-12


def test_capture_provenance_reports_main_and_ladder_ages_separately():
    tip = datetime(2026, 11, 1, 23, 0, tzinfo=timezone.utc)
    rows = [{"event_id": "e1", "market": "player_points", "captured_at": tip - timedelta(minutes=11), "commence_time": tip},
            {"event_id": "e1", "market": "player_points_alternate", "captured_at": tip - timedelta(minutes=92), "commence_time": tip},
            {"event_id": "e2", "market": "player_points", "captured_at": tip - timedelta(minutes=30), "commence_time": tip}]
    p = ev.capture_provenance(rows)
    assert p["games"] == 2 and p["gamesWithoutLadder"] == 1
    assert p["mainLadderGapMinutes"]["median"] == 81 and p["ladderMinutesBeforeTip"]["max"] == 92
