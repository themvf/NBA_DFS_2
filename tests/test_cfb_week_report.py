"""The report must not let a winning record stand in for closing-line value."""

from research.cfb_week_report import MIN_GAMES_FOR_CI, clv_verdict, summarise


def _row(matchup_id: int, outcome, line_clv, pnl_units, *,
         alert_type: str = "spread_steam", origin: str = "prospective",
         week: int = 1, overtime: bool = False) -> dict:
    return {
        "id": matchup_id * 10, "alert_type": alert_type, "side": "home",
        "signal_version": "cfb-lines-v1", "origin": origin,
        "matchup_id": matchup_id, "created_at": None, "game_date": None,
        "week": week, "season_type": "regular", "completed": True,
        "went_to_overtime": overtime, "outcome": outcome,
        "line_clv": line_clv, "pnl_units": pnl_units,
        "close_history_id": 1, "grading_version": "v1",
    }


def test_every_alert_lands_in_exactly_one_outcome_bucket() -> None:
    """Rule 7: pushes and voids are counted, never silently removed."""
    rows = [
        _row(1, "won", 0.5, 0.95), _row(2, "lost", -0.5, -1.0),
        _row(3, "push", 0.0, 0.0), _row(4, "void", None, None),
        _row(5, None, None, None),
    ]
    s = summarise(rows)
    assert s["won"] + s["lost"] + s["push"] + s["void"] + s["pending"] == s["n"] == 5
    assert s["decisions"] == 2
    assert s["settled"] == 3


def test_a_missing_verified_close_is_excluded_not_imputed() -> None:
    """Settlement contract: a missing close is never zero and never the latest row."""
    rows = [_row(1, "won", 0.5, 0.95), _row(2, "won", None, 0.95)]
    s = summarise(rows)
    assert s["clv_n"] == 1
    assert s["no_close"] == 1
    assert s["mean_clv"] == 0.5  # not 0.25, which imputing zero would give


def test_two_alerts_on_one_game_are_one_cluster() -> None:
    """Rule 4: correlated same-game signals may not inflate the sample."""
    rows = [_row(1, "won", 0.5, 0.95), _row(1, "lost", -0.5, -1.0)]
    s = summarise(rows)
    assert s["n"] == 2
    assert s["games"] == 1
    assert s["clv_games"] == 1


def test_no_interval_is_computed_below_the_game_cluster_floor() -> None:
    rows = [_row(i, "won", 0.4, 0.95) for i in range(MIN_GAMES_FOR_CI - 1)]
    s = summarise(rows)
    assert s["clv_ci"] is None
    assert "DESCRIPTIVE ONLY" in clv_verdict(s)


def test_a_winning_record_at_flat_clv_does_not_read_as_a_pass() -> None:
    """The Week 0 pilot's exact shape: 4-1 on units at 0.0 average CLV.

    This is the assertion the whole script exists for.
    """
    rows = [
        _row(i, "won" if i % 5 else "lost", 0.0, 0.95 if i % 5 else -1.0)
        for i in range(40)
    ]
    s = summarise(rows)
    assert s["won"] > s["lost"]
    assert s["units"] > 0
    assert "INCLUDES ZERO" in clv_verdict(s)


def test_a_backwards_signal_is_named_backwards() -> None:
    """A negative interval is a confirmed negative, not an absence of evidence."""
    s = summarise([_row(i, "lost", -0.7, -1.0) for i in range(40)])
    assert "NEGATIVE" in clv_verdict(s)


def test_positive_clv_claims_only_the_one_gate_it_clears() -> None:
    s = summarise([_row(i, "won", 0.6, 0.95) for i in range(40)])
    verdict = clv_verdict(s)
    assert "EXCLUDES ZERO (positive)" in verdict
    assert "gate only" in verdict
    for forbidden in ("edge", "EDGE", "profitable", "PLAY"):
        assert forbidden not in verdict
