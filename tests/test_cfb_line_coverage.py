from datetime import datetime, timedelta, timezone

from research.cfb_line_coverage import classify_game


NOW = datetime(2026, 10, 3, 15, 0, tzinfo=timezone.utc)


def game(*, lead_minutes=60, mapped=True, captured_minutes_ago=10):
    return {
        "commence_time": NOW + timedelta(minutes=lead_minutes),
        "odds_event_id": "event" if mapped else None,
        "start_time_tbd": False,
        "last_capture": None if captured_minutes_ago is None else NOW - timedelta(minutes=captured_minutes_ago),
    }


def test_unmapped_upcoming_game_is_reported_before_kickoff():
    assert "unmapped_within_24h" in classify_game(game(mapped=False), [], NOW)
    assert "unmapped_within_24h" not in classify_game(game(lead_minutes=1500, mapped=False), [], NOW)


def test_capture_age_follows_dense_game_day_cadence():
    assert "capture_overdue" in classify_game(game(captured_minutes_ago=26), [], NOW)
    assert "capture_overdue" not in classify_game(game(captured_minutes_ago=24), [], NOW)
    assert "no_pregame_capture" in classify_game(game(captured_minutes_ago=None), [], NOW)


def test_checkpoint_warning_distinguishes_live_window_from_reschedule():
    pending = {"checkpoint": "closing_candidate", "status": "pending",
               "target_at": NOW - timedelta(minutes=4), "due_until": NOW + timedelta(minutes=1),
               "failure_reason": None}
    superseded = {**pending, "status": "missed", "failure_reason": "superseded by kickoff reschedule"}
    assert "due_now:closing_candidate" in classify_game(game(), [pending], NOW)
    assert not any(issue.startswith("missed:") for issue in classify_game(game(), [superseded], NOW))


def test_early_checkpoint_created_after_its_window_is_a_provider_limit_not_an_alert():
    window = {"checkpoint": "cfb_t_minus_7d", "status": "missed", "failure_reason": None,
              "target_at": NOW - timedelta(days=3), "due_until": NOW - timedelta(days=3) + timedelta(hours=6)}
    listed_late = {**window, "created_at": NOW - timedelta(days=1)}
    listed_in_time = {**window, "created_at": NOW - timedelta(days=4)}
    assert "missed:cfb_t_minus_7d" not in classify_game(game(lead_minutes=4 * 1440), [listed_late], NOW)
    assert "missed:cfb_t_minus_7d" in classify_game(game(lead_minutes=4 * 1440), [listed_in_time], NOW)
    # A core checkpoint is never excused, however late its row was created.
    core = {**listed_late, "checkpoint": "t_minus_6h"}
    assert "missed:t_minus_6h" in classify_game(game(lead_minutes=4 * 1440), [core], NOW)
