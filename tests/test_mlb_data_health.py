from __future__ import annotations

from model.mlb_data_health import collect_mlb_data_health


class FakeDb:
    def __init__(self, *, stats: dict, schedule: dict, bullpen: dict | None = None, weather: dict | None = None) -> None:
        self.stats = stats
        self.schedule = schedule
        self.bullpen = bullpen or {
            "relief_appearances": 100, "relief_missing_provenance": 0,
            "bullpen_team_games": 30, "empty_quality": 0, "post_start_snapshots": 0,
        }
        self.weather = weather or {
            "forecasts": 15, "invalid_forecasts": 0,
        }

    def execute_one(self, sql, params=None):
        if "mlb_bullpen_snapshots" in sql:
            return self.bullpen
        if "mlb_weather_forecast_snapshots" in sql:
            return self.weather
        return self.schedule if "FROM mlb_matchups" in sql else self.stats


def test_health_passes_only_with_population_provenance_and_revisions() -> None:
    report = collect_mlb_data_health(
        FakeDb(
            stats={
                "team_entities": 30, "team_captures": 30, "pitcher_captures": 735,
                "team_missing_provenance": 0, "pitcher_missing_provenance": 0,
                "team_leakage": 0, "pitcher_leakage": 0,
                "team_age_hours": 1, "pitcher_age_hours": 1,
            },
            schedule={
                "games": 15, "starts": 15, "revisions": 15,
                "revision_missing_provenance": 0,
            },
        ),  # type: ignore[arg-type]
        "2026-07-17",
    )
    assert report["status"] == "pass"
    assert all(check["status"] == "pass" for check in report["checks"])


def test_health_fails_with_exact_remedies() -> None:
    report = collect_mlb_data_health(
        FakeDb(
            stats={
                "team_entities": 0, "team_captures": 0, "pitcher_captures": 0,
                "team_missing_provenance": 0, "pitcher_missing_provenance": 0,
                "team_leakage": 1, "pitcher_leakage": 0,
            },
            schedule={
                "games": 15, "starts": 14, "revisions": 0,
                "revision_missing_provenance": 0,
            },
        ),  # type: ignore[arg-type]
        "2026-07-17",
    )
    assert report["status"] == "fail"
    failed = [check for check in report["checks"] if check["status"] == "fail"]
    assert failed
    assert all(check["remedy"] for check in failed)


def _health(bullpen: dict):
    return collect_mlb_data_health(
        FakeDb(
            stats={
                "team_entities": 30, "team_captures": 30, "pitcher_captures": 735,
                "team_missing_provenance": 0, "pitcher_missing_provenance": 0,
                "team_leakage": 0, "pitcher_leakage": 0,
                "team_age_hours": 1, "pitcher_age_hours": 1,
            },
            schedule={
                "games": 15, "starts": 15, "revisions": 15,
                "revision_missing_provenance": 0,
            },
            bullpen=bullpen,
        ),  # type: ignore[arg-type]
        "2026-07-17",
    )


def _check(report, key):
    return next(c for c in report["checks"] if c["key"] == key)


def test_bullpen_gate_measures_team_game_coverage_not_row_count() -> None:
    """The regression that starved the prop board (2026-08-23).

    mlb_bullpen_snapshots is append-only with UNIQUE(matchup_id, team_id,
    raw_checksum), so re-ingesting a date appends another row for a team-game
    that is ALREADY covered. The gate used to count rows and demand exactly
    games*2, so a single extra revision reported '31/30' and failed -- which
    exits the MLB refresh non-zero and SKIPS prop capture and the alert scan.
    Coverage is unchanged by a revision, so the gate must still pass.
    """
    report = _health({
        "relief_appearances": 100, "relief_missing_provenance": 0,
        "bullpen_team_games": 30, "empty_quality": 0, "post_start_snapshots": 0,
    })
    assert report["status"] == "pass"
    assert _check(report, "bullpen_snapshots")["status"] == "pass"


def test_bullpen_gate_still_fails_on_genuinely_missing_coverage() -> None:
    """The fix must not blunt the check: a team-game with NO snapshot still fails."""
    report = _health({
        "relief_appearances": 100, "relief_missing_provenance": 0,
        "bullpen_team_games": 29, "empty_quality": 0, "post_start_snapshots": 0,
    })
    check = _check(report, "bullpen_snapshots")
    assert check["status"] == "fail"
    assert "29/30" in check["detail"]
    assert check["remedy"]


def test_bullpen_provenance_still_scans_every_row_not_just_the_latest() -> None:
    """Coverage counts distinct team-games; VALIDITY still counts every row, so a
    bad appended revision cannot hide behind a covered team-game."""
    report = _health({
        "relief_appearances": 100, "relief_missing_provenance": 0,
        "bullpen_team_games": 30, "empty_quality": 1, "post_start_snapshots": 0,
    })
    assert _check(report, "bullpen_snapshots")["status"] == "pass"
    assert _check(report, "bullpen_provenance")["status"] == "fail"


def _health_sched(schedule: dict, weather: dict | None = None):
    return collect_mlb_data_health(
        FakeDb(
            stats={
                "team_entities": 30, "team_captures": 30, "pitcher_captures": 735,
                "team_missing_provenance": 0, "pitcher_missing_provenance": 0,
                "team_leakage": 0, "pitcher_leakage": 0,
                "team_age_hours": 1, "pitcher_age_hours": 1,
            },
            schedule=schedule,
            weather=weather,
        ),  # type: ignore[arg-type]
        "2026-08-22",
    )


def test_post_start_captures_on_in_progress_games_do_not_fail_the_day() -> None:
    """The second bug that starved the prop board (22:10 UTC slot, 0/10 runs).

    The evening refresh re-captures schedule and weather for EVERY game on the
    date, including ones already in progress, so the globally-latest revision
    for those is legitimately post-start. The gates took that row, correctly
    judged it unusable pregame, and failed the whole run -- which skipped prop
    capture for the games that had NOT started.

    The queries now select the latest capture before each game's OWN commence,
    so a post-start row cannot be selected at all. Every game here has a good
    pregame revision and forecast, so the day is healthy.
    """
    report = _health_sched(
        {"games": 15, "starts": 15, "revisions": 15, "revision_missing_provenance": 0},
        {"forecasts": 15, "invalid_forecasts": 0},
    )
    assert _check(report, "schedule_revisions")["status"] == "pass"
    assert _check(report, "schedule_provenance")["status"] == "pass"
    assert _check(report, "weather_forecasts")["status"] == "pass"
    assert _check(report, "weather_provenance")["status"] == "pass"


def test_a_game_with_no_pregame_capture_at_all_still_fails() -> None:
    """The rescoping must not blunt the gate. A game whose only revision or
    forecast landed AFTER first pitch has no usable pregame input, and that is
    the real defect the check exists to catch."""
    sched = _health_sched(
        {"games": 16, "starts": 16, "revisions": 15, "revision_missing_provenance": 0},
        {"forecasts": 15, "invalid_forecasts": 0},
    )
    assert _check(sched, "schedule_revisions")["status"] == "fail"
    assert "15/16" in _check(sched, "schedule_revisions")["detail"]
    assert _check(sched, "weather_forecasts")["status"] == "fail"


def test_pregame_revision_missing_provenance_still_fails() -> None:
    report = _health_sched(
        {"games": 15, "starts": 15, "revisions": 15, "revision_missing_provenance": 1},
        {"forecasts": 15, "invalid_forecasts": 0},
    )
    assert _check(report, "schedule_provenance")["status"] == "fail"


def test_missing_commence_time_is_reported_once_not_twice() -> None:
    """A game with no start time cannot be judged pregame at all. schedule_starts
    owns that defect; the provenance gates must not also count it, or one problem
    reads as three."""
    report = _health_sched(
        {"games": 16, "starts": 15, "revisions": 15, "revision_missing_provenance": 0},
        {"forecasts": 15, "invalid_forecasts": 0},
    )
    assert _check(report, "schedule_starts")["status"] == "fail"
    assert _check(report, "schedule_revisions")["status"] == "pass"
    assert _check(report, "weather_forecasts")["status"] == "pass"
