from __future__ import annotations

from model.mlb_data_health import collect_mlb_data_health


class FakeDb:
    def __init__(self, *, stats: dict, schedule: dict, bullpen: dict | None = None, weather: dict | None = None,
                 missing_team_games: list[dict] | None = None) -> None:
        self.stats = stats
        self.schedule = schedule
        self.bullpen = bullpen or {
            "relief_appearances": 100, "relief_missing_provenance": 0,
            "bullpen_team_games": 30, "empty_quality": 0, "post_start_snapshots": 0,
        }
        self.weather = weather or {
            "forecasts": 15, "invalid_forecasts": 0,
        }
        self.missing_team_games = missing_team_games or []

    def execute_one(self, sql, params=None):
        if "mlb_bullpen_snapshots" in sql:
            return self.bullpen
        if "mlb_weather_forecast_snapshots" in sql:
            return self.weather
        return self.schedule if "FROM mlb_matchups" in sql else self.stats

    def execute(self, sql, params=None):
        assert "NOT EXISTS" in sql and "mlb_bullpen_snapshots" in sql
        return self.missing_team_games


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


# ── Stats-history freshness (2026-09-29) ─────────────────────────────────────
# refresh_mlb_stats.yml stayed green for 79 days while writing no pitcher rows
# (MAX(mlb_pitcher_stats_history.snapshot_date) = 2026-07-12). The age was
# only ever *observed* here; now it gates, on dates that have games.

from datetime import datetime, timezone

from model.mlb_data_health import STATS_HISTORY_MAX_AGE_HOURS


def _health_stats(stats: dict, games: int = 15, missing: list[dict] | None = None):
    return collect_mlb_data_health(
        FakeDb(
            stats=stats,
            schedule={"games": games, "starts": games, "revisions": games, "revision_missing_provenance": 0},
            bullpen={
                "relief_appearances": 100, "relief_missing_provenance": 0,
                "bullpen_team_games": games * 2 - len(missing or []), "empty_quality": 0,
                "post_start_snapshots": 0,
            },
            weather={"forecasts": games, "invalid_forecasts": 0},
            missing_team_games=missing,
        ),  # type: ignore[arg-type]
        "2026-09-29",
    )


_FRESH_STATS = {
    "team_entities": 30, "team_captures": 2400, "pitcher_captures": 1470,
    "team_missing_provenance": 0, "pitcher_missing_provenance": 0,
    "team_leakage": 0, "pitcher_leakage": 0,
    "team_age_hours": 0.19, "pitcher_age_hours": 1889.58,   # run 36613262320's observed block
}


def test_stale_pitcher_history_fails_a_date_with_games_and_names_the_age() -> None:
    report = _health_stats(_FRESH_STATS)
    check = _check(report, "pitcher_history_freshness")
    assert report["status"] == "fail"
    assert check["status"] == "fail"
    assert "78.7 days old" in check["detail"]
    assert "refresh_mlb_stats.yml" in check["remedy"]
    assert _check(report, "team_history_freshness")["status"] == "pass"


def test_stale_history_does_not_fail_an_off_day() -> None:
    """No games, no decision to make: the age is reported, not gated."""
    report = _health_stats(_FRESH_STATS, games=0)
    assert _check(report, "pitcher_history_freshness")["status"] == "pass"
    assert report["observed"]["pitcher_age_hours"] == 1889.58


def test_history_inside_budget_passes() -> None:
    report = _health_stats({**_FRESH_STATS, "pitcher_age_hours": STATS_HISTORY_MAX_AGE_HOURS - 1})
    assert _check(report, "pitcher_history_freshness")["status"] == "pass"


def test_no_history_rows_at_all_fails_when_games_exist() -> None:
    report = _health_stats({**_FRESH_STATS, "pitcher_age_hours": None})
    check = _check(report, "pitcher_history_freshness")
    assert check["status"] == "fail"
    assert "no pitcher history captures at all" in check["detail"]


# ── Bullpen coverage names the team-games and separates the unrepairable ─────
# 2026-09-29: three refresh_mlb_vegas runs failed on "6/8 team-games have a
# bullpen snapshot". The missing game was PHI@ATL 18:00Z, whose first pitch came
# before the day's first refresh (the 13:10 UTC schedule fired at 18:36 UTC), so
# build_bullpen_snapshots -- pregame only -- could never build it, and every
# later run that day failed and skipped prop capture for the three games still
# to come.

_GAME_TIME = datetime(2026, 9, 29, 18, 0, tzinfo=timezone.utc)


def _missing(team: str, *, started: bool, matchup_id: int = 6447) -> dict:
    return {"matchup_id": matchup_id, "commence_time": _GAME_TIME, "team_id": 1,
            "team": team, "home": "ATL", "away": "PHI", "started": started}


def test_missing_snapshot_for_a_game_still_to_come_fails_and_names_it() -> None:
    fresh = {**_FRESH_STATS, "pitcher_age_hours": 1}
    report = _health_stats(fresh, games=4, missing=[_missing("ATL", started=False), _missing("PHI", started=False)])
    check = _check(report, "bullpen_snapshots")
    assert report["status"] == "fail"
    assert check["status"] == "fail"
    assert "6/8" in check["detail"]
    assert "PHI@ATL 2026-09-29 18:00Z [ATL]" in check["detail"]
    assert "PHI@ATL 2026-09-29 18:00Z [PHI]" in check["detail"]


def test_a_game_that_started_without_a_pregame_snapshot_is_a_named_warning_not_a_failed_day() -> None:
    fresh = {**_FRESH_STATS, "pitcher_age_hours": 1}
    report = _health_stats(fresh, games=4, missing=[_missing("ATL", started=True), _missing("PHI", started=True)])
    assert report["status"] == "pass"
    assert _check(report, "bullpen_snapshots")["status"] == "pass"
    warned = _check(report, "bullpen_pregame_missed")
    assert warned["status"] == "warn"
    assert warned["severity"] == "warning"
    assert "2 team-game(s) started with no pregame bullpen snapshot" in warned["detail"]
    assert "PHI@ATL 2026-09-29 18:00Z [ATL]" in warned["detail"]
    assert "fired late" in warned["remedy"]
    assert report["observed"]["bullpen_missing_team_games"] == [
        {"matchup_id": 6447, "team": "ATL", "game": "PHI@ATL", "commence_time": str(_GAME_TIME), "started": True},
        {"matchup_id": 6447, "team": "PHI", "game": "PHI@ATL", "commence_time": str(_GAME_TIME), "started": True},
    ]


def test_full_coverage_reports_no_warning() -> None:
    fresh = {**_FRESH_STATS, "pitcher_age_hours": 1}
    report = _health_stats(fresh, games=4)
    assert _check(report, "bullpen_pregame_missed")["status"] == "pass"
    assert report["status"] == "pass"
