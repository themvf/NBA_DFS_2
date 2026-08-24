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
                "post_start_revisions": 0, "revision_missing_provenance": 0,
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
                "post_start_revisions": 0, "revision_missing_provenance": 0,
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
                "post_start_revisions": 0, "revision_missing_provenance": 0,
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
