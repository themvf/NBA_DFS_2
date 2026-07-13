from __future__ import annotations

from model.mlb_data_health import collect_mlb_data_health


class FakeDb:
    def __init__(self, *, stats: dict, schedule: dict, bullpen: dict | None = None) -> None:
        self.stats = stats
        self.schedule = schedule
        self.bullpen = bullpen or {
            "relief_appearances": 100, "relief_missing_provenance": 0,
            "bullpen_snapshots": 30, "empty_quality": 0, "post_start_snapshots": 0,
        }

    def execute_one(self, sql, params=None):
        if "mlb_bullpen_snapshots" in sql:
            return self.bullpen
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
