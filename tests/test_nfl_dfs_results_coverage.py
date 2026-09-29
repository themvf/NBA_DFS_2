"""The post-week coverage check: a completed game that cannot be scored is
named, never silently skipped, and fails the run only once it is overdue."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ingest import nfl_dfs_results_coverage as coverage

NOW = datetime(2026, 9, 29, 10, 7, tzinfo=timezone.utc)
SUNDAY = datetime(2026, 9, 27, 17, 0, tzinfo=timezone.utc)
MONDAY = datetime(2026, 9, 29, 0, 15, tzinfo=timezone.utc)


def game(gid, key, kickoff, completed=True, week=3):
    return {"id": gid, "week": week, "game_key": key, "kickoff": kickoff, "completed": completed}


def row(rid, gid, team, position="WR"):
    return {"id": rid, "player_id": rid, "game_id": gid, "team": team, "position": position,
            "actual_dk_fpts": 7.0, "scoring_status": "exact", "computed_at": NOW - timedelta(hours=1)}


def test_the_2026_09_29_monday_night_case_is_a_warning_not_silence():
    games = [game(1, "LAC@BUF", SUNDAY), game(2, "PHI@CHI", MONDAY)]
    results = [row(1, 1, "LAC"), row(2, 1, "BUF")]
    out = coverage.assess(games, results, NOW)
    assert out["checked"] == 2 and out["scorable"] == 1
    assert [g["game"] for g in out["awaiting_recent"]] == ["PHI@CHI"]
    assert out["awaiting_recent"][0]["teams_missing_skill_results"] == ["CHI", "PHI"]
    assert out["missing_overdue"] == []
    assert coverage.report(out, 2026) == 0


def test_an_overdue_game_fails_the_run():
    games = [game(1, "LAC@BUF", SUNDAY)]
    # Two DST rows (team feed) and nothing from the player feed, 41 hours on.
    results = [row(1, 1, "LAC", "DST"), row(2, 1, "BUF", "DST")]
    out = coverage.assess(games, results, NOW)
    assert [g["game"] for g in out["missing_overdue"]] == ["LAC@BUF"]
    assert coverage.report(out, 2026) == 1


def test_one_team_missing_is_named():
    out = coverage.assess([game(1, "LAC@BUF", SUNDAY)], [row(1, 1, "BUF")], NOW)
    assert out["missing_overdue"][0]["teams_missing_skill_results"] == ["LAC"]


def test_schedule_lag_and_future_games():
    games = [game(1, "PHI@CHI", MONDAY, completed=False),               # 10h old, not marked final
             game(2, "PIT@CLE", NOW + timedelta(days=2), completed=False),  # not started
             game(3, "ATL@NO", NOW - timedelta(hours=2), completed=False)]  # in progress
    out = coverage.assess(games, [], NOW)
    assert out["checked"] == 0
    assert [g["game"] for g in out["not_marked_completed"]] == ["PHI@CHI"]
    assert coverage.report(out, 2026) == 0


def test_results_computed_after_now_do_not_count():
    late = row(1, 1, "LAC") | {"computed_at": NOW + timedelta(hours=1)}
    out = coverage.assess([game(1, "LAC@BUF", SUNDAY)], [late, row(2, 1, "BUF")], NOW)
    assert out["scorable"] == 0


def test_step_summary_is_written(tmp_path, monkeypatch):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    out = coverage.assess([game(2, "PHI@CHI", MONDAY)], [], NOW)
    coverage.report(out, 2026)
    text = summary.read_text(encoding="utf-8")
    assert "0 of 1 completed games are scorable" in text and "PHI@CHI" in text
