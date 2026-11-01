import pandas as pd

from ingest.nfl_roster_status import changed_rows, completeness_problem, current_week_rows


def _frame(rows):
    return pd.DataFrame(rows, columns=["week", "team", "position", "status", "status_description_abbr", "gsis_id"])


def test_reads_only_the_newest_week_and_offensive_positions():
    frame = _frame([
        (6, "CHI", "RB", "DEV", "P07", "00-1"),
        (7, "CHI", "RB", "DEV", "P07", "00-1"),
        (7, "CHI", "LB", "DEV", "P01", "00-2"),
        (7, "KC", "FB", "ACT", "A01", "00-3"),
        (6, "NYJ", "RB", "ACT", "A01", "00-4"),  # absent from week 7: no row, not a guessed status
    ])
    rows = {row["gsis_id"]: row for row in current_week_rows(frame)}
    assert set(rows) == {"00-1", "00-3"}
    assert rows["00-1"] == {"gsis_id": "00-1", "roster_week": 7, "team": "CHI", "status": "DEV", "status_description": "P07"}


def test_change_only_writes():
    current = [{"gsis_id": "00-1", "roster_week": 7, "team": "CHI", "status": "DEV", "status_description": "P07"},
               {"gsis_id": "00-2", "roster_week": 7, "team": "CHI", "status": "ACT", "status_description": "A01"},
               {"gsis_id": "00-3", "roster_week": 7, "team": "KC", "status": "ACT", "status_description": "A01"}]
    latest = {"00-1": {"roster_week": 7, "team": "CHI", "status": "DEV", "status_description": "P07"},
              # elevated since the last capture: DEV -> ACT is a new row
              "00-2": {"roster_week": 7, "team": "CHI", "status": "DEV", "status_description": "P01"}}
    assert [row["gsis_id"] for row in changed_rows(current, latest)] == ["00-2", "00-3"]


def test_week_roll_is_a_change_even_with_the_same_status():
    current = [{"gsis_id": "00-1", "roster_week": 8, "team": "CHI", "status": "DEV", "status_description": "P07"}]
    latest = {"00-1": {"roster_week": 7, "team": "CHI", "status": "DEV", "status_description": "P07"}}
    assert len(changed_rows(current, latest)) == 1


def _week(teams: int, per_team: int):
    return [{"gsis_id": f"{t}-{i}", "roster_week": 8, "team": f"T{t}", "status": "ACT", "status_description": "A01"}
            for t in range(teams) for i in range(per_team)]


def test_a_real_week_with_byes_is_whole():
    # week 8 of 2026: 28 teams (four on bye), 734 offensive rows
    assert completeness_problem(_week(28, 26)) is None
    assert completeness_problem(_week(32, 30)) is None


def test_a_truncated_week_is_refused():
    assert "teams" in completeness_problem(_week(12, 26))
    assert "a team" in completeness_problem(_week(28, 10))
