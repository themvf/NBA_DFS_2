from model import nfl_receiving_grid_chart as g


def _row(pid, week, yds, rec, team="PIT", opp="CLE", pos="WR"):
    return {"player_id": pid, "player_display_name": pid.upper(), "position": pos, "team": team,
            "opponent_team": opp, "week": week, "receiving_yards": yds, "receptions": rec}


def test_shade_steps_and_gold_bonus():
    assert g.shade(5) == g.SHADES[0][1]
    assert g.shade(85) == g.SHADES[-1][1]
    assert g.shade(100) == g.GOLD


def test_player_grid_filters_and_averages_over_games_with_a_line():
    rows = [_row("a", w, 50, 4) for w in (1, 2, 4)] + [_row("q", 1, 0, 20, pos="QB"), _row("b", 1, 90, 3)]
    ps = g.player_grid(rows, "PIT")
    assert [p["name"] for p in ps] == ["A"]
    assert ps[0]["games"] == 3 and ps[0]["avg"] == 50 and ps[0]["weeks"][4] == (50, 4)


def test_bye_and_trailing_week(tmp_path):
    rows = [_row("a", w, 40, 5) for w in (1, 3)] + [_row("x", 2, 1, 1, team="CLE"), _row("x", 4, 1, 1, team="CLE")]
    weeks, opp = g.team_weeks(rows, "PIT")
    assert sorted(opp) == [1, 3] and weeks == [1, 2, 3, 4]
    assert g.grid_chart({"PIT": (g.player_grid(rows, "PIT"), weeks, opp)}, tmp_path / "a.png")
    assert not g.grid_chart({"PIT": ([], weeks, opp)}, tmp_path / "b.png")
