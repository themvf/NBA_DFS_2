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


def test_rushing_grid_includes_qbs_and_counts_carries(tmp_path):
    rows = [{"player_id": "q", "player_display_name": "Q", "position": "QB", "team": "PIT", "opponent_team": "CLE",
             "week": w, "rushing_yards": y, "carries": c} for w, y, c in ((1, 20, 4), (2, 35, 7))]
    rows.append({"player_id": "w", "player_display_name": "W", "position": "WR", "team": "PIT",
                 "opponent_team": "CLE", "week": 1, "rushing_yards": 9, "carries": 1})
    ps = g.player_grid(rows, "PIT", stat="rushing")
    assert [p["name"] for p in ps] == ["Q"] and ps[0]["weeks"][2] == (35, 7) and ps[0]["avg"] == 27.5
    weeks, opp = g.team_weeks(rows, "PIT")
    assert g.grid_chart({"PIT": (ps, weeks, opp)}, tmp_path / "r.png", "rushing")
