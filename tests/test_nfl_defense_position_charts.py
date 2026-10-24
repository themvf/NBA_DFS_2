from model import nfl_defense_position_charts as d


def _row(pid, week, defense, points, position="WR"):
    return {"player_id": pid, "week": week, "position": position, "team": "X", "defense": defense, "points": points}


def test_usual_is_other_games_and_defense_games_sum_players():
    rows = d.with_usual([_row(1, 1, "A", 10.0), _row(1, 2, "B", 20.0), _row(1, 3, "C", 30.0),
                         _row(2, 1, "A", 5.0), _row(9, 1, "A", 7.0, "RB")])
    assert [r["usual"] for r in rows[:3]] == [25.0, 20.0, 15.0] and rows[3]["usual"] is None
    games = {(g["defense"], g["week"]): g for g in d.defense_games(rows, "WR")}
    assert games[("A", 1)]["raw"] == 15.0 and games[("A", 1)]["over"] == -15.0 and games[("A", 1)]["adjusted"] == 1
    agg = d.by_defense(list(games.values()))
    assert agg["C"]["over"] == 15.0
    assert set(d.by_defense(list(games.values()), {2})) == {"B"}


def test_plays_profile_and_share():
    base = {"season_type": "REG", "two_point_attempt": 0, "game_id": "g1", "week": 1, "defteam": "LA",
            "sack": 0, "success": 1, "rush_touchdown": 0, "pass_touchdown": 0}
    pbp = [base | {"rush_attempt": 1, "pass_attempt": 0, "rusher_player_id": "rb", "yards_gained": 6},
           base | {"rush_attempt": 1, "pass_attempt": 0, "rusher_player_id": "rb", "yards_gained": 2, "success": 0,
                   "rush_touchdown": 1},
           base | {"rush_attempt": 0, "pass_attempt": 1, "receiver_player_id": "wr", "complete_pass": 1,
                   "yards_gained": 12},
           base | {"rush_attempt": 0, "pass_attempt": 1, "receiver_player_id": "te", "complete_pass": 0,
                   "yards_gained": 0},
           base | {"rush_attempt": 0, "pass_attempt": 1, "receiver_player_id": "qb", "complete_pass": 1,
                   "yards_gained": 3},
           base | {"rush_attempt": 0, "pass_attempt": 1, "sack": 1, "receiver_player_id": "wr", "yards_gained": -7}]
    ps = d.plays(pbp, {"rb": "RB", "wr": "WR", "te": "TE", "qb": "QB"})
    assert len(ps) == 4 and ps[0]["defense"] == "LAR"
    rb = d.position_profile(ps, "RB", {"LAR": 20.0})["LAR"]
    assert rb["carries_pg"] == 2 and rb["ypc"] == 4.0 and rb["success"] == 0.5 and rb["rush_td_pg"] == 1
    te = d.position_profile(ps, "TE", {})["LAR"]
    assert te["targets_pg"] == 1 and te["catch_rate"] == 0.0 and te["dk"] is None
    share = d.yards_share(ps)["LAR"]
    assert share["WR"] == 1.0 and share["total"] == 12.0


def test_charts_write_pngs(tmp_path):
    teams = [f"T{i}" for i in range(8)]
    defs = {pos: {t: {"games": 5, "raw": 20.0 + i, "over": i - 4.0} for i, t in enumerate(teams)}
            for pos in d.POSITIONS}
    assert d.points_chart(defs, tmp_path / "a.png", "Weeks 1-6")
    flipped = {pos: {t: v | {"over": -v["over"]} for t, v in defs[pos].items()} for pos in d.POSITIONS}
    assert d.stability_chart(defs, flipped, tmp_path / "b.png", ("Weeks 1-3", "Weeks 4-6"))
    prof = {t: {"games": 5, "dk": 20.0 + i, "targets_pg": 6.0, "ypt": 7.0, "catch_rate": 0.6, "rec_td_pg": 0.5,
                "carries_pg": 20.0, "ypc": 4.0, "success": 0.4, "rush_td_pg": 0.6} for i, t in enumerate(teams)}
    for pos in d.POSITIONS:
        assert d.profile_chart(prof, pos, tmp_path / f"p{pos}.png", "Weeks 1-6")
    shares = {t: {"total": 1000.0, "WR": 0.6, "TE": 0.25, "RB": 0.15} for t in teams}
    assert d.share_chart(shares, tmp_path / "s.png", "Weeks 1-6")
    assert not d.share_chart({}, tmp_path / "x.png", "")
