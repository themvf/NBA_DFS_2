from model import nfl_defense_qb_charts as d


def test_starter_is_the_qb_with_most_attempts():
    rows = [{"week": 1, "team": "NE", "player_id": 1, "attempts": 30},
            {"week": 1, "team": "NE", "player_id": 2, "attempts": 3},
            {"week": 1, "team": "NYJ", "player_id": 3, "attempts": 25}]
    s = d.starters(rows)
    assert s[(1, "NE")]["player_id"] == 1 and s[(1, "NYJ")]["player_id"] == 3


def test_expectation_is_the_qbs_other_starts_only():
    starts = [{"player_id": 1, "week": w, "defense": dfn, "points": p}
              for w, dfn, p in ((1, "A", 10.0), (2, "B", 20.0), (3, "C", 30.0))]
    out = d.defense_effects(starts, min_other=2)
    assert [o["expected"] for o in out] == [25.0, 20.0, 15.0]
    assert d.defense_effects(starts[:2], min_other=2)[0]["expected"] is None
    agg = d.by_defense(out)
    assert agg["A"]["over"] == -15.0 and agg["C"]["raw"] == 30.0
    assert set(d.by_defense(out, {1, 2})) == {"A", "B"}


def test_dropbacks_and_splits():
    base = {"season_type": "REG", "qb_dropback": 1, "two_point_attempt": 0, "game_id": "g", "week": 1,
            "posteam": "LA", "defteam": "SF", "passer_player_name": "M.Stafford", "passer_player_id": "x",
            "interception": 0, "pass_touchdown": 0, "passing_yards": 8}
    plays = [base | {"epa": 0.5, "qb_hit": 0, "sack": 0, "pass_attempt": 1},
             base | {"epa": -1.5, "qb_hit": 0, "sack": 1, "pass_attempt": 1, "passing_yards": None},
             base | {"epa": -0.5, "qb_hit": 1, "sack": 0, "pass_attempt": 1},
             base | {"epa": 0.1, "qb_dropback": 0, "qb_hit": 0, "sack": 0, "pass_attempt": 0},
             base | {"epa": None, "qb_hit": 0, "sack": 0, "pass_attempt": 1}]
    dbs = d.dropbacks(plays)
    assert len(dbs) == 3 and dbs[0]["offense"] == "LAR"
    assert sum(x["hit"] for x in dbs) == 2 and sum(x["attempt"] for x in dbs) == 2
    prof = d.defense_profile(dbs)["SF"]
    assert prof["sack_rate"] == 1 / 3 and prof["ypa"] == 8.0
    split = d.qb_hit_splits(dbs, min_dropbacks=3, min_hit=2)[0]
    assert split["clean"] == 0.5 and split["hit"] == -1.0 and split["drop"] == 1.5


def test_charts_write_pngs(tmp_path):
    teams = [f"T{i}" for i in range(8)]
    defs = {t: {"games": 5, "raw": 18.0 + i, "over": i - 4.0, "adjusted_games": 5} for i, t in enumerate(teams)}
    assert d.points_chart(defs, tmp_path / "a.png", "Weeks 1-6")
    assert d.stability_chart(defs, {t: v | {"over": -v["over"]} for t, v in defs.items()}, tmp_path / "b.png",
                             ("Weeks 1-3", "Weeks 4-6"))
    prof = {t: {"dropbacks": 200, "hit_rate": 0.1 + i / 100, "sack_rate": 0.05, "epa": i / 50 - 0.1, "ypa": 7.0,
                "td_rate": 0.04, "int_rate": 0.02} for i, t in enumerate(teams)}
    assert d.pressure_chart(prof, tmp_path / "c.png", "Weeks 1-6")
    assert d.profile_chart(prof, tmp_path / "e.png", "Weeks 1-6")
    splits = [{"name": f"Q{i}", "team": "X", "dropbacks": 200, "hit_n": 30, "hit_rate": 0.15, "clean": 0.2,
               "hit": -0.8, "drop": 1.0} for i in range(5)]
    assert d.qb_hit_chart(splits, tmp_path / "d.png", "Weeks 1-6")
    for n in "abcde":
        assert (tmp_path / f"{n}.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not d.points_chart({}, tmp_path / "x.png", "")
