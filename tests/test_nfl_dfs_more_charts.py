import pytest

from model import nfl_dfs_more_charts as mc


def test_kind_and_label():
    assert mc.kind({"format": "classic", "contest_name": "NFL $2.75M Fantasy Football Millionaire [$1M to 1st]"}) == "millionaire"
    assert mc.kind({"format": "classic", "contest_name": "NFL MEGA Millionaire"}) is None
    sd = {"format": "showdown", "week": 6, "contest_name": "NFL Showdown $1.5M Monday Night Showdown [$500K to 1st] (WAS @ SF)"}
    assert mc.kind(sd) == "showdown" and mc.game_label(sd) == "Wk 6 Monday WAS @ SF"
    assert mc.kind({"format": "showdown", "contest_name": "NFL Showdown $10K Special (A @ B)"}) is None


def test_top_scores_and_field_ownership():
    entries = [{"rank": r, "points": 200 - r} for r in (1, 2, 50, 100, 150)] + [{"rank": None, "points": 300}]
    assert mc.top_scores(entries) == {"winner": 199, "cut": 100, "cut_rank": 100}
    assert mc.top_scores([]) is None
    # Two players at 50% and one at 100%: a random lineup's expected total is .5*50 + .5*50 + 1*100.
    assert mc.field_avg_ownership([50, 50, 100]) == pytest.approx(150)


def test_captain_mix_and_stack_rates():
    lineups = [{"rank": 1, "roster": [["CPT", "A", "X", "QB", 1, 1, 1]]},
               {"rank": 2, "roster": [["CPT", "B", "X", "RB", 1, 1, 1]]},
               {"rank": 300, "roster": [["CPT", "C", "X", "WR", 1, 1, 1]]}]
    mix = mc.captain_mix(lineups)
    assert mix["n"] == 2 and mix["by_position"] == {"QB": 1, "RB": 1}
    s1 = {"by_rank": {"all": {"entries": 100, "qb_plus_catchers": {"0": 0.1, "1": 0.5, "2": 0.3, "3+": 0.1},
                              "bring_back_any": 0.4, "qb_plus_rb": 0.2}}}
    s2 = {"by_rank": {"all": {"entries": 300, "qb_plus_catchers": {"0": 0.1, "1": 0.3, "2": 0.5, "3+": 0.1},
                              "bring_back_any": 0.6, "qb_plus_rb": 0.2}}}
    r = mc.stack_rates([s1, s2])["all"]
    assert r["entries"] == 400 and r["2"] == pytest.approx(0.45) and r["bring_back"] == pytest.approx(0.55)


def test_charts_write_pngs(tmp_path):
    scores = [{"kind": "millionaire", "label": "Wk 1", "week": 1, "winner": 250.0, "cut": 210.0, "entries": 1},
              {"kind": "showdown", "label": "Wk 1 Monday A @ B", "week": 1, "winner": 120.0, "cut": None, "entries": 1}]
    assert mc.winning_scores_chart(scores, tmp_path / "a.png")
    assert mc.winners_ownership_chart([{"week": 1, "sums": [90.0, 120.0], "winner_sum": 90.0, "field_avg": 110.0}],
                                      tmp_path / "b.png")
    assert mc.captains_chart([{"label": "Wk 1 Monday A @ B", "n": 2, "by_position": {"QB": 1, "RB": 1},
                               "names": [("A", 1)], "winner_captain": "A (QB)"}], tmp_path / "c.png")
    rates = {"all": {"0": 0.1, "1": 0.5, "2": 0.3, "3+": 0.1, "bring_back": 0.4, "qb_plus_rb": 0.2, "entries": 10}}
    assert mc.stack_shapes_chart(rates, [1, 2], tmp_path / "d.png")
    assert mc.us_vs_winners_chart([{"label": "Wk 1", "ours": 150.0, "winner": 250.0, "cut": 210.0, "lineups": 20}],
                                  tmp_path / "e.png")
    for n in "abcde":
        assert (tmp_path / f"{n}.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not mc.winning_scores_chart([], tmp_path / "x.png")
    assert not mc.us_vs_winners_chart([], tmp_path / "y.png")
