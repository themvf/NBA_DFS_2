import math

from model import cfb_first_td as m


def test_parse_scorer_reads_both_play_text_formats():
    assert m.parse_scorer("(04:04) Shotgun #2 J.Denman rush left for 2 yards gain to the UNC00 TOUCHDOWN", "rush") == "J.Denman"
    assert m.parse_scorer("Shotgun #2 N.Hayes pass complete short right to #81 R.Kessel caught at JSU00, for 6 yards",
                          "rec") == "R.Kessel"
    assert m.parse_scorer("Jaylen Raynor 8 Yd Run (Kyle Konrardy Kick)", "rush") == "Jaylen Raynor"
    assert m.parse_scorer("W. Parker run for 54 yds, for a TD (D. Curtis KICK)", "rush") == "W. Parker"
    assert m.parse_scorer("Derrick Salley Jr. 4 Yd pass from Avery Johnson (Luis Rodriguez Kick)", "rec") \
        == "Derrick Salley Jr."
    assert m.name_key("Derrick Salley Jr.") == ("d", "salley") == m.name_key("D.Salley")


def test_resolve_scorer_uses_box_score_and_initials():
    box = [{"pid": "1", "name": "Jaylen Smith", "rush_td": 1, "rec_td": 0},
           {"pid": "2", "name": "Marcus Smith", "rush_td": 2, "rec_td": 0},
           {"pid": "3", "name": "Kai Jones", "rush_td": 0, "rec_td": 1}]
    assert m.resolve_scorer("#4 M.Smith rush left for 3 yards TOUCHDOWN", "rush", box) == ("2", "name")
    # One receiver caught a TD that game, so he is the scorer whatever the text says.
    assert m.resolve_scorer("garbled", "rec", box) == ("3", "only_scorer")
    assert m.resolve_scorer("#4 T.Brown rush left TOUCHDOWN", "rush", box) == (None, "unresolved")


def test_team_probability_power():
    assert m.team_probability(28, 28, 1.2) == 0.5
    assert math.isclose(m.team_probability(30, 20, 1.0), 0.6)
    assert m.team_probability(30, 20, 2.0) > m.team_probability(30, 20, 1.0)
    assert m.team_probability(30, 20, 0.0) == 0.5


def test_game_distribution_sums_to_one_and_splits_by_score():
    p = m.Params(k=1.0, phi=0.05, theta=0.1, kappa=0.0, w_prior=0.0, rho=0.05)
    home = {"a": {"td": 3, "vol": 0, "td_prior": 0, "vol_prior": 0}, "b": {"td": 1, "vol": 0, "td_prior": 0, "vol_prior": 0}}
    away = {"c": {"td": 0, "vol": 0, "td_prior": 0, "vol_prior": 0}}
    d = m.game_distribution(0.6, home, away, p)
    assert math.isclose(sum(d.values()), 1.0)
    assert math.isclose(d["a"], 0.6 * 0.95 * 0.9 * 0.75)
    assert math.isclose(d["a"] / d["b"], 3.0)
    # A team with nothing to share hands its whole offensive mass to "other".
    assert math.isclose(d["other:away"], 0.4 * 0.95) and "c" not in d


def test_history_candidates_use_only_earlier_games():
    rows = []
    for week, pid, td in ((1, "x", 2), (2, "x", 1), (3, "y", 1), (4, "z", 5)):
        rows.append({"cfbd_game_id": week, "pid": pid, "name": pid.upper() + " Q", "team": "T", "season": 2026,
                     "week": week, "season_type": "regular", "rush_att": 10, "rush_td": td, "receptions": 0,
                     "rec_td": 0})
    h = m.History(rows)
    c = h.candidates("T", (2026, 4))
    # Last two games before week 4 are weeks 2 and 3; week 4 (the target) is never read.
    assert set(c) == {"x", "y"} and c["x"]["td"] == 3 and c["x"]["vol"] == 20


def test_fair_american():
    assert m.fair_american(0.5) == -100
    assert m.fair_american(0.1) == 900
    assert m.fair_american(0.75) == -300
