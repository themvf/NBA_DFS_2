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


def _cands(team, names):
    return {f"{team}{i}": {"td": 1.0, "vol": 10.0, "td_prior": 0.0, "vol_prior": 0.0, "name": n, "team": team}
            for i, n in enumerate(names)}


def test_apply_lines_drops_unlisted_only_on_covered_teams():
    home = _cands("H", ["Al Bee", "Cy Dee", "Ed Eff", "Gus Hay", "Ian Jay"])
    away = _cands("A", ["Kim Lee", "Mo Noe", "Ox Poe"])
    listed = {m.name_key(n) for n in ["Al Bee", "Cy Dee", "Ed Eff", "Gus Hay", "Kim Lee", "Mo Noe"]}
    h, a, info = m.apply_lines(home, away, listed)
    assert set(h) == {"H0", "H1", "H2", "H3"} and [d["name"] for d in info["dropped"]] == ["Ian Jay"]
    # Only 2 away candidates are listed: too few to read an absence, so the team is left alone.
    assert a == away and info["covered"]["away"]["covered"] is False


def test_apply_lines_without_a_capture_changes_nothing():
    home, away = _cands("H", ["Al Bee"]), _cands("A", ["Kim Lee"])
    h, a, info = m.apply_lines(home, away, None)
    assert h is home and a is away and info["status"] == "no_lines"


def test_dropping_a_player_raises_his_teammates():
    p = m.Params(k=1.0, phi=0.05, theta=0.1, kappa=0.0, w_prior=0.0, rho=0.05)
    home = _cands("H", ["Al Bee", "Cy Dee", "Ed Eff", "Gus Hay", "Ian Jay"])
    away = _cands("A", ["Kim Lee"])
    before = m.game_distribution(0.5, home, away, p)
    listed = {m.name_key(n) for n in ["Al Bee", "Cy Dee", "Ed Eff", "Gus Hay"]}
    h, a, _ = m.apply_lines(home, away, listed)
    after = m.game_distribution(0.5, h, a, p)
    assert math.isclose(sum(after.values()), 1.0) and "H4" not in after
    assert math.isclose(after["H0"], before["H0"] * 5 / 4)


def test_dist_probability_reads_the_stored_distribution():
    dist = {"p1": 0.2, "other:home": 0.05, "other:away": 0.04, "non_offense": 0.04}
    assert m.dist_probability(dist, {"outcome": "offense", "side": "home", "pid": "p1"}) == 0.2
    assert m.dist_probability(dist, {"outcome": "offense", "side": "away", "pid": "zz"}) == 0.04
    assert m.dist_probability(dist, {"outcome": "non_offense", "side": "home"}) == 0.04
    assert m.dist_probability(dist, {"outcome": "offense", "side": "home", "pid": None}) is None


def _row(gid, week, pid, rush_att=0, rush_td=0, receptions=0, rec_td=0, pass_att=0, team="T"):
    return {"cfbd_game_id": gid, "pid": pid, "name": pid.upper() + " Q", "team": team, "season": 2026,
            "week": week, "season_type": "regular", "rush_att": rush_att, "rush_td": rush_td,
            "receptions": receptions, "rec_td": rec_td, "pass_att": pass_att}


def test_player_chips_read_only_earlier_games():
    rows = []
    for week in range(1, 5):  # weeks 1-2 a backup, weeks 3-4 the lead back who scores
        lead = week >= 3
        rows += [_row(week, week, "rb", rush_att=18 if lead else 4, rush_td=1 if lead else 0),
                 _row(week, week, "qb", rush_att=6, rush_td=1, pass_att=30),
                 _row(week, week, "x", rush_att=5, rush_td=1 if week == 1 else 0)]
    rows.append(_row(5, 5, "rb", rush_att=30, rush_td=4))  # the target week itself: never read
    h = m.History(rows)
    codes = {c["code"] for c in m.player_chips(h, "rb", "T", (2026, 5))}
    assert codes == {"HOT", "NEW_ROLE"}
    qb = {c["code"]: c["label"] for c in m.player_chips(h, "qb", "T", (2026, 5))}
    assert qb["RUNNING_QB"] == "QB, 4 rush TDs" and qb["GOAL_LINE"] == "4 of 7 team rush TDs"


def test_team_chips_favourite_and_first_scores():
    firsts = {"A": [((2026, 1), True), ((2026, 2), False), ((2026, 9), True)], "B": []}
    g = {"home": "A", "away": "B", "home_implied": 38.0, "away_implied": 17.0}
    t = m.team_chips(g, firsts, (2026, 5))
    assert [c["label"] for c in t["home"]] == ["Favoured by 21", "Scored first TD in 1 of 2"]
    assert t["away"] == []


def test_apply_lines_matches_a_unique_surname_across_first_names():
    home = _cands("H", ["Marquise Brown", "Cy Dee", "Ed Eff", "Gus Hay", "Ian Jay"])
    away = _cands("A", ["Kim Lee"])
    listed = {m.name_key(n) for n in ["Hollywood Brown", "Cy Dee", "Ed Eff", "Gus Hay", "Ian Jay"]}
    h, _, info = m.apply_lines(home, away, listed)
    assert "H0" in h and info["dropped"] == []
    # Two Browns among the candidates: the surname is no longer enough.
    home2 = {**home, "H9": {**home["H1"], "name": "Zed Brown"}}
    h2, _, _ = m.apply_lines(home2, away, listed)
    assert "H0" not in h2
