from model import cfb_first_td as cf
from model import nfl_first_td as n


def _g(**kw):
    base = {"home": "KC", "away": "NYJ", "posteam": "KC", "scorer": "p1", "scorer_team": "KC", "offensive": True}
    return {**base, **kw}


def test_label_offense_only_when_the_scorer_carried_or_caught_it():
    assert n.label(_g()) == {"outcome": "offense", "side": "home", "pid": "p1", "method": "pbp"}
    # A return or a defensive score: the scorer is not the play's rusher or receiver.
    assert n.label(_g(offensive=False, posteam="NYJ")) == {"outcome": "non_offense", "side": "home"}
    assert n.label(_g(scorer_team=None)) == {"outcome": "no_td"}


def _row(gid, week, pid, team="KC", **kw):
    r = {"cfbd_game_id": gid, "pid": pid, "name": pid, "team": team, "season": 2026, "week": week,
         "season_type": "regular", "rush_att": 0, "receptions": 0, "pass_att": 0, "inside10": 0,
         "rush_td": 0, "rec_td": 0}
    return {**r, **kw}


def test_nfl_chips_use_only_earlier_weeks():
    rows = []
    for w in range(1, 5):
        rows += [_row(w, w, "rb", rush_att=18, inside10=4, rush_td=1),
                 _row(w, w, "wr", receptions=8, inside10=1, rec_td=1 if w >= 3 else 0)]
    rows.append(_row(5, 5, "wr", receptions=30, rec_td=3))  # the target week: never read
    h = cf.History(rows)
    rb = {c["code"] for c in n.player_chips(h, "rb", "KC", (2026, 5))}
    assert rb == {"GOAL_LINE", "INSIDE_TEN", "HOT"}
    assert {c["code"] for c in n.player_chips(h, "wr", "KC", (2026, 5))} == {"HOT"}


def test_nfl_team_chips():
    g = {"home": "KC", "away": "NYJ", "home_implied": 28.75, "away_implied": 18.75}
    firsts = {"KC": [((2026, 1), True), ((2026, 2), False)]}
    t = n.team_chips(g, firsts, (2026, 9))
    assert [c["label"] for c in t["home"]] == ["Favoured by 10", "Scored first TD in 1 of 2"]
    assert t["away"] == []
