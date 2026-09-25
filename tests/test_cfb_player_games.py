"""CFBD box score parsing and DraftKings CFB Classic scoring."""
from ingest.cfb_player_games import dk_points, parse_game


def _t(name, athletes):
    return {"name": name, "athletes": athletes}


GAME = {"id": 401, "teams": [{"team": "Indiana", "categories": [
    {"name": "passing", "types": [_t("C/ATT", [{"id": "1", "name": "QB One", "stat": "24/33"}]),
                                   _t("YDS", [{"id": "1", "name": "QB One", "stat": "312"}]),
                                   _t("TD", [{"id": "1", "name": "QB One", "stat": "3"}]),
                                   _t("INT", [{"id": "1", "name": "QB One", "stat": "1"}]),
                                   _t("QBR", [{"id": "1", "name": "QB One", "stat": "80.1"}])]},
    {"name": "rushing", "types": [_t("CAR", [{"id": "1", "name": "QB One", "stat": "6"}, {"id": "2", "name": "RB Two", "stat": "18"},
                                             {"id": "-9999", "name": "TEAM", "stat": "2"}]),
                                   _t("YDS", [{"id": "1", "name": "QB One", "stat": "-4"}, {"id": "2", "name": "RB Two", "stat": "104"}]),
                                   _t("TD", [{"id": "1", "name": "QB One", "stat": "0"}, {"id": "2", "name": "RB Two", "stat": "1"}])]},
    {"name": "receiving", "types": [_t("REC", [{"id": "3", "name": "WR Three", "stat": "7"}]),
                                     _t("YDS", [{"id": "3", "name": "WR Three", "stat": "96"}]),
                                     _t("TD", [{"id": "3", "name": "WR Three", "stat": "2"}])]},
    {"name": "fumbles", "types": [_t("LOST", [{"id": "2", "name": "RB Two", "stat": "1"}])]},
    {"name": "kickReturns", "types": [_t("TD", [{"id": "3", "name": "WR Three", "stat": "1"}, {"id": "9", "name": "Returner", "stat": "0"}])]},
    {"name": "defensive", "types": [_t("TOT", [{"id": "50", "name": "LB", "stat": "9"}])]},
]}]}


def test_parse_and_score():
    rows = {r["player_name"]: r for r in parse_game(GAME)}
    assert set(rows) == {"QB One", "RB Two", "WR Three"}, "no TEAM row, no defenders, no zero-TD returner"
    qb, rb, wr = rows["QB One"], rows["RB Two"], rows["WR Three"]
    assert (qb["pass_cmp"], qb["pass_att"]) == (24, 33)
    # 312*0.04 + 3*4 + 300 bonus - 1 INT - 0.4 rush = 12.48 + 12 + 3 - 1 - 0.4
    assert qb["dk_points"] == 26.08
    # 104*0.1 + 6 + 100 bonus - 1 fumble = 10.4 + 6 + 3 - 1
    assert rb["dk_points"] == 18.4
    # 7 rec + 9.6 + 12 + kick return TD 6; 96 yds -> no bonus
    assert wr["dk_points"] == 34.6


def test_bonus_thresholds_are_inclusive():
    base = dict(pass_yds=300, pass_td=0, interceptions=0, rush_yds=0, rush_td=0, rec_yds=0, rec_td=0,
                receptions=0, return_td=0, fumbles_lost=0)
    assert dk_points(base) == 15.0
