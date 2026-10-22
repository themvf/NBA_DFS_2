"""nfl-prop-under-fringe-v1: frozen constants, the board-rank tag and the one-look verdict."""
from datetime import datetime, timezone

from research import nfl_prop_under_fringe as fr

REG = fr.REGISTRATION
FIRST_WEEK7_KICKOFF = datetime(2026, 10, 23, 0, 15, tzinfo=timezone.utc)


def test_registration_constants_are_the_frozen_ones():
    assert REG["study"] == "nfl-prop-under-fringe-v1"
    assert datetime.fromisoformat(REG["registered_at"]) < FIRST_WEEK7_KICKOFF
    assert REG["parent_registration"] == "docs/nfl-prop-under-study-v2.json"
    assert REG["tag"]["markets"] == ["receiving_yards", "receptions"]
    assert REG["tag"]["fringe_min_rank"] == 3
    assert REG["floors"] == {"fringe_bets": 400, "top_bets": 200, "game_weeks": 8}
    assert REG["bootstrap"] == {"resamples": 2000, "seed": 20261022, "cluster": "game_week", "interval": 0.95}
    assert REG["verdict"]["last_week"] == 18
    assert fr.parent_registration()["study"] == "nfl-prop-under-v2"


def _r(identity, line, team="KC", metric="receiving_yards", game="2026_07_LAC_KC"):
    return {"game_id": game, "metric": metric, "identity": identity, "team": team, "line": line}


def test_rank_is_competition_rank_within_game_team_and_market():
    rows = [_r("kelce", 50.5), _r("worthy", 50.5), _r("royals", 25.5), _r("allen", 12.5),
            _r("mcconkey", 41.5, team="LAC"), _r("kelce", 4.5, metric="receptions"),
            _r("walker", 94.5, metric="rushing_yards")]
    ranks = fr.board_ranks(rows, REG["tag"]["markets"])
    assert ranks[("2026_07_LAC_KC", "receiving_yards", "kelce")] == 1
    assert ranks[("2026_07_LAC_KC", "receiving_yards", "worthy")] == 1      # tie shares the rank
    assert ranks[("2026_07_LAC_KC", "receiving_yards", "royals")] == 3      # two teammates strictly above
    assert ranks[("2026_07_LAC_KC", "receiving_yards", "allen")] == 4
    assert ranks[("2026_07_LAC_KC", "receiving_yards", "mcconkey")] == 1    # other team ranked separately
    assert ranks[("2026_07_LAC_KC", "receptions", "kelce")] == 1            # other market ranked separately
    assert ("2026_07_LAC_KC", "rushing_yards", "walker") not in ranks


def test_rank_does_not_depend_on_row_order():
    rows = [_r("a", 30.5), _r("b", 50.5), _r("c", 40.5), _r("d", 40.5)]
    assert fr.board_ranks(rows, REG["tag"]["markets"]) == fr.board_ranks(rows[::-1], REG["tag"]["markets"])


def test_tag_thresholds_and_missing_team():
    assert fr.tag_for(1) == "top" and fr.tag_for(2) == "top"
    assert fr.tag_for(3) == "fringe" and fr.tag_for(7) == "fringe"
    assert fr.tag_for(None) == "unranked"
    assert fr.board_ranks([{**_r("x", 10.5), "team": None}], REG["tag"]["markets"]) == {}


def _bets(n_weeks, fringe_per_week, top_per_week, fringe_pnl, top_pnl):
    bets = []
    for w in range(7, 7 + n_weeks):
        for i in range(fringe_per_week):
            bets.append({"week": w, "game_id": f"g{w}", "metric": "receptions", "result": "win" if fringe_pnl > 0 else "loss",
                         "pnl": fringe_pnl if i % 2 == 0 else fringe_pnl * 0.8, "fringe": {"tag": "fringe", "rank": 3 + i % 3}})
        for i in range(top_per_week):
            bets.append({"week": w, "game_id": f"g{w}", "metric": "receiving_yards", "result": "win" if top_pnl > 0 else "loss",
                         "pnl": top_pnl if i % 2 == 0 else top_pnl * 1.2, "fringe": {"tag": "top", "rank": 1}})
    return bets


def test_no_verdict_before_the_final_look():
    out = fr.summarize(_bets(12, 50, 30, 0.9, -1.0), final=False)
    assert out["floors_met"] and out["verdict_if_first"] is None


def test_final_look_verdicts():
    assert fr.summarize(_bets(12, 50, 30, 0.9, -1.0), final=True)["verdict_if_first"] == "FRINGE_BETTER"
    assert fr.summarize(_bets(12, 50, 30, -1.0, 0.9), final=True)["verdict_if_first"] == "FRINGE_WORSE"
    assert fr.summarize(_bets(3, 50, 30, 0.9, -1.0), final=True)["verdict_if_first"] == "INCONCLUSIVE"


def test_rushing_and_voids_are_outside_both_arms():
    bets = _bets(12, 50, 30, 0.9, -1.0)
    bets.append({"week": 7, "game_id": "g7", "metric": "rushing_yards", "result": "win", "pnl": 0.9,
                 "fringe": {"tag": "out_of_scope", "rank": None}})
    bets.append({"week": 7, "game_id": "g7", "metric": "receptions", "result": "void", "pnl": None,
                 "fringe": {"tag": "fringe", "rank": 4}})
    out = fr.summarize(bets, final=False)
    assert out["fringe"]["bets"] == 600 and out["top"]["bets"] == 360
    assert out["descriptive"]["rushing_flagged_out_of_scope"]["bets"] == 1
