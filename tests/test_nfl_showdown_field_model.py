"""nfl-ownership-field-showdown-v2: the committed model reproduces the shared fixture."""
import json
from pathlib import Path

from model.nfl_showdown_field_model import (CAPTAIN_MULTIPLIER, KEEP_LINEUPS, POSITIONS, TOP_PLAYERS, VERSION, build, crowd_points,
                                            eligible, has_lines, predict, seat_points, seat_values)

ROOT = Path(__file__).resolve().parents[1]
MODEL = json.loads((ROOT / "web/src/data/nfl-showdown-field-model-v2.json").read_text(encoding="utf-8"))
PLAYERS = json.loads((ROOT / "tests/fixtures/nfl_showdown_field_model_fixture.json").read_text(encoding="utf-8"))["players"]


def test_committed_model_reproduces_fixture():
    assert MODEL["version"] == VERSION
    assert set(MODEL["flex_offsets"]) == set(MODEL["captain_offsets"]) == set(POSITIONS)
    got = predict(MODEL, PLAYERS)
    assert abs(sum(c for c, _ in got) - 100) < 1e-6 and abs(sum(f for _, f in got) - 500) < 1e-6
    for (captain, flex), row in zip(got, PLAYERS):
        assert abs(captain - row["captain"]) < 0.01 and abs(flex - row["flex"]) < 0.01
        assert captain + flex <= 100 + 1e-9


def test_only_the_top_players_of_a_plausible_lineup_are_owned():
    built = build(PLAYERS, MODEL)
    assert built["n"] == TOP_PLAYERS and len(built["cap"]) == KEEP_LINEUPS
    got = predict(MODEL, PLAYERS)
    for (captain, flex), row in zip(got, PLAYERS):
        if not eligible(row):
            assert captain == flex == 0


def test_stand_ins_and_the_lines_rule():
    kicker = next(p for p in PLAYERS if p["position"] == "K" and eligible(p))
    defense = next(p for p in PLAYERS if p["position"] == "DST" and eligible(p))
    assert crowd_points({**kicker, "pts": 1}, MODEL) == crowd_points({**kicker, "pts": 20}, MODEL)
    assert crowd_points({**kicker, "team_implied": 28}, MODEL) > crowd_points({**kicker, "team_implied": 17}, MODEL)
    assert crowd_points({**defense, "opponent_implied": 28}, MODEL) < crowd_points({**defense, "opponent_implied": 17}, MODEL)
    skill = {"position": "WR", "pts": 10.0, "anchor": "td_only"}
    assert crowd_points(skill, MODEL) == 10.0 * MODEL["fringe"]["td_only"]
    assert crowd_points({**skill, "anchor": None}, MODEL) == 10.0 * MODEL["fringe"]["none"]
    assert crowd_points(skill, MODEL, lined=False) == 10.0
    assert has_lines(PLAYERS) and not has_lines([{**p, "anchor": None} for p in PLAYERS])


def test_low_end_term_bends_seat_value_only_under_the_threshold():
    threshold, penalty = MODEL["low_end"]["threshold"], MODEL["low_end"]["penalty"]
    assert threshold > 0 and penalty > 0
    assert seat_points(threshold, MODEL) == threshold and seat_points(threshold + 6, MODEL) == threshold + 6
    assert abs(seat_points(threshold - 1.5, MODEL) - (threshold - 1.5 - penalty * 1.5)) < 1e-12
    # Kept players and the trim go by plain crowd points; the seat values carry the bend, captain at 1.5x.
    built = build(PLAYERS, MODEL)
    flex_value, captain_value = seat_values(built, MODEL)
    for k, pts in enumerate(built["pts"]):
        position = POSITIONS[built["position"][k]]
        bent = pts - penalty * max(0.0, threshold - pts)
        assert abs(flex_value[k] - (bent + MODEL["flex_offsets"][position])) < 1e-9
        assert abs(captain_value[k] - (CAPTAIN_MULTIPLIER * bent + MODEL["captain_offsets"][position])) < 1e-9
    assert any(pts < threshold for pts in built["pts"]), "the fixture slate exercises the term"


def test_a_pool_that_cannot_form_a_lineup_returns_none():
    ownable = [p for p in PLAYERS if eligible(p)]
    assert predict(MODEL, ownable[:5]) is None
    assert predict(MODEL, [{**p, "team": "ONE"} for p in PLAYERS]) is None
