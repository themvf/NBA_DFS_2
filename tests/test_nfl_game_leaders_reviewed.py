"""Reviewed depth overrides survive the automatic rebuild and keep the captured provider data."""
import copy
import json
from pathlib import Path

from research.nfl_game_leaders_reviewed import REVIEWED_PATH, apply_reviewed, load_reviewed, with_reviewed_ranks


def request():
    return {"game": {"game_id": "G"}, "dual_depth": {"HOU": {"comparisons": [
        {"name": "Jawhar Jordan", "position": "RB", "fantasypros": {"rank": 2}, "sleeper": {"rank": 5}, "decision": "conflict"},
        {"name": "Dare Ogunbowale", "position": "RB", "fantasypros": {"rank": 3}, "sleeper": {"rank": 2}, "decision": "conflict"},
        {"name": "Woody Marks", "position": "RB", "fantasypros": {"rank": 1}, "sleeper": {"rank": 1}, "decision": "agree"}]}}}


REVIEW = {"HOU": {"RB": {"Dare Ogunbowale": 2, "Jawhar Jordan": 4}}, "reason": "two of three sources", "sources": {"espn": "x"},
          "reviewed_at": "2026-10-17T21:05:00+00:00"}


def fake_attach(req):
    ranks = {c["name"]: c["fantasypros"]["rank"] for c in req["dual_depth"]["HOU"]["comparisons"]}
    return {**req, "role_scenarios": {"HOU": {}}, "scenario_evidence": {"description": "Default.", "order": sorted(ranks, key=ranks.get)}}


def test_reviewed_ranks_drive_the_scenario_but_the_request_keeps_what_providers_said():
    original = request()
    out = apply_reviewed("G", original, fake_attach, {"G": REVIEW})
    assert out["scenario_evidence"]["order"] == ["Woody Marks", "Dare Ogunbowale", "Jawhar Jordan"]
    assert out["dual_depth"] == request()["dual_depth"]
    assert out["scenario_evidence"]["reviewed_override"]["sources"] == {"espn": "x"}
    assert "two of three sources" in out["scenario_evidence"]["description"]
    assert original == request()


def test_no_review_is_the_default_attachment():
    out = apply_reviewed("G", request(), fake_attach, {"OTHER": REVIEW})
    assert out["scenario_evidence"]["order"] == ["Woody Marks", "Jawhar Jordan", "Dare Ogunbowale"]
    assert "reviewed_override" not in out["scenario_evidence"]


def test_unknown_team_or_player_changes_nothing():
    adjusted = with_reviewed_ranks(request(), {"KC": {"RB": {"X": 1}}, "HOU": {"WR": {"Jawhar Jordan": 9}}})
    assert adjusted == request()


def test_checked_in_reviews_are_well_formed():
    for game_id, review in load_reviewed().items():
        assert game_id.count("_") == 3 and review["reason"] and review["sources"] and review["reviewed_at"]
        teams = [k for k in review if k not in ("reason", "sources", "reviewed_at")]
        assert teams and all(isinstance(review[t], dict) for t in teams)
    assert REVIEWED_PATH == Path("artifacts/nfl-game-leaders/reviewed-depth.json")
