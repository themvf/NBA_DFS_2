"""The detector proposes; these tests pin what it may and may not propose."""
from __future__ import annotations

from model.nfl_dfs_removal import (
    appearances, classify, silence_share, team_offensive_plays,
)


def row(play, role, name, team="CHI", side="offense", player_id=None):
    return {"play_id": play, "role": role, "player_name": name, "team": team,
            "side": side, "player_id": player_id}


def drive(team="CHI", plays=range(1, 41), passer="QB1", receiver="WR1"):
    out = []
    for p in plays:
        out.append(row(p, "passer", passer, team))
        out.append(row(p, "receiver", receiver, team))
    return out


def test_team_plays_are_deduplicated_across_roles():
    """One snap yields a passer row and a receiver row. Counting both would
    double the denominator and halve every share computed from it."""
    plays = team_offensive_plays(drive(plays=range(1, 11)))
    assert len(plays["CHI"]) == 10


def test_defensive_rows_never_count_as_team_offensive_plays():
    rows = drive(plays=range(1, 6)) + [row(99, "rusher", "X", "CHI", side="defense")]
    assert team_offensive_plays(rows)["CHI"] == [1, 2, 3, 4, 5]


def test_quarterback_who_stops_appearing_is_proposed_as_removed():
    rows = drive(plays=range(1, 41))
    rows = [r for r in rows if not (r["role"] == "passer" and r["play_id"] > 20)]
    rows += [row(p, "passer", "QB2") for p in range(21, 41)]
    app = appearances(rows)
    verdict = classify(position="QB", appearance=app["name:QB1"],
                       team_plays=team_offensive_plays(rows)["CHI"])
    assert verdict["verdict"] == "LIKELY_REMOVED"
    assert verdict["confidence"] == "high"


def test_backup_quarterback_absent_late_is_not_called_a_removal():
    """The relief QB never held a starter's snap share, so his silence in the
    first half is ordinary. Calling it a removal would invert the story."""
    rows = [row(p, "passer", "QB1") for p in range(1, 36)]
    rows += [row(p, "passer", "QB2") for p in range(36, 41)]
    rows += [row(p, "receiver", "WR1") for p in range(1, 41)]
    # QB2 appears only at the end; invert to make him the one who vanishes.
    rows = [r for r in rows if not (r["player_name"] == "QB2")]
    rows += [row(p, "passer", "QB2") for p in range(1, 6)]
    app = appearances(rows)
    verdict = classify(position="QB", appearance=app["name:QB2"],
                       team_plays=team_offensive_plays(rows)["CHI"])
    assert verdict["verdict"] == "UNKNOWN"


def test_targeted_receiver_with_no_catch_is_not_an_availability_problem():
    """The Loveland case: the ball came and did not stick."""
    rows = drive(plays=range(1, 41))
    app = appearances(rows)
    verdict = classify(position="TE", appearance=app["name:WR1"],
                       team_plays=team_offensive_plays(rows)["CHI"], receptions=0)
    assert verdict["verdict"] == "OPPORTUNITY_NO_CONVERSION"
    assert verdict["evidence"]["targets"] == 40


def test_receiver_never_targeted_is_no_opportunity_not_removal():
    rows = drive(plays=range(1, 41))
    app = appearances(rows)
    assert classify(position="WR", appearance=None,
                    team_plays=team_offensive_plays(rows)["CHI"])["verdict"] == "NO_OPPORTUNITY"


def test_receiver_silence_is_never_proposed_as_a_removal():
    """Participation rows record touches, not snaps. A receiver can run a full
    second half of routes without producing one row, so silence at this
    position must never reach the same verdict it reaches at quarterback."""
    rows = drive(plays=range(1, 41))
    rows = [r for r in rows if not (r["role"] == "receiver" and r["play_id"] > 10)]
    app = appearances(rows)
    verdict = classify(position="WR", appearance=app["name:WR1"],
                       team_plays=team_offensive_plays(rows)["CHI"])
    assert verdict["verdict"] == "UNKNOWN"
    assert "touches, not snaps" in verdict["reason"]


def test_silence_share_is_none_without_team_plays():
    """No denominator means no share — not a zero, which would read as
    'played to the whistle'."""
    app = appearances(drive(plays=range(1, 5)))["name:WR1"]
    assert silence_share(app, []) is None
