"""The detector proposes; these tests pin what it may and may not propose."""
from __future__ import annotations

from model.nfl_dfs_removal import (
    appearances, classify, find, injuries_for, injury_events, name_keys,
    team_offensive_plays,
)

GAME = "2026_01_TB_ATL"


def part(play, role, name, team="TB", side="offense", game=GAME):
    return {"game_id": game, "play_id": play, "role": role, "player_name": name,
            "team": team, "side": side}


def drive(plays, passer="B.Mayfield", receiver="M.Evans", game=GAME):
    return [r for p in plays for r in
            (part(p, "passer", passer, game=game), part(p, "receiver", receiver, game=game))]


def quarters(plays, per_quarter=15, game=GAME):
    """Play 1-15 in Q1, 16-30 in Q2, and so on."""
    return {(game, p): min(4, (p - 1) // per_quarter + 1) for p in plays}


def injury(play, name, quarter=2, clock="11:42", game=GAME):
    return {"game_id": game, "play_id": play, "quarter": quarter, "clock": clock,
            "description": f"(11:42) Pass short right to {name}. {name} was injured "
                           "during the play. His return is Questionable."}


# ── Reading the injury out of the play text ───────────────────────────

def test_injury_name_is_read_from_the_description_not_inferred():
    events = injury_events([injury(20, "M.Evans")])
    assert [e.player_name for e in events] == ["M.Evans"]
    assert events[0].quarter == 2 and events[0].clock == "11:42"


def test_a_name_repeated_in_the_same_play_text_is_not_run_together():
    """The description names the receiver, then names him again as injured.
    A greedy match spanning the sentence boundary yields "M.Evans. M.Evans",
    which matches no player and silently drops the injury."""
    assert injury_events([injury(20, "M.Evans")])[0].player_name == "M.Evans"


def test_plays_without_an_injury_produce_no_event():
    assert injury_events([{"game_id": GAME, "play_id": 1, "quarter": 1, "clock": None,
                           "description": "B.Mayfield pass deep left to M.Evans for 30 yards."}]) == []


# ── Denominators ──────────────────────────────────────────────────────

def test_team_plays_are_deduplicated_across_roles():
    """One snap yields a passer row AND a receiver row. Counting rows would
    double the denominator and halve every share derived from it."""
    assert len(team_offensive_plays(drive(range(1, 11)))[(GAME, "TB")]) == 10


def test_defensive_and_non_ball_rows_are_excluded_from_the_denominator():
    rows = drive(range(1, 6)) + [part(99, "rusher", "X", side="defense"),
                                 part(50, "blocker", "T1")]
    assert team_offensive_plays(rows)[(GAME, "TB")] == [1, 2, 3, 4, 5]


# ── The four availability verdicts ────────────────────────────────────

def test_named_injured_and_never_seen_again_is_observed_not_inferred():
    rows = drive(range(1, 61))
    rows = [r for r in rows if not (r["player_name"] == "M.Evans" and r["play_id"] > 20)]
    index = appearances(rows, quarters(range(1, 61)))
    verdict = classify(position="WR", appearance=find(index, GAME, "Mike Evans"),
                       team_plays=team_offensive_plays(rows)[(GAME, "TB")],
                       injuries=injuries_for(injury_events([injury(20, "M.Evans")]), GAME, "Mike Evans"))
    assert verdict["verdict"] == "INJURED_OUT"
    assert verdict["confidence"] == "high"


def test_named_injured_but_seen_afterwards_counts_as_having_played():
    """A man who tweaked something and came back is, for this purpose, a man
    who played. We are removing the unavailable, not the uncomfortable."""
    rows = drive(range(1, 61))
    index = appearances(rows, quarters(range(1, 61)))
    verdict = classify(position="WR", appearance=find(index, GAME, "Mike Evans"),
                       team_plays=team_offensive_plays(rows)[(GAME, "TB")],
                       injuries=injuries_for(injury_events([injury(20, "M.Evans")]), GAME, "Mike Evans"))
    assert verdict["verdict"] == "INJURED_RETURNED"


def test_a_target_in_the_fourth_quarter_clears_a_player_at_any_position():
    """Presence is the strong test, and unlike absence it works at receiver."""
    rows = drive(range(1, 61))
    index = appearances(rows, quarters(range(1, 61)))
    verdict = classify(position="WR", appearance=find(index, GAME, "Mike Evans"),
                       team_plays=team_offensive_plays(rows)[(GAME, "TB")])
    assert verdict["verdict"] == "PLAYED_LATE"
    assert verdict["confidence"] == "high"


def test_early_exit_without_an_injury_note_stays_ambiguous():
    rows = drive(range(1, 61))
    rows = [r for r in rows if not (r["player_name"] == "M.Evans" and r["play_id"] > 20)]
    index = appearances(rows, quarters(range(1, 61)))
    verdict = classify(position="WR", appearance=find(index, GAME, "Mike Evans"),
                       team_plays=team_offensive_plays(rows)[(GAME, "TB")])
    assert verdict["verdict"] == "LAST_SEEN_EARLY"
    # Receiver absence must never reach a confident verdict.
    assert verdict["confidence"] == "low"


def test_the_same_absence_is_more_meaningful_at_quarterback():
    rows = drive(range(1, 61))
    rows = [r for r in rows if not (r["player_name"] == "B.Mayfield" and r["play_id"] > 20)]
    rows += [part(p, "passer", "K.Trask") for p in range(21, 61)]
    index = appearances(rows, quarters(range(1, 61)))
    verdict = classify(position="QB", appearance=find(index, GAME, "Baker Mayfield"),
                       team_plays=team_offensive_plays(rows)[(GAME, "TB")])
    assert verdict["verdict"] == "LAST_SEEN_EARLY"
    assert verdict["confidence"] == "medium"


def test_a_last_touch_near_the_whistle_is_the_game_ending_not_an_exit():
    rows = drive(range(1, 61))
    rows = [r for r in rows if not (r["player_name"] == "M.Evans" and r["play_id"] > 55)]
    index = appearances(rows, quarters(range(1, 61)))
    # Force the last quarter unknown so the plays-after rule is what decides.
    find(index, GAME, "Mike Evans").last_quarter = None
    verdict = classify(position="WR", appearance=find(index, GAME, "Mike Evans"),
                       team_plays=team_offensive_plays(rows)[(GAME, "TB")])
    assert verdict["verdict"] == "PLAYED_LATE"
    assert verdict["confidence"] == "medium"


def test_a_player_with_no_rows_is_absent_not_removed():
    rows = drive(range(1, 61))
    assert classify(position="WR", appearance=None,
                    team_plays=team_offensive_plays(rows)[(GAME, "TB")]
                    )["verdict"] == "NO_OPPORTUNITY"


# ── Identity ──────────────────────────────────────────────────────────

def test_the_two_sources_spell_names_differently_and_both_must_match():
    """The report card says "Colston Loveland"; play-by-play says
    "C.Loveland". A missed join looks exactly like a player who never touched
    the ball -- the one distinction this module exists to draw."""
    index = appearances([part(p, "receiver", "C.Loveland") for p in range(1, 6)], {})
    assert find(index, GAME, "Colston Loveland") is not None
    assert find(index, GAME, "C.Loveland") is not None
    assert find(index, GAME, "Rome Odunze") is None
    assert "cloveland" in name_keys("Colston Loveland")


def test_appearances_never_merge_across_games():
    """Two games in a week can carry the same surname. A cross-game merge
    would invent a late appearance that never happened, which is precisely
    the error that would clear an injured player."""
    rows = ([part(p, "receiver", "M.Evans", game="A") for p in range(1, 6)]
            + [part(p, "receiver", "M.Evans", game="B") for p in range(40, 61)])
    index = appearances(rows, {})
    assert find(index, "A", "M.Evans").last_play == 5
    assert find(index, "B", "M.Evans").last_play == 60


def test_an_injury_in_another_game_is_not_attached_to_this_player():
    events = injury_events([injury(20, "M.Evans", game="B")])
    assert injuries_for(events, "A", "M.Evans") == []
    assert len(injuries_for(events, "B", "Mike Evans")) == 1
