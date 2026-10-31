"""The research grader (Codex's revised pre-tip rules) used only by the NBA ghost research code.

The live nightly grade stays model/nba_points_calibration.py (nba-points-grade-v1),
which the registered ladder-v2 tail test reads; nba-points-grade-v2 is reserved
for that test's outcome, so this module carries its own version.
"""
from datetime import date, timedelta

from research import nba_points_research_grade as m
from datetime import datetime, timezone

from model.nba_points_ladder import name_key

TIP = datetime(2026, 10, 21, 23, 0, tzinfo=timezone.utc)
CAP = datetime(2026, 10, 21, 22, 7, tzinfo=timezone.utc)


def _books(line=24.5, over=-110, under=-110):
    return {"draftkings": {"line": line, "over": over, "under": under}, "fanduel": {"line": line, "over": over, "under": under}}


def _capture(player):
    return m.Capture(event_id="evt1", commence_time=TIP, captured_at=CAP, home_team="Orlando Magic", away_team="Atlanta Hawks",
                     player=player, books=_books())


def _game(players):
    return m.BoxGame(espn_event_id="401909834", home_team="Orlando Magic", away_team="Atlanta Hawks", players=players)


def _player(name, team, points):
    return m.BoxPlayer(player=name, player_key=name_key(name), team=team, is_home=False, played=True, points=points, minutes=30.0)


def test_version_does_not_take_the_registered_name():
    assert m.GRADE_VERSION == "nba-points-research-grade-v1"


def test_verified_alias_is_matched_but_collisions_still_fail_closed():
    cap = _capture("Herb Jones")
    game = _game([_player("Herbert Jones", "Atlanta Hawks", 5)])
    row = m.grade_captures(date(2026, 10, 21), [cap], {}, [game])[0]
    assert row.status == "graded" and row.actual_points == 5
    game.players.append(_player("Herb Jones", "Orlando Magic", 10))
    assert m.grade_captures(date(2026, 10, 21), [cap], {}, [game])[0].status == "ambiguous"


def test_provider_start_disagreement_reselects_whole_snapshot_without_stale_players():
    game = _game([])
    game.start_time = TIP - timedelta(minutes=10)

    def source(player, when):
        return dict(event_id="evt1", market="player_points", player=player, books=_books(),
                    commence_time=TIP, captured_at=when, home_team_name=game.home_team, away_team_name=game.away_team)
    rows = [source("Removed", TIP - timedelta(hours=2)), source("A", TIP - timedelta(hours=1)),
            source("A", TIP - timedelta(minutes=6)), source("B", TIP - timedelta(minutes=6)),
            source("At boundary", game.start_time)]
    selected = m.select_pretip_rows(rows, [game])
    assert [(r["player"], r["captured_at"]) for r in selected] == [("A", TIP - timedelta(hours=1))]


def test_direct_grading_rejects_capture_after_either_known_scheduled_start():
    cap = _capture("A")
    game = _game([_player("A", "Atlanta Hawks", 31)])
    game.start_time = CAP
    row = m.grade_captures(date(2026, 10, 21), [cap], {}, [game])[0]
    assert row.status == "after_start" and row.actual_points is None
    assert m.summarize([row])["cells"]["over"]["n"] == 0
