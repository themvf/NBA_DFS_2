"""Slate-scoped report card: DraftKings' convention, with guards against
mistaking a lagging results feed for a slate of zeros."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from model.nfl_dfs_slate_reportcard import (
    COHORTS, VERSION, build_slate_report, cohort_for, latest_per_slate, pooled_summary,
)

KICK = datetime(2026, 9, 20, 17, tzinfo=timezone.utc)
NOW = KICK + timedelta(days=2)
UPLOAD = {"upload_id": "u1", "slate_signature": "sig", "format": "classic", "season": 2026, "week": 2,
          "model_version": "nfl-dfs-historical-v3", "projection_run_id": "run", "created_at": KICK - timedelta(days=1)}
GAMES = [{"id": 1, "game_key": "CAR@ATL", "kickoff": KICK, "completed": True},
         {"id": 2, "game_key": "SEA@ARI", "kickoff": KICK + timedelta(hours=3), "completed": True}]


def player(pid, **kw):
    base = {"ff_player_id": pid, "name": f"P{pid}", "position": "RB", "team": "ATL", "game_key": "CAR@ATL",
            "projection_status": "historical", "history_games": 20, "is_out": False,
            "our_proj": 10.0, "floor_fpts": 3.0, "ceiling_fpts": 20.0}
    return {**base, **kw}


def result(pid, gid=1, pts=12.0, **kw):
    return {"id": pid * 10, "player_id": pid, "game_id": gid, "position": "RB", "team": "ATL",
            "actual_dk_fpts": pts, "scoring_status": "exact", "computed_at": KICK + timedelta(hours=8), **kw}


def feed(gid=1):
    """The player feed having run for a game: one skill result per team, for
    players who are not on the slate (ids 900+)."""
    teams = {1: ("CAR", "ATL"), 2: ("SEA", "ARI")}[gid]
    return [result(900 + gid * 10 + i, gid=gid, pts=5.0, team=team, position="WR") for i, team in enumerate(teams)]


def test_missing_stat_row_scores_zero_only_when_the_game_has_results():
    players = [player(1), player(2), player(3, game_key="SEA@ARI")]
    rep = build_slate_report(upload=UPLOAD, players=players, games=GAMES, results=[result(1), *feed(1)], now=NOW)
    rows = {r["ff_player_id"]: r for r in rep["rows"]}
    assert rows[1]["actual"] == 12.0 and rows[1]["stat_row_present"]
    # Game 1 has results, so player 2's missing row is the 0 DK paid.
    assert rows[2]["actual"] == 0.0 and rows[2]["status"] == "scored" and not rows[2]["stat_row_present"]
    # Game 2 is completed but the results source has not run for it: unknown, not zero.
    assert rows[3]["status"] == "awaiting_source" and rows[3]["actual"] is None
    assert rep["scorable_games"] == 1 and rep["version"] == VERSION


def test_defense_rows_alone_do_not_make_a_game_scorable():
    """DST results come from the team-week feed. When the player feed lags, a
    game can carry both defenses and no skill player at all; grading that as
    'results-bearing' would score every listed skill player a DK 0."""
    players = [player(1), player(2, position="WR")]
    dst = [result(701, pts=6.0, position="DST", team="ATL"), result(702, pts=2.0, position="DST", team="CAR")]
    rep = build_slate_report(upload=UPLOAD, players=players, games=GAMES, results=dst, now=NOW)
    assert rep["scorable_games"] == 0
    assert {r["status"] for r in rep["rows"]} == {"awaiting_source"}
    assert all(r["actual"] is None for r in rep["rows"])
    assert rep["games_awaiting_player_feed"] == {"CAR@ATL": ["ATL", "CAR"]}


def test_one_team_without_player_results_holds_the_game_back():
    # ATL's skill rows are in, CAR's are not (a per-team identity failure):
    # CAR's listed players must not be graded as zeros.
    players = [player(1), player(2, team="CAR")]
    rep = build_slate_report(upload=UPLOAD, players=players, games=GAMES,
                             results=[result(1), result(701, position="DST", team="CAR")], now=NOW)
    assert rep["scorable_games"] == 0
    assert rep["games_awaiting_player_feed"] == {"CAR@ATL": ["CAR"]}
    # Once CAR has a skill result the game scores, and player 2's absence is the 0 DK paid.
    rep = build_slate_report(upload=UPLOAD, players=players, games=GAMES,
                             results=[result(1), result(801, team="CAR", position="QB")], now=NOW)
    rows = {r["ff_player_id"]: r for r in rep["rows"]}
    assert rep["scorable_games"] == 1 and rows[2]["actual"] == 0.0 and rows[2]["status"] == "scored"
    assert rep["games_awaiting_player_feed"] == {}


def test_team_aliases_count_toward_coverage():
    # Results and schedule spell four teams differently across sources.
    games = [{"id": 5, "game_key": "WSH@LAR", "kickoff": KICK, "completed": True}]
    rep = build_slate_report(upload=UPLOAD, players=[player(1, team="LAR", game_key="WSH@LAR")], games=games,
                             results=[result(1, gid=5, team="LA"), result(2, gid=5, team="WAS")], now=NOW)
    assert rep["scorable_games"] == 1


def test_result_rows_without_position_cannot_vouch_for_the_feed():
    bare = {k: v for k, v in result(1).items() if k != "position"}
    rep = build_slate_report(upload=UPLOAD, players=[player(1)], games=GAMES, results=[bare], now=NOW)
    assert rep["scorable_games"] == 0


def test_cohorts_separate_prior_rows_and_out_players():
    assert cohort_for(player(1, projection_status="position_prior", history_games=0)) == "hist_0"
    assert cohort_for(player(1, history_games=3)) == "hist_1_5"
    assert cohort_for(player(1, history_games=6)) == "hist_6_plus"
    assert cohort_for(player(1, is_out=True)) == "out"
    assert cohort_for(player(1, projection_status="out", our_proj=0.0)) == "out"
    players = [player(1, projection_status="position_prior", history_games=0, our_proj=8.0),
               player(2, is_out=True, our_proj=0.0), player(3)]
    rep = build_slate_report(upload=UPLOAD, players=players, games=GAMES, results=[result(3), *feed(1)], now=NOW)
    s = rep["summary"]["RB"]
    assert s["hist_0"]["scored"] == 1 and s["hist_0"]["bias_actual_minus_projected"] == pytest.approx(-8.0)
    assert s["out"]["scored"] == 1 and s["out"]["mae"] == 0.0
    assert s["hist_6_plus"]["scored"] == 1 and s["hist_6_plus"]["bias_actual_minus_projected"] == pytest.approx(2.0)
    assert set(s) == set(COHORTS)


def test_pending_game_and_unlinked_identity_are_not_scored():
    games = [dict(GAMES[0], completed=False, kickoff=NOW + timedelta(days=1))]
    rep = build_slate_report(upload=UPLOAD, players=[player(1), player(None)], games=games,
                             results=[result(1)], now=NOW)
    statuses = sorted(r["status"] for r in rep["rows"])
    assert statuses == ["pending_game", "unlinked_identity"]
    assert all(r["actual"] is None for r in rep["rows"])


def test_alternative_forecast_stream_grades_the_same_population():
    players = [player(1, our_proj=10.0), player(2, our_proj=10.0)]
    rep = build_slate_report(upload=UPLOAD, players=players, games=GAMES,
                             results=[result(1), result(2, pts=4.0), *feed(1)],
                             now=NOW, forecasts={1: {"mean": 12.0, "p10": 5.0, "p90": 20.0}, 2: {"mean": 4.0, "p10": 0.0, "p90": 9.0}})
    assert rep["forecast_stream"] == "alternative"
    rows = {r["ff_player_id"]: r for r in rep["rows"]}
    assert rows[1]["error"] == pytest.approx(0.0) and rows[2]["error"] == pytest.approx(0.0)
    # A player the alternative stream did not project is not scored under it.
    rep2 = build_slate_report(upload=UPLOAD, players=players, games=GAMES, results=[result(1), *feed(1)], now=NOW,
                              forecasts={1: {"mean": 12.0}})
    assert {r["ff_player_id"]: r["status"] for r in rep2["rows"]} == {1: "scored", 2: "no_projection"}


def test_latest_upload_per_slate_and_weeks_clustered_pooling():
    def rep(week, upload_id, created, errors):
        return {"season": 2026, "week": week, "format": "classic", "slate_signature": f"s{week}",
                "upload_id": upload_id, "upload_created_at": created,
                "rows": [{"position": "RB", "cohort": "hist_0", "error": e, "absolute_error": abs(e)} for e in errors]}
    reports = [rep(2, "old", "2026-09-19", [-100.0]), rep(2, "new", "2026-09-20", [-4.0, -6.0]),
               rep(3, "w3", "2026-09-27", [-5.0, -3.0])]
    kept = latest_per_slate(reports)
    assert {r["upload_id"] for r in kept} == {"new", "w3"}
    pooled = pooled_summary(reports, positions=("RB",), cohorts=("hist_0",), iters=300)
    cell = pooled["cells"]["RB:hist_0"]
    assert cell["n"] == 4 and cell["weeks"] == 2
    assert cell["bias"] == pytest.approx(-4.5)
    assert cell["bias_ci"] is not None and cell["bias_ci"][0] <= -4.5 <= cell["bias_ci"][1]
    # A single week cannot produce a clustered interval.
    single = pooled_summary(reports[:2], positions=("RB",), cohorts=("hist_0",), iters=300)
    assert single["cells"]["RB:hist_0"]["bias_ci"] is None
