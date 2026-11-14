from datetime import datetime, timezone, timedelta
from model.nfl_dfs_reportcard import build_report

KICK = datetime(2026,9,13,17,tzinfo=timezone.utc)
GAME = dict(id=1, kickoff=KICK, completed=True, home_team="BUF", away_team="MIA")
PLAYER = dict(player_id=1, name="Example", position="WR", team="BUF")


def forecast(**kw):
    return dict(**{**PLAYER, "forecast_id": "1", "variant": "production", "mean": 10.,
                "p10": 2., "p90": 20., "captured_at": KICK-timedelta(hours=1), "stat_means": {"receptions": 5}, **kw})


def result(**kw):
    return dict(**{"id": 1, "player_id": 1, "game_id": 1, "team": "BUF", "position": "WR", "computed_at": KICK+timedelta(hours=8),
        "actual_dk_fpts": 12., "scoring_status": "exact", "scoring_version": "v1", "input_digest": "r1",
        "scoring_evidence": {"scoring_input": {"receptions": 6}}, **kw})


def report(forecasts=None, results=None, **kw):
    return build_report(**{"season": 2026, "week": 1, "games": [GAME], "players": [PLAYER],
        "forecasts": forecasts if forecasts is not None else [forecast()],
        "results": results if results is not None else [result()], "now": KICK+timedelta(days=3), **kw})


def test_latest_valid_snapshot_once_and_variants_separate():
    r = report([forecast(), forecast(forecast_id="2",mean=11.,captured_at=KICK-timedelta(minutes=2)),
                forecast(forecast_id="3",mean=100.,captured_at=KICK)])
    assert r["summary"]["production"]["scored"] == 1
    assert r["rows"][0]["error"] == 1
    assert r["summary"]["opportunity"]["forecasted"] == 0
    assert r["rejected_non_pregame_snapshots"] == 1


def test_corrected_outcome_never_changes_forecast_and_latest_exclusion_wins():
    original = report()["rows"][0]
    corrected = report(results=[result(),result(id=2, actual_dk_fpts=0,computed_at=KICK+timedelta(days=2))])["rows"][0]
    assert corrected["forecast"] == original["forecast"]
    assert corrected["status"] == "corrected" and corrected["actual"] == 0
    excluded = report(results=[result(),result(id=2,actual_dk_fpts=None,scoring_status="excluded",computed_at=KICK+timedelta(days=2))])["rows"][0]
    assert excluded["actual"] is None and excluded["status"] == "excluded_scoring"


def test_missing_stats_is_not_zero_and_is_overdue_after_grace():
    row = report(results=[])["rows"][0]
    assert row["actual"] is None and row["error"] is None
    assert row["status"] == "awaiting_source" and row["overdue"]
    pending = report(results=[],games=[{**GAME,"completed":False}],now=KICK-timedelta(hours=1))["rows"][0]
    assert pending["status"] == "pending_game" and not pending["overdue"]


def test_forecasted_inactive_player_retained_components_not_recreated():
    row = report(players=[])["rows"][0]
    assert row["player_id"] == 1 and row["components"][0]["error"] == 1
    assert report([forecast(stat_means={})])["rows"][0]["components"] == []


def test_no_forecast_is_in_denominator_not_graded_as_zero():
    r = report(forecasts=[])
    assert r["summary"]["production"]["players"] == 1
    assert r["summary"]["production"]["scored"] == 0
    assert r["rows"][0]["status"] == "forecast_unavailable"


def test_future_correction_cannot_change_an_earlier_evaluation():
    r = report(results=[result(), result(id=2, actual_dk_fpts=99,computed_at=KICK+timedelta(days=4))])
    assert r["rows"][0]["actual"] == 12


def test_context_variants_share_exact_shadow_capture_and_missing_stays_missing():
    old = forecast(variant="shadow_baseline", forecast_id="old", run_id="pin")
    context = {**old, "variant": "context:opp_carries", "mean": 12}
    r = report([old, context])
    assert r["summary"]["context:opp_carries"]["scored"] == 1
    newer = {**old, "forecast_id": "new", "captured_at": KICK-timedelta(minutes=1)}
    r = report([old, context, newer])
    assert r["summary"]["context:opp_carries"]["forecasted"] == 0


def test_snapshots_the_loader_dropped_still_count_as_rejected():
    # ingest.nfl_dfs_reportcard loads only the selected production forecast and
    # passes how many non-pregame rows it dropped; the report must count them.
    r = report([forecast(), forecast(forecast_id="3", mean=100., captured_at=KICK)], prior_rejected=7)
    assert r["rejected_non_pregame_snapshots"] == 8
    assert r["rows"][0]["forecast"]["forecast_id"] == "1"


def test_a_report_that_only_changed_its_evaluation_time_is_not_rewritten():
    from ingest.nfl_dfs_reportcard import same_content
    import json
    first = report()
    later = report(now=KICK + timedelta(days=3, minutes=30))
    stored = json.loads(json.dumps(first, default=str))
    assert later["evaluated_at"] != first["evaluated_at"]
    assert same_content(stored, later)
    changed = report(results=[result(actual_dk_fpts=20.)])
    assert not same_content(stored, changed)
    assert not same_content(None, later)


OTHER = dict(id=2, kickoff=KICK, completed=True, home_team="KC", away_team="DEN")


def test_live_roster_team_never_places_a_player_in_a_played_game():
    # 2026-11-10/11: Josh Downs (IND in week 1, KC since) was listed in week 1
    # twice, under IND (forecast) and KC (live roster), whenever the last
    # roster writer had moved him; the card flipped 10440/10530 rows run to run.
    traded = {**PLAYER, "team": "KC"}
    stale = report(games=[GAME, OTHER], players=[PLAYER])
    moved = report(games=[GAME, OTHER], players=[traded])
    assert {(r["team"], r["game_id"]) for r in moved["rows"]} == {("BUF", 1)}
    assert moved["rows"] == stale["rows"]


def test_played_game_population_is_forecasts_plus_result_rows():
    # A rostered player with no forecast and no stat row is not gradable.
    bench = dict(player_id=2, name="Bench", position="TE", team="BUF")
    r = report(players=[PLAYER, bench])
    assert {row["player_id"] for row in r["rows"]} == {1}
    # A result-only player (no forecast, gone from the live roster) is kept
    # under the game his result names, with that game's opponent.
    played = result(id=7, player_id=3, game_id=2, team="DEN", position="RB", player_name="Result Only")
    r = report(games=[GAME, OTHER], results=[result(), played])
    row = next(x for x in r["rows"] if x["player_id"] == 3 and x["variant"] == "production")
    assert (row["team"], row["opponent"], row["name"], row["status"]) == ("DEN", "KC", "Result Only", "forecast_unavailable")
    assert row["actual"] == 12.
    kicker = result(id=8, player_id=4, game_id=2, team="DEN", position="K")
    assert all(x["player_id"] != 4 for x in report(games=[GAME, OTHER], results=[kicker])["rows"])


def test_live_roster_still_seeds_games_not_yet_kicked_off():
    upcoming = report(forecasts=[], results=[], games=[{**GAME, "completed": False}], now=KICK - timedelta(hours=2))
    assert {row["player_id"] for row in upcoming["rows"]} == {1}
    assert upcoming["rows"][0]["status"] == "forecast_unavailable"


# ── a settled week is left alone when nothing it reads has changed ─────────
# 2026-11-12: every research run rebuilt every week's card from scratch
# (656 MB read over ten weeks) to arrive at a card it already had.
import json

import pytest

from ingest import nfl_dfs_reportcard as rc
from model.nfl_dfs_reportcard import GRACE_HOURS

SETTLED_NOW = KICK + timedelta(hours=GRACE_HOURS, minutes=1)


def test_a_week_settles_only_when_every_game_is_final_and_past_the_grace_window():
    late = {**GAME, "id": 2, "kickoff": KICK + timedelta(days=1)}
    assert rc.settled([GAME], SETTLED_NOW)
    assert not rc.settled([GAME], KICK + timedelta(hours=GRACE_HOURS))
    assert not rc.settled([GAME, late], SETTLED_NOW)
    assert not rc.settled([{**GAME, "completed": False}], SETTLED_NOW)
    assert not rc.settled([], SETTLED_NOW)


def test_a_settled_card_does_not_depend_on_the_clock_or_the_live_roster():
    # The two facts the skip rests on. `overdue` is the last clock-dependent field.
    first = report(results=[], now=SETTLED_NOW)
    much_later = report(results=[], now=SETTLED_NOW + timedelta(days=40))
    assert rc.content_digest(first) == rc.content_digest(much_later)
    bench = dict(player_id=2, name="Bench", position="TE", team="MIA")
    assert rc.content_digest(report(players=[], now=SETTLED_NOW)) == rc.content_digest(report(players=[PLAYER, bench], now=SETTLED_NOW))
    # Before the week settles, the clock does change the card.
    assert rc.content_digest(report(results=[], now=KICK + timedelta(hours=3))) != rc.content_digest(first)


def test_content_digest_ignores_only_the_evaluation_time():
    stored = json.loads(json.dumps(report(), default=str))
    later = report(now=KICK + timedelta(days=4))
    assert rc.content_digest(later) == rc.content_digest(stored) and rc.same_content(stored, later)
    assert rc.content_digest(report(results=[result(actual_dk_fpts=20.)])) != rc.content_digest(stored)


class Ledger:
    """The tables the loader reads, answered by table name; every statement is kept."""

    def __init__(self, **tables):
        self.tables, self.statements = tables, []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "md5(" in sql:
            return [{"rejected": 0, "production": "p", "shadows": "s", "efficiency": "e", "results": "r"}]
        for marker, table in (("FROM nfl_dfs_shadow_predictions", "shadows"), ("FROM nfl_dfs_efficiency_runs", "efficiency"),
                              ("FROM nfl_dfs_player_week_results", "results"), ("FROM pick JOIN", "production"),
                              ("count(*) FROM m", "rejected"), ("FROM ff_players WHERE season", "players"),
                              ("FROM nfl_season_games g JOIN", "games")):
            if marker in sql:
                return self.tables.get(table, [])
        raise AssertionError(f"unexpected query: {sql[:90]}")


def shadow(player_id, **payload):
    return {"id": player_id, "player_id": player_id, "study_run_id": "pin", "captured_at": KICK - timedelta(hours=2),
            "kickoff": KICK, "input_digest": "d", "current_team": "KC",
            "payload": {"player_name": "Example", "position": "WR", "history_games": 9, "history_digest": "h",
                        "source_study_digest": "s", "baseline": 10., "p10": 2., "p90": 20., "boom_probability": .1, **payload}}


def efficiency(*identities):
    players = [{"identity": identity, "name": "Example", "position": "WR", "mean_fpts": 9., "median_fpts": 8.,
                "p10_fpts": 1., "p90_fpts": 19., "boom_rate": .1, "history_games": 9, "stat_means": {},
                "coherence_scope": "team", "seed": 1} for identity in identities]
    return {"run_digest": "run", "as_of_at": KICK - timedelta(hours=3),
            "payload": {"version": "e1", "dataset_digest": "d", "workload_run_digest": "w", "config": {},
                        "forecasts": [{"team": "BUF", "players": players}]}}


ROSTER = [dict(player_id=1, gsis_id="00-1", name="Example", position="WR", team="BUF"),
          dict(player_id=9, gsis_id=None, name="Bills", position="DST", team="BUF")]


def test_the_build_records_exactly_what_it_asked_the_live_roster():
    db = Ledger(games=[GAME], players=ROSTER, rejected=[{"n": 0}],
                shadows=[shadow(1, team="BUF"), shadow(2)],  # player 2's stored forecast carries no team
                efficiency=[efficiency("00-1", "00-gone", "DST:BUF")])
    data, lookups = rc.inputs(db, 2026, 1, SETTLED_NOW)
    assert lookups == {"identities": {"00-1": 1, "00-gone": None, "DST:BUF": 9}, "team_fallback": {"2": "KC"}}
    assert {f["variant"] for f in data["forecasts"]} == {"shadow_baseline", "efficiency_research"}
    # Asking the same questions of the same roster gives the same answers...
    keys = {"identities": sorted(lookups["identities"]), "team_fallback": [2]}
    assert rc.roster_lookups(keys, rc.identity_map(ROSTER), {2: "KC"}) == lookups
    # ...a player the week never asked about changes nothing...
    signed = ROSTER + [dict(player_id=50, gsis_id="00-new", name="New", position="RB", team="MIA")]
    assert rc.roster_lookups(keys, rc.identity_map(signed), {2: "KC"}) == lookups
    # ...and one it did ask about does.
    assert rc.roster_lookups(keys, rc.identity_map(signed + [dict(player_id=51, gsis_id="00-gone", name="Back", position="WR", team="BUF")]), {2: "KC"}) != lookups
    assert rc.roster_lookups(keys, rc.identity_map(ROSTER), {2: "DEN"}) != lookups


def test_evidence_names_every_table_the_loader_reads():
    import re

    db = Ledger(games=[GAME], players=ROSTER, rejected=[{"n": 0}])
    rc.inputs(db, 2026, 1, SETTLED_NOW)
    loaded = set(re.findall(r"\b(?:nfl|ff)_[a-z_]+", " ".join(db.statements)))
    check = Ledger()
    first = rc.evidence(check, 2026, 1, SETTLED_NOW, [GAME], True)
    named = set(re.findall(r"\b(?:nfl|ff)_[a-z_]+", " ".join(check.statements)))
    # Games are passed in and hashed as values; the roster is covered by the recorded lookups.
    assert loaded - named <= {"nfl_season_games", "nfl_teams"}, loaded - named
    assert first == rc.evidence(Ledger(), 2026, 1, SETTLED_NOW, [dict(GAME)], True)
    assert first != rc.evidence(Ledger(), 2026, 1, SETTLED_NOW, [GAME], False)
    assert first != rc.evidence(Ledger(), 2026, 1, SETTLED_NOW, [{**GAME, "completed": False}], True)
    assert first != rc.evidence(Ledger(), 2026, 2, SETTLED_NOW, [GAME], True)


class Builds:
    """`unchanged` reads three things: the recorded builds, the newest card, and current teams."""

    def __init__(self, builds, newest="card-1", teams=None):
        self.builds, self.newest, self.teams = builds, newest, teams or {}

    def execute(self, sql, params=None):
        if "FROM nfl_dfs_report_card_builds" in sql:
            return self.builds
        if "FROM nfl_dfs_weekly_report_cards" in sql:
            return [{"report_digest": self.newest}] if self.newest else []
        if "FROM ff_players WHERE id = ANY" in sql:
            asked = {int(value) for value in params[0].strip("{}").split(",")}
            return [{"id": player, "team_abbrev": team} for player, team in self.teams.items() if player in asked]
        raise AssertionError(sql[:90])


def build(lookups, card="card-1", age=timedelta(hours=7)):
    keys = {"identities": sorted(lookups["identities"]), "team_fallback": sorted(int(p) for p in lookups["team_fallback"])}
    return {"roster_keys": keys, "roster_digest": rc.artifact_digest(lookups), "report_digest": card, "built_at": SETTLED_NOW - age}


def test_a_week_is_skipped_only_when_a_rebuild_would_reproduce_the_newest_card():
    lookups = {"identities": {"00-1": 1, "00-gone": None}, "team_fallback": {"2": "KC"}}
    identities = {"00-1": 1}

    def skip(db, known=identities):
        return rc.unchanged(db, 2026, 1, "evidence", known, SETTLED_NOW)

    assert skip(Builds([build(lookups)], teams={2: "KC"})) == "card-1"
    # A roster change the week never asked about does not rebuild it.
    assert skip(Builds([build(lookups)], teams={2: "KC"}), {**identities, "00-new": 50}) == "card-1"
    # Nothing recorded for these stored rows.
    assert skip(Builds([])) is None
    # The card those inputs produced is no longer the newest one.
    assert skip(Builds([build(lookups)], newest="card-2", teams={2: "KC"})) is None
    # The roster now answers one of the week's own lookups differently.
    assert skip(Builds([build(lookups)], teams={2: "KC"}), {"00-1": 1, "00-gone": 77}) is None
    assert skip(Builds([build(lookups)], teams={2: "DEN"})) is None
    # Old enough to be rebuilt as a check.
    assert skip(Builds([build(lookups, age=rc.RECHECK_AFTER)], teams={2: "KC"})) is None
    # Two roster states recorded for the same rows: either one matches.
    other = {"identities": {"00-1": 1, "00-gone": None}, "team_fallback": {"2": "DEN"}}
    assert skip(Builds([build(other), build(lookups)], teams={2: "KC"})) == "card-1"


class Job:
    """Runs `rc.main` with the database replaced; records what the job did."""

    def __init__(self, monkeypatch, *, settled=True, unchanged=None, evidence=("e1", "e1"), previous=None, argv=()):
        self.calls, self.evidence = [], list(evidence)
        monkeypatch.setattr(rc, "load_config", lambda: type("C", (), {"database_url": "unused"})())
        monkeypatch.setattr(rc, "PipelineDatabase", lambda url, initialize_schema: type("D", (), {"execute": lambda self, sql, params=None: [{"week": 1}]})())
        monkeypatch.setattr(rc, "roster", lambda db, season: ROSTER)
        monkeypatch.setattr(rc, "week_games", lambda db, season, week: [GAME])
        monkeypatch.setattr(rc, "settled", lambda games, now: settled)
        monkeypatch.setattr(rc, "evidence", lambda *a: self.calls.append("evidence") or self.evidence.pop(0))
        monkeypatch.setattr(rc, "unchanged", lambda *a: self.calls.append("unchanged") or unchanged)
        monkeypatch.setattr(rc, "inputs", lambda db, season, week, now, **kw: self.calls.append("inputs") or (
            dict(games=[GAME], players=[PLAYER], forecasts=[forecast()], results=[result()], prior_rejected=0),
            {"identities": {}, "team_fallback": {}}))
        monkeypatch.setattr(rc, "persist", lambda db, report: self.calls.append("persist") or ("card-2", "content-2"))
        monkeypatch.setattr(rc, "remember", lambda *a: self.calls.append("remember") or previous)
        import ingest.nfl_dfs_storage as storage
        monkeypatch.setattr(storage, "prune_report_cards", lambda db, season, apply: self.calls.append("prune") or {"deleted": 0})
        monkeypatch.setattr("sys.argv", ["reportcard", "--season", "2026", *argv])


def test_an_unchanged_settled_week_reads_nothing_and_says_so(monkeypatch, capsys):
    job = Job(monkeypatch, unchanged="card-1")
    rc.main()
    assert job.calls == ["evidence", "unchanged", "prune"]
    out = capsys.readouterr().out
    assert '"unchanged": true' in out and "::notice title=NFL report cards unchanged::weeks [1] of [1]" in out


def test_a_changed_settled_week_is_rebuilt_and_recorded(monkeypatch):
    job = Job(monkeypatch)
    rc.main()
    assert job.calls == ["evidence", "unchanged", "inputs", "persist", "evidence", "remember", "prune"]


def test_rebuild_flag_and_unsettled_weeks_always_build(monkeypatch):
    forced = Job(monkeypatch, unchanged="card-1", argv=["--rebuild"])
    rc.main()
    assert "unchanged" not in forced.calls and "inputs" in forced.calls and "remember" in forced.calls
    current = Job(monkeypatch, settled=False, unchanged="card-1")
    rc.main()
    # An unsettled week is never skipped; its record only spares `persist` a read, so no second evidence pass.
    assert current.calls == ["evidence", "inputs", "persist", "remember", "prune"]


def test_rows_that_changed_during_the_build_are_not_recorded(monkeypatch):
    job = Job(monkeypatch, evidence=("e1", "e2"))
    rc.main()
    assert "persist" in job.calls and "remember" not in job.calls


def test_the_same_inputs_giving_a_different_card_fails_the_job(monkeypatch):
    job = Job(monkeypatch, previous="content-1")
    with pytest.raises(SystemExit) as failure:
        rc.main()
    assert "same recorded inputs produced a different card" in str(failure.value)
    assert job.calls[-1] == "prune"  # the fresh card is stored and trimmed before the job fails
    same = Job(monkeypatch, previous="content-2")
    rc.main()


def test_a_dry_run_never_touches_the_build_record(monkeypatch):
    job = Job(monkeypatch, unchanged="card-1", argv=["--dry-run"])
    rc.main()
    assert job.calls == ["inputs"]


def test_a_result_dated_after_the_run_started_blocks_the_record():
    data = dict(results=[result(computed_at=SETTLED_NOW + timedelta(seconds=5))], forecasts=[forecast()])
    assert rc.dated_after(data, SETTLED_NOW)
    assert not rc.dated_after(dict(results=[result()], forecasts=[forecast(), forecast(captured_at=None)]), SETTLED_NOW)
