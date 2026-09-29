"""The pre-kickoff rule as the real build_week runs it, against a stub database."""
from datetime import datetime, timedelta, timezone

import pytest

import ingest.nfl_dfs_projections as proj
from model.nfl_dfs_historical import HistoricalWeek

KICKOFF = datetime(2026, 9, 20, 17, 0, tzinfo=timezone.utc)
TUESDAY = KICKOFF - timedelta(days=5)
AFTER = KICKOFF + timedelta(hours=1)

def week(pid, name, wk, attempts, yards, tds):
    return HistoricalWeek(pid, f"00-{pid}", name, "QB", 2025, wk, "LAR", "SEA",
                          {"attempts": attempts, "passing_yards": yards, "passing_tds": tds,
                           "passing_interceptions": 0.6, "rushing_yards": 8, "rushing_tds": 0.1,
                           "receiving_yards": 0, "receiving_tds": 0, "receptions": 0,
                           "fumbles_lost_total": 0.2, "carries": 2})

HISTORY = ([week(1, "Starter", w, 34, 272, 1.8) for w in range(1, 9)] +
           [week(2, "Backup", w, 9, 54, 0.3) for w in range(1, 9)])

class StubDB:
    """Answers build_week's four queries by matching on SQL text."""
    def __init__(self, captured_at):
        self.captured_at = captured_at
    def execute(self, sql, params=None):
        if "ff_player_injury_observations" in sql:
            return [{"observation_id": 10, "player_id": 1, "source": "sleeper",
                     "status": "OUT", "source_snapshot_id": 20,
                     "model_eligible": True, "snapshot_status": "success",
                     "available_at": self.captured_at, "game_scope_valid": True}]
        if "ff_source_snapshots" in sql and "DISTINCT ON (season,dataset)" in sql:
            return []
        return []

def run(captured_at, config=None, *, as_of_at=TUESDAY, kickoff=KICKOFF):
    monkey = {
        "_history": lambda db, season, wk: HISTORY,
        "_slate_environment": lambda db, season, wk: {
            "LAR": {"opponent": "SEA", "event_id": "e1", "commence_time": kickoff,
                    "team_implied_total": 24.0}},
        "_players": lambda db, season, teams: [
            {"id": 1, "gsis_id": "00-1", "canonical_name": "Starter", "normalized_name": "starter",
             "position": "QB", "team_abbrev": "LAR", "depth_order": 1, "roster_fetched_at": TUESDAY},
            {"id": 2, "gsis_id": "00-2", "canonical_name": "Backup", "normalized_name": "backup",
             "position": "QB", "team_abbrev": "LAR", "depth_order": 2, "roster_fetched_at": TUESDAY}],
    }
    originals = {name: getattr(proj, name) for name in monkey}
    for name, fn in monkey.items():
        setattr(proj, name, fn)
    try:
        return proj.build_week(StubDB(captured_at), season=2026, week=3,
                               as_of_at=as_of_at, seed=7, config=config)
    finally:
        for name, fn in originals.items():
            setattr(proj, name, fn)

def by_name(projections):
    return {p["player_name"]: p for p in projections}

def test_a_tuesday_out_zeroes_the_starter_and_promotes_the_backup():
    projections, manifest = run(TUESDAY)
    players = by_name(projections)
    assert players["Starter"]["model_proj_fpts"] == 0.0
    assert players["Starter"]["projection_status"] == "out"
    assert players["Backup"]["availability"]["applied"] is True
    # The invariant: he ends up at exactly the volume the starter was projected
    # for. (Not the starter's RAW 34 — the starter's own projection is blended
    # with the peer pool, which in a two-quarterback fixture is the backup.)
    note = players["Backup"]["availability"]
    assert players["Backup"]["stat_means"]["attempts"] == pytest.approx(
        note["target_opportunity"], rel=1e-3)
    assert note["target_opportunity"] > note["current_opportunity"], "volume went up"
    assert manifest["availability"]["zeroed"][0]["player"] == "Starter"
    assert manifest["availability"]["transfers"][0]["to"] == "Backup"

def test_the_backup_gains_but_does_not_become_the_starter():
    projections, _ = run(TUESDAY)
    backup = by_name(projections)["Backup"]
    baseline, _ = run(AFTER)   # same seed, rule not applied
    untouched = by_name(baseline)["Backup"]
    assert backup["model_proj_fpts"] > untouched["model_proj_fpts"]
    assert backup["model_proj_fpts"] < by_name(baseline)["Starter"]["model_proj_fpts"]

def test_a_status_published_after_kickoff_is_ignored():
    """The guard that keeps this from becoming a leak."""
    projections, manifest = run(AFTER)
    assert by_name(projections)["Starter"]["model_proj_fpts"] > 0
    assert manifest["availability"]["zeroed"] == []

def test_a_wednesday_capture_cannot_affect_a_tuesday_decision():
    """Decision time, not merely kickoff, is the feature cutoff."""
    projections, manifest = run(TUESDAY + timedelta(days=1))
    assert by_name(projections)["Starter"]["model_proj_fpts"] > 0
    decision = manifest["availability_decisions"]["1"]
    assert decision["state"] == "UNKNOWN"
    assert decision["display_only_observation_ids"] == [10]

def test_ineligible_fantasypros_cannot_zero_or_create_a_decision_conflict():
    rows = [
        {"observation_id": 1, "source": "sleeper", "status": "HEALTHY",
         "available_at": TUESDAY, "model_eligible": True, "snapshot_status": "success"},
        {"observation_id": 2, "source": "fantasypros", "status": "OUT",
         "available_at": TUESDAY, "model_eligible": False, "snapshot_status": "success"},
    ]
    decision = proj.resolve_game_availability(rows, as_of_at=TUESDAY, kickoff=KICKOFF)
    assert decision.state == "EXPECTED_ACTIVE"
    assert decision.projection_status is None
    assert decision.display_only_observation_ids == (2,)

def test_the_manifest_records_what_was_done():
    _, manifest = run(TUESDAY)
    report = manifest["availability"]
    assert report["version"] and report["transfers"][0]["opportunity_key"] == "attempts"
    assert "multiplier" in report["transfers"][0]
    assert manifest["availability_health"]["state_counts"] == {"OUT_CONFIRMED": 1, "UNKNOWN": 1}
    assert "direct ineligible FantasyPros reads remain disabled" in manifest["availability_health"]["rollback_policy"]

def test_safety_rollback_preserves_out_zero_but_disables_transfer():
    projections, manifest = run(TUESDAY, {"availability_qb_transfer_enabled": False})
    players = by_name(projections)
    assert players["Starter"]["model_proj_fpts"] == 0
    untouched, _ = run(AFTER, {"availability_qb_transfer_enabled": False})
    assert players["Backup"]["model_proj_fpts"] == by_name(untouched)["Backup"]["model_proj_fpts"]
    assert manifest["availability"]["policy_mode"] == "safety_rollback_v1"
    assert manifest["availability"]["transfers"] == []


# ── picking the right capture when kickoffs differ within a week ────────
from ingest.nfl_dfs_projections import status_before_kickoff

THURS = datetime(2026, 9, 18, 0, 15, tzinfo=timezone.utc)   # Thu night kickoff
SUN = datetime(2026, 9, 20, 17, 0, tzinfo=timezone.utc)     # Sun afternoon

def cap(status, when):
    return {"status": status, "captured_at": when}

def test_a_thursday_player_stays_out_when_sundays_run_looks_again():
    """The bug the Thursday question found: after kickoff, the latest capture
    is post-game, and taking it would silently un-zero a ruled-out player."""
    captures = [cap("HEALTHY", SUN - timedelta(hours=2)),   # newest, but post-Thursday
                cap("OUT", THURS - timedelta(hours=8))]     # the one that was true pregame
    assert status_before_kickoff(captures, THURS) == "OUT"

def test_a_sunday_player_gets_sundays_later_capture():
    """Same week, same capture list — the Sunday player should use the newer one."""
    captures = [cap("HEALTHY", SUN - timedelta(hours=2)),
                cap("OUT", THURS - timedelta(hours=8))]
    assert status_before_kickoff(captures, SUN) == "HEALTHY"

def test_only_post_kickoff_captures_means_no_status():
    assert status_before_kickoff([cap("OUT", THURS + timedelta(hours=1))], THURS) is None

def test_no_kickoff_time_means_no_pregame_claim():
    """Absence of a kickoff is not permission to use a status of unknown vintage."""
    assert status_before_kickoff([cap("OUT", THURS)], None) is None

def test_no_captures_at_all():
    assert status_before_kickoff([], SUN) is None
    assert status_before_kickoff(None, SUN) is None


@pytest.mark.parametrize("captured", [None, TUESDAY-timedelta(days=4), TUESDAY+timedelta(seconds=1), TUESDAY.replace(tzinfo=None)])
def test_unverified_roster_age_cannot_authorize_a_promotion(captured):
    assert proj.qualified_depth({"depth_order":1,"roster_fetched_at":captured},TUESDAY) is None

def test_fresh_role_evidence_keeps_depth():
    assert proj.qualified_depth({"depth_order":2,"roster_fetched_at":TUESDAY},TUESDAY)==2


# ── a rebuild after kickoff keeps the pregame decision ─────────────────
# Kickoff two hours after the Tuesday roster capture so the depth evidence is
# still fresh for a run built an hour after kickoff.
EARLY = TUESDAY + timedelta(hours=2)


def test_a_rebuild_after_kickoff_keeps_the_pregame_out_and_promotion():
    projections, manifest = run(TUESDAY, as_of_at=EARLY + timedelta(hours=1), kickoff=EARLY)
    players = by_name(projections)
    assert players["Starter"]["model_proj_fpts"] == 0.0, "a post-kickoff UNKNOWN must not un-zero him"
    assert players["Backup"]["availability"]["applied"] is True
    decision = manifest["availability_decisions"]["1"]
    assert decision["state"] == "OUT_CONFIRMED"
    assert decision["as_of_at"] == (EARLY - timedelta(microseconds=1)).isoformat()
    assert manifest["availability"]["pregame_frozen_games"] == ["e1"]


def test_the_rebuild_reproduces_the_last_pregame_decision_exactly():
    before, pregame = run(TUESDAY, as_of_at=EARLY - timedelta(microseconds=1), kickoff=EARLY)
    after, rebuilt = run(TUESDAY, as_of_at=EARLY + timedelta(hours=1), kickoff=EARLY)
    assert rebuilt["availability_decisions"] == pregame["availability_decisions"]
    assert [p["model_proj_fpts"] for p in after] == [p["model_proj_fpts"] for p in before]


def test_a_capture_after_kickoff_still_cannot_change_a_started_game():
    projections, manifest = run(EARLY + timedelta(minutes=10), as_of_at=EARLY + timedelta(hours=1), kickoff=EARLY)
    assert by_name(projections)["Starter"]["model_proj_fpts"] > 0
    assert manifest["availability_decisions"]["1"]["display_only_observation_ids"] == [10]


def test_a_game_not_yet_started_resolves_at_the_run_time():
    _, manifest = run(TUESDAY, as_of_at=TUESDAY + timedelta(hours=1), kickoff=EARLY)
    assert manifest["availability_decisions"]["1"]["as_of_at"] == (TUESDAY + timedelta(hours=1)).isoformat()
    assert manifest["availability"]["pregame_frozen_games"] == []


def test_pregame_decision_time():
    from model.nfl_game_availability import pregame_decision_time
    assert pregame_decision_time(TUESDAY, EARLY) == TUESDAY
    assert pregame_decision_time(EARLY, EARLY) == EARLY - timedelta(microseconds=1)
    assert pregame_decision_time(EARLY + timedelta(days=1), EARLY) == EARLY - timedelta(microseconds=1)
    assert pregame_decision_time(TUESDAY, None) == TUESDAY
