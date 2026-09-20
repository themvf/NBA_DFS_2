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
            return [{"player_id": 1, "normalized_status": "OUT", "fetched_at": self.captured_at}]
        if "ff_source_snapshots" in sql and "DISTINCT ON (season,dataset)" in sql:
            return []
        return []

def run(captured_at):
    monkey = {
        "_history": lambda db, season, wk: HISTORY,
        "_slate_environment": lambda db, season, wk: {
            "LAR": {"opponent": "SEA", "event_id": "e1", "commence_time": KICKOFF,
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
                               as_of_at=TUESDAY, seed=7)
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

def test_the_manifest_records_what_was_done():
    _, manifest = run(TUESDAY)
    report = manifest["availability"]
    assert report["version"] and report["transfers"][0]["opportunity_key"] == "attempts"
    assert "multiplier" in report["transfers"][0]


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
