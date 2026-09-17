"""Zeroing and the handoff. Nothing here may depend on a hardcoded player."""
import pytest

from model.nfl_dfs_availability import (
    MAX_TRANSFER_MULTIPLIER, OUT_CLASS, apply, is_out, replacement_for,
    transfer_opportunity, zero_out,
)

def qb(pid, name, depth, attempts, yards, tds, points, team="LAR"):
    return {"player_id": pid, "player_name": name, "position": "QB", "team": team,
            "depth_order": depth, "model_proj_fpts": points, "baseline_fpts": points,
            "floor_fpts": points * 0.5, "median_fpts": points, "ceiling_fpts": points * 1.6,
            "boom_rate": 0.2, "projection_status": "historical",
            "stat_means": {"attempts": attempts, "passing_yards": yards, "passing_tds": tds,
                           "passing_interceptions": 0.8, "rushing_yards": 10.0,
                           "rushing_tds": 0.1, "fumbles_lost_total": 0.2}}

# ── which statuses mean "not playing" ──────────────────────────────────
@pytest.mark.parametrize("status", sorted(OUT_CLASS))
def test_out_class_statuses_are_out(status):
    assert is_out(status) and is_out(status.lower())

@pytest.mark.parametrize("status", ["QUESTIONABLE", "DOUBTFUL", "HEALTHY", "UNKNOWN", "", None])
def test_everything_else_is_not_out(status):
    assert not is_out(status)

def test_questionable_is_never_zeroed():
    """The boundary that matters: most Questionable players play."""
    starter = qb(1, "Starter", 1, 35, 250, 1.6, 20.0)
    out, report = apply([starter], {1: "QUESTIONABLE"})
    assert out[0]["model_proj_fpts"] == 20.0
    assert report["zeroed"] == []

# ── zeroing ────────────────────────────────────────────────────────────
def test_zeroing_is_zero_not_a_discount():
    zeroed = zero_out(qb(1, "Starter", 1, 35, 250, 1.6, 23.6), "OUT")
    for key in ("model_proj_fpts", "floor_fpts", "median_fpts", "ceiling_fpts", "boom_rate"):
        assert zeroed[key] == 0.0, key
    assert all(v == 0.0 for v in zeroed["stat_means"].values())
    assert zeroed["projection_status"] == "out"

# ── who replaces him ───────────────────────────────────────────────────
def test_replacement_is_the_shallowest_available_teammate():
    starter, backup, third = qb(1, "Starter", 1, 35, 250, 1.6, 23.6), qb(2, "Backup", 2, 6, 40, 0.2, 4.0), qb(3, "Third", 3, 0, 0, 0, 1.0)
    assert replacement_for(starter, [starter, backup, third], {1: "OUT"})["player_id"] == 2

def test_an_injured_backup_is_skipped():
    starter, backup, third = qb(1, "S", 1, 35, 250, 1.6, 23.6), qb(2, "B", 2, 6, 40, 0.2, 4.0), qb(3, "T", 3, 5, 30, 0.1, 3.0)
    assert replacement_for(starter, [starter, backup, third], {1: "OUT", 2: "OUT"})["player_id"] == 3

def test_other_teams_are_never_the_replacement():
    starter, other = qb(1, "S", 1, 35, 250, 1.6, 23.6), qb(9, "Other", 2, 30, 200, 1.2, 18.0, team="SEA")
    assert replacement_for(starter, [starter, other], {1: "OUT"}) is None

def test_no_depth_order_means_no_guess():
    starter, backup = qb(1, "S", 1, 35, 250, 1.6, 23.6), qb(2, "B", None, 6, 40, 0.2, 4.0)
    assert replacement_for(starter, [starter, backup], {1: "OUT"}) is None
    _, report = apply([starter, backup], {1: "OUT"})
    assert report["unresolved"], "an unresolvable handoff must be reported, not silently skipped"
    assert report["transfers"] == []

# ── the handoff itself ─────────────────────────────────────────────────
def test_volume_transfers_and_efficiency_does_not():
    """The backup gets the starter's attempts at his OWN yards per attempt."""
    starter, backup = qb(1, "Starter", 1, 32, 256, 1.6, 23.6), qb(2, "Backup", 2, 8, 48, 0.2, 4.0)
    updated, note = transfer_opportunity(starter, backup)
    assert note["applied"] and note["multiplier"] == 4.0
    assert updated["stat_means"]["attempts"] == pytest.approx(32.0)
    # 6.0 yards/attempt before and after — his rate, the starter's volume.
    assert updated["stat_means"]["passing_yards"] / updated["stat_means"]["attempts"] == pytest.approx(6.0)
    # And crucially NOT the starter's 8.0 yards/attempt.
    assert updated["stat_means"]["passing_yards"] != pytest.approx(starter["stat_means"]["passing_yards"])

def test_the_backup_does_not_simply_inherit_the_starters_points():
    starter, backup = qb(1, "Starter", 1, 32, 288, 2.0, 23.6), qb(2, "Backup", 2, 8, 40, 0.1, 3.0)
    updated, _ = transfer_opportunity(starter, backup)
    assert updated["model_proj_fpts"] < starter["model_proj_fpts"], (
        "a backup at the starter's volume should score less than the starter")
    assert updated["model_proj_fpts"] > backup["model_proj_fpts"], "but more than he did in mop-up"

def test_the_multiplier_is_capped_and_says_so():
    starter, backup = qb(1, "Starter", 1, 35, 245, 1.6, 23.6), qb(2, "Backup", 2, 1, 7, 0.0, 0.5)
    _, note = transfer_opportunity(starter, backup)
    assert note["capped"] and note["multiplier"] == MAX_TRANSFER_MULTIPLIER
    assert note["raw_multiplier"] == 35.0, "the uncapped figure is still reported"

def test_a_backup_with_no_history_is_reported_not_invented():
    starter, backup = qb(1, "Starter", 1, 35, 245, 1.6, 23.6), qb(2, "Rookie", 2, 0, 0, 0, 0.0)
    updated, note = transfer_opportunity(starter, backup)
    assert not note["applied"] and "no usable opportunity" in note["reason"]
    assert updated["model_proj_fpts"] == 0.0, "left exactly as it was, not fabricated"

def test_turnovers_scale_with_volume_too():
    starter, backup = qb(1, "S", 1, 32, 256, 1.6, 23.6), qb(2, "B", 2, 16, 96, 0.4, 8.0)
    updated, _ = transfer_opportunity(starter, backup)
    assert updated["stat_means"]["passing_interceptions"] == pytest.approx(1.6)

# ── end to end ─────────────────────────────────────────────────────────
def test_apply_zeroes_the_starter_and_promotes_the_backup():
    starter, backup = qb(1, "Stafford", 1, 32, 256, 1.6, 23.6), qb(2, "Bennett", 2, 8, 48, 0.3, 4.0)
    out, report = apply([starter, backup], {1: "OUT"})
    by_name = {p["player_name"]: p for p in out}
    assert by_name["Stafford"]["model_proj_fpts"] == 0.0
    assert by_name["Bennett"]["model_proj_fpts"] > 4.0
    assert len(report["zeroed"]) == 1 and len(report["transfers"]) == 1
    # The pair no longer claims two starting quarterbacks.
    assert sum(p["model_proj_fpts"] for p in out) < starter["model_proj_fpts"] + backup["model_proj_fpts"]

def test_positions_outside_the_scope_are_zeroed_but_not_handed_off():
    rb = {**qb(5, "Back", 1, 0, 0, 0, 14.0), "position": "RB",
          "stat_means": {"carries": 18.0, "rushing_yards": 80.0, "rushing_tds": 0.5}}
    rb2 = {**qb(6, "Back2", 2, 0, 0, 0, 5.0), "position": "RB",
           "stat_means": {"carries": 5.0, "rushing_yards": 20.0, "rushing_tds": 0.1}}
    out, report = apply([rb, rb2], {5: "OUT"}, positions=("QB",))
    assert out[0]["model_proj_fpts"] == 0.0, "still zeroed"
    assert out[1]["model_proj_fpts"] == 5.0, "but no transfer outside the scoped positions"
    assert report["transfers"] == []

def test_no_injuries_changes_nothing():
    starter, backup = qb(1, "S", 1, 32, 256, 1.6, 23.6), qb(2, "B", 2, 8, 48, 0.3, 4.0)
    out, report = apply([starter, backup], {})
    assert [p["model_proj_fpts"] for p in out] == [23.6, 4.0]
    assert report["zeroed"] == [] and report["transfers"] == []
