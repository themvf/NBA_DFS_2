"""Zeroing and the handoff. Nothing here may depend on a hardcoded player."""
import pytest

from model.nfl_dfs_availability import (
    MAX_TRANSFER_MULTIPLIER, OUT_CLASS, apply, is_out, replacement_for,
    transfer_opportunity, zero_out, mark_transferred,
)

def qb(pid, name, depth, attempts, yards, tds, points, team="LAR"):
    return {"player_id": pid, "player_name": name, "position": "QB", "team": team,
            "depth_order": depth, "history_games": 10, "model_proj_fpts": points, "baseline_fpts": points,
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
    assert zeroed["projection_status"] == "out"


def test_zeroing_preserves_the_workload_he_is_vacating():
    # The projection is the claim "he will not score" and is zero. The stat
    # line is the record of the work now going spare, and blanking it is what
    # left 79 ruled-out receivers/backs/tight ends on a real slate with their
    # work deleted rather than reassigned -- `apply()` only offers a transfer
    # for `positions` (quarterbacks by default), and the slate layer that
    # handles every position had nothing left to share out.
    zeroed = zero_out(qb(1, "Starter", 1, 35, 250, 1.6, 23.6), "OUT")
    assert zeroed["stat_means"]["attempts"] == 35
    assert zeroed["stat_means"]["passing_yards"] == 250
    assert zeroed["availability"]["transferred"] is False, "nobody has been paid yet"


def test_marking_transferred_closes_the_pool_so_nothing_is_paid_twice():
    zeroed = zero_out(qb(1, "Starter", 1, 35, 250, 1.6, 23.6), "OUT")
    moved = mark_transferred(zeroed, "Backup")
    assert all(v == 0.0 for v in moved["stat_means"].values()), "his work is gone because it was placed"
    assert moved["availability"]["transferred"] is True
    assert moved["availability"]["transferred_to"] == "Backup"
    assert moved["availability"]["rule"] == "zeroed", "the original reason survives"
    assert zeroed["stat_means"]["attempts"] == 35, "the input is not mutated"


def test_a_real_transfer_closes_the_donor_pool_and_a_failed_one_does_not():
    starter, backup = qb(1, "Starter", 1, 35, 250, 1.6, 23.6), qb(2, "Backup", 2, 6, 40, 0.2, 4.0)
    out, _ = apply([starter, backup], {1: "OUT"})
    donor = next(p for p in out if p["player_id"] == 1)
    assert donor["availability"]["transferred"] is True
    assert donor["stat_means"]["attempts"] == 0.0

    # Backup has never thrown a pass, so there is nothing to scale and the
    # transfer does not apply. The donor's line MUST survive that, or the work
    # is lost to both layers -- exactly the bug this pair of tests pins.
    empty = qb(3, "Third", 2, 0, 0, 0, 0.0)
    out2, _ = apply([starter, empty], {1: "OUT"})
    donor2 = next(p for p in out2 if p["player_id"] == 1)
    assert donor2["availability"]["transferred"] is False
    assert donor2["stat_means"]["attempts"] == 35, "still available for the slate layer to place"


def test_a_ruled_out_receiver_keeps_his_line_because_nobody_offers_him_a_transfer():
    # positions defaults to ("QB",), so an absent receiver is zeroed and never
    # even considered for a handoff. That is the 79-of-86 case.
    wr = {"player_id": 9, "player_name": "WR1", "position": "WR", "team": "HOU",
          "depth_order": 1, "model_proj_fpts": 14.0, "floor_fpts": 5.0, "median_fpts": 13.0,
          "ceiling_fpts": 25.0, "boom_rate": 0.2, "baseline_fpts": 14.0,
          "stat_means": {"receptions": 5.8, "receiving_yards": 84.0, "receiving_tds": 0.5}}
    out, report = apply([wr], {9: "OUT"})
    assert out[0]["model_proj_fpts"] == 0.0
    assert out[0]["stat_means"]["receptions"] == 5.8, "his 5.8 catches are still on the table"
    assert out[0]["availability"]["transferred"] is False
    assert report["transfers"] == [], "no transfer was even attempted for a receiver"

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
    assert note["offered_opportunity"] == 34
    assert note["assigned_opportunity"] == 3
    assert note["unassigned_opportunity"] == 31

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


@pytest.mark.parametrize("history", [0, 1, 2, 30])
def test_absent_backup_never_rescales_healthy_starter(history):
    starter, backup = qb(1, "Starter", 1, 30, 250, 2, 20), qb(2, "Backup", 3, 20, 150, 1, 12)
    backup["history_games"] = history
    rows, report = apply([starter, backup], {2: "IR"})
    assert rows[0] == starter
    assert not report["transfers"]
    assert rows[1]["availability"]["slate_transfer_allowed"] is False

@pytest.mark.parametrize("which", [0, 1])
def test_prior_only_stats_cannot_donate_or_receive(which):
    players = [qb(1, "S", 1, 32, 256, 2, 20), qb(2, "B", 2, 8, 50, .3, 4)]
    players[which]["history_games"] = 0
    rows, report = apply(players, {1: "OUT"})
    assert rows[1] == players[1]
    assert report["unresolved"]
    assert not rows[0]["availability"]["transferred"]

def test_existing_starter_workload_is_not_reduced():
    starter, backup = qb(1, "S", 1, 20, 200, 1, 15), qb(2, "B", 2, 30, 250, 2, 20)
    updated, note = transfer_opportunity(starter, backup)
    assert updated == backup and not note["applied"]

def test_transfer_preserves_simulated_bonus_expectation():
    starter, backup = qb(1, "S", 1, 32, 310, 2, 25), qb(2, "B", 2, 16, 160, 1, 14)
    updated, note = transfer_opportunity(starter, backup)
    # Doubling crosses 300 yards, but must not invent a deterministic +3 bonus.
    expected_delta = 160/25 + 4 - .8 + 10/10 + .6 - .2
    assert updated["model_proj_fpts"] == pytest.approx(14 + expected_delta)
