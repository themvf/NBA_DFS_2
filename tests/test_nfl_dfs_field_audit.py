"""Field-ownership audit: parsing DraftKings' export, and the verdicts.

Every fixture is a real shape from the 2026 week-2 exports (contests 195648006
and 195786073).
"""

import pytest

from model.nfl_dfs_field_audit import (
    IGNORED_BY_FIELD_PCT,
    NO_PRODUCTION_FPTS,
    ROSTERABLE_PROJECTION_SHARE,
    audit_slate,
    is_showdown,
    normalize_name,
    parse_contest_export,
    pooled_summary,
)

HEADER = ["Rank", "EntryId", "EntryName", "TimeRemaining", "Points", "Lineup",
          "", "Player", "Roster Position", "%Drafted", "FPTS"]


def entry_row(rank, points, lineup, player=None, slot=None, drafted=None, fpts=None):
    """One physical csv row, which may carry an entry, an ownership row, or both."""
    row = [str(rank) if rank else "", "id", "name", "0", str(points) if points else "", lineup, ""]
    if player is None:
        return row[:9]          # narrow row: past the end of the ownership table
    return row + [player, slot, f"{drafted}%", str(fpts)]


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------

def test_the_two_tables_are_split_by_row_width_not_by_order():
    rows = [
        entry_row(1, 243.26, "QB Dak", "Bijan Robinson", "RB", 37.91, 11.1),
        entry_row(2, 236.96, "QB Purdy", "Derrick Henry", "RB", 32.44, 17.7),
        entry_row(3, 232.66, "QB Allen"),     # narrow: entry only
    ]
    parsed = parse_contest_export(rows)
    assert parsed["entry_count"] == 3
    assert len(parsed["players"]) == 2
    assert parsed["winning_score"] == 243.26
    assert parsed["median_score"] == 236.96
    assert parsed["min_score"] == 232.66


def test_showdown_captain_points_are_not_read_as_the_base_score():
    """The trap: CPT FPTS is 1.5x FLEX FPTS for the same player.

    Taking whichever row comes last inflates every captain by 50%. Adams
    scored 42.5; his CPT row says 63.8. His base is 42.5.
    """
    rows = [
        entry_row(1, 149.33, "CPT Adams", "Davante Adams", "FLEX", 45.59, 42.5),
        entry_row(2, 140.0, "CPT Stafford", "Davante Adams", "CPT", 15.68, 63.8),
    ]
    parsed = parse_contest_export(rows)
    adams = parsed["players"][normalize_name("Davante Adams")]
    assert adams["fpts"] == 42.5, "base score comes from the FLEX row"
    # Ownership is the TOTAL across slots: he was rostered by both groups.
    assert adams["drafted_pct"] == pytest.approx(45.59 + 15.68)
    assert is_showdown(parsed) is True


def test_a_player_only_ever_captained_is_divided_back_to_his_base():
    rows = [entry_row(1, 100.0, "CPT X", "Only Captained", "CPT", 0.5, 30.0)]
    parsed = parse_contest_export(rows)
    assert parsed["players"][normalize_name("Only Captained")]["fpts"] == 20.0


def test_classic_slots_do_not_multiply_so_the_rule_is_a_no_op():
    rows = [
        entry_row(1, 200.0, "x", "CeeDee Lamb", "WR", 17.53, 38.3),
        entry_row(2, 190.0, "x", "CeeDee Lamb", "FLEX", 12.0, 38.3),
    ]
    parsed = parse_contest_export(rows)
    assert parsed["players"][normalize_name("CeeDee Lamb")]["fpts"] == 38.3
    assert is_showdown(parsed) is False


# --------------------------------------------------------------------------
# Verdicts
# --------------------------------------------------------------------------

def player(pid, name, position, salary, proj, is_out=False):
    return {"dk_player_id": pid, "name": name, "position": position,
            "salary": salary, "our_proj": proj, "is_out": is_out}


def wr_name(i: int) -> str:
    """Distinct, ALPHABETIC names.

    `normalize_name` strips everything but letters, so "WR0" and "WR1" both
    normalize to "wr" and a digit-suffixed fixture silently collapses into one
    player. Harmless for real NFL names, fatal for a fixture.
    """
    return f"WR{chr(97 + i // 26)}{chr(97 + i % 26)}"


def receivers(n=40, top=20.0):
    """A full position group so the depth cut and the relative floor both bite."""
    return [player(1000 + i, wr_name(i), "WR", 8000 - 100 * i, top - 0.4 * i) for i in range(n)]


def field(n=40, **overrides):
    base = {normalize_name(wr_name(i)): {"drafted_pct": 12.0, "fpts": 11.0} for i in range(n)}
    base.update({normalize_name(k): v for k, v in overrides.items()})
    return base


def test_a_player_who_never_took_the_field_is_an_availability_failure():
    """The Brock Bowers shape: we projected him, 0.01% owned, never played."""
    result = audit_slate(receivers(), field(WRac={"drafted_pct": 0.01, "fpts": 0.0, "played": False}))
    row = next(r for r in result["flagged"] if r["name"] == "WRac")
    assert row["verdict"] == "DID_NOT_PLAY"
    assert result["summary"]["did_not_play"] == 1
    assert result["summary"]["played_and_failed"] == 0
    assert result["summary"]["market_knew"] == 1, "both verdicts still count as a blind spot"
    assert result["summary"]["projected_points_on_market_knew"] == row["our_proj"]


def test_a_player_who_played_and_produced_nothing_is_a_modelling_failure():
    """The Wan'Dale Robinson shape: 1 catch for 9 yards. Lumping him with the
    absentees is what produced a wrong conclusion on the first real run."""
    result = audit_slate(receivers(), field(WRac={"drafted_pct": 0.01, "fpts": 1.9, "played": True}))
    assert next(r for r in result["flagged"] if r["name"] == "WRac")["verdict"] == "PLAYED_AND_FAILED"
    assert result["summary"]["played_and_failed"] == 1
    assert result["summary"]["did_not_play"] == 0


def test_unknown_participation_never_claims_an_absence():
    """No participation data is not evidence he was absent."""
    result = audit_slate(receivers(), field(WRac={"drafted_pct": 0.01, "fpts": 0.0}))
    assert next(r for r in result["flagged"] if r["name"] == "WRac")["verdict"] == "PLAYED_AND_FAILED"
    assert result["summary"]["did_not_play"] == 0


def test_ignored_by_the_field_and_produced_is_a_real_edge():
    result = audit_slate(receivers(), field(WRad={"drafted_pct": 0.59, "fpts": 20.5}))
    assert next(r for r in result["flagged"] if r["name"] == "WRad")["verdict"] == "REAL_EDGE"
    assert result["summary"]["real_edge"] == 1


def test_a_player_the_field_actually_rostered_is_never_flagged():
    result = audit_slate(receivers(), field(WRac={"drafted_pct": IGNORED_BY_FIELD_PCT, "fpts": 0.0}))
    assert result["summary"]["flagged"] == 0, "at the threshold he is not ignored"


def test_a_player_we_also_ignored_is_agreement_not_a_blind_spot():
    """Deep on our own board: the field ignored him and so did we."""
    pool = receivers(n=60)
    observed = field(n=60, WRcd={"drafted_pct": 0.0, "fpts": 0.0})
    assert all(r["name"] != "WRcd" for r in audit_slate(pool, observed)["flagged"])


def test_a_small_slate_does_not_flag_every_minimum_priced_body():
    """Showdown has ~53 players, so an absolute depth cut alone includes all of
    them. The first version flagged $200 players projected at 0.3 points."""
    pool = [player(1, "Star", "WR", 11400, 25.0), player(2, "Scrub", "WR", 200, 0.3)]
    observed = {normalize_name("Star"): {"drafted_pct": 40.0, "fpts": 30.0},
                normalize_name("Scrub"): {"drafted_pct": 0.01, "fpts": 0.0}}
    result = audit_slate(pool, observed)
    assert result["summary"]["flagged"] == 0
    # ...and he WOULD be flagged without the relative floor, which is the bug.
    assert audit_slate(pool, observed, projection_share=0.0)["summary"]["market_knew"] == 1


def test_a_player_we_ruled_out_ourselves_is_excluded():
    """We did not disagree with the field about him -- we agreed."""
    pool = receivers()
    pool[2] = player(1002, wr_name(2), "WR", 7800, 19.2, is_out=True)
    assert audit_slate(pool, field(WRac={"drafted_pct": 0.01, "fpts": 0.0}))["summary"]["flagged"] == 0


def test_absent_from_the_field_table_is_zero_percent_not_unknown():
    """DraftKings lists a player once somebody rosters him. Absent means nobody
    did, which is information -- but with no FPTS we cannot judge the outcome."""
    observed = field()
    del observed[normalize_name(wr_name(2))]
    result = audit_slate(receivers(), observed)
    assert result["summary"]["unmatched_to_field_table"] == 1
    assert all(r["name"] != "WRac" for r in result["flagged"]), "no outcome, no verdict"


def test_the_production_threshold_is_the_boundary():
    at = audit_slate(receivers(), field(WRac={"drafted_pct": 0.1, "fpts": NO_PRODUCTION_FPTS}))
    just_over = audit_slate(receivers(), field(WRac={"drafted_pct": 0.1, "fpts": NO_PRODUCTION_FPTS + 0.1}))
    assert at["summary"]["market_knew"] == 1
    assert just_over["summary"]["real_edge"] == 1


def test_thresholds_are_recorded_on_every_audit():
    t = audit_slate(receivers(), field())["thresholds"]
    assert t["ignored_pct"] == IGNORED_BY_FIELD_PCT
    assert t["projection_share"] == ROSTERABLE_PROJECTION_SHARE
    assert t["no_production"] == NO_PRODUCTION_FPTS


# --------------------------------------------------------------------------
# Pooling
# --------------------------------------------------------------------------

def test_a_small_pooled_sample_refuses_to_read_as_a_finding():
    audits = [audit_slate(receivers(), field(WRac={"drafted_pct": 0.01, "fpts": 0.0}))]
    pooled = pooled_summary(audits)
    assert pooled["market_knew"] == 1
    assert pooled["descriptive_only"] is True, "one blind spot is not a rate"


def test_pooling_counts_across_slates():
    blind = audit_slate(receivers(), field(WRac={"drafted_pct": 0.01, "fpts": 0.0}))
    edge = audit_slate(receivers(), field(WRad={"drafted_pct": 0.5, "fpts": 20.0}))
    pooled = pooled_summary([blind] * 20 + [edge] * 20)
    assert pooled["slates"] == 40
    assert pooled["market_knew"] == 20 and pooled["real_edge"] == 20
    assert pooled["market_knew_share"] == 0.5
    assert pooled["descriptive_only"] is False
