"""DST components derived from play-by-play, and the aggregate they replace.

Every case here is a real shape found while reconciling the 2026 week-2 main
slate against DraftKings' own published contest scoring (contest 195648006).
The aggregate agreed with DraftKings on 24 of 26 team-defenses; the derivation
under test agreed on 26 of 26.
"""

from ingest.nfl_dfs_results import score_source_row
from model.nfl_dst_components import (
    DERIVED_COMPONENTS,
    compare_components,
    derive_dst_components,
)
from model.nfl_team_aliases import NFL_TEAM_ALIASES, normalize_team


def play(**over):
    base = {
        "defteam": "NE", "turnover_type": None, "had_sack": False,
        "st_outcome": None, "description": "",
    }
    base.update(over)
    return base


# --- The two real failures ---------------------------------------------------

def test_scoop_and_score_is_counted_when_the_aggregate_missed_it() -> None:
    """New England, week 2: aggregate said `def_tds: 0`; DraftKings paid the TD."""
    plays = [play(
        turnover_type="fumble_lost",
        description=("(13:46) (Shotgun) 8-A.Rodgers sacked at PIT 21 for -9 yards "
                     "(90-C.Barmore). FUMBLES (90-C.Barmore), RECOVERED by NE-91-E.Pond "
                     "and returned 21 yards for a TOUCHDOWN."),
        had_sack=True,
    )]
    derived = derive_dst_components(plays)["NE"]
    assert derived["defensive_tds"] == 1
    assert derived["sacks"] == 1
    assert derived["fumble_recoveries"] == 1


def test_every_sack_is_counted_individually() -> None:
    """Carolina, week 2: aggregate recorded two of three sacks."""
    plays = [play(defteam="CAR", had_sack=True) for _ in range(3)]
    assert derive_dst_components(plays)["CAR"]["sacks"] == 3


# --- The rules that were measured rather than assumed ------------------------

def test_a_touchdown_without_a_turnover_is_the_offense_scoring() -> None:
    """A defense is not paid for the touchdown scored against it."""
    plays = [play(description="(11:40) 9-B.Young pass short right to 30-C.Hubbard for 4 yards, TOUCHDOWN.")]
    assert derive_dst_components(plays)["NE"]["defensive_tds"] == 0


def test_a_nullified_touchdown_is_not_a_touchdown() -> None:
    plays = [play(
        turnover_type="interception",
        description=("9-B.Young pass short middle INTERCEPTED by 55-D.Lloyd, TOUCHDOWN "
                     "NULLIFIED by Penalty. PENALTY on CAR, Offensive Pass Interference."),
    )]
    derived = derive_dst_components(plays)["NE"]
    assert derived["defensive_tds"] == 0
    # The interception itself still happened; only the score was wiped.
    assert derived["interceptions"] == 1


def test_fumble_recoveries_come_from_the_turnover_field_not_the_play_text() -> None:
    """A recovery on a play with no turnover is not credited.

    Parsing `RECOVERED by <TEAM>-` also catches a muffed kick return the
    kicking team recovers. That rule scored 23/26 against DraftKings; this one
    scored 26/26, so the narrower rule is the one in force. Locked down here so
    the wider rule is not reintroduced as an apparent improvement.
    """
    kickoff = play(
        st_outcome="returned", turnover_type=None,
        description=("19-J.Sanders kicks 55 yards from NYJ 35 to TEN 10. 17-C.Dike to TEN 30 "
                     "for 20 yards (41-M.McCrary-Ball). FUMBLES (41-M.McCrary-Ball), "
                     "RECOVERED by NE-37-Q.Stiggers."),
    )
    assert derive_dst_components([kickoff])["NE"]["fumble_recoveries"] == 0


# --- Team identity -----------------------------------------------------------

def test_play_by_play_abbreviations_are_normalized() -> None:
    """`LA`/`WAS` in the play feed are `LAR`/`WSH` everywhere else.

    Without this the Rams and Commanders silently derive zero of everything --
    which is exactly what happened, and was invisible because the slate under
    validation contained neither team.
    """
    plays = [play(defteam="LA", had_sack=True), play(defteam="WAS", had_sack=True)]
    derived = derive_dst_components(plays)
    assert set(derived) == {"LAR", "WSH"}
    assert normalize_team("AZ") == "ARI" and normalize_team("JAC") == "JAX"
    assert normalize_team(None) is None
    assert normalize_team("  ") is None
    assert normalize_team("ne") == "NE", "casing is normalized, unknown codes pass through"
    assert "LA" in NFL_TEAM_ALIASES


def test_a_play_with_no_defense_credits_nobody() -> None:
    assert derive_dst_components([play(defteam=None, had_sack=True)]) == {}


def test_a_quiet_defense_is_present_with_zeros() -> None:
    """Distinguishable from a team missing from the feed entirely."""
    derived = derive_dst_components([play(defteam="CHI")])
    assert derived["CHI"] == {name: 0.0 for name in DERIVED_COMPONENTS}


# --- The evidence ledger -----------------------------------------------------

def test_disagreements_are_reported_per_component() -> None:
    assert compare_components({"sacks": 3.0}, {"sacks": 2.0}) == {
        "sacks": {"play_by_play": 3.0, "team_aggregate": 2.0}
    }
    assert compare_components({"sacks": 2.0}, {"sacks": 2.0}) == {}
    assert compare_components(None, {"sacks": 2.0}) == {}


def _dst(pbp=None):
    context = {
        "opponent_final_points": 21,
        "opponent_raw_team_stats": {"def_tds": 1, "def_safeties": 0, "def_2pt_made": 0},
    }
    if pbp is not None:
        context["pbp_components"] = pbp
    return score_source_row(
        "DST",
        {"raw_team_stats": {
            "def_sacks": 2, "def_interceptions": 2, "fumble_recovery_opp": 1,
            "def_safeties": 1, "def_tds": 0, "special_teams_tds": 1,
            "def_fg_blocks": 1, "def_pat_blocks": 0, "def_punt_blocks": 1, "def_2pt_made": 1,
        }},
        dst_context=context,
    )


def test_play_by_play_components_are_preferred_and_the_aggregate_is_retained() -> None:
    scored = _dst({"sacks": 3, "interceptions": 2, "fumble_recoveries": 1,
                   "safeties": 1, "defensive_tds": 1, "blocked_kicks": 2})
    evidence = scored.evidence
    assert evidence["component_source"] == "play_by_play"
    assert evidence["scoring_components"]["sacks"] == 3
    assert evidence["scoring_components"]["defensive_tds"] == 1
    # The aggregate is kept alongside, with the disagreement named.
    assert evidence["team_aggregate_components"]["sacks"] == 2
    assert evidence["component_disagreements"]["sacks"] == {"play_by_play": 3.0, "team_aggregate": 2.0}
    assert evidence["component_disagreements"]["defensive_tds"] == {"play_by_play": 1.0, "team_aggregate": 0.0}
    # Special-teams return TDs and two-point returns still come from the
    # aggregate: play `defteam` does not identify the returning team.
    assert evidence["scoring_components"]["special_teams_return_tds"] == 1
    assert evidence["scoring_components"]["two_point_returns"] == 1


def test_missing_play_by_play_falls_back_to_the_aggregate_rather_than_failing() -> None:
    scored = _dst(None)
    assert scored.status == "exact"
    assert scored.evidence["component_source"] == "team_aggregate"
    assert scored.evidence["scoring_components"]["sacks"] == 2
    assert scored.evidence["component_disagreements"] is None
    assert scored.evidence["team_aggregate_components"] is None


def test_the_two_sources_produce_different_points_and_both_are_auditable() -> None:
    with_pbp = _dst({"sacks": 3, "interceptions": 2, "fumble_recoveries": 1,
                     "safeties": 1, "defensive_tds": 1, "blocked_kicks": 2})
    without = _dst(None)
    # One extra sack (+1) and one defensive touchdown (+6).
    assert with_pbp.actual_dk_fpts - without.actual_dk_fpts == 7.0
