"""DST components derived from play-by-play, and the aggregate they replace.

Every case here is a real play found while reconciling 2026 contests against
DraftKings' own published scoring: week 2 (contests 195648006, 195786073) and
week 3 (196128919, 195943238). On those 56 team-defenses the aggregate agreed
with DraftKings 53 times, play-by-play v1 51 times, and v2 (under test) 56.
"""

from ingest.nfl_dfs_results import SCORING_VERSION, input_digest, score_source_row
from model.nfl_dst_components import (
    DERIVED_COMPONENTS,
    VERSION,
    compare_components,
    derive_dst_components,
    fumble_recoveries,
    standing_ruling,
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


# --- Special-teams fumble recoveries (v2) ------------------------------------
#
# Week 3 of 2026 (contest 196128919): five defenses were exactly 2 points under
# DraftKings. Each is one special-teams recovery by the KICKING team, which v1
# could not see because `turnover_type` is set on scrimmage snaps only. The
# play rows below are the real ones, roles included: on a kickoff the kicking
# team is `defteam`; on a punt it is `posteam`.

WEEK_3_MISSES = {
    # team: (DK score, v1 score, play)
    "CAR": (4.0, 2.0, play(  # 2026_03_CAR_CLE play 2339
        defteam="CAR", st_outcome="returned",
        description=("10-R.Fitzgerald kicks 66 yards from CAR 30 to CLE 4. 11-M.Corley to CLE 27 "
                     "for 23 yards (45-M.Njongmeta). FUMBLES (45-M.Njongmeta), RECOVERED by "
                     "CAR-48-T.Incoom at CLE 30."))),
    "NO": (1.0, -1.0, play(  # 2026_03_LV_NO play 2817
        defteam="NO", st_outcome="returned",
        description=("10-D.Carlson kicks 63 yards from NO 35 to LV 2. 23-D.Laube to LV 30 for "
                     "28 yards (33-A.Jennings). FUMBLES (33-A.Jennings), RECOVERED by "
                     "NO-31-J.Howden at LV 32."))),
    "NYG": (8.0, 6.0, play(  # 2026_03_TEN_NYG play 40
        defteam="NYG", st_outcome="returned",
        description=("34-D.Zvada kicks 63 yards from NYG 35 to TEN 2. 17-C.Dike to TEN 36 for "
                     "34 yards (41-M.McFadden, 46-Z.Barnes). FUMBLES (46-Z.Barnes), RECOVERED by "
                     "NYG-54-M.Harrison at TEN 37."))),
    "ARI": (-2.0, -4.0, play(  # 2026_03_ARI_SF play 3293: ARI punts, so SF is `defteam`
        defteam="SF", st_outcome="no_return",
        description=("(:41) 12-B.Gillikin punts 46 yards to SF 30, Center-59-C.Kreiter. "
                     "33-S.Neal MUFFS catch, RECOVERED by ARI-80-S.Fehoko at SF 28."))),
    "PIT": (5.0, 3.0, play(  # 2026_03_CIN_PIT play 3280: PIT punts, so CIN is `defteam`
        defteam="CIN", st_outcome="no_return",
        description=("(11:04) 19-C.Johnston punts 51 yards to CIN 13, Center-46-C.Kuntz. "
                     "12-K.Williams MUFFS catch, touched at CIN 16, RECOVERED by PIT-15-B.Skowronek "
                     "at CIN 14. PIT-26-B.Echols was injured during the play."))),
}


def test_each_week_3_miss_is_one_special_teams_recovery_by_the_kicking_team() -> None:
    for team, (dk, v1, kick) in WEEK_3_MISSES.items():
        derived = derive_dst_components([kick])
        assert derived[team]["fumble_recoveries"] == 1, team
        # The receiving team is credited with nothing.
        receiving = normalize_team(kick["defteam"]) if kick["defteam"] != team else None
        if receiving:
            assert derived[receiving]["fumble_recoveries"] == 0, team
        # One recovery is exactly the 2 points DraftKings paid that v1 did not.
        assert dk - v1 == 2.0, team


def test_a_punt_recovery_is_credited_to_the_punting_team_which_is_the_offense() -> None:
    """The recovering team comes from the play text, not from `defteam`."""
    _, _, muff = WEEK_3_MISSES["ARI"]
    assert muff["defteam"] == "SF"
    assert fumble_recoveries(muff["description"]) == ["ARI"]


def test_week_1_aggregate_disagreements_were_the_same_mechanism() -> None:
    """v1 notes flagged Chicago and the Jets in week 1 as unresolved. Both are
    kicking-team recoveries: a muffed punt and a kickoff-return fumble."""
    chicago = play(defteam="CAR", st_outcome="no_return", description=(  # 2026_01_CHI_CAR 1812
        "(7:07) 19-T.Taylor punts 48 yards to CAR 22, Center-45-B.Gardner. 15-Ji.Horn MUFFS "
        "catch, RECOVERED by CHI-81-K.Davis at CAR 28."))
    jets = play(defteam="NYJ", st_outcome="returned", description=(  # 2026_01_NYJ_TEN 1553
        "19-J.Sanders kicks 55 yards from NYJ 35 to TEN 10. 17-C.Dike to TEN 30 for 20 yards "
        "(41-M.McCrary-Ball). FUMBLES (41-M.McCrary-Ball), RECOVERED by NYJ-37-Q.Stiggers at "
        "TEN 38. ** Injury Update: TEN-17-C.Dike has returned to the game."))
    derived = derive_dst_components([chicago, jets])
    assert derived["CHI"]["fumble_recoveries"] == 1
    assert derived["NYJ"]["fumble_recoveries"] == 1


# --- What is NOT a fumble recovery -------------------------------------------
#
# v1 recorded that parsing `RECOVERED by` scored 23/26 in week 2 and blamed
# DraftKings for not paying every recovery. Reproduced, the text rule's misses
# were the first two cases below: recoveries that did not stand.

def test_a_recovery_the_replay_official_reversed_is_not_credited() -> None:
    """New Orleans, week 2 (2026_02_NO_BAL 2250). DK did not pay it."""
    reversed_kick = play(defteam="NO", st_outcome="returned", description=(
        "10-D.Carlson kicks 65 yards from NO 35 to BAL 0. 17-C.Moore to BAL 33 for 33 yards "
        "(28-D.Stutsman; 58-C.Rumph). FUMBLES (28-D.Stutsman), RECOVERED by NO-7-J.Sanker at "
        "BAL 33. The Replay Official reviewed the runner was not down by contact ruling, and "
        "the play was REVERSED. 10-D.Carlson kicks 65 yards from NO 35 to BAL 0. 17-C.Moore to "
        "BAL 32 for 32 yards (28-D.Stutsman; 58-C.Rumph)."))
    assert derive_dst_components([reversed_kick])["NO"]["fumble_recoveries"] == 0


def test_a_recovery_on_a_play_wiped_by_penalty_is_not_credited() -> None:
    """Denver, week 2 (2026_02_JAX_DEN 261). DK did not pay it."""
    wiped = play(defteam="DEN", had_sack=False, description=(
        "(11:13) 64-W.Milum reported in as eligible. 16-T.Lawrence sacked at JAX 34 for -9 yards "
        "(15-N.Bonitto). FUMBLES (15-N.Bonitto), RECOVERED by DEN-49-A.Singleton at JAX 30. "
        "49-A.Singleton to JAX 21 for 9 yards (33-B.Tuten). PENALTY on DEN-21-R.Moss, Illegal "
        "Contact, 5 yards, enforced at JAX 43 - No Play."))
    assert derive_dst_components([wiped])["DEN"]["fumble_recoveries"] == 0


def test_a_team_recovering_its_own_muff_is_not_credited() -> None:
    """Lower-case `recovered by` is the gamebook's no-change-of-possession form."""
    own = play(defteam="NYJ", st_outcome="no_return", description=(  # 2026_02_GB_NYJ 162
        "(13:34) 19-D.Whelan punts 60 yards to NYJ 8, Center-42-M.Orzech. 18-I.Williams MUFFS "
        "catch, recovered by NYJ-37-Q.Stiggers at NYJ 2. 37-Q.Stiggers to NYJ 5 for 3 yards "
        "(87-J.Sturdivant; 59-T.Hopper)."))
    assert fumble_recoveries(own["description"]) == []


def test_blocked_kicks_and_onside_kicks_are_not_fumble_recoveries() -> None:
    blocked = play(defteam="LAR", st_outcome="blocked", description=(  # 2024_14_BUF_LA 1107
        "(12:38) 8-S.Martin punt is BLOCKED by 35-J.Hummel, Center-69-R.Ferguson, RECOVERED by "
        "LA-84-H.Long at BUF 22. 84-H.Long for 22 yards, TOUCHDOWN."))
    onside = play(defteam="DAL", st_outcome="returned", description=(  # 2024_03_BAL_DAL 3777
        "17-B.Aubrey kicks onside 9 yards from DAL 35 to DAL 44, impetus ends at DAL 44. "
        "RECOVERED by DAL-29-C.Goodwin."))
    derived = derive_dst_components([blocked, onside])
    assert derived["LAR"]["fumble_recoveries"] == 0
    assert derived["LAR"]["blocked_kicks"] == 1, "the block itself is still scored"
    assert derived["DAL"]["fumble_recoveries"] == 0


def test_a_fumble_out_of_the_end_zone_is_a_turnover_but_not_a_recovery() -> None:
    """Touchback: possession changes, nobody recovers (2025_04_IND_LA 2376).
    v1 credited it through `turnover_type`; the aggregate does not."""
    touchback = play(defteam="LA", turnover_type="fumble_lost", description=(
        "(11:44) (Shotgun) 17-D.Jones pass deep left to 10-A.Mitchell to LA 1 for 75 yards "
        "[8-J.Verse]. FUMBLES, ball out of bounds in End Zone, Touchback."))
    assert derive_dst_components([touchback])["LAR"]["fumble_recoveries"] == 0


def test_every_change_of_possession_recovery_on_a_play_is_counted() -> None:
    """Indianapolis recovers, then fumbles it back to Tennessee (2023_13_IND_TEN 845)."""
    chain = play(defteam="IND", turnover_type="fumble_lost", had_sack=True, description=(
        "(4:12) (Shotgun) 8-W.Levis sacked at TEN 31 for -9 yards (52-S.Ebukam). FUMBLES "
        "(52-S.Ebukam), RECOVERED by IND-32-J.Blackmon at 50. 32-J.Blackmon to TEN 48 for 2 "
        "yards. FUMBLES, RECOVERED by TEN-8-W.Levis at TEN 48. 8-W.Levis to TEN 48 for no gain "
        "(23-K.Moore)."))
    derived = derive_dst_components([chain])
    assert derived["IND"]["fumble_recoveries"] == 1
    assert derived["TEN"]["fumble_recoveries"] == 1


# --- Only the ruling that stands (v2) ----------------------------------------

def test_standing_ruling_is_the_text_after_the_last_reversal() -> None:
    assert standing_ruling("A. The play was REVERSED. B.") == " B."
    assert standing_ruling("A. The play was Upheld. The ruling on the field stands.") == (
        "A. The play was Upheld. The ruling on the field stands.")


def test_a_touchdown_overturned_by_replay_is_not_a_defensive_touchdown() -> None:
    """Pick-six reversed to down-by-contact (2024_18_SF_ARI 4027). The
    interception stands; the touchdown does not. v1 paid both."""
    reversed_td = play(defteam="ARI", turnover_type="interception", description=(
        "(6:40) (Shotgun) 5-J.Dobbs pass short left intended for 14-R.Pearsall INTERCEPTED by "
        "13-K.Clark at SF 39. 13-K.Clark for 39 yards, TOUCHDOWN. The Replay Official reviewed "
        "the runner was not down by contact ruling, and the play was REVERSED. (Shotgun) "
        "5-J.Dobbs pass short left intended for 14-R.Pearsall INTERCEPTED by 13-K.Clark at SF "
        "39. 13-K.Clark to SF 39 for no gain (14-R.Pearsall)."))
    derived = derive_dst_components([reversed_td])["ARI"]
    assert derived["interceptions"] == 1
    assert derived["defensive_tds"] == 0


def test_an_interception_fumbled_back_and_scored_by_the_offense_is_not_a_defensive_td() -> None:
    """2025_05_TEN_ARI 4224: Arizona intercepts, fumbles, Tennessee recovers in
    the end zone. Arizona keeps its interception; Tennessee its recovery; the
    touchdown is Tennessee's, not Arizona's defense's."""
    fumbled_back = play(defteam="ARI", turnover_type="interception", description=(
        "(4:53) (Shotgun) 1-C.Ward pass short left intended for 0-C.Ridley INTERCEPTED by "
        "42-D.Taylor-Demerson (2-Ma.Wilson) at ARI 5. 42-D.Taylor-Demerson to ARI 5 for no gain. "
        "FUMBLES, touched at ARI 6, RECOVERED by TEN-4-T.Lockett at ARI 0. TOUCHDOWN."))
    derived = derive_dst_components([fumbled_back])
    assert derived["ARI"]["interceptions"] == 1
    assert derived["ARI"]["defensive_tds"] == 0
    assert derived["TEN"]["fumble_recoveries"] == 1


def test_a_safety_overturned_by_replay_is_not_a_safety() -> None:
    """2025_14_DAL_DET 646: the sack stands, one yard outside the end zone."""
    reversed_safety = play(defteam="DET", had_sack=True, description=(
        "(5:48) (Shotgun) 4-D.Prescott sacked in End Zone for -11 yards, SAFETY (46-J.Campbell). "
        "The Replay Official reviewed the safety ruling, and the play was REVERSED. (Shotgun) "
        "4-D.Prescott sacked at DAL 1 for -10 yards (46-J.Campbell)."))
    derived = derive_dst_components([reversed_safety])["DET"]
    assert derived["safeties"] == 0
    assert derived["sacks"] == 1


def test_a_safety_enforced_on_a_penalty_still_counts() -> None:
    """Holding in the end zone is a safety even though the down is `No Play`
    (2024_01_DEN_SEA 1386); the aggregate and the defense's score both carry it."""
    penalty_safety = play(defteam="DEN", description=(
        "(11:28) (Shotgun) 7-G.Smith pass incomplete short left to 14-D.Metcalf (6-P.Locke). "
        "PENALTY on SEA-75-A.Bradford, Offensive Holding, 1 yard, enforced in End Zone, "
        "SAFETY - No Play."))
    assert derive_dst_components([penalty_safety])["DEN"]["safeties"] == 1


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


# --- End to end: Carolina, week 3 --------------------------------------------

def test_carolina_week_3_scores_what_draftkings_paid() -> None:
    """The real inputs: 2 sacks, 21 points allowed (worth 0), and the kickoff
    recovery. v1 scored 2.0 and recorded the aggregate's recovery as a
    disagreement; DraftKings paid 4.0. v2 scores 4.0 and the two records agree."""
    _, _, kick = WEEK_3_MISSES["CAR"]
    sacks = [play(defteam="CAR", had_sack=True) for _ in range(2)]
    derived = derive_dst_components([*sacks, kick])["CAR"]
    scored = score_source_row(
        "DST",
        {"raw_team_stats": {"def_sacks": 2, "def_interceptions": 0, "fumble_recovery_opp": 1,
                            "def_safeties": 0, "def_tds": 0, "special_teams_tds": 0}},
        dst_context={"opponent_final_points": 21,
                     "opponent_raw_team_stats": {"def_tds": 0, "def_safeties": 0, "def_2pt_made": 0},
                     "pbp_components": derived},
    )
    assert scored.actual_dk_fpts == 4.0
    assert scored.evidence["component_disagreements"] is None
    assert scored.evidence["component_version"] == VERSION == "nfl-dst-components-pbp-v2"


# --- Versioning --------------------------------------------------------------

def test_the_dst_component_change_is_a_new_realized_version() -> None:
    assert SCORING_VERSION == "nfl-dk-realized-v4"


def test_a_dst_row_scored_under_v4_never_shares_a_digest_with_v3() -> None:
    import hashlib
    import json

    row = {"raw_team_stats": {"def_sacks": 2}}
    context = {"opponent_final_points": 21, "opponent_raw_team_stats": {"def_tds": 0}}
    v4 = input_digest(position="DST", source="nflverse", source_row=row, dst_context=context)
    # The v3 digest of the same inputs, rebuilt by hand.
    v3_payload = {"position": "DST", "source": "nflverse", "source_row": row,
                  "scoring_version": "nfl-dk-realized-v3", "dst_context": context}
    v3 = hashlib.sha256(json.dumps(v3_payload, sort_keys=True, separators=(",", ":"),
                                   default=str).encode("utf-8")).hexdigest()
    assert v4 != v3
