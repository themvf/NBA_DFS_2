import pandas as pd

from model.nfl_play_facts import build_play_facts, facts_frame, penalty_facts


def frame(descriptions: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for play_id, (play_type, description) in enumerate(descriptions, start=1):
        rows.append(
            {
                "game_id": "g1",
                "play_id": play_id,
                "desc": description,
                "play_type": play_type,
                "posteam": "NYG",
                "drive": 1,
                "qtr": 1,
                "qb_kneel": 0,
                "qb_spike": 0,
                "two_point_attempt": 0,
                "score_differential": 0,
                "game_seconds_remaining": 3600 - play_id * 30,
                "penalty_type": None,
            }
        )
    return pd.DataFrame(rows)


def facts(rows: pd.DataFrame):
    return build_play_facts(rows, source_observation_id="source-v1", fact_release_id="facts-v1")


def test_semi_merged_no_play_retains_wiped_action_without_counting_it() -> None:
    result = facts(
        frame([("no_play", "D.Jones pass short right to M.Nabers for 8 yards. PENALTY NYG Holding, No Play")])
    )[0]
    assert result.snap_execution == "executed"
    assert result.action_validity == "voided"
    assert result.regime == "semi_merged"
    assert result.payload["wipedAction"] == {
        "event": "completion",
        "yards": 8.0,
        "touchdown": False,
        "turnover": False,
        "sack": False,
        "defender": None,
    }
    assert result.payload["sentinel"] is True


def test_no_snap_penalty_is_not_a_voided_executed_action() -> None:
    result = facts(frame([("no_play", "PENALTY NYG False Start, No Play")]))[0]
    assert result.snap_execution == "no_snap"
    assert result.action_validity == "administrative"
    assert result.penalties[0].adjudication == "accepted"


def test_incomplete_no_play_records_an_executed_but_voided_snap() -> None:
    result = facts(
        frame([("no_play", "D.Jones pass incomplete. PENALTY LA DPI, No Play")])
    )[0]
    assert result.snap_execution == "executed"
    assert result.action_validity == "voided"
    assert result.payload["wipedAction"]["event"] == "incompletion"


def test_counted_action_and_declined_penalty_coexist() -> None:
    result = facts(
        frame([("run", "S.Barkley left end for 9 yards. PENALTY LA Offside, declined")])
    )[0]
    assert result.snap_execution == "executed"
    assert result.action_validity == "counted"
    assert result.regime == "merged"
    assert result.penalties[0].adjudication == "declined"


def test_multiple_penalties_remain_separate_occurrences() -> None:
    result = penalty_facts(
        "Pass complete. PENALTY NYG Holding, 10 yards. PENALTY LA Offside, declined"
    )
    assert len(result) == 2
    assert [penalty.occurrence for penalty in result] == [1, 2]
    assert [penalty.adjudication for penalty in result] == ["accepted", "declined"]
    assert result[0].team == "NYG" and result[1].team == "LA"


def test_second_events_are_independent_tags() -> None:
    result = facts(
        frame([("run", "Direct snap to S.Barkley for 4 yards. M.Nabers was injured")])
    )[0]
    assert result.payload["secondEventTags"] == ["PLAYER_INJURY", "DIRECT_SNAP"]


def test_context_frame_is_derived_from_facts_not_raw_pbp() -> None:
    result = facts(
        frame(
            [
                ("run", "S.Barkley left guard for 4 yards"),
                ("no_play", "PENALTY NYG False Start, No Play"),
            ]
        )
    )
    materialized = facts_frame(result)
    assert materialized.loc[0, "action_validity"] == "counted"
    assert materialized.loc[1, "snap_execution"] == "no_snap"
    assert len(materialized) == 2
