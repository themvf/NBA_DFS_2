from datetime import datetime, timedelta, timezone

from model.cfb_context_features import offensive_drive_volume
from model.cfb_detector_funnel import summarize_opportunities


NOW = datetime(2026, 9, 23, tzinfo=timezone.utc)


def drive(game, drive_id, *, ingested=None, offense=1, classification="fbs"):
    return {"game_id": game, "cfbd_drive_id": drive_id, "offense_team_id": offense,
            "completed": True, "home_classification": classification, "away_classification": "fbs",
            "commence_time": NOW - timedelta(days=10-game), "ingested_at": ingested or NOW-timedelta(days=1)}


def test_drive_volume_uses_distinct_latest_four_and_preserves_asof_missingness():
    rows = [drive(game, game*100+n) for game in range(1, 6) for n in range(game)]
    rows += [drive(5, 500), drive(6, 601, ingested=NOW+timedelta(seconds=1)), drive(7, 701, classification="fcs")]
    result = offensive_drive_volume(rows, team_id=1, target_event_id=99, as_of_at=NOW)
    assert result["included_game_ids"] == [5, 4, 3, 2]
    assert result["drive_counts"] == [5, 4, 3, 2]
    assert result["scalar_value"] == 3.5
    assert result["coverage_state"] == "complete"
    assert result["excluded_row_counts"]["unavailable_at_as_of"] == 1


def test_drive_volume_does_not_turn_no_history_into_zero():
    result = offensive_drive_volume([], team_id=1, target_event_id=99, as_of_at=NOW)
    assert result["scalar_value"] is None and result["coverage_state"] == "missing"


def test_funnel_partitions_and_samples_are_bounded_and_stable():
    rows = [{"opportunity_key": f"bad-{n}", "rejection_reasons": ["stale_quote"]} for n in range(5)]
    rows += [{"opportunity_key": "below", "threshold_match": False},
             {"opportunity_key": "saved", "threshold_match": True, "persistence_state": "persisted"},
             {"opportunity_key": "dupe", "threshold_match": True, "persistence_state": "deduped"}]
    result = summarize_opportunities(rows)
    assert result.candidate_count == 8 and result.eligible_count == 3
    assert result.matched_count == 2 and result.persisted_count == 1 and result.deduped_count == 1
    assert result.rejection_counts == {"stale_quote": 5}
    assert len(result.rejection_samples["stale_quote"]) == 3
