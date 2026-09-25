from model.cfb_detector_funnel import summarize_opportunities
from model.cfb_moneyline_funnel import DETECTORS, MoneylineFunnelCollector


def test_moneyline_scan_funnel_tracks_missing_inputs_and_first_breach_dedupe():
    collector = MoneylineFunnelCollector("postgresql://unused")
    row = {"matchup_id": 42, "history_id": 100}
    collector.begin_event(row)
    collector.reject(row, "steam", "home", "missing_endpoint")
    collector.match(row, "dk_value", "home", inserted=True)
    collector.match(row, "dk_value", "away", inserted=False)
    collector.match(row, "late_move", "away", inserted=False)

    dk = summarize_opportunities(collector.opportunities[("dk_value", "cfb:event:42")].values())
    assert (dk.candidate_count, dk.eligible_count, dk.matched_count, dk.persisted_count, dk.deduped_count) == (2, 2, 2, 1, 1)
    steam = summarize_opportunities(collector.opportunities[("steam", "cfb:event:42")].values())
    assert steam.rejection_counts == {"missing_endpoint": 1}
    assert steam.rejection_samples == {"missing_endpoint": ("100:steam:home",)}
    late = summarize_opportunities(collector.opportunities[("late_move", "cfb:event:42")].values())
    assert (late.eligible_count, late.below_threshold_count, late.deduped_count) == (2, 1, 1)
    assert set(DETECTORS) == {"dk_value", "pinnacle_divergence", "steam", "walking", "late_move"}
