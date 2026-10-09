from ingest.cfb_context_publish import _rejection_reasons
from model.cfb_detector_funnel import summarize_opportunities
from model.cfb_market_context import MovementResult


def test_market_publication_rejections_keep_stale_quote_as_primary_reason():
    result = MovementResult("rejected", "insufficient_book_intersection", {
        "start_rejections": {"draftkings": "stale_quote"},
        "endpoint_rejections": {"fanduel": "future_bookmaker_timestamp"},
    })
    reasons = _rejection_reasons(result)
    funnel = summarize_opportunities([{
        "opportunity_key": "capture:spread", "rejection_reasons": reasons,
        "threshold_match": False,
    }])
    assert funnel.candidate_count == 1
    assert funnel.eligible_count == 0
    assert funnel.rejection_counts == {"stale_quote": 1}
    assert funnel.rejection_samples == {"stale_quote": ("capture:spread",)}


def test_market_publication_acceptance_distinguishes_first_write_and_replay():
    funnel = summarize_opportunities([
        {"opportunity_key": "new", "threshold_match": True, "persistence_state": "persisted"},
        {"opportunity_key": "seen", "threshold_match": True, "persistence_state": "deduped"},
    ])
    assert (funnel.matched_count, funnel.persisted_count, funnel.deduped_count) == (2, 1, 1)
