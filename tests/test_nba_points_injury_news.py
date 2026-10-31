from datetime import date
import hashlib

import pytest

from research import nba_points_injury_news as n

URL = "https://www.nba.com/raptors/news/injury-update"
RAW = b"<html><script>x</script><p>Barnes will miss at least a week with a sprained ankle.</p></html>"


def claim(**kw):
    base = dict(claim_type="reported_unavailable_interval", player="Scottie Barnes", team="TOR", source_url=URL,
                content_sha256=hashlib.sha256(RAW).hexdigest(), passage="Barnes will miss at least a week",
                observed_at="2026-10-28T15:00:00+00:00", start_date="2026-10-28", min_days=7)
    base.update(kw)
    return n.make_claim(**base)


def test_passage_text_and_official_source():
    assert "at least a week" in n.article_text(RAW) and "x" not in n.article_text(RAW).split()
    assert n.official_url(URL) and not n.official_url("https://www.espn.com/nba/story") and not n.official_url("http://www.nba.com/x")
    with pytest.raises(ValueError, match="nba.com"):
        claim(source_url="https://www.espn.com/x")


def test_interval_is_a_minimum_never_an_end():
    with pytest.raises(ValueError, match="end or return"):
        claim(expected_date="2026-11-04")
    rec = n.claim_record(claim())
    assert n.applies(rec, date(2026, 10, 30)) == (True, "inside_minimum_interval")
    assert n.applies(rec, date(2026, 11, 4)) == (False, "past_minimum_interval_historical_only")
    assert n.applies(rec, date(2026, 10, 27))[0] is False


def test_unknown_timing_stays_unknown_and_no_backdating():
    with pytest.raises(ValueError, match="offset"):
        claim(published_at="2026-10-28T10:00:00")
    rec = n.claim_record(claim(observed_at="2026-10-31T15:00:00+00:00"))
    assert n.applies(rec, date(2026, 10, 30)) == (False, "observed_after_review_date")


def test_status_needs_date_and_conflicts_stay_visible():
    with pytest.raises(ValueError, match="reported_status"):
        claim(claim_type="reported_status", start_date=None, min_days=None, status="sore")
    out = n.claim_record(claim())
    ok = n.claim_record(claim(claim_type="reported_status", start_date=None, min_days=None, status="Available",
                              game_date="2026-10-30", passage="Barnes is available"))
    assert ok["status"] == "available"
    r = n.review([out, ok], date(2026, 10, 30))
    assert r["players"]["scottiebarnes"]["review"] == "conflicting_claims"
    assert n.review([], date(2026, 10, 30))["players"] == {}


def test_expected_return_is_not_confirmed_and_archive_detects_tampering(tmp_path):
    rec = n.claim_record(claim(claim_type="reported_expected_return", start_date=None, min_days=None, expected_date="2026-11-02"))
    assert n.applies(rec, date(2026, 10, 30)) == (True, "expected_return_not_confirmed")
    folder = n.store(claim(), RAW, tmp_path)
    assert len(n.load(tmp_path)) == 1
    (folder / "article.html").write_bytes(b"changed")
    with pytest.raises(ValueError, match="changed"):
        n.load(tmp_path)
