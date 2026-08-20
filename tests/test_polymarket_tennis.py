from __future__ import annotations

from ingest import polymarket_tennis


def test_singles_slug_regex_matches_real_atp_and_wta_match_events():
    # Real slugs live-verified against gamma-api.polymarket.com 2026-08-19.
    assert polymarket_tennis._SINGLES_SLUG_RE.match("atp-sinner-alcaraz-2026-06-08")
    assert polymarket_tennis._SINGLES_SLUG_RE.match("wta-yastremska-vs-siegemund-2025-10-06")


def test_singles_slug_regex_excludes_doubles():
    assert polymarket_tennis._SINGLES_SLUG_RE.match("atp-doubles-blanjac-jungmag-2026-08-18") is None
    assert polymarket_tennis._SINGLES_SLUG_RE.match("wta-doubles-chanjoi-errafer-2026-08-18") is None


def test_singles_slug_regex_excludes_futures_and_props():
    # These are real event slugs under the same generic tag_id=864 that
    # capture_matches() now queries -- MATCH_TAG_ID is a supercategory tag,
    # not exclusively match events, so the slug filter is load-bearing.
    assert polymarket_tennis._SINGLES_SLUG_RE.match("2026-mens-french-open-winner") is None
    assert polymarket_tennis._SINGLES_SLUG_RE.match("2026-womens-wimbledon-winner") is None
    assert polymarket_tennis._SINGLES_SLUG_RE.match("itf-arma-benjam-2026-08-18") is None


def test_match_tag_id_is_not_the_near_empty_tour_tags():
    # Regression guard for the 2026-08-19 fix: live verification found
    # ATP_TAG_ID/WTA_TAG_ID carry almost no real match events (1 of 500
    # closed events scanned, 0 live at capture time). capture_matches()
    # must use MATCH_TAG_ID, not either tour tag, for match discovery.
    assert polymarket_tennis.MATCH_TAG_ID == 864
    assert polymarket_tennis.MATCH_TAG_ID not in (polymarket_tennis.ATP_TAG_ID, polymarket_tennis.WTA_TAG_ID)
