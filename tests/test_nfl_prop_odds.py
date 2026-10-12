import pytest

from ingest.nfl_prop_odds import normalize_prop_payload, require_credit_budget, select_events


def test_normalizes_paired_and_yes_only_props_without_erasing_raw_shape() -> None:
    payload = {
        "bookmakers": [
            {
                "key": "draftkings",
                "title": "DraftKings",
                "last_update": "2026-09-20T12:00:00Z",
                "markets": [
                    {
                        "key": "player_reception_yds",
                        "outcomes": [
                            {"name": "Over", "description": "Malik Nabers", "point": 74.5, "price": -115},
                            {"name": "Under", "description": "Malik Nabers", "point": 74.5, "price": -105},
                        ],
                    },
                    {
                        "key": "player_anytime_td",
                        "outcomes": [{"name": "Kyren Williams", "price": 120}],
                    },
                ],
            }
        ]
    }
    rows = normalize_prop_payload(payload)
    keyed = {(row["market"], row["player"]): row for row in rows}
    receiving = keyed[("player_reception_yds", "Malik Nabers")]["books"]["draftkings"]
    touchdown = keyed[("player_anytime_td", "Kyren Williams")]["books"]["draftkings"]
    assert receiving["line"] == 74.5
    assert receiving["over"] == -115 and receiving["under"] == -105
    assert len(receiving["outcomes"]) == 2
    assert touchdown["yes"] == 120
    assert touchdown["outcomes"][0]["name"] == "Kyren Williams"


def test_drops_anonymous_sided_outcome_instead_of_guessing_player() -> None:
    payload = {
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {"key": "player_pass_yds", "outcomes": [{"name": "Over", "point": 250.5, "price": -110}]}
                ],
            }
        ]
    }
    assert normalize_prop_payload(payload) == []


def test_paid_capture_requires_budget_and_fails_before_overage() -> None:
    with pytest.raises(ValueError, match="positive"):
        require_credit_budget(estimated_credits=35, max_credits=0)
    with pytest.raises(ValueError, match="exceeds"):
        require_credit_budget(estimated_credits=35, max_credits=34)
    require_credit_budget(estimated_credits=35, max_credits=35)


def test_scheduled_selection_takes_only_pregame_events_inside_the_window() -> None:
    from datetime import datetime, timezone
    now = datetime(2026, 10, 11, 15, 15, tzinfo=timezone.utc)
    events = [{"id": i, "commence_time": t} for i, t in (
        ("started", "2026-10-11T13:30:00Z"), ("late", "2026-10-11T20:25:00Z"),
        ("early", "2026-10-11T17:00:00Z"), ("next_week", "2026-10-18T17:00:00Z"))]
    assert [e["id"] for e in select_events(events, now, 0, 2)] == ["early"]
    assert [e["id"] for e in select_events(events, now, 0, 0)] == ["early", "late", "next_week"]
    assert [e["id"] for e in select_events(events, now, 1, 0)] == ["early"]


def test_t75_run_skips_the_group_it_already_priced() -> None:
    from datetime import datetime, timezone
    # 15:22 UTC tick: the 16:25 group is 63 minutes out; the 16:05 group (43 min) was priced at 14:52.
    now = datetime(2026, 10, 11, 15, 22, tzinfo=timezone.utc)
    events = [{"id": i, "commence_time": t} for i, t in (("g1605", "2026-10-11T16:05:00Z"), ("g1625", "2026-10-11T16:25:00Z"))]
    assert [e["id"] for e in select_events(events, now, 0, 1.25, 1.0)] == ["g1625"]


def test_day_ahead_takes_tomorrows_eastern_games_only() -> None:
    from datetime import datetime, timezone
    # Sunday 18:07 ET: tomorrow is Monday; tonight's 8:20 PM game and next Thursday's are not taken.
    now = datetime(2026, 10, 11, 22, 7, tzinfo=timezone.utc)
    events = [{"id": i, "commence_time": t} for i, t in (
        ("snf", "2026-10-12T00:20:00Z"), ("mnf", "2026-10-13T00:15:00Z"), ("tnf", "2026-10-16T00:15:00Z"))]
    assert [e["id"] for e in select_events(events, now, 0, 0, 0, True)] == ["mnf"]
    # Saturday 18:07 ET takes every Sunday game, the 8:20 PM one included (00:20 UTC Monday).
    sat = datetime(2026, 10, 10, 22, 7, tzinfo=timezone.utc)
    sunday = [{"id": i, "commence_time": t} for i, t in (("early", "2026-10-11T17:00:00Z"), ("snf", "2026-10-12T00:20:00Z"))]
    assert [e["id"] for e in select_events(sunday, sat, 0, 0, 0, True)] == ["early", "snf"]


def test_unbounded_capture_is_refused() -> None:
    from ingest import nfl_prop_odds as m
    m_fetch = m.fetch_events
    m.fetch_events = lambda key: []
    try:
        with pytest.raises(ValueError, match="unbounded"):
            m.capture(db=None, api_key="k", event_limit=0, apply=False)
    finally:
        m.fetch_events = m_fetch


def test_alternate_ladders_keep_every_rung_and_no_misleading_single_line() -> None:
    payload = {"bookmakers": [{"key": "draftkings", "markets": [
        {"key": "player_reception_yds_alternate", "outcomes": [
            {"name": "Over", "description": "Puka Nacua", "price": -293, "point": 69.5},
            {"name": "Over", "description": "Puka Nacua", "price": 105, "point": 99.5},
            {"name": "Over", "description": "Puka Nacua", "price": -1500, "point": 39.5}]},
        {"key": "player_reception_yds", "outcomes": [
            {"name": "Over", "description": "Puka Nacua", "price": -112, "point": 94.5},
            {"name": "Under", "description": "Puka Nacua", "price": -112, "point": 94.5}]}]}]}
    rows = {r["market"]: r for r in normalize_prop_payload(payload)}
    ladder_book = rows["player_reception_yds_alternate"]["books"]["draftkings"]
    assert [r["point"] for r in ladder_book["ladder"]] == [39.5, 69.5, 99.5]
    assert "line" not in ladder_book and "over" not in ladder_book
    main_book = rows["player_reception_yds"]["books"]["draftkings"]
    assert main_book["line"] == 94.5 and main_book["over"] == -112 and "ladder" not in main_book


def test_backfill_matches_the_archived_event_by_teams_and_kickoff() -> None:
    from datetime import datetime, timezone
    import pytest
    from ingest.nfl_prop_backfill import match_event
    kickoff = datetime(2026, 9, 14, 17, 0, tzinfo=timezone.utc)
    game = {"game_id": "g", "home": "Detroit Lions", "away": "Chicago Bears", "kickoff": kickoff}
    events = [{"id": "x", "home_team": "Detroit Lions", "away_team": "Chicago Bears", "commence_time": "2026-09-14T17:00:00Z"},
              {"id": "y", "home_team": "Detroit Lions", "away_team": "Chicago Bears", "commence_time": "2026-12-01T17:00:00Z"},
              {"id": "z", "home_team": "Buffalo Bills", "away_team": "Miami Dolphins", "commence_time": "2026-09-14T17:00:00Z"}]
    assert match_event(events, game)["id"] == "x"
    with pytest.raises(ValueError, match="0 archived events"):
        match_event(events[1:], game)


def test_multi_touchdown_market_keeps_its_over_only_line() -> None:
    payload = {"bookmakers": [{"key": "draftkings", "markets": [{"key": "player_tds_over", "outcomes": [
        {"name": "Over", "description": "Kyren Williams", "price": 320, "point": 1.5}]}]}]}
    book = normalize_prop_payload(payload)[0]["books"]["draftkings"]
    assert book["line"] == 1.5 and book["over"] == 320 and "under" not in book


def test_backfill_requests_only_markets_a_game_is_missing() -> None:
    from ingest.nfl_prop_backfill import missing_markets
    from ingest.nfl_prop_odds import CAPTURE_MARKETS, MARKETS, VOLUME_MARKETS
    assert len(CAPTURE_MARKETS) == 21 and len(set(CAPTURE_MARKETS)) == 21
    have = set(MARKETS)
    missing = missing_markets(have)
    assert not set(missing) & have and set(VOLUME_MARKETS) <= set(missing)
    assert missing_markets(set(CAPTURE_MARKETS)) == []
