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
