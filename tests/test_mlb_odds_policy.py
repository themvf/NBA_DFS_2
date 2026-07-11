from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import pytest

from ingest.mlb_odds_policy import (
    MlbOddsPolicyError,
    consensus_american,
    require_pregame_capture,
    resolve_mlb_odds_event,
    validate_american_price,
    validate_event_prices,
)
from ingest import backfill_mlb_odds, mlb_schedule


def _event(**overrides):
    value = {
        "id": "odds-1",
        "home_team": "New York Mets",
        "away_team": "Milwaukee Brewers",
        "commence_time": "2026-07-11T23:10:00Z",
        "bookmakers": [
            {"markets": [{"key": "h2h", "outcomes": [
                {"name": "New York Mets", "price": -120},
                {"name": "Milwaukee Brewers", "price": 105},
            ]}]},
        ],
    }
    value.update(overrides)
    return value


def _candidate(identifier: int, commence: str, *, away: str = "Milwaukee Brewers"):
    return {
        "id": identifier,
        "home_name": "New York Mets",
        "away_name": away,
        "commence_time": datetime.fromisoformat(commence.replace("Z", "+00:00")),
    }


@pytest.mark.parametrize("price", [-99, 0, 99, 12.5, float("nan")])
def test_invalid_american_prices_fail_closed(price) -> None:
    with pytest.raises(MlbOddsPolicyError):
        validate_american_price(price)


@pytest.mark.parametrize("price", [-101, -100, 100, 250])
def test_valid_american_prices_pass(price) -> None:
    assert validate_american_price(price) == price


def test_one_invalid_book_rejects_whole_event() -> None:
    event = _event()
    event["bookmakers"][0]["markets"][0]["outcomes"][1]["price"] = -42
    with pytest.raises(MlbOddsPolicyError):
        validate_event_prices(event)


def test_consensus_uses_probability_space_and_remains_valid() -> None:
    consensus = consensus_american([102, -112, 105, -110])
    assert consensus is not None
    assert consensus <= -100 or consensus >= 100


def test_resolver_requires_both_teams() -> None:
    with pytest.raises(MlbOddsPolicyError, match="no exact team/time match"):
        resolve_mlb_odds_event(
            _event(),
            [_candidate(1, "2026-07-11T23:10:00Z", away="Boston Red Sox")],
        )


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"id": ""}, "missing provider event id"),
        ({"commence_time": None}, "missing event commence_time"),
        ({"away_team": "New York Mets"}, "same-team identity"),
    ],
)
def test_resolver_rejects_incomplete_event_identity(overrides, message) -> None:
    with pytest.raises(MlbOddsPolicyError, match=message):
        resolve_mlb_odds_event(
            _event(**overrides),
            [_candidate(1, "2026-07-11T23:10:00Z")],
        )


def test_resolver_uses_time_to_distinguish_doubleheader() -> None:
    candidates = [
        _candidate(1, "2026-07-11T17:10:00Z"),
        _candidate(2, "2026-07-11T23:12:00Z"),
    ]
    assert resolve_mlb_odds_event(_event(), candidates)["id"] == 2


def test_resolver_rejects_ambiguous_doubleheader() -> None:
    candidates = [
        _candidate(1, "2026-07-11T23:09:30Z"),
        _candidate(2, "2026-07-11T23:10:30Z"),
    ]
    with pytest.raises(MlbOddsPolicyError, match="ambiguous"):
        resolve_mlb_odds_event(_event(), candidates)


def test_resolver_rejects_excessive_time_delta() -> None:
    with pytest.raises(MlbOddsPolicyError, match="no exact team/time match"):
        resolve_mlb_odds_event(
            _event(),
            [_candidate(1, "2026-07-12T12:00:00Z")],
            max_commence_delta=timedelta(hours=6),
        )


def test_known_provider_mapping_cannot_override_team_mismatch() -> None:
    with pytest.raises(MlbOddsPolicyError):
        resolve_mlb_odds_event(
            _event(),
            [_candidate(7, "2026-07-11T23:10:00Z", away="Boston Red Sox")],
            known_event_matchup_id=7,
        )


def test_known_provider_mapping_selects_only_mapped_matchup() -> None:
    candidates = [
        _candidate(7, "2026-07-11T23:09:00Z"),
        _candidate(8, "2026-07-11T23:10:00Z"),
    ]
    assert resolve_mlb_odds_event(
        _event(),
        candidates,
        known_event_matchup_id=7,
    )["id"] == 7


def test_athletics_aliases_resolve_to_same_team() -> None:
    event = _event(
        home_team="Sacramento Athletics",
        away_team="Seattle Mariners",
    )
    candidate = {
        "id": 9,
        "home_name": "Athletics",
        "away_name": "Seattle Mariners",
        "commence_time": datetime(2026, 7, 11, 23, 10, tzinfo=timezone.utc),
    }
    assert resolve_mlb_odds_event(event, [candidate])["id"] == 9


def test_capture_must_precede_provider_and_authoritative_start() -> None:
    with pytest.raises(MlbOddsPolicyError, match="at or after"):
        require_pregame_capture(
            event_commence="2026-07-11T23:30:00Z",
            matchup_commence="2026-07-11T23:00:00Z",
            captured_at=datetime(2026, 7, 11, 23, 5, tzinfo=timezone.utc),
        )


def test_live_and_backfill_writers_use_canonical_policy() -> None:
    for writer in (mlb_schedule.fetch_odds, backfill_mlb_odds._backfill_date):
        source = inspect.getsource(writer)
        assert "validate_event_prices" in source
        assert "resolve_mlb_odds_event" in source
        assert "require_pregame_capture" in source


def test_missing_only_backfill_includes_partial_moneyline_rows() -> None:
    source = inspect.getsource(backfill_mlb_odds._dates_with_games)
    assert "vegas_total IS NULL OR home_ml IS NULL OR away_ml IS NULL" in source
