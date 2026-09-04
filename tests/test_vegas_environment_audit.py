from __future__ import annotations

from datetime import datetime, timedelta, timezone

from model.vegas_environment_audit import (
    RecommendationPolicy,
    checkpoint_is_satisfied,
    digest,
    percent,
    recommend,
)


def test_checkpoint_windows_are_windows_not_instants() -> None:
    leads = [50.0, 24.2, 6.5, 1.5, 20 / 60]
    assert checkpoint_is_satisfied("open", leads)
    assert checkpoint_is_satisfied("t_minus_48h", leads)
    assert checkpoint_is_satisfied("t_minus_24h", leads)
    assert checkpoint_is_satisfied("t_minus_6h", leads)
    assert checkpoint_is_satisfied("t_minus_90m", leads)
    assert checkpoint_is_satisfied("t_minus_15m", leads)
    assert checkpoint_is_satisfied("close", leads)
    assert not checkpoint_is_satisfied("t_minus_15m", [1.0, -0.1])


def test_percent_is_explicit_when_denominator_is_zero() -> None:
    assert percent(1, 4) == 25.0
    assert percent(0, 0) is None


def test_recommendation_fails_markets_before_player_linkage() -> None:
    policy = RecommendationPolicy()
    status, reasons = recommend(
        pregame_market_games=99,
        market_seasons=5,
        evaluable_player_rows=5000,
        evaluable_slates=100,
        policy=policy,
    )
    assert status == "BLOCKED_MISSING_MARKETS"
    assert "99" in reasons[0]


def test_recommendation_requires_same_slate_player_linkage() -> None:
    status, _ = recommend(
        pregame_market_games=200,
        market_seasons=3,
        evaluable_player_rows=499,
        evaluable_slates=20,
    )
    assert status == "BLOCKED_MISSING_PLAYER_LINKAGE"


def test_limited_season_and_go_gates() -> None:
    limited, _ = recommend(
        pregame_market_games=200,
        market_seasons=1,
        evaluable_player_rows=1000,
        evaluable_slates=20,
    )
    ready, _ = recommend(
        pregame_market_games=200,
        market_seasons=2,
        evaluable_player_rows=1000,
        evaluable_slates=20,
    )
    assert limited == "GO_WITH_LIMITED_SEASONS"
    assert ready == "GO_NFL_MVP"


def test_digest_is_order_independent_and_timestamp_helper_is_sane() -> None:
    assert digest({"b": 2, "a": 1}) == digest({"a": 1, "b": 2})
    kickoff = datetime(2026, 9, 6, 17, tzinfo=timezone.utc)
    assert kickoff - timedelta(hours=6) < kickoff
