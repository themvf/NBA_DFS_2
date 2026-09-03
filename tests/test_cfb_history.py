import json
from datetime import datetime, timezone

from ingest.cfb_history import (
    _line_rows,
    audit_season,
    choose_canonical_provider,
    fetch_cfbd,
    payload_hash,
)


def _game():
    return {
        "id": 401000001,
        "season": 2025,
        "seasonType": "regular",
        "week": 1,
        "startDate": "2025-09-01T00:00:00Z",
        "completed": True,
        "neutralSite": False,
        "homeId": 1,
        "homeTeam": "Home",
        "homeConference": "Test",
        "homeClassification": "fbs",
        "homePoints": 31,
        "awayId": 2,
        "awayTeam": "Away",
        "awayConference": "Test",
        "awayClassification": "fbs",
        "awayPoints": 14,
    }


def _line_game():
    return {
        **_game(),
        "homeTeamId": 1,
        "awayTeamId": 2,
        "homeScore": 31,
        "awayScore": 14,
        "lines": [{
            "provider": "consensus",
            "spread": -14.5,
            "spreadOpen": -13.5,
            "overUnder": 51.5,
            "overUnderOpen": 50.5,
            "homeMoneyline": -700,
            "awayMoneyline": 500,
        }],
    }


def test_audit_reports_coverage_and_never_claims_verified_close() -> None:
    report = audit_season([_game()], [_line_game()], 2025)
    assert report["fbs_vs_fbs_games"] == 1
    assert report["market_rows"] == {"spread": 1, "total": 1, "moneyline": 1}
    assert report["spread_price_rows"] == 0
    assert report["line_timing_contract"]["verified_close_available"] is False


def test_line_parser_keeps_open_and_reference_semantics_separate() -> None:
    rows = _line_rows(9, _line_game(), datetime(2026, 9, 3, tzinfo=timezone.utc))
    assert len(rows) == 5
    spread = {(row["line_designation"], row["home_value"]) for row in rows if row["market_type"] == "spread"}
    assert spread == {("historical_reference", -14.5), ("source_reported_open", -13.5)}
    assert all(row["available_at"] is None for row in rows)
    assert all(not row["is_canonical_reference"] for row in rows)


def test_canonical_provider_prefers_cfbd_consensus() -> None:
    rows = [{"provider": "fanduel"}, {"provider": "consensus"}, {"provider": "draftkings"}]
    assert choose_canonical_provider(rows) == "consensus"
    assert choose_canonical_provider([{"provider": "zeta"}, {"provider": "alpha"}]) == "alpha"


def test_payload_hash_is_order_independent() -> None:
    assert payload_hash({"a": 1, "b": 2}) == payload_hash({"b": 2, "a": 1})


def test_fetch_can_use_cached_payload_without_api_key(tmp_path) -> None:
    path = tmp_path / "games-2025.json"
    path.write_text(json.dumps([_game()]), encoding="utf-8")
    assert fetch_cfbd("games", api_key="", season=2025, cache_dir=tmp_path) == [_game()]
