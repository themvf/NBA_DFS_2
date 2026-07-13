import requests
import pytest

from ingest import tennis_schedule


def test_discover_tournaments_filters_to_active_atp_and_wta(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {"key": "tennis_atp_bastad", "title": "ATP Bastad", "active": True},
                {"key": "tennis_wta_iasi", "title": "WTA Iasi", "active": True},
                {"key": "tennis_atp_wimbledon", "title": "ATP Wimbledon", "active": False},
                {"key": "baseball_mlb", "title": "MLB", "active": True},
            ]

    monkeypatch.setattr(tennis_schedule.requests, "get", lambda *args, **kwargs: Response())

    assert tennis_schedule.discover_tournaments("test-key") == [
        ("ATP", "tennis_atp_bastad", "ATP Bastad"),
        ("WTA", "tennis_wta_iasi", "WTA Iasi"),
    ]


def test_discover_tournaments_fails_closed_when_provider_is_unavailable(monkeypatch):
    def fail(*args, **kwargs):
        raise requests.ConnectionError("network unavailable")

    monkeypatch.setattr(tennis_schedule.requests, "get", fail)

    with pytest.raises(tennis_schedule.TennisOddsDiscoveryError):
        tennis_schedule.discover_tournaments("test-key")


def test_empty_provider_discovery_is_an_intentional_skip(monkeypatch, capsys):
    monkeypatch.setattr(tennis_schedule, "discover_tournaments", lambda api_key: [])

    assert tennis_schedule.fetch_schedule_and_odds(None, "test-key") == 0
    assert "provider_not_covered" in capsys.readouterr().out


def test_missing_api_key_is_a_pipeline_error():
    with pytest.raises(tennis_schedule.TennisOddsDiscoveryError):
        tennis_schedule.fetch_schedule_and_odds(None, "")
