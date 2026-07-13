from __future__ import annotations

from ingest import mlb_schedule


class Response:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


def test_nws_forecast_preserves_issue_and_valid_times(monkeypatch) -> None:
    point = {"properties": {"forecastHourly": "https://forecast", "gridId": "BOU", "gridX": 62, "gridY": 60}}
    forecast = {"properties": {
        "generatedAt": "2026-07-13T01:00:00+00:00",
        "periods": [{
            "startTime": "2026-07-17T19:00:00-04:00", "temperature": 82,
            "relativeHumidity": {"value": 35},
            "probabilityOfPrecipitation": {"value": 10},
            "windSpeed": "8 to 12 mph", "windDirection": "SW",
        }],
    }}
    monkeypatch.setattr(
        mlb_schedule.requests, "get",
        lambda url, **_: Response(point if "points" in url else forecast),
    )
    result = mlb_schedule._fetch_nws_forecast(
        latitude=39.7, longitude=-104.9,
        game_start_iso="2026-07-17T23:10:00Z", timeout_seconds=5,
    )
    assert result is not None
    assert result["provider_issued_at"] == "2026-07-13T01:00:00+00:00"
    assert result["valid_at"] == "2026-07-17T19:00:00-04:00"
    assert result["wind_speed_mph"] == 10
    assert result["precipitation_probability_pct"] == 10


def test_roof_capability_does_not_infer_retractable_state() -> None:
    assert mlb_schedule._roof_capability("Chase Field") == "retractable"
    assert mlb_schedule._roof_capability("Coors Field") == "open_air"
    assert mlb_schedule._roof_capability(None) == "unknown"
