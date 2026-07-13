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


def test_environment_canada_daily_forecast_keeps_partial_resolution() -> None:
    xml = b"""<siteData><forecastGroup>
      <dateTime name="forecastIssue" zone="UTC"><timeStamp>20260713010000</timeStamp></dateTime>
      <forecast><period textForecastName="Friday night">Friday night</period>
        <abbreviatedForecast><pop>30</pop></abbreviatedForecast>
        <temperatures><temperature>17</temperature></temperatures>
        <relativeHumidity>80</relativeHumidity>
      </forecast>
    </forecastGroup><hourlyForecastGroup>
      <hourlyForecast dateTimeUTC="202607140100"><temperature>22</temperature></hourlyForecast>
    </hourlyForecastGroup></siteData>"""

    result = mlb_schedule._parse_environment_canada_forecast(
        xml, "2026-07-17T23:15:00Z",
    )

    assert result is not None
    assert result["provider_issued_at"] == "2026-07-13T01:00:00+00:00"
    assert result["provider_model"] == "toronto_citypage_daily"
    assert result["source_status"] == "partial_daily_resolution"
    assert result["temperature_f"] == 62.6
    assert result["wind_speed_mph"] is None


def test_environment_canada_hourly_forecast_is_complete() -> None:
    xml = b"""<siteData><forecastGroup>
      <dateTime name="forecastIssue" zone="UTC"><timeStamp>20260717180000</timeStamp></dateTime>
      <forecast><period textForecastName="Friday night">Friday night</period>
        <relativeHumidity>70</relativeHumidity></forecast>
    </forecastGroup><hourlyForecastGroup>
      <hourlyForecast dateTimeUTC="202607172300"><temperature>25</temperature><lop>20</lop>
        <wind><speed>16</speed><direction>SW</direction></wind></hourlyForecast>
    </hourlyForecastGroup></siteData>"""

    result = mlb_schedule._parse_environment_canada_forecast(
        xml, "2026-07-17T23:15:00Z",
    )

    assert result is not None
    assert result["provider_model"] == "toronto_citypage_hourly"
    assert result["source_status"] == "complete"
    assert result["temperature_f"] == 77
    assert result["wind_direction"] == "SW"
