from __future__ import annotations

import json

import pandas as pd

from model.mlb_game_total_model import snapshot_weather_context


def test_weather_context_serializes_timestamps_and_numpy_scalars() -> None:
    row = pd.Series({
        "weather_provider": "weather_gov_nws",
        "weather_provider_issued_at": pd.Timestamp("2026-07-13T01:00:00Z"),
        "weather_valid_at": pd.Timestamp("2026-07-17T23:00:00Z"),
        "weather_temp": 82.0,
        "weather_humidity": 35.0,
        "weather_precip_probability": 10.0,
        "wind_speed": 8.0,
        "wind_direction": "SW",
        "roof_capability": "open_air",
        "roof_state": "not_applicable",
        "roof_source": "static_venue_capability",
        "weather_source_status": "complete",
    })
    context = snapshot_weather_context(row)
    assert context["weather_provider_issued_at"] == "2026-07-13T01:00:00+00:00"
    json.dumps(context)
