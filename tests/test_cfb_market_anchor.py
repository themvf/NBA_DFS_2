from research.cfb_market_anchor import FOOTBALL_WEIGHT, RETIRED, VERSION, anchored_values

MARKETS = {
    "spread": {"eligible": True, "value": 7},
    "total": {"eligible": True, "value": 45},
    "moneyline": {"eligible": False, "value": 0.7},
}
FORECAST = {"home_points": 30, "away_points": 20, "home_win_probability": 0.8}


def test_v2_anchor_is_the_market_quote():
    result = anchored_values(FORECAST, MARKETS)
    assert VERSION == "cfb-market-anchor-v2" and FOOTBALL_WEIGHT == 0.0
    assert result["markets"] == {"spread": 7.0, "total": 45.0, "moneyline": None}
    assert result["football_weight"] == 0.0
    assert result["evidence"] == "cfb-market-weight-v1"


def test_v1_is_recorded_as_retired():
    assert RETIRED["cfb-market-anchor-v1"]["football_weight"] == 0.25
