from research.cfb_market_anchor import anchored_values


def test_anchor_blends_only_eligible_markets():
    result = anchored_values(
        {"home_points": 30, "away_points": 20, "home_win_probability": 0.8},
        {
            "spread": {"eligible": True, "value": 7},
            "total": {"eligible": True, "value": 45},
            "moneyline": {"eligible": False, "value": 0.7},
        },
    )
    assert result["markets"] == {
        "spread": 7.75, "total": 46.25, "moneyline": None,
    }
    assert result["football_weight"] == 0.25
