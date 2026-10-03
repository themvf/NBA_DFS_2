"""Frozen, market-anchored CFB research baseline.

The 25% model weight is fixed before prospective results are observed. It
tests whether opponent-adjusted football context improves the observed market.
It is not an executable price or betting signal.
"""

from __future__ import annotations

VERSION = "cfb-market-anchor-v1"
FOOTBALL_WEIGHT = 0.25
SOURCE_VERSION = "cfb-score-opponent-ppa-v3"


def anchored_values(forecast: dict, markets: dict) -> dict:
    football = {
        "spread": float(forecast["home_points"]) - float(forecast["away_points"]),
        "total": float(forecast["home_points"]) + float(forecast["away_points"]),
        "moneyline": float(forecast["home_win_probability"]),
    }
    return {
        "version": VERSION, "source_version": SOURCE_VERSION,
        "football_weight": FOOTBALL_WEIGHT,
        "markets": {
            name: round(float(quote["value"]) + FOOTBALL_WEIGHT *
                        (football[name] - float(quote["value"])), 6)
            if quote["eligible"] and quote["value"] is not None else None
            for name, quote in markets.items()
        },
    }
