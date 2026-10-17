"""Frozen, market-anchored CFB research baseline.

v2 (2026-10-17) sets the football weight to 0. The pre-registered study
cfb-market-weight-v1 (docs/cfb-market-weight-study.md) fitted the weight out
of sample on 2023-2025 against CFBD closing lines and found no residual skill
in spread, total or moneyline (margin weight -0.008, 95% interval -0.117 to
+0.107). With weight 0 the anchored value is the frozen market quote itself.

v1 used a fixed 25% football weight, chosen before any prospective result.
Comparisons frozen under v1 keep their v1 stamp and are graded as the retired
challenger; they are never rewritten. Research only; never a betting signal.
"""

from __future__ import annotations

VERSION = "cfb-market-anchor-v2"
FOOTBALL_WEIGHT = 0.0
EVIDENCE = "cfb-market-weight-v1"
SOURCE_VERSION = "cfb-score-opponent-ppa-v3"
RETIRED = {"cfb-market-anchor-v1": {"football_weight": 0.25, "retired": "2026-10-17"}}


def anchored_values(forecast: dict, markets: dict) -> dict:
    football = {
        "spread": float(forecast["home_points"]) - float(forecast["away_points"]),
        "total": float(forecast["home_points"]) + float(forecast["away_points"]),
        "moneyline": float(forecast["home_win_probability"]),
    }
    return {
        "version": VERSION, "source_version": SOURCE_VERSION,
        "football_weight": FOOTBALL_WEIGHT, "evidence": EVIDENCE,
        "markets": {
            name: round(float(quote["value"]) + FOOTBALL_WEIGHT *
                        (football[name] - float(quote["value"])), 6)
            if quote["eligible"] and quote["value"] is not None else None
            for name, quote in markets.items()
        },
    }
