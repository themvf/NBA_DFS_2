"""Turn sportsbook player-prop lines into an expected NFL stat line and DK points.

A prop line is the book's MEDIAN for one stat; the two prices around it say
which way the book leans. This module reads that back as an expected value per
stat, then scores the stat line with DraftKings rules. Nothing here is fitted
inside the function: the per-market spread and the skew ratio are constants in
``MARKETS`` (stated priors in v1) or passed in by the study that calibrates them.

Conventions (``nfl-prop-implied-v1``):

* Two-way markets (over/under) are de-vigged proportionally and read as a
  normal around the line: ``mean = line * skew + spread * z(p_over)``. ``skew``
  > 1 for yardage, because yards are right-skewed and the line is the median.
* Touchdown and interception counts use a Poisson read of the over price.
* Anytime TD has only a "yes" price; its margin is removed with
  ``ANYTIME_TD_MARGIN`` before it becomes a Poisson rate. For a quarterback
  that rate is rushing/receiving touchdowns only (passing TDs have their own).
* A market with a missing price is missing, never zero. ``stat_line`` returns
  None for a position whose required markets are absent.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist

VERSION = "nfl-prop-implied-v1"
_N = NormalDist()
ANYTIME_TD_MARGIN = 1.08


@dataclass(frozen=True)
class Market:
    stat: str
    kind: str            # "normal" | "poisson"
    spread: float = 0.0  # normal: absolute spread floor
    spread_ratio: float = 0.0  # normal: spread as a share of the line
    skew: float = 1.0    # normal: mean/median at an even price


MARKETS: dict[str, Market] = {
    "player_pass_yds": Market("passing_yards", "normal", spread=60.0),
    "player_pass_tds": Market("passing_tds", "poisson"),
    "player_pass_interceptions": Market("passing_interceptions", "poisson"),
    "player_rush_yds": Market("rushing_yards", "normal", spread=8.0, spread_ratio=0.5),
    "player_receptions": Market("receptions", "normal", spread=1.0),
    "player_reception_yds": Market("receiving_yards", "normal", spread=6.0, spread_ratio=0.6),
}

REQUIRED = {
    "QB": ("player_pass_yds", "player_pass_tds"),
    "RB": ("player_rush_yds", "player_receptions", "player_anytime_td"),
    "WR": ("player_receptions", "player_reception_yds", "player_anytime_td"),
    "TE": ("player_receptions", "player_reception_yds", "player_anytime_td"),
}

# Mean actual / implied over 2026 weeks 1-4 (research/nfl_prop_implied.py, full
# sample): the median-to-mean correction the web's market anchor applies
# (web/src/lib/nfl-dfs/market-anchor.ts mirrors these; its test pins the two).
CALIBRATION_V1 = {
    "player_pass_yds": 1.023, "player_pass_tds": 1.020, "player_pass_interceptions": 0.985,
    "player_rush_yds": 1.057, "player_receptions": 1.078, "player_reception_yds": 1.175, "player_anytime_td": 0.998,
}

# Yards a reception is worth when a back has a receptions line but no
# receiving-yards line (most backs). League RB yards per catch, a stated prior.
RB_YARDS_PER_RECEPTION = 7.5


def implied_prob(american: float) -> float:
    return -american / (-american + 100) if american < 0 else 100 / (american + 100)


def p_over(over: float, under: float) -> float:
    """Proportional de-vig of a two-way market."""
    a, b = implied_prob(over), implied_prob(under)
    return a / (a + b)


def _poisson_rate_for_tail(k: int, p: float) -> float:
    """The Poisson mean with P(X >= k) = p (bisection; k >= 1)."""
    lo, hi = 0.0, 12.0
    for _ in range(80):
        m = (lo + hi) / 2
        tail = 1 - sum(math.exp(-m) * m ** i / math.factorial(i) for i in range(k))
        lo, hi = (m, hi) if tail < p else (lo, m)
    return (lo + hi) / 2


def market_mean(market: str, line: float | None, over: float | None, under: float | None,
                market_def: Market | None = None) -> float | None:
    """The expected value of one two-way market, or None when any input is missing."""
    md = market_def or MARKETS.get(market)
    if md is None or line is None or over is None or under is None:
        return None
    p = min(max(p_over(over, under), 0.02), 0.98)
    if md.kind == "poisson":
        return _poisson_rate_for_tail(math.floor(line) + 1, p)
    spread = max(md.spread, md.spread_ratio * line)
    return line * md.skew + spread * _N.inv_cdf(p)


def anytime_td_rate(yes_price: float | None) -> float | None:
    if yes_price is None:
        return None
    p = min(implied_prob(yes_price) / ANYTIME_TD_MARGIN, 0.95)
    return -math.log(1 - p)


def _normal_tail(mean: float, spread: float, threshold: float) -> float:
    return 1 - _N.cdf((threshold - mean) / spread) if spread > 0 else float(mean >= threshold)


def stat_line(position: str, lines: dict[str, dict], markets: dict[str, Market] | None = None,
              calibration: dict[str, float] | None = None) -> dict | None:
    """Expected stats for one player from his lines.

    ``lines`` maps a market key to ``{"line", "over", "under"}`` (two-way) or
    ``{"yes"}`` (anytime TD). Returns None when a required market is missing.
    """
    defs = markets or MARKETS
    if position not in REQUIRED or any(_missing(m, lines.get(m)) for m in REQUIRED[position]):
        return None

    cal = calibration or {}

    def mean(m: str) -> float | None:
        q = lines.get(m) or {}
        v = market_mean(m, q.get("line"), q.get("over"), q.get("under"), defs.get(m))
        return None if v is None else v * cal.get(m, 1.0)

    td = anytime_td_rate((lines.get("player_anytime_td") or {}).get("yes"))
    td = None if td is None else td * cal.get("player_anytime_td", 1.0)
    out = {"passing_yards": 0.0, "passing_tds": 0.0, "passing_interceptions": 0.0, "rushing_yards": 0.0,
           "receptions": 0.0, "receiving_yards": 0.0, "scrimmage_tds": td or 0.0}
    if position == "QB":
        out["passing_yards"] = mean("player_pass_yds")
        out["passing_tds"] = mean("player_pass_tds")
        out["passing_interceptions"] = mean("player_pass_interceptions") if mean("player_pass_interceptions") is not None else 0.8
        out["rushing_yards"] = mean("player_rush_yds") or 0.0
    elif position == "RB":
        out["rushing_yards"] = mean("player_rush_yds")
        out["receptions"] = mean("player_receptions")
        recy = mean("player_reception_yds")
        out["receiving_yards"] = recy if recy is not None else out["receptions"] * RB_YARDS_PER_RECEPTION
    else:
        out["receptions"] = mean("player_receptions")
        out["receiving_yards"] = mean("player_reception_yds")
        out["rushing_yards"] = mean("player_rush_yds") or 0.0
    if any(v is None for v in out.values()):
        return None
    return out


def _missing(market: str, q: dict | None) -> bool:
    if not q:
        return True
    if market == "player_anytime_td":
        return q.get("yes") is None
    return q.get("line") is None or q.get("over") is None or q.get("under") is None


def dk_points(position: str, s: dict, markets: dict[str, Market] | None = None) -> float:
    """DraftKings points of an expected stat line, with the yardage bonuses as probabilities.

    Bonuses (+3 at 300 passing, 100 rushing, 100 receiving yards) use the same
    normal spread the market read used; fumbles and 2-point conversions are left out.
    """
    defs = markets or MARKETS
    pts = (0.04 * s["passing_yards"] + 4 * s["passing_tds"] - s["passing_interceptions"]
           + 0.1 * s["rushing_yards"] + s["receptions"] + 0.1 * s["receiving_yards"] + 6 * s["scrimmage_tds"])
    if position == "QB":
        pts += 3 * _normal_tail(s["passing_yards"], defs["player_pass_yds"].spread, 300)
    for stat, key in (("rushing_yards", "player_rush_yds"), ("receiving_yards", "player_reception_yds")):
        md = defs[key]
        if s[stat] > 0:
            pts += 3 * _normal_tail(s[stat], max(md.spread, md.spread_ratio * s[stat]), 100)
    return pts
