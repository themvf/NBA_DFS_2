"""nfl-archetype-thesis-v1: DraftKings scoring of nflverse rows and the registered slices."""
import numpy as np

from research import nfl_archetype_thesis as t


def test_dk_skill_scoring_matches_draftkings_classic_rules():
    qb = {"passing_yards": 300, "passing_tds": 2, "passing_interceptions": 1, "rushing_yards": 12, "rushing_tds": 0,
          "receptions": 0, "receiving_yards": 0, "receiving_tds": 0, "sack_fumbles_lost": 1}
    # 300*0.04 + 8 - 1 + 1.2 - 1 + 3 (300-yard bonus)
    assert abs(t.dk_skill_points(qb) - (12 + 8 - 1 + 1.2 - 1 + 3)) < 1e-9
    wr = {"receptions": 7, "receiving_yards": 100, "receiving_tds": 1, "rushing_yards": 0}
    assert abs(t.dk_skill_points(wr) - (7 + 10 + 6 + 3)) < 1e-9
    # older nflverse rows name passing interceptions `interceptions`
    old = {"passing_yards": 250, "passing_tds": 1, "interceptions": 2}
    assert abs(t.dk_skill_points(old) - (10 + 4 - 2)) < 1e-9
    assert t.dk_skill_points({"passing_yards": None, "receptions": "3"}) == 3.0


def test_registered_constants():
    assert t.VERSION == "nfl-archetype-thesis-v1"
    assert [b[0] for b in t.SPREAD_BUCKETS] == ["0-3", "3.5-6.5", "7+"]
    assert t.spread_bucket(3.0) == "0-3" and t.spread_bucket(3.5) == "3.5-6.5" and t.spread_bucket(14) == "7+"
    assert t.TEAM_ALIASES["WAS"] == "WSH" and t.TEAM_ALIASES["LA"] == "LAR"


def test_rate_and_bootstrap_are_deterministic():
    a = t.rate("x", [1, 0, 1, 1, 0, 1, 0, 1], 200, baseline=0.5)
    b = t.rate("x", [1, 0, 1, 1, 0, 1, 0, 1], 200, baseline=0.5)
    assert a.value == 0.625 and (a.lo, a.hi) == (b.lo, b.hi) and a.lo <= a.value <= a.hi
    x = np.arange(50, dtype=float); y = x * 2 + 1
    r, lo, hi = t.corr_boot(x, y, 100)
    assert abs(r - 1) < 1e-9 and lo > 0.99
