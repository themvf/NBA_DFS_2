"""Offline tests for model/nfl_ownership_charts.py: pure computations and that each chart writes a PNG."""

from __future__ import annotations

import random
from datetime import datetime, timezone

from model import nfl_ownership_charts as oc


def _own(name, pct, fpts=None):
    return {"player_name": name, "normalized_name": oc.norm(name), "drafted_pct": pct, "fpts": fpts}


def _snap(name, team, pos, proj, out=False, salary=5000):
    return {"name": name, "team": team, "position": pos, "salary": salary, "isOut": out,
            "ownership": {"total": proj, "source": "nfl-ownership-prior-v3"}}


# ── names and kickoffs ───────────────────────────────────────────────────────
def test_norm_and_suffix_fallback():
    assert oc.norm("Ja'Marr Chase") == "jamarrchase"
    assert oc.norm_loose("Kenneth Walker III") == "kennethwalker"
    assert oc.norm_loose("Marvin Harrison Jr.") == "marvinharrison"
    assert oc.norm_loose("Rams") == "rams"


def test_parse_kickoff_eastern_to_utc():
    k = oc.parse_kickoff("ARI@LAR 10/18/2026 04:05PM ET")
    assert k == datetime(2026, 10, 18, 20, 5, tzinfo=timezone.utc)
    assert oc.parse_kickoff(None) is None
    assert oc.first_kickoff([{"gameInfo": "A@B 10/18/2026 04:05PM ET"},
                             {"gameInfo": "C@D 10/18/2026 01:00PM ET"}]) == datetime(2026, 10, 18, 17, 0,
                                                                                      tzinfo=timezone.utc)


# ── matching and the absent-as-zero rule ─────────────────────────────────────
def test_match_by_name_suffix_and_team_when_ambiguous():
    own = [_own("Kenneth Walker", 10.0, 12.0), _own("Mike Williams", 4.0, 3.0)]
    snap = [_snap("Kenneth Walker III", "SEA", "RB", 8.0),
            _snap("Mike Williams", "NYJ", "WR", 2.0), _snap("Mike Williams", "LAC", "WR", 6.0)]
    info = {"mikewilliams": {"team": "LAC", "position": "WR"}}
    players, stats = oc.match_week(own, snap, info, min_mass=2.0)
    by = {p["name"]: p for p in players if p["in_contest"]}
    assert by["Kenneth Walker"]["proj"] == 8.0
    assert by["Mike Williams"]["proj"] == 6.0 and by["Mike Williams"]["team"] == "LAC"
    assert stats["matched"] == 2


def test_ambiguous_without_team_is_unmatched_not_guessed():
    own = [_own("Mike Williams", 4.0, 3.0)]
    snap = [_snap("Mike Williams", "NYJ", "WR", 2.0), _snap("Mike Williams", "LAC", "WR", 6.0)]
    players, stats = oc.match_week(own, snap, {}, min_mass=2.0)
    assert stats["ambiguous"] == 1 and players[0]["proj"] is None


def test_absent_players_zero_only_when_mass_is_high():
    own = [_own(f"P{i}", 90.0, 10.0) for i in range(10)]              # 900% mass: complete
    snap = [_snap(f"P{i}", "AAA", "WR", 5.0) for i in range(10)]
    snap += [_snap("Bench Guy", "AAA", "RB", 1.5), _snap("Hurt Guy", "AAA", "RB", 0.0, out=True)]
    players, stats = oc.match_week(own, snap)
    zero = [p for p in players if not p["in_contest"]]
    assert [p["name"] for p in zero] == ["Bench Guy"] and zero[0]["actual"] == 0.0
    assert stats["absent_zeroed"] == 1 and stats["absent_out"] == 1

    thin = [_own(f"P{i}", 89.0, 10.0) for i in range(10)]              # 890 / 900 = 98.9%
    players, stats = oc.match_week(thin, snap)
    assert not [p for p in players if not p["in_contest"]]
    assert stats["absent_dropped"] == 1 and stats["mass_pct"] < 99.5


# ── selections and errors ────────────────────────────────────────────────────
def test_low_owned_smashes_threshold_and_order():
    weeks = {1: [{"name": "A", "actual": 4.9, "points": 25.0, "in_contest": True},
                 {"name": "B", "actual": 5.0, "points": 40.0, "in_contest": True},      # not under 5%
                 {"name": "C", "actual": 1.0, "points": 19.9, "in_contest": True},      # under 20 points
                 {"name": "Z", "actual": 0.0, "points": None, "in_contest": False}],
             2: [{"name": "D", "actual": 0.5, "points": 31.0, "in_contest": True}]}
    out = oc.low_owned_smashes(weeks)
    assert [(p["name"], p["week"]) for p in out] == [("D", 2), ("A", 1)]


def test_error_stats_and_rank_ties():
    pairs = [{"proj": 10, "actual": 12}, {"proj": 5, "actual": 4}, {"proj": 1, "actual": 1},
             {"proj": 1, "actual": 0}, {"proj": None, "actual": 30}]
    s = oc.error_stats(pairs)
    assert s["n"] == 4
    assert abs(s["bias"] - (-2 + 1 + 0 + 1) / 4) < 1e-9
    assert abs(s["mae"] - (2 + 1 + 0 + 1) / 4) < 1e-9
    assert s["rho"] > 0.9
    assert oc._ranks([3, 1, 1, 2]) == [4.0, 1.5, 1.5, 3.0]
    assert oc.error_stats([])["n"] == 0


def test_low_group_and_biggest_misses():
    pairs = [{"name": "chalk", "proj": 20, "actual": 22}, {"name": "surprise", "proj": 2, "actual": 18},
             {"name": "fade", "proj": 15, "actual": 2}, {"name": "quiet", "proj": 1, "actual": 1}]
    assert {p["name"] for p in oc.low_group(pairs)} == {"surprise", "fade", "quiet"}
    under, over = oc.biggest_misses(pairs)
    assert [p["name"] for p in under] == ["surprise"]
    assert [p["name"] for p in over] == ["fade"]


def test_qb_filter_and_popular_bias():
    week = [{"name": f"QB{i}", "position": "QB", "proj": 10 - i, "actual": 20 - 2 * i} for i in range(8)]
    week += [{"name": "WR", "position": "WR", "proj": 30, "actual": 30},
             {"name": "tiny", "position": "QB", "proj": 0.2, "actual": 0.1},
             {"name": "noproj", "position": "QB", "proj": None, "actual": 9}]
    rows = oc.qb_rows(week)
    assert [r["name"] for r in rows][:2] == ["QB0", "QB1"] and len(rows) == 8
    pb = oc.popular_qb_bias({4: week}, top=5)
    assert pb["popular"]["n"] == 5 and pb["popular"]["bias"] < 0


def test_pct_never_rounds_up_to_the_line():
    assert oc._pct(4.98) == "4.98%"
    assert oc._pct(4.3) == "4.3%"
    assert oc._pct(None) == "n/a"


# ── charts write PNGs ────────────────────────────────────────────────────────
def _weeks(seed=2):
    rng = random.Random(seed)
    pos = ["QB", "RB", "WR", "TE", "DST"]
    out = {}
    for w in (4, 5, 6):
        ps = []
        for i in range(60):
            proj = round(30 * rng.random() ** 3, 2)
            ps.append({"name": f"Player {w}-{i}", "position": pos[i % 5], "team": "AAA", "salary": 5000,
                       "proj": proj, "actual": max(0.0, round(proj * rng.uniform(0.3, 2.0), 2)),
                       "points": round(35 * rng.random(), 1), "in_contest": True})
        out[w] = ps
    return out


def test_charts_write_pngs(tmp_path):
    weeks = _weeks()
    smashes = oc.low_owned_smashes(weeks)
    assert smashes
    assert oc.smashes_chart(smashes, sorted(weeks), tmp_path / "a.png")
    assert oc.projection_chart(weeks, smashes, tmp_path / "b.png", ["Weeks 1-3 left out."])
    assert oc.qb_chart(weeks, tmp_path / "c.png")
    for name in ("a.png", "b.png", "c.png"):
        assert (tmp_path / name).stat().st_size > 10_000


def test_charts_return_false_without_data(tmp_path):
    assert not oc.smashes_chart([], [], tmp_path / "a.png")
    assert not oc.projection_chart({}, [], tmp_path / "b.png")
    assert not oc.qb_chart({}, tmp_path / "c.png")
