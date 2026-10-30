import random

from model import nfl_ghost_grade as g

CONTEST = {"entries": 1000, "score_curve": [[1, 150.0], [10, 120.0], [100, 100.0], [1000, 40.0]],
           "payout_curve": {"breakpoints": [[140.0, 500.0], [110.0, 50.0], [90.0, 25.0]]}, "entry_fee": 10.0}


def slots(*pairs):
    return [{"slot": s, "name": n} for s, n in pairs]


def test_unknown_player_leaves_the_lineup_unscored_never_zero():
    pts = {"A": 30.0, "B": 20.0}.get
    score, missing = g.score_lineup(slots(("CPT", "A"), ("FLEX", "B")), lambda s: pts(s["name"]))
    assert score == 65.0 and missing == []
    score, missing = g.score_lineup(slots(("FLEX", "A"), ("FLEX", "Z")), lambda s: pts(s["name"]))
    assert score is None and missing == ["Z"]


def test_grade_subject_places_and_pays_against_the_contest():
    pts = {"A": 60.0, "B": 50.0, "C": 10.0}
    lineups = [slots(("FLEX", "A"), ("FLEX", "B")), slots(("FLEX", "A"), ("FLEX", "C")), slots(("FLEX", "Q"))]
    m = g.grade_subject(lineups, 5, lambda s: pts.get(s["name"]), CONTEST)
    assert m["built"] == 3 and m["scored"] == 2 and m["lineups"] == 5
    assert m["top1_hits"] == 0 and m["top01_hits"] == 0  # 110 < 120 (rank 10), 70 < 120
    assert m["cashes"] == 1 and m["payout"] == 50.0 and m["best_score"] == 110.0
    assert m["detail"]["unscored_lineups"] == 1 and m["detail"]["unscored_players"] == ["Q"]
    assert g.outcomes(m)["roi"] == 50.0 / (10.0 * 2) - 1 and g.outcomes(m)["completion"] == 3 / 5


def test_benjamini_hochberg():
    assert g.benjamini_hochberg({"a": 0.001, "b": 0.02, "c": 0.5}, 0.10) == {"a", "b"}
    assert g.benjamini_hochberg({"a": 0.2, "b": 0.5}, 0.10) == set()


def test_effects_recover_a_planted_factor_and_withhold_before_the_milestone():
    # Seed 1 draws a chance false positive on y=2 (bootstrap over 10 slates is
    # slightly anti-conservative, one reason candidates need fresh confirmation).
    rng = random.Random(2)
    house = {"x": "on", "y": 1}
    arms = [{"x": x, "y": y} for x in ("on", "off") for y in (1, 2, 3)] * 2
    graded = []
    for s in range(10):
        base = rng.random()
        for i, lv in enumerate(arms):
            rate = base + (0.2 if lv["x"] == "off" else 0.0) + rng.gauss(0, 0.02)
            graded.append({"freeze_id": f"s{s}", "arm": i, "levels": lv, "lineups": 20, "built": 20, "scored": 20,
                           "top1_hits": rate * 20, "payout": None, "entry_fee": None})
    early = g.report("classic", [r for r in graded if r["freeze_id"] in {"s0", "s1"}], house)
    assert early["screening"] is None and "withheld" in early["note"]
    full = g.report("classic", graded, house)
    eff = {e["level"]: e for e in full["screening"]["top1_rate"]["effects"]}
    assert abs(eff["x=off"]["effect"] - 0.2) < 0.02 and eff["x=off"]["candidate"]
    assert not eff["y=2"]["candidate"] and not eff["y=3"]["candidate"]
    assert full["screening"]["roi"]["effects"] is None or full["screening"]["roi"]["effects"] == []


def test_extra_arms_stay_out_of_the_screen_and_are_graded_as_pairs_against_the_house():
    rng = random.Random(3)
    house = {"x": "on", "y": 1}
    design = [{"x": x, "y": y} for x in ("on", "off") for y in (1, 2, 3)] * 2
    extra = {33: {"x": "on", "y": 1, "boomWeight": 1}, 34: {"x": "on", "y": 1, "boomWeight": 4}}
    graded = []
    for s in range(10):
        base = rng.random()
        for i, lv in enumerate(design):
            rate = base + (0.2 if lv["x"] == "off" else 0.0) + rng.gauss(0, 0.02)
            graded.append({"freeze_id": f"s{s}", "arm": i, "levels": lv, "lineups": 20, "built": 20, "scored": 20,
                           "top1_hits": rate * 20, "payout": None, "entry_fee": None})
        for arm, lv in extra.items():
            bump = 0.1 if lv["boomWeight"] == 4 else 0.0
            graded.append({"freeze_id": f"s{s}", "arm": arm, "levels": lv, "lineups": 20, "built": 20, "scored": 20,
                           "top1_hits": (base + bump + rng.gauss(0, 0.02)) * 20, "payout": None, "entry_fee": None})
    full = g.report("classic", graded, house)
    screen = full["screening"]["top1_rate"]
    assert not any(e["level"].startswith("boomWeight") for e in screen["effects"]), "extra arms never enter the factorial OLS"
    pairs = {e["level"]: e for e in full["extra_arms"]["paired"]["top1_rate"]["levels"]}
    assert set(pairs) == {"boomWeight=1", "boomWeight=4"}
    assert pairs["boomWeight=4"]["slates"] == 10 and abs(pairs["boomWeight=4"]["mean_diff"] - 0.1) < 0.03
    assert pairs["boomWeight=4"]["candidate"] and not pairs["boomWeight=1"]["candidate"]
    # Withheld below the milestone, on the extra arms' own slate count.
    early = g.report("classic", [r for r in graded if r["freeze_id"] in {"s0", "s1"} or r["arm"] <= 32], house)
    assert early["extra_arms"]["slates"] == 2 and early["extra_arms"]["paired"] is None and "withheld" in early["extra_arms"]["note"]
    assert early["milestone_reached"], "the design screen is unaffected by how many slates carry the extra arms"
    # A slate whose house is ungraded contributes no pair.
    missing_house = [r for r in graded if not (r["freeze_id"] == "s0" and r["arm"] == 0)]
    pairs2 = {e["level"]: e for e in g.report("classic", missing_house, house)["extra_arms"]["paired"]["top1_rate"]["levels"]}
    assert pairs2["boomWeight=4"]["slates"] == 9


class _FakeDb:
    """Answers contest_for's three queries from canned rows; records nothing."""

    def __init__(self, contests, ownership):
        self.contests, self.ownership = contests, ownership

    def execute(self, sql, params=()):
        if "FROM nfl_dfs_field_ownership" in sql:
            return [{"normalized_name": n} for n in self.ownership.get(params[0], [])]
        if "SELECT 1 FROM nfl_dfs_field_contests" in sql:
            return [{"?column?": 1}] if self.contests else []
        return [c for c in self.contests if c["score_curve"] is not None and c["payout_curve"] is not None]


def _freeze(names):
    return {"format": "classic", "first_kickoff": "2026-10-25T17:00:00Z",
            "input_snapshot": [{"name": n} for n in names]}


def test_contest_for_accepts_a_draftkings_export_with_both_curves():
    from research import nfl_ghost_grade as r
    names = [f"player{c}" for c in "abcdefghij"]
    contests = [{"contest_id": "196905290", "contest_name": "Millionaire", "entry_count": 147058,
                 "score_curve": [[1, 200.0]], "payout_curve": {"breakpoints": [[150.0, 20.0]]},
                 "entry_fee": 20.0, "file_name": "contest-standings-196905290"}]
    best, reason = r.contest_for(_FakeDb(contests, {"196905290": names}), _freeze(names))
    assert best is not None and best["contest_id"] == "196905290"
    assert best["source"] == "draftkings-export" and best["overlap"] == 1.0


def test_contest_for_still_refuses_a_stat_api_preview_without_curves():
    from research import nfl_ghost_grade as r
    names = [f"player{c}" for c in "abcdefghij"]
    contests = [{"contest_id": "1", "contest_name": "Millionaire", "entry_count": 147058,
                 "score_curve": None, "payout_curve": None, "entry_fee": 20.0, "file_name": "stat-api:1"}]
    best, reason = r.contest_for(_FakeDb(contests, {"1": names}), _freeze(names))
    assert best is None and "payout table" in reason
    assert r.contest_source("stat-api:1") == "stat-api"
