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
