"""nfl-ownership-rank-shape-v1: frozen constants, frozen curves, and the layer's invariants."""
import json
from pathlib import Path

import numpy as np

from model import nfl_ownership_rank_shape as rs
from model import nfl_ownership_scorecard as sc

ROOT = Path(__file__).resolve().parents[1]


def _curves():
    return json.loads((ROOT / "model/nfl_ownership_rank_shape_v1.json").read_text(encoding="utf-8"))


def test_registered_constants_are_pinned():
    assert rs.VERSION == "nfl-ownership-rank-shape-v1"
    assert rs.BASE_VERSION == "nfl-ownership-prior-v5"
    assert rs.SIGMA == {"classic": 0.6, "showdown": 0.3}
    assert (rs.DRAWS, rs.SEED, rs.LOG_FLOOR) == (200, 7, 0.05)
    assert list(rs.DESIGN_WEEKS) == list(range(2, 10))
    assert list(sc.RS1_DESIGN_WEEKS) == list(rs.DESIGN_WEEKS)
    assert sc.design_tag("rs1", "classic", 9) == "design" and sc.design_tag("rs1", "showdown", 10) == "not_design"


def test_frozen_curves_match_the_module_and_the_registration():
    doc = _curves()
    assert doc["version"] == rs.VERSION and doc["base_version"] == rs.BASE_VERSION
    assert doc["sigma"] == rs.SIGMA and doc["draws"] == rs.DRAWS and doc["seed"] == rs.SEED
    assert doc["curves_digest"] == rs.curves_digest(doc)
    assert doc["curves_digest"].startswith("bbd895213850")
    classic = doc["curves"]["classic"]["position"]
    assert set(classic) == {"QB", "RB", "WR", "TE", "DST"}
    assert abs(sum(v["total"] for v in classic.values()) - 900) < 12
    assert all(v["slates"] >= 7 for v in classic.values())
    cpt, flex = doc["curves"]["showdown"]["CPT"]["all"], doc["curves"]["showdown"]["FLEX"]["all"]
    assert abs(cpt["total"] - 100) < 1.5 and abs(flex["total"] - 500) < 5
    assert cpt["slates"] >= 16 and flex["slates"] >= 14
    for spec in list(classic.values()) + [cpt, flex]:
        c = np.asarray(spec["curve"])
        assert (np.diff(c) <= 1e-9).all(), "curves are non-increasing in rank"
        assert c[0] > c[-1] >= 0
    for key, srcs in doc["sources"].items():
        assert srcs and all(s["week"] in rs.DESIGN_WEEKS for s in srcs), key


def test_hard_rank_map_follows_the_ordering_and_conserves_the_group_total():
    curve_set = {"RB": {"curve": [30.0, 20.0, 10.0, 5.0, 2.0], "total": 65.0}}
    pred = np.array([6.0, 40.0, 1.0, 12.0, 7.0])
    groups = np.array(["RB"] * 5)
    out = rs.hard_rank_shape(pred, groups, curve_set)
    assert np.argmax(out) == 1 and np.argmin(out) == 2
    assert abs(out.sum() - 65.0) < 1e-9
    assert out[3] > out[4] > out[0] > out[2]  # 12 > 7 > 6 > 1 keeps its order
    # a group without a frozen curve keeps the base forecast
    mixed = rs.hard_rank_shape(pred, np.array(["RB", "RB", "RB", "K", "K"]), curve_set)
    assert mixed[3] == 12.0 and mixed[4] == 7.0


def test_soft_map_is_deterministic_conserves_mass_and_shrinks_the_top_rung():
    curve_set = {"all": {"curve": [50.0, 30.0, 20.0, 10.0, 5.0, 2.0], "total": 117.0}}
    pred = np.array([40.0, 38.0, 20.0, 10.0, 4.0, 1.0])
    groups = np.array(["all"] * 6)
    hard = rs.soft_rank_shape(pred, groups, curve_set, 0.0)
    assert np.allclose(hard, rs.hard_rank_shape(pred, groups, curve_set))
    soft_a = rs.soft_rank_shape(pred, groups, curve_set, 0.6)
    soft_b = rs.soft_rank_shape(pred, groups, curve_set, 0.6)
    assert np.allclose(soft_a, soft_b), "same seed, same answer"
    assert abs(soft_a.sum() - 117.0) < 1e-6
    assert soft_a.max() < hard.max(), "two near-tied leaders share rank 1's value instead of one taking all of it"
    assert soft_a[0] > soft_a[2] > soft_a[5]


def test_apply_uses_the_frozen_curves_per_format():
    rng = np.random.default_rng(1)
    positions = np.array(["QB"] * 12 + ["RB"] * 20 + ["WR"] * 30 + ["TE"] * 12 + ["DST"] * 12)
    pred = rng.uniform(0, 30, len(positions))
    out = rs.apply(pred, positions, "classic", "overall")
    doc = _curves()
    for pos, spec in doc["curves"]["classic"]["position"].items():
        assert abs(out[positions == pos].sum() - spec["total"]) < 1e-6
    cpt = rs.apply(rng.uniform(0, 20, 30), None, "showdown", "CPT")
    flex = rs.apply(rng.uniform(0, 60, 30), None, "showdown", "FLEX")
    assert abs(cpt.sum() - doc["curves"]["showdown"]["CPT"]["all"]["total"]) < 1e-6
    assert abs(flex.sum() - doc["curves"]["showdown"]["FLEX"]["all"]["total"]) < 1e-6
    assert (cpt >= 0).all() and (flex >= 0).all()
