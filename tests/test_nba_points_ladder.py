"""model/nba_points_ladder.py is a port of the page's TypeScript readers.

The fixture is written by ``web/scripts/write-nba-points-ladder-fixture.ts``
from the TypeScript; ``web/scripts/test-nba-points-ladder.ts`` asserts the
TypeScript still produces it, and this file asserts the port does. A change
to either side that is not mirrored fails one of the two.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from model import nba_points_ladder as m

FIXTURE = Path(__file__).parent / "fixtures" / "nba_points_ladder_fixture.json"


@pytest.fixture(scope="module")
def fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _close(a, b, tol=1e-9) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=0, abs_tol=tol)


def test_name_and_team_keys_match_the_typescript(fixture):
    for name, key in fixture["nameKeys"].items():
        assert m.name_key(name) == key, name
    for name, key in fixture["teamKeys"].items():
        assert m.team_key(name) == key, name


def _assert_distribution(dist, want, label) -> None:
    if want is None:
        assert dist is None, label
        return
    assert dist is not None, label
    assert dist.ladder_books == want["ladderBooks"] and dist.skipped_rungs == want["skippedRungs"], label
    assert _close(dist.floor, want["floor"]) and _close(dist.ceiling, want["ceiling"]), label
    for n in m.THRESHOLDS:
        assert _close(dist.at_least[n], want["atLeast"][str(n)]), (label, n)
    assert [c.point for c in dist.curve] == [c["point"] for c in want["curve"]], label
    for got, w in zip(dist.curve, want["curve"]):
        assert _close(got.p, w["p"]) and got.books == w["books"], label


def _all_cases(fixture) -> list[dict]:
    return fixture["cases"] + fixture["powerCases"]


@pytest.mark.parametrize("index", range(10))
def test_each_fixture_case_reproduces_the_typescript(fixture, index):
    case = _all_cases(fixture)[index]
    expected = case["expected"]
    lines = m.book_lines(case["books"])
    assert [b.book for b in lines] == [b["book"] for b in expected["bookLines"]], case["name"]
    for got, want in zip(lines, expected["bookLines"]):
        assert got.line == want["line"] and got.over == want["over"] and got.under == want["under"]
        assert _close(got.p_over, want["pOver"]), (case["name"], got.book)

    c = m.consensus(lines)
    if expected["consensus"] is None:
        assert c is None
    else:
        assert c[0] == expected["consensus"]["line"], case["name"]
        assert _close(c[1], expected["consensus"]["pOver"]), case["name"]

    v1 = m.build_distribution(m.main_quotes(lines), case["ladders"])
    _assert_distribution(v1, expected["distribution"], case["name"])
    v2 = m.build_distribution(m.main_quotes(lines), case["ladders"], devig="power")
    _assert_distribution(v2, expected["distributionPower"], f"{case['name']} (power)")
    if v1 is not None:
        assert v1.version == m.LADDER_VERSION and v2 is not None and v2.version == m.LADDER_V2_VERSION


def test_fixture_case_counts(fixture):
    assert len(fixture["cases"]) == 6 and len(fixture["powerCases"]) == 4


def test_power_exponents_match_the_typescript(fixture):
    for row in fixture["powerExponents"]:
        k = m.power_exponent(m.implied(row["over"]), m.implied(row["under"]))
        assert _close(k, row["k"]), row
    # The registration's anchor: about 1.072 at -110/-110.
    assert abs(m.power_exponent(m.implied(-110), m.implied(-110)) - 1.072) < 5e-4


def test_power_changes_only_one_sided_rungs():
    """Main line and two-sided rungs are proportional in both readers, so P(over the line) never moves."""
    main = {"draftkings": {"line": 24.5, "over": -110, "under": -110}}
    ladders = {"draftkings": {"ladder": [
        {"point": 19.5, "side": "over", "price": -300}, {"point": 19.5, "side": "under", "price": 240},
        {"point": 29.5, "side": "over", "price": 200}, {"point": 29.5, "side": "under", "price": -250},
    ]}}
    v1 = m.build_distribution(main, ladders)
    v2 = m.build_distribution(main, ladders, devig="power")
    assert [(c.point, c.p) for c in v1.curve] == [(c.point, c.p) for c in v2.curve]
    assert v1.one_sided_rungs == 0 and v2.one_sided_rungs == 0
    k = v2.book_k["draftkings"]
    single = {"draftkings": {"ladder": [{"point": 34.5, "side": "over", "price": 400}, {"point": 14.5, "side": "under", "price": 500}]}}
    v2s = m.build_distribution(main, single, devig="power")
    by_point = {c.point: c.p for c in v2s.curve}
    assert _close(by_point[34.5], m.implied(400) ** k)
    assert _close(by_point[14.5], 1 - m.implied(500) ** k)
    assert v2s.one_sided_rungs == 2


def test_unknown_devig_is_refused():
    with pytest.raises(ValueError):
        m.build_distribution({}, {}, devig="shin")


def test_read_player_combines_consensus_and_ladder(fixture):
    case = fixture["cases"][0]
    reading = m.read_player(case["books"], case["ladders"])
    assert reading is not None
    assert reading.line == 24.5 and reading.distribution is not None
    assert m.read_player({}, {}) is None


def test_curve_is_never_extrapolated():
    curve = [m.ExceedancePoint(20, 0.8, 1), m.ExceedancePoint(30, 0.2, 1)]
    assert m.exceedance_at(curve, 19) is None and m.exceedance_at(curve, 31) is None
    assert m.quantile_at(curve, 0.9) is None
    assert _close(m.exceedance_at(curve, 25), 0.5)
