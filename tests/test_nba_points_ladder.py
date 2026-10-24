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


@pytest.mark.parametrize("index", range(6))
def test_each_fixture_case_reproduces_the_typescript(fixture, index):
    case = fixture["cases"][index]
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

    dist = m.build_distribution(m.main_quotes(lines), case["ladders"])
    want = expected["distribution"]
    if want is None:
        assert dist is None, case["name"]
        return
    assert dist is not None, case["name"]
    assert dist.ladder_books == want["ladderBooks"] and dist.skipped_rungs == want["skippedRungs"]
    assert _close(dist.floor, want["floor"]) and _close(dist.ceiling, want["ceiling"]), case["name"]
    for n in m.THRESHOLDS:
        assert _close(dist.at_least[n], want["atLeast"][str(n)]), (case["name"], n)
    assert [c.point for c in dist.curve] == [c["point"] for c in want["curve"]]
    for got, w in zip(dist.curve, want["curve"]):
        assert _close(got.p, w["p"]) and got.books == w["books"]


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
