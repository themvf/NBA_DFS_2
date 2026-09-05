import pytest

from ingest.tennis_results_tennisexplorer import _completion_evidence, _parse_day


def result(a=(6, 7), b=(3, 6), score=(2, 0), marker=""):
    return {"a_games": list(a), "b_games": list(b), "a_sets": score[0], "b_sets": score[1],
            "source_rows": marker}


def test_confirms_complete_sets_only_and_requires_match_format():
    assert _completion_evidence(result(), 3)
    assert not _completion_evidence(result(), 5)
    assert not _completion_evidence(result(), None)
    assert _completion_evidence(result((6, 6, 6), (1, 4, 2), (3, 0)), 5)


@pytest.mark.parametrize("r", [
    result((6, 2), (3, 1), (1, 0)),  # leading, unfinished
    result((6, 6), (3, 4), (1, 1)),  # aggregate disagrees
    result((6, None, 6), (3, None, 4)),  # missing middle column
    result((6, 6, 6), (3, 4, 1), (3, 0)),  # extra set after WTA match ended
    result((6, 6), (3, None)),
])
def test_incomplete_or_inconsistent_evidence_stays_unknown(r):
    assert not _completion_evidence(r, 3)


@pytest.mark.parametrize("marker", ["Retired", 'title="Walkover"', "Suspended", "Postponed", "ret.", "Awarded"])
def test_exception_markers_prevent_ordinary_completion(marker):
    assert not _completion_evidence(result(marker=marker), 3)


def test_parser_preserves_superscript_and_pairing_evidence():
    html = '''<table><tr id="r10"><td class="t-name"><a>Ace A.</a></td>
      <td class="result">2</td><td class="score">6</td><td class="score">7<sup>8</sup></td>
      <td><a href="/match-detail/?id=123">info</a></td></tr>
      <tr id="r10b"><td class="t-name"><a>Ball B.</a></td><td class="result">0</td>
      <td class="score">3</td><td class="score">6<sup>6</sup></td></tr></table>'''
    rows = _parse_day(html)
    assert rows[0]["a_games"] == [6, 7]
    assert rows[0]["source_match_id"] == "123"
    assert _completion_evidence(rows[0], 3)
    assert _parse_day(html.replace('id="r10b"', 'id="r11b"')) == []
