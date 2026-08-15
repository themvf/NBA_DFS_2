"""MLB prop detector: selection/execution split and CLV book resolution.

Covers the 2026-08-15 rebuild. Two subtle invariants are easy to break and
neither fails loudly:

  1. SELECTION must be single-book (DraftKings). Taking the max EV across N
     books while holding the threshold fixed is a biased estimator -- it picks
     whichever quote is most erroneous in our favour, and quote error shares a
     tail with genuine value. Measured live: every executable book's median EV
     is ~-6.5% (i.e. the two-way hold), so a +3% alert is a ~9.5pp tail
     outlier, and best-of-6 fired 10-19x the DraftKings-only rate at EVERY
     threshold from 3.0% to 6.0%.
  2. CLV must be graded at the book whose price is stored as the entry, across
     three detector generations. Reading the wrong one compares an entry at one
     book to a close at another.
"""

from __future__ import annotations

import ast

from model.line_alerts import _prop_pair, _selection_prices


def _alert(details: dict, side: str = "Someone K O5.5") -> dict:
    return {"details_json": details, "side": side, "sport": "mlb"}


def test_clv_book_resolution_across_all_three_generations() -> None:
    """v3 grades at the selection book, v2 at its execution book, v1 at DK."""
    books = {
        "draftkings": {"line": 5.5, "over": -110, "under": -110},
        "betrivers": {"line": 5.5, "over": +150, "under": -190},
    }
    base = {"market": "pitcher_strikeouts", "line": 5.5, "bet": "Over"}

    # v1 — no book keys at all: DraftKings.
    dec_v1, _ = _selection_prices(_alert(dict(base)), books)

    # v2 — exec_book only; that generation stored the EXECUTION book's price
    # in dk_decimal, so grading must follow exec_book.
    dec_v2, _ = _selection_prices(_alert({**base, "exec_book": "betrivers"}), books)

    # v3 — clv_book wins over exec_book: dk_decimal holds the SELECTION book's
    # price even when execution happened elsewhere.
    dec_v3, _ = _selection_prices(
        _alert({**base, "clv_book": "draftkings", "exec_book": "betrivers"}), books
    )

    assert dec_v1 is not None and abs(dec_v1 - 1.9091) < 1e-3      # DK -110
    assert dec_v2 is not None and abs(dec_v2 - 2.50) < 1e-3        # BetRivers +150
    assert dec_v3 is not None and abs(dec_v3 - 1.9091) < 1e-3      # back to DK
    assert dec_v3 != dec_v2, "clv_book must override exec_book, not be ignored"


def test_a_moved_line_is_not_gradable() -> None:
    """A different line is a different proposition — never a price comparison."""
    books = {"draftkings": {"line": 6.5, "over": -110, "under": -110}}
    dec, fair = _selection_prices(
        _alert({"market": "pitcher_strikeouts", "line": 5.5, "bet": "Over"}), books
    )
    assert dec is None, "line 6.5 must not grade an alert taken at 5.5"
    assert fair is None


def test_devig_is_proportional_and_therefore_cannot_detect_margin_placement() -> None:
    """Documents WHY the rejected 'model disagreement' gate was circular.

    Proportional de-vig forces a book's two sides to sum to 1, so "this book's
    posted price is generous" and "this book's own fair differs from Pinnacle"
    are the same statement. Separating margin placement from genuine model
    disagreement needs an asymmetric (Shin/power) de-vig. Do not re-add the
    symmetric version as a filter — it rejects nothing.
    """
    line, over, under = _prop_pair({"line": 5.5, "over": +150, "under": -190})
    assert line == 5.5
    assert abs((over + under) - 1.0) < 1e-9, "proportional de-vig sums to 1 by construction"


def test_selection_is_single_book_in_the_shipped_code() -> None:
    """The trigger must be evaluated on DraftKings' price, not a max over books.

    Checked on the AST so prose about max-of-N in the comments cannot satisfy
    it. The execution loop may still compare prices — what must not exist is a
    threshold comparison inside a loop over the executable-book tuple.
    """
    import model.line_alerts as la

    src = open(la.__file__, encoding="utf-8").read()
    tree = ast.parse(src)

    scan = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "scan_props"
    )
    for loop in (n for n in ast.walk(scan) if isinstance(n, ast.For)):
        iterates_books = any(
            isinstance(x, ast.Name) and x.id == "_PROP_EXECUTION_BOOKS"
            for x in ast.walk(loop.iter)
        )
        if not iterates_books:
            continue
        compares_threshold = any(
            isinstance(x, ast.Name) and x.id == "_PROP_VALUE_MIN_EV"
            for x in ast.walk(loop)
        )
        # A bare count of qualifying books is fine; a branch that CONTINUES or
        # selects on it is the biased-selection pattern.
        assert not any(
            isinstance(n2, ast.If)
            and any(isinstance(x, ast.Name) and x.id == "_PROP_VALUE_MIN_EV"
                    for x in ast.walk(n2.test))
            for n2 in ast.walk(loop)
        ), "selection threshold must not be applied inside the per-book loop"
        assert compares_threshold, (
            "expected the book loop to only COUNT qualifiers, via _PROP_VALUE_MIN_EV"
        )
