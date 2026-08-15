"""Every alert must freeze a price it could actually have been taken at.

Before 2026-08-15, 77 of 80 settled NFL alerts carried no price. That makes an
alert countable but not gradable: you get a win rate against an ASSUMED -110,
which is not a result. The MLB prop work showed why this matters — CLV at real
prices separated a positive detector from a negative one where pooled win rate
showed a single mushy number.

The subtle part is SAME-PROPOSITION. `_consensus_book_line` is a MEAN across
books, so it routinely lands on a number no book offers (-2.55, 36.222).
Pricing at the consensus would invent a bet that never existed.
"""

from __future__ import annotations

from model.line_alerts import _EXECUTION_BOOKS, freeze_execution_price


def _books() -> dict:
    """Three books at total 36.5, two at 37.5 — a realistic split market."""
    return {
        "draftkings":     {"ml_home": -150, "ml_away": 130, "total_line": 36.5,
                           "over": -105, "under": -115,
                           "spread_home": -3.0, "spread_away": 3.0,
                           "spread_home_price": -110, "spread_away_price": -110},
        "betmgm":         {"ml_home": -145, "ml_away": 125, "total_line": 36.5,
                           "over": -110, "under": -110,
                           "spread_home": -3.0, "spread_away": 3.0,
                           "spread_home_price": -105, "spread_away_price": -115},
        "fanduel":        {"ml_home": -155, "ml_away": 135, "total_line": 36.5,
                           "over": -108, "under": -112,
                           "spread_home": -3.0, "spread_away": 3.0,
                           "spread_home_price": -112, "spread_away_price": -108},
        "betrivers":      {"ml_home": -148, "ml_away": 128, "total_line": 37.5,
                           "over": +140, "under": -170,
                           "spread_home": -2.5, "spread_away": 2.5,
                           "spread_home_price": -120, "spread_away_price": +100},
        "williamhill_us": {"ml_home": -152, "ml_away": 132, "total_line": 37.5,
                           "over": +135, "under": -165,
                           "spread_home": -2.5, "spread_away": 2.5,
                           "spread_home_price": -118, "spread_away_price": +102},
        # Not executable in this jurisdiction — must never be selected.
        "pinnacle":       {"ml_home": -140, "ml_away": 145, "total_line": 36.5,
                           "over": +200, "under": -108,
                           "spread_home": -3.0, "spread_away": 3.0,
                           "spread_home_price": +150, "spread_away_price": -120},
    }


def test_prices_at_the_modal_line_not_the_mean_consensus() -> None:
    """Three books at 36.5 beat two at 37.5, even though 37.5 shows a fat +140.

    The mean of those five lines is 36.9 — a number no book offers. Taking the
    best decimal across ALL books would return betrivers' +140 Over, which is a
    bet on a DIFFERENT total.
    """
    got = freeze_execution_price(_books(), market="total", side="over")
    assert got["exec_line"] == 36.5, "modal line, not the 36.9 mean"
    assert got["exec_books_at_line"] == 3
    assert got["exec_book"] == "draftkings"        # -105 is best of the three
    assert got["exec_odds"] == -105
    assert got["exec_odds"] != 140, "must not cross lines to grab a fatter price"


def test_never_prices_at_a_book_the_user_cannot_bet() -> None:
    """Pinnacle offers +200 Over and +150 home spread at the modal line and is
    still never selected — it is a reference, not a venue."""
    for market, side in (("total", "over"), ("spread", "home"), ("moneyline", "away")):
        got = freeze_execution_price(_books(), market=market, side=side)
        assert got["exec_book"] in _EXECUTION_BOOKS
        assert got["exec_book"] != "pinnacle"
    assert "pinnacle" not in _EXECUTION_BOOKS


def test_each_market_and_side_resolves_to_the_best_executable_price() -> None:
    b = _books()
    # Moneyline has no line, so all six executable books compete.
    assert freeze_execution_price(b, market="moneyline", side="away")["exec_odds"] == 135
    assert freeze_execution_price(b, market="moneyline", side="home")["exec_odds"] == -145
    # Spread: modal line is -3.0 / +3.0 (three books).
    home = freeze_execution_price(b, market="spread", side="home")
    away = freeze_execution_price(b, market="spread", side="away")
    assert home["exec_line"] == -3.0 and home["exec_odds"] == -105
    assert away["exec_line"] == 3.0 and away["exec_odds"] == -108


def test_absence_is_recorded_not_fabricated() -> None:
    """No priceable quote must yield an explicit flag, never a guessed number."""
    got = freeze_execution_price({"pinnacle": {"ml_home": -140}},
                                 market="moneyline", side="home")
    assert got == {"exec_price_available": False}
    assert "dk_decimal" not in got, "never invent a price"
    assert freeze_execution_price(_books(), market="nonsense", side="home") == {}


def test_legacy_keys_carry_the_execution_price_for_downstream_grading() -> None:
    """`dk_decimal`/`clv_book` are what the ROI query and _selection_prices read.
    They must hold the EXECUTION book's price so CLV is graded at the book the
    bet was actually taken at, not at DraftKings by assumption."""
    got = freeze_execution_price(_books(), market="total", side="under")
    assert got["dk_decimal"] == got["exec_decimal"]
    assert got["dk_odds"] == got["exec_odds"]
    assert got["clv_book"] == got["exec_book"]
