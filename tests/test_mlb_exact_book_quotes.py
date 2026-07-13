from __future__ import annotations

import pytest

from model.mlb_game_bets import (
    MODEL_VERSION,
    select_exact_moneyline_quote,
    select_exact_total_quote,
)


def test_moneyline_selects_best_valid_paired_quote() -> None:
    books = {
        "draftkings": {"ml_home": -120, "ml_away": 105, "last_update": "dk"},
        "fanduel": {"ml_home": -110, "ml_away": -105, "last_update": "fd"},
        "betmgm": {"ml_home": -105},
    }

    quote = select_exact_moneyline_quote(
        books, side="home", allowed_books=("draftkings", "fanduel", "betmgm"),
    )

    assert quote is not None
    assert quote["book"] == "fanduel"
    assert quote["price"] == -110
    assert quote["paired_price"] == -105
    assert quote["market_prob"] == pytest.approx((110 / 210) / ((110 / 210) + (105 / 205)))


def test_moneyline_rejects_unpaired_or_invalid_quotes() -> None:
    books = {
        "draftkings": {"ml_home": -115},
        "fanduel": {"ml_home": 50, "ml_away": -120},
    }
    assert select_exact_moneyline_quote(
        books, side="home", allowed_books=("draftkings", "fanduel"),
    ) is None


def test_total_requires_the_exact_line_and_selects_best_price() -> None:
    books = {
        "draftkings": {"total_line": 8.5, "over": -115, "under": -105},
        "fanduel": {"total_line": 8.5, "over": -105, "under": -115},
        "betmgm": {"total_line": 9.0, "over": 110, "under": -130},
    }
    quote = select_exact_total_quote(
        books,
        side="over",
        line=8.5,
        allowed_books=("draftkings", "fanduel", "betmgm"),
    )
    assert quote is not None
    assert quote["book"] == "fanduel"
    assert quote["price"] == -105
    assert quote["line"] == 8.5


def test_exact_book_repair_starts_a_new_model_cohort() -> None:
    assert MODEL_VERSION == "mlb-gameline-v4"
