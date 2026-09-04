from __future__ import annotations

from datetime import datetime, timedelta, timezone

from model.line_alerts import DETECTOR_REGISTRY, TOP_TEN_SIGNAL_TYPES, _moneyline_structure_signals


RETAIL = ("draftkings", "fanduel", "betmgm", "betrivers", "fanatics")


def _american(probability: float) -> int:
    if probability >= 0.5:
        return round(-100 * probability / (1 - probability))
    return round(100 * (1 - probability) / probability)


def _quote(home_probability: float) -> dict:
    return {"ml_home": _american(home_probability),
            "ml_away": _american(1 - home_probability)}


def _books(retail_probability: float, *, offsets=None, pinnacle=None) -> dict:
    offsets = offsets or {}
    books = {book: _quote(retail_probability + offsets.get(book, 0)) for book in RETAIL}
    books["pinnacle"] = _quote(retail_probability if pinnacle is None else pinnacle)
    return books


def _history(specs: list[dict], *, minutes=30, commence_minutes=240):
    start = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
    rows = [{"history_id": i + 1, "capture_key": f"cap-{i + 1}",
             "captured_at": start + timedelta(minutes=i * minutes), "books": spec}
            for i, spec in enumerate(specs)]
    return rows, start + timedelta(minutes=commence_minutes)


def _types(rows, commence, *, flip=True):
    return {signal["alert_type"]: signal
            for signal in _moneyline_structure_signals(rows, commence, include_favorite_flip=flip)}


def test_detects_material_reversal_with_same_book_support():
    rows, commence = _history([_books(.40), _books(.36), _books(.396)])
    reversal = _types(rows, commence)["reversal"]
    assert reversal["side"] == "home"
    assert reversal["details"]["first_leg_pp"] <= -3.9
    assert reversal["details"]["reversal_leg_pp"] >= 3.5
    assert reversal["details"]["retracement_pct"] >= 85
    assert reversal["details"]["comparable_books"] == 5


def test_detects_sub_steam_price_pressure_and_favorite_flip():
    rows, commence = _history([_books(.49), _books(.51)])
    signals = _types(rows, commence)
    assert signals["price_pressure"]["details"]["books_moved"] == 5
    assert signals["favorite_flip"]["side"] == "home"


def test_detects_wide_book_disagreement_then_convergence():
    wide = _books(.50, offsets={"draftkings": -.04, "fanduel": .04})
    rows, commence = _history([wide, _books(.505)])
    current = _types(rows[:1] + [rows[:1][0] | {"history_id": 2, "captured_at": rows[1]["captured_at"]}], commence)
    assert "book_disagreement" in current
    assert "market_convergence" in _types(rows, commence)


def test_detects_pinnacle_lead_followed_by_retail():
    rows, commence = _history([
        _books(.40, pinnacle=.40),
        _books(.40, pinnacle=.42),
        _books(.41, pinnacle=.42),
    ])
    led = _types(rows, commence)["reference_led"]
    assert led["side"] == "home"
    assert led["details"]["reference_book"] == "pinnacle"


def test_detects_late_move_from_last_pre_window_anchor():
    rows, commence = _history([_books(.40), _books(.40), _books(.415)], minutes=45, commence_minutes=120)
    late = _types(rows, commence)["late_move"]
    assert late["side"] == "home"
    assert late["details"]["minutes_to_start"] == 30


def test_provider_churn_cannot_manufacture_reversal():
    rows, commence = _history([
        {"draftkings": _quote(.40), "fanduel": _quote(.40), "pinnacle": _quote(.40)},
        {"betmgm": _quote(.36), "betrivers": _quote(.36), "pinnacle": _quote(.36)},
        _books(.40),
    ])
    assert "reversal" not in _types(rows, commence)


def test_top_ten_are_registered_for_both_sports():
    registered = {(row["sport"], row["alert_type"]) for row in DETECTOR_REGISTRY}
    for sport, signals in TOP_TEN_SIGNAL_TYPES.items():
        assert len(signals) == len(set(signals)) == 10
        assert all((sport, signal) in registered for signal in signals)
