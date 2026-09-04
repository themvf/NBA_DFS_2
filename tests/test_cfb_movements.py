from datetime import datetime, timedelta, timezone

from ingest.cfb_movements import FIELDS, movements


def history(quotes, minutes=120):
    return [{"history_id": i + 1, "captured_at": datetime(2026, 9, 4, tzinfo=timezone.utc)
             + timedelta(minutes=i * minutes), "books": q} for i, q in enumerate(quotes)]


def test_full_and_repeated_slow_reversals_are_preserved():
    rows = history([{"dk": {"spread_home": n}} for n in (-14.5, -17, -14.5, -17, -14.5)])
    result = list(movements(rows))
    assert [r["after"] for r in result] == [-17, -14.5, -17, -14.5]
    assert [r["previous_history_id"] for r in result] == [1, 2, 3, 4]


def test_all_supported_fields_and_small_changes():
    before = {f: -110 for fields in FIELDS.values() for f in fields}
    after = {f: -109.99 for f in before}
    result = list(movements(history([{"dk": before}, {"dk": after}])))
    assert {r["field"] for r in result} == set(before)
    assert all(r["kind"] == "changed" for r in result)


def test_availability_not_mislabeled_as_movement():
    result = list(movements(history([{"dk": {"total_line": 50}}, {}, {"dk": {"total_line": 52}}])))
    assert [r["kind"] for r in result] == ["disappeared", "appeared"]
    assert result[1]["before"] is None


def test_unchanged_quotes_timestamp_only_and_initial_baseline():
    assert list(movements(history([{"dk": {"ml_home": -110, "last_update": "a"}},
                                   {"dk": {"ml_home": -110, "last_update": "b"}}]))) == []
    assert list(movements([])) == []


def test_order_is_deterministic_and_books_separate():
    rows = history([{"a": {"over": -110}, "b": {"over": -110}},
                    {"a": {"over": -115}, "b": {"over": -105}}])
    assert list(movements(rows)) == list(movements(reversed(rows)))
    assert len(list(movements(rows))) == 2
