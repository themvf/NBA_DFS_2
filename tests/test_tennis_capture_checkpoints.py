from datetime import datetime, timezone

from ingest import tennis_capture_checkpoints as checkpoints


class FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self.rows.pop(0)


def test_due_checkpoints_queries_each_window() -> None:
    db = FakeDb([
        [{"id": 1, "tour": "ATP", "commence_time": "soon"}],
        [], [], [],
    ])
    now = datetime(2026, 8, 29, tzinfo=timezone.utc)
    due = checkpoints.due_checkpoints(db, now)
    assert due == [{"checkpoint": "open", "id": 1, "tour": "ATP", "commence_time": "soon"}]
    assert len(db.calls) == len(checkpoints.CHECKPOINTS)
    assert all(call[1][0] == "%us open%" for call in db.calls)


def test_no_due_checkpoint_spends_no_api_request(monkeypatch) -> None:
    db = FakeDb([[], [], [], []])
    monkeypatch.setattr(
        checkpoints, "discover_tournaments",
        lambda *_: (_ for _ in ()).throw(AssertionError("must not discover")),
    )
    result = checkpoints.capture_due_checkpoints(db, "key")
    assert result["due_matches"] == 0
    assert result["captured_events"] == 0


def test_one_due_tour_costs_one_tournament_refresh(monkeypatch) -> None:
    due = {"id": 7, "tour": "ATP", "commence_time": "soon"}
    db = FakeDb([[due], [], [], []])
    monkeypatch.setattr(
        checkpoints, "discover_tournaments",
        lambda *_: [("ATP", "tennis_atp_us_open", "ATP US Open")],
    )
    calls = []

    def fake_fetch(*args, **kwargs):
        calls.append((args, kwargs))
        return 64

    monkeypatch.setattr(checkpoints, "fetch_tournament", fake_fetch)
    result = checkpoints.capture_due_checkpoints(db, "key")
    assert result["due_tours"] == ["ATP"]
    assert result["captured_events"] == 64
    assert len(calls) == 1
