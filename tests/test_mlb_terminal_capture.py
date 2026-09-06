from contextlib import contextmanager
import pytest
from ingest.mlb_terminal_capture import capture_movement, BOOKMAKERS
from ingest import mlb_schedule


class Db:
    def __init__(self, *, acquired=True, upcoming=2, missing=2, remaining=1000, spent=0):
        self.acquired, self.upcoming, self.missing = acquired, upcoming, missing
        self.remaining, self.spent = remaining, spent
        self.writes, self.commands = [], []

    @contextmanager
    def connect(self):
        yield self

    def cursor(self):
        return self

    def execute(self, query, params=None):
        self.commands.append(query)
        if "INSERT INTO odds_api_usage" in query:
            self.writes.append(params)
        return []

    def fetchone(self):
        return {"acquired": self.acquired}

    def execute_one(self, query, params=None):
        if "AS upcoming" in query:
            return {"upcoming": self.upcoming, "missing": self.missing}
        return {"remaining": self.remaining, "spent": self.spent}


@pytest.mark.parametrize("kwargs", [{"acquired": False}, {"upcoming": 0}, {"missing": 0}, {"remaining": 250}, {"spent": 120}])
def test_no_purchase_when_locked_covered_empty_or_budget_reserved(monkeypatch, kwargs):
    monkeypatch.setattr(mlb_schedule, "fetch_odds", lambda *a, **k: pytest.fail("Must not purchase"))
    db = Db(**kwargs)
    assert capture_movement(db, "secret", "2026-09-06") == 0
    assert not db.writes


def test_partially_covered_slate_is_purchased_and_usage_audited(monkeypatch):
    def fetch(db, key, date, *, bookmakers, request_audit, capture_policy):
        assert bookmakers == BOOKMAKERS and len(bookmakers.split(",")) <= 10
        assert 'polymarket' in bookmakers and 'pinnacle' in bookmakers
        assert capture_policy == 'mlb-movement-ten-books-v1'
        request_audit.update(status=200, endpoint="/sports/baseball_mlb/odds", requests_last="3", requests_used="103", requests_remaining="897")
        return 2
    monkeypatch.setattr(mlb_schedule, "fetch_odds", fetch)
    db = Db(missing=1)
    assert capture_movement(db, "secret", "2026-09-06") == 2
    assert len(db.writes) == 1 and db.writes[0][3:7] == (3, 103, 897, 200)
    assert "secret" not in repr(db.writes)
    assert "pg_advisory_unlock" in db.commands[-1]


def test_failed_purchase_releases_lock_and_preserves_headers(monkeypatch):
    def fetch(*args, **kwargs):
        kwargs["request_audit"].update(status=429, requests_last="0")
        raise RuntimeError("rate limited")
    monkeypatch.setattr(mlb_schedule, "fetch_odds", fetch)
    db = Db()
    with pytest.raises(RuntimeError):
        capture_movement(db, "secret", "2026-09-06")
    assert db.writes[0][6] == 429
    assert "pg_advisory_unlock" in db.commands[-1]
