from datetime import datetime, timedelta, timezone

from ingest.nfl_pickem_refresh import refresh_changed_quotes


def test_no_new_quote_does_not_rebuild_or_make_paid_requests(monkeypatch):
    import ingest.nfl_pickem_refresh as module
    now = datetime.now(timezone.utc)
    class DB:
        def execute(self, statement, params=None):
            if "SELECT DISTINCT season,week" in statement:
                return [{"season": 2026, "week": 4}]
            if "quote_at" in statement:
                return [{"game_id": "game", "quote_at": now.isoformat()}]
            return []
    monkeypatch.setattr(module, "current_quotes", lambda *args: [{"game_id": "game", "captured_at": now}])
    monkeypatch.setattr(module, "freeze_forecasts", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unchanged odds")))
    assert refresh_changed_quotes(DB()) == {"refreshed": [], "paid_requests": 0}


def test_changed_quote_rebuilds_paired_forecast_after_capture(monkeypatch):
    import ingest.nfl_pickem_refresh as module
    now = datetime.now(timezone.utc)
    class DB:
        def execute(self, statement, params=None):
            if "SELECT DISTINCT season,week" in statement:
                return [{"season": 2026, "week": 4}]
            if "quote_at" in statement:
                return [{"game_id": "game", "quote_at": (now-timedelta(minutes=1)).isoformat()}]
            return []
    calls = []
    monkeypatch.setattr(module, "current_quotes", lambda *args: [{"game_id": "game", "captured_at": now}])
    def freeze(db, model, season, week, persist):
        calls.append((season, week, persist))
        return {"forecasts": [{"covered": True}]}
    monkeypatch.setattr(module, "freeze_forecasts", freeze)
    assert refresh_changed_quotes(DB())["refreshed"] == [{"season": 2026, "week": 4, "forecasts": 1, "adjusted": 1}]
    assert calls == [(2026, 4, True)]
