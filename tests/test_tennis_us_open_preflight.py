import runpy
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

from ingest import tennis_us_open_preflight as preflight


class FakeDb:
    def __init__(self, coverage, metadata=None, captures=None):
        self.coverage = coverage
        self.metadata = metadata or {
            "first_round_events": 128,
            "surface_known": 128,
            "best_of_known": 128,
            "outdoor_known": 128,
        }
        self.captures = captures or {
            "matches": 128,
            "with_two_sportsbook_captures": 128,
            "avg_sportsbook_captures": 4,
        }

    def execute(self, sql, params=None):
        if "FROM tennis_matches tm" in sql:
            return self.coverage
        raise AssertionError(sql)

    def execute_one(self, sql, params=None):
        if "FROM tennis_events" in sql:
            return self.metadata
        if "capture_counts" in sql:
            return self.captures
        raise AssertionError(sql)


def _coverage(atp=64, wta=64):
    return [
        {"tour": "ATP", "fixtures": atp, "priced": atp, "canonicalized": atp},
        {"tour": "WTA", "fixtures": wta, "priced": wta, "canonicalized": wta},
    ]


def test_complete_draw_and_metadata_are_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        preflight, "discover_tournaments",
        lambda *_: [
            ("ATP", "tennis_atp_us_open", "ATP US Open"),
            ("WTA", "tennis_wta_us_open", "WTA US Open"),
        ],
    )
    result = preflight.preflight(FakeDb(_coverage()), "key")
    assert result["ready"] is True
    assert result["issues"] == []


def test_incomplete_wta_draw_fails_readiness(monkeypatch) -> None:
    monkeypatch.setattr(
        preflight, "discover_tournaments",
        lambda *_: [("ATP", "tennis_atp_us_open", "ATP US Open")],
    )
    result = preflight.preflight(FakeDb(_coverage(wta=62)), "key")
    assert result["ready"] is False
    assert "WTA first-round draw has 62/64 fixtures" in result["issues"]


# --- Tournament window gate (2026-09-29: the preflight turned every tennis
# refresh red after the 2026 final because the provider no longer advertised
# a US Open). -------------------------------------------------------------

EDITION = {
    "first_at": datetime(2026, 8, 30, 15, 0, tzinfo=timezone.utc),
    "last_at": datetime(2026, 9, 13, 18, 0, tzinfo=timezone.utc),
}


class WindowDb(FakeDb):
    def __init__(self, window, coverage=None):
        super().__init__(coverage or _coverage(atp=88, wta=88))
        self.window = window
        self.writes = 0

    def execute(self, sql, params=None):
        if "UPDATE tennis_events" in sql:
            self.writes += 1
            return []
        return super().execute(sql, params)

    def execute_one(self, sql, params=None):
        if "WITH latest AS" in sql:
            return self.window
        return super().execute_one(sql, params)


def test_after_the_final_the_preflight_skips_instead_of_failing(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "discover_tournaments", lambda *_: [])
    db = WindowDb(EDITION)
    result = preflight.preflight(db, "key", enrich=True, now=datetime(2026, 9, 29, tzinfo=timezone.utc))
    assert result["skipped"] is True
    assert result["ready"] is None
    assert "2026-08-30 to 2026-09-13" in result["reason"]
    assert db.writes == 0  # no metadata enrichment outside the window


def test_inside_the_stored_window_readiness_is_still_judged(monkeypatch) -> None:
    # The provider dropping the tournament mid-Slam is exactly what the gate
    # must still catch, so the stored schedule keeps the window open.
    monkeypatch.setattr(preflight, "discover_tournaments", lambda *_: [])
    result = preflight.preflight(WindowDb(EDITION), "key", now=datetime(2026, 9, 5, tzinfo=timezone.utc))
    assert result["ready"] is False
    assert "The Odds API does not advertise an active US Open tournament" in result["issues"]


def test_window_opens_a_week_before_the_first_match(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "discover_tournaments", lambda *_: [])
    early = preflight.preflight(WindowDb(EDITION), "key", now=datetime(2026, 8, 22, tzinfo=timezone.utc))
    lead = preflight.preflight(WindowDb(EDITION), "key", now=datetime(2026, 8, 24, tzinfo=timezone.utc))
    assert early.get("skipped") is True
    assert lead.get("skipped") is None


def test_no_stored_fixtures_and_no_provider_listing_skips(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "discover_tournaments", lambda *_: [])
    result = preflight.preflight(WindowDb({"first_at": None, "last_at": None}), "key")
    assert result["skipped"] is True
    assert "no US Open fixtures are stored" in result["reason"]


def test_provider_listing_opens_the_window_even_off_calendar(monkeypatch) -> None:
    monkeypatch.setattr(
        preflight, "discover_tournaments",
        lambda *_: [("ATP", "tennis_atp_us_open", "ATP US Open"), ("WTA", "tennis_wta_us_open", "WTA US Open")],
    )
    result = preflight.preflight(FakeDb(_coverage()), "key", now=datetime(2027, 8, 20, tzinfo=timezone.utc))
    assert result["ready"] is True


def test_cli_exits_zero_when_skipped(monkeypatch, capsys) -> None:
    import config as config_module
    import db.database as database_module
    import ingest.tennis_schedule as schedule_module

    monkeypatch.setattr(sys, "argv", ["tennis_us_open_preflight", "--enrich", "--fail-on-unready"])
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(
        database_url="postgres://x", odds_api=SimpleNamespace(api_key="k")))
    monkeypatch.setattr(database_module, "DatabaseManager", lambda url: WindowDb(EDITION))
    monkeypatch.setattr(schedule_module, "discover_tournaments", lambda *_: [])
    runpy.run_module("ingest.tennis_us_open_preflight", run_name="__main__")  # no SystemExit
    out = capsys.readouterr().out
    assert out.startswith("US Open preflight skipped: outside the US Open window")
