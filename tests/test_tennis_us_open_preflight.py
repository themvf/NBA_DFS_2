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
