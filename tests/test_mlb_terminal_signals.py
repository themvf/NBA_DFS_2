from datetime import datetime, timedelta, timezone
import pytest
from model.mlb_terminal_signals import candidates, quote, settlement, run, VERSION
from ingest.mlb_terminal_quotes import enrich_terminal_books

NOW = datetime(2026, 9, 6, 18, tzinfo=timezone.utc)


def history(lines=(8, 8.5), minutes=(30, 0), prices=None):
    rows = []
    for index, (line, lead) in enumerate(zip(lines, minutes)):
        at = NOW - timedelta(minutes=lead)
        price = prices[index] if prices else -110
        book = {"total_line": line, "over": -110, "under": -110,
                "spread_home": -1.5, "spread_home_price": price,
                "spread_away": 1.5, "spread_away_price": -110,
                "ml_home": price, "ml_away": -110, "last_update": at.isoformat()}
        rows.append({"id": index+1, "captured_at": at, "books": {key: dict(book) for key in ("draftkings", "fanduel", "betmgm")}})
    return rows


def test_total_steam_freezes_real_execution_and_evidence():
    signal = candidates(history(), NOW)[0]
    assert signal["type"] == "mlb_total_steam" and signal["side"] == "over"
    assert signal["details"]["exec_line"] == 8.5
    assert signal["details"]["dk_odds"] == -110
    assert signal["details"]["trigger_history_id"] == 2
    assert signal["details"]["signal_version"] == VERSION


def test_stale_or_long_interval_is_not_steam():
    assert candidates(history(), NOW + timedelta(minutes=36)) == []
    assert candidates(history(minutes=(60, 0)), NOW) == []


def test_requires_three_same_books_and_not_exchange_turnover():
    rows = history()
    rows[0]["books"].pop("betmgm")
    rows[0]["books"]["polymarket"] = dict(rows[0]["books"]["draftkings"])
    assert candidates(rows, NOW) == []


def test_walking_requires_monotone_three_observations():
    assert any(s["type"] == "mlb_total_walking" for s in candidates(history((8, 8.5, 9), (90, 60, 0)), NOW))
    assert not any(s["type"] == "mlb_total_walking" for s in candidates(history((8, 9, 8.5), (90, 60, 0)), NOW))
    assert any(s["type"] == "mlb_total_reversal" and s["side"] == "under" for s in candidates(history((8, 9, 8.5), (90, 60, 0)), NOW))


def test_run_line_price_movement_at_fixed_handicap():
    rows = history(prices=(120, -110))
    assert any(s["type"] == "mlb_run_line_steam" and s["side"] == "home" for s in candidates(rows, NOW))
    for b in rows[0]["books"].values():
        b["spread_home"], b["spread_away"] = -2.5, 2.5
    signals = candidates(rows, NOW)
    assert not any(s["type"] == "mlb_run_line_steam" for s in signals)
    assert any(s["type"] == "mlb_run_line_points_steam" and s["side"] == "away" for s in signals)


def test_total_price_movement_is_separate_from_total_line_movement():
    rows = history((8, 8))
    for b in rows[1]["books"].values():
        b["over"] = -150
    signals = candidates(rows, NOW)
    assert any(s["type"] == "mlb_total_price_steam" and s["side"] == "over" for s in signals)
    assert not any(s["type"] == "mlb_total_steam" for s in signals)


def test_reference_books_do_not_become_execution_books():
    rows = history()
    for row in rows:
        sample = row["books"]["draftkings"]
        row["books"] = {key: dict(sample) for key in ("bovada", "caesars", "hardrockbet")}
    assert candidates(rows, NOW) == []


@pytest.mark.parametrize("market,side,line,home,away,expected", [
    ("moneyline", "home", None, 5, 3, ("won", "final_score")),
    ("moneyline", "away", None, 5, 3, ("lost", "final_score")),
    ("moneyline", "home", None, 3, 3, ("void", "tied_game")),
    ("total", "over", 8, 5, 3, ("void", "push")),
    ("total", "under", 8.5, 5, 3, ("won", "final_score")),
    ("run_line", "away", 1.5, 5, 4, ("won", "final_score")),
    ("run_line", "home", -1.5, 5, 4, ("lost", "final_score")),
    ("run_line", "home", -1, 5, 4, ("void", "push")),
])
def test_settle_frozen_selection(market, side, line, home, away, expected):
    assert settlement(market, side, line, "Final", home, away) == expected


def test_live_suspended_missing_and_shortened_scores_never_grade_as_final():
    for status in ("In Progress", "Suspended", "Scheduled", "Completed Early"):
        assert settlement("total", "over", 8, status, 5, 6)[0] is None
    assert settlement("total", "over", 8, "Final", None, 6)[0] is None
    assert settlement("run_line", "home", None, "Final", 5, 3)[0] is None
    assert settlement("moneyline", "draw", None, "Final", 5, 3)[0] is None


def test_reschedule_and_cancellation_are_void_not_losses():
    assert settlement("total", "over", 8, "Cancelled", None, None) == ("void", "cancelled")
    assert settlement("total", "over", 8, "Final", 5, 6, rescheduled=True) == ("void", "rescheduled")


def test_unpaired_total_and_legacy_away_run_line_are_unavailable():
    assert quote({"total_line": 8, "over_line": 8, "under_line": 9, "over": -110, "under": -110}, "total", "over") is None
    assert quote({"spread_home": -1.5, "spread_price": 120}, "run_line", "away") is None


def test_enrichment_preserves_legacy_scalar_and_athletics_alias():
    books = {"draftkings": {"spread_price": 120, "ml_home": -110}}
    event = {"home_team": "Athletics", "away_team": "New York Mets", "bookmakers": [{"key": "draftkings", "title": "DraftKings", "markets": [{"key": "spreads", "last_update": NOW.isoformat(), "outcomes": [
        {"name": "Oakland Athletics", "point": -1.5, "price": 120}, {"name": "New York Mets", "point": 1.5, "price": -140}]}]}]}
    enrich_terminal_books(event, books)
    assert books["draftkings"]["spread_price"] == 120
    assert books["draftkings"]["spread_away_price"] == -140
    assert books["draftkings"]["spread_home_price"] == 120
    assert books["draftkings"]["ml_home"] == -110


def test_scan_is_first_breach_only_and_keeps_entry_price(monkeypatch):
    import model.mlb_terminal_signals as signals
    captures = history()
    for capture in captures:
        capture.update(matchup_id=42, game_date="2026-09-06", home_team_name="Home", away_team_name="Away",
                       commence_time=NOW + timedelta(hours=1), capture_key=str(capture["id"]))
    detector = signals.candidates
    monkeypatch.setattr(signals, "candidates", lambda rows: detector(rows, NOW))

    class Ledger:
        entries = {}
        def execute(self, sql, params=None):
            if "INSERT INTO line_alerts" not in sql:
                return captures
            assert "DO NOTHING" in sql
            key = (params[0], params[4], params[5])
            if key in self.entries:
                return []
            self.entries[key] = params[-1]
            return [{"id": len(self.entries)}]

    db = Ledger()
    assert run(db, scan_only=True) == 1
    original = dict(db.entries)
    assert run(db, scan_only=True) == 0
    assert db.entries == original


def test_results_worker_refreshes_only_scores_and_never_scans(monkeypatch):
    from ingest import mlb_terminal_settlement as worker
    from model import line_alerts, mlb_terminal_signals
    calls = []
    class Db:
        def execute(self, sql):
            assert "mlb_matchups" in sql and "'mlb'" in sql
            return [{"game_date": "2026-09-05"}]
    monkeypatch.setattr(worker, "fetch_scores", lambda db, date: calls.append(("scores", date)))
    monkeypatch.setattr(line_alerts, "settle", lambda db, sport: calls.append(("legacy", sport)) or 2)
    monkeypatch.setattr(mlb_terminal_signals, "run", lambda db, **kwargs: calls.append(("terminal", kwargs)))
    assert worker.settle_results(Db()) == {"score_dates": 1, "legacy_grades": 2}
    assert calls == [("scores", "2026-09-05"), ("legacy", "mlb"), ("terminal", {"settle_only": True})]


def test_settle_only_never_queries_upcoming_capture_trails():
    class Db:
        def execute(self, sql, params):
            assert "FROM line_alerts a" in sql
            return []
    assert run(Db(), settle_only=True) == 0


def test_push_and_positive_verified_clv_are_separate_grades():
    import json
    alert = {"id": 12, "details_json": {"signal_version": VERSION, "market": "total", "metric": "runs",
      "exec_line": 8, "exec_book": "draftkings", "observed_at": NOW.isoformat()}, "game_date": "2026-09-06",
      "current_date": "2026-09-06", "side": "over", "game_status": "Final", "home_score": 5, "away_score": 3,
      "outcome": None, "grading_json": {}, "alert_prob": .5, "close_at": NOW + timedelta(minutes=10),
      "close_quality": "A", "close_history_id": 42,
      "closing_books": {"draftkings": {"total_line": 8.5, "over": -110, "under": -110, "last_update": (NOW+timedelta(minutes=10)).isoformat()}}}
    class Db:
        written = None
        def execute(self, sql, params):
            if "FROM line_alerts a" in sql:
                return [alert]
            self.written = params
            return []
    db = Db()
    run(db, settle_only=True)
    assert db.written[0] == "void"
    grade = json.loads(db.written[2])
    assert grade["settlement_reason"] == "push"
    assert grade["verified_clv"] == .5
    assert grade["clv_unit"] == "runs"
