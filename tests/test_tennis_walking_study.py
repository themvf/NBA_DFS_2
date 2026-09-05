from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from model.line_alerts import _selection_prices
from model.tennis_walking_study import VERSION, build_report, enrollment

NOW = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)


def candidate():
    books = {k: {"ml_home": -200, "ml_away": 180,
                  "last_update": (NOW - timedelta(minutes=2)).isoformat()}
             for k in ("draftkings", "fanduel", "betmgm")}
    return dict(
        context=dict(tour="WTA", tournament="Test", surface=None,
                     captured_at=NOW - timedelta(minutes=1), commence_time=NOW + timedelta(hours=1),
                     history_id=10, books=books),
        opening={"id": 1, "books": deepcopy(books)},
        details=dict(drift_pp=2.1, overlap_books=3, exec_book="draftkings",
                     exec_decimal=1.5, exec_price_available=True),
        probability=.65, now=NOW,
    )


@pytest.mark.parametrize("probability,accepted", [(.59999, False), (.6, True), (.69999, True), (.7, False)])
def test_frozen_probability_boundaries(probability, accepted):
    args = candidate()
    args["probability"] = probability
    assert bool(enrollment(**args)) is accepted


@pytest.mark.parametrize("change", ["atp", "old_capture", "old_quote", "future_quote", "started", "missing_price", "few_books", "mixed"])
def test_enrollment_exclusions(change):
    args = candidate()
    ctx, d = args["context"], args["details"]
    if change == "atp": ctx["tour"] = "ATP"
    if change == "old_capture": ctx["captured_at"] = NOW - timedelta(minutes=16)
    if change == "old_quote": ctx["books"]["draftkings"]["last_update"] = NOW - timedelta(minutes=16)
    if change == "future_quote": ctx["books"]["draftkings"]["last_update"] = NOW + timedelta(minutes=1)
    if change == "started": ctx["commence_time"] = NOW
    if change == "missing_price": d.pop("exec_decimal")
    if change == "few_books": d["overlap_books"] = 2
    if change == "mixed": ctx["books"]["polymarket"] = {}
    assert enrollment(**args) == {}


def record(i=1, status="completed", winner="home"):
    args = candidate()
    details = {**args["details"], **enrollment(**args)}
    return dict(id=i, matchup_id=i, created_at=NOW, commence_time=NOW + timedelta(hours=1),
                game_date=NOW.date(), side="home", winner=winner,
                completion_status=status, details_json=details)


def test_only_new_version_and_confirmed_results_enter_primary():
    old = record(5)
    old["details_json"].pop("walking_study_version")
    report = build_report([record(), record(2, "unknown"), record(3, "retired"),
                           record(4, "scheduled", None), old, record()])
    assert report["enrolled_unique_matches"] == 4
    assert report["primary_completed"]["n"] == 1
    assert report["primary_completed"]["roi"] == .5
    assert report["uncertain_winners_sensitivity_only"]["n"] == 1
    assert report["pending_no_winner"] == 1
    assert report["excluded_statuses"] == {"retired": 1}
    assert report["primary_completed"]["roi_95ci"] is None
    assert report["status"] == "collecting"


def test_report_uses_exact_moneyline_and_excludes_missing_or_stale_close():
    r = record()
    r.update(close_captured_at=NOW + timedelta(minutes=59),
             close_boundary_at=NOW + timedelta(hours=1),
             close_books={"draftkings": {"ml_home": -200, "ml_away": 180,
                          "under": -107, "over": -124,
                          "last_update": NOW + timedelta(minutes=58)}})
    report = build_report([r])["primary_completed"]
    assert report["paired_primary_closes"] == 1
    assert report["same_book_execution_clv"] == pytest.approx(0)
    assert report["closing_fair_ticket_ev"] == pytest.approx(1.5 * (2/3) / (2/3 + 1/2.8) - 1)
    for kind in ("missing", "stale"):
        bad = deepcopy(r)
        if kind == "missing": bad["close_books"]["draftkings"].pop("ml_home")
        else: bad["close_books"]["draftkings"]["last_update"] = NOW
        assert build_report([bad])["primary_completed"]["paired_primary_closes"] == 0


def test_moneyline_grader_does_not_read_total_under_price():
    alert = {"side": "away", "details_json": {"market": "moneyline", "exec_book": "betrivers"}}
    price, _ = _selection_prices(alert, {"betrivers": {
        "ml_away": -286, "ml_home": 225, "under": -107, "over": -124, "total_line": 20.5}})
    assert price == pytest.approx(1 + 100/286)


def test_zero_enrollment_is_explicit_and_does_not_validate():
    report = build_report([])
    assert report["enrolled_unique_matches"] == 0
    assert report["status"] == "collecting"
    assert report["primary_completed"]["roi"] is None
    assert report["study_version"] == VERSION


def test_nonprestart_frozen_record_is_rejected():
    r = record()
    r["created_at"] = r["commence_time"]
    report = build_report([r])
    assert report["invalid_frozen_records"] == 1
    assert report["primary_completed"]["n"] == 0


@pytest.mark.parametrize("prior_exists", [False, True])
def test_live_scan_tags_only_first_walking_without_relabeling(monkeypatch, prior_exists):
    from model import line_alerts
    now = datetime.now(timezone.utc)
    books = {key: {"ml_home": -200, "ml_away": 180,
                   "last_update": (now - timedelta(seconds=30)).isoformat()}
             for key in ("draftkings", "fanduel", "betmgm")}
    opening_books = {key: {"ml_home": -110, "ml_away": -110} for key in books}
    row = dict(tour="WTA", tournament="Test", surface="hard", history_id=10,
               matchup_id=1, game_date=now.date(), home_team_name="A", away_team_name="B",
               captured_at=now - timedelta(seconds=20), capture_key="test", books=books,
               commence_time=now + timedelta(hours=1))

    class DB:
        def execute(self, sql, params):
            return [row] if "SELECT DISTINCT" in sql else []

        def execute_one(self, sql, params):
            if "FROM line_alerts" in sql:
                return {"id": 2} if prior_exists else None
            if "ORDER BY captured_at ASC" in sql:
                return {"history_id": 1, "captured_at": now - timedelta(hours=3), "books": opening_books}
            return None

    inserted = []
    def capture(db, **kwargs):
        inserted.append(kwargs)
        return []
    monkeypatch.setattr(line_alerts, "_insert", capture)
    line_alerts.scan(DB(), "tennis")
    walking = [r for r in inserted if r["alert_type"] == "walking"]
    assert len(walking) == 1
    assert ("walking_study_version" in walking[0]["details"]) is not prior_exists
