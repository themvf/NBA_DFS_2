from datetime import date, datetime, timezone

from ingest import nba_game_lines as g


def _event(**over):
    e = {"id": "e1", "commence_time": "2026-11-08T00:00:00Z", "home_team": "Boston Celtics", "away_team": "Utah Jazz",
         "bookmakers": [
             {"key": "draftkings", "markets": [
                 {"key": "spreads", "outcomes": [{"name": "Utah Jazz", "price": -110, "point": 11.5},
                                                 {"name": "Boston Celtics", "price": -110, "point": -11.5}]},
                 {"key": "totals", "outcomes": [{"name": "Over", "price": -108, "point": 224.5},
                                                {"name": "Under", "price": -112, "point": 224.5}]}]},
             {"key": "fanduel", "markets": [
                 {"key": "spreads", "outcomes": [{"name": "Boston Celtics", "price": -105, "point": -12.5},
                                                 {"name": "Utah Jazz", "price": -115, "point": 12.5}]}]},
             {"key": "bovada", "markets": [
                 {"key": "spreads", "outcomes": [{"name": "Boston Celtics", "price": -110, "point": -30.0},
                                                 {"name": "Utah Jazz", "price": -110, "point": 30.0}]}]},
         ]}
    e.update(over)
    return e


def test_book_lines_reads_home_spread_and_total_for_configured_books_only():
    books = g.book_lines(_event())
    assert set(books) == {"draftkings", "fanduel"}
    assert books["draftkings"] == {"spread": -11.5, "home_price": -110, "away_price": -110,
                                   "total": 224.5, "over_price": -108, "under_price": -112}
    assert books["fanduel"] == {"spread": -12.5, "home_price": -105, "away_price": -115}


def test_summary_uses_medians_and_a_stable_content_hash():
    s = g.summarize(_event())
    assert s["home_spread"] == -12.0 and s["total"] == 224.5 and s["book_count"] == 2
    assert s["content_sha"] == g.summarize(_event())["content_sha"]
    moved = _event()
    moved["bookmakers"][0]["markets"][0]["outcomes"][1]["point"] = -10.5
    assert g.summarize(moved)["content_sha"] != s["content_sha"]
    assert g.summarize(_event(bookmakers=[])) is None


def test_select_events_keeps_unstarted_games_on_the_eastern_date():
    now = datetime(2026, 11, 7, 23, 45, tzinfo=timezone.utc)
    events = [_event(id="started", commence_time="2026-11-07T23:30:00Z"),
              _event(id="late", commence_time="2026-11-08T03:00:00Z"),
              _event(id="tomorrow", commence_time="2026-11-08T23:30:00Z")]
    assert [e["id"] for e in g.select_events(events, date(2026, 11, 7), now)] == ["late"]


def test_insert_skips_a_game_whose_lines_did_not_change():
    sql = " ".join(g.INSERT_IF_CHANGED.split())
    assert "IS DISTINCT FROM %(content_sha)s" in sql and "ORDER BY captured_at DESC LIMIT 1" in sql
