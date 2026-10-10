"""dfsdb contest import: the pure layer, and the client's manners.

Fixtures are trimmed from the real 2026-10-08 TB @ DAL showdown payload
(contest 3605c6fb-b8ce-46bb-b829-9396d0982010) and a 2025 Milly Maker.
"""

import pytest

from model import dfsdb_contest as m

LINK = "https://www.dfsdb.com/contest/3605c6fb-b8ce-46bb-b829-9396d0982010"
CID = "3605c6fb-b8ce-46bb-b829-9396d0982010"

CONTEST = {
    "id": CID, "platform": "draftkings", "sport": "nfl",
    "contest_name": "NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (TB @ DAL)",
    "contest_date": "2026-10-08", "buy_in": 20, "prize_pool": 1500000, "total_entries": 88235,
    "contest_category": "Other", "contest_type": "Large GPP", "contest_series": "Showdown",
    "validation_status": "validated",
}
STATS = {"cashingEntries": 7503, "minCash": 3.33, "firstPlacePrize": 129809.17, "totalsComplete": True}
ATHLETES = [
    {"athlete_name": "Bucky Irving", "position": "RB", "team": "TB", "salary": 12600, "fantasy_points": 34.5, "ownership_pct": 23.55},
    {"athlete_name": "George Pickens", "position": "WR", "team": "DAL", "salary": 14100, "fantasy_points": 31, "ownership_pct": 27.63},
    {"athlete_name": "Dak Prescott", "position": "QB", "team": "DAL", "salary": 15600, "fantasy_points": 17.64, "ownership_pct": 62.53},
    {"athlete_name": "Evan Deckers", "position": "WR", "team": "TB", "salary": 200, "fantasy_points": 0, "ownership_pct": 0},
]


def result(rank, points, winnings, entries, user="u", uid=None):
    uid = uid or f"00000000-0000-0000-0000-{abs(hash(user)) % 10**12:012d}"
    return {"id": f"e-{rank}-{user}", "rank": rank, "points": points, "winnings": winnings,
            "entry_cost": 20 * entries, "entry_count": entries, "player_id": uid,
            "players": {"id": uid, "display_name": user}, "cash_winnings": winnings}


RESULTS = [
    result(1, 136.59, 128888.33, 22, "SouthernDraft"),
    result(1, 136.59, 128453.33, 4, "niro421"),
    result(4, 134.1, 30000, 1, "solo"),
    result(5, 133.0, 15000, 1, "solo2"),
    result(5, 133.0, 15500, 3, "multi"),
    result(7503, 90.0, 3.33, 1, "lastcash"),
    result(7504, 89.9, 0, 1, "bubble"),
]


# --------------------------------------------------------------------------
# Link + card
# --------------------------------------------------------------------------

def test_the_uuid_comes_out_of_a_pasted_link_or_a_bare_id():
    assert m.contest_id_from_link(LINK) == CID
    assert m.contest_id_from_link(LINK + "?tab=standings") == CID
    assert m.contest_id_from_link(CID.upper()) == CID
    with pytest.raises(ValueError):
        m.contest_id_from_link("https://www.dfsdb.com/contests")


def test_format_is_read_from_series_or_name():
    assert m.contest_format(CONTEST) == "showdown"
    assert m.contest_format({"contest_series": "Milly Maker", "contest_name": "NFL $2.5M Fantasy Football Millionaire"}) == "classic"
    assert m.contest_format({"contest_series": None, "contest_name": "NFL Showdown $100K (KC @ LV)"}) == "showdown"


# --------------------------------------------------------------------------
# Payout curve: one row per USER, winnings pooled across entries
# --------------------------------------------------------------------------

def test_payout_curve_uses_single_entry_users_only():
    rows = m.standings_rows(CID, RESULTS)
    curve = m.payout_curve(rows, STATS)
    payouts = dict((r, w) for r, w in curve["payout_by_rank"])
    # Rank 1: both users are multi-entry, their winnings pool other entries -> no payout resolved.
    assert 1 not in payouts
    assert payouts[4] == 30000
    # Rank 5: the single-entry user's number wins over the multi-entry user's pooled one.
    assert payouts[5] == 15000
    assert payouts[7503] == 3.33
    assert curve["ranks_fetched"] == 5
    assert curve["coverage"] == pytest.approx(4 / 5)


def test_cash_line_is_the_last_cashing_rank_only_when_the_fetch_reaches_it():
    rows = m.standings_rows(CID, RESULTS)
    assert m.payout_curve(rows, STATS)["cash_line_points"] == 90.0
    short = m.payout_curve(rows[:4], STATS)
    assert short["cash_line_points"] is None
    assert m.payout_curve([], STATS)["ranks_fetched"] == 0


def test_score_by_rank_counts_users_tied_at_a_rank():
    curve = m.payout_curve(m.standings_rows(CID, RESULTS), STATS)
    assert curve["score_by_rank"][0] == [1, 136.59, 2]


# --------------------------------------------------------------------------
# Athletes
# --------------------------------------------------------------------------

def test_athletes_are_normalized_and_sorted_by_ownership():
    rows = m.athlete_rows(CID, ATHLETES)
    assert [r["athlete_name"] for r in rows][:2] == ["Dak Prescott", "George Pickens"]
    assert rows[0]["normalized_name"] == "dakprescott"
    assert rows[-1]["ownership_pct"] == 0


def test_a_duplicate_athlete_keeps_the_higher_ownership():
    rows = m.athlete_rows(CID, ATHLETES + [{**ATHLETES[0], "ownership_pct": 30.0}])
    irving = [r for r in rows if r["athlete_name"] == "Bucky Irving"]
    assert len(irving) == 1 and irving[0]["ownership_pct"] == 30.0


# --------------------------------------------------------------------------
# Contest row + mirror
# --------------------------------------------------------------------------

def contest_row(**overrides):
    payload = {"contest": {**CONTEST, **overrides}, "stats": STATS, "athletes": ATHLETES, "results": RESULTS}
    standings = m.standings_rows(CID, RESULTS)
    return m.contest_row(CID, payload, source_url=m.contest_url(CID), digest=m.payload_digest(payload),
                         standings=standings, standings_total=26592, complete=False)


def test_contest_row_carries_provenance_and_the_curve():
    row = contest_row()
    assert row["format"] == "showdown" and row["sport"] == "nfl"
    assert row["standings_fetched"] == 7 and row["standings_users"] == 26592
    assert row["standings_complete"] is False
    assert row["payout_curve"]["first_place_prize"] == 129809.17
    assert len(row["payload_digest"]) == 64
    assert row["import_version"] == m.VERSION


def test_mirror_is_showdown_flex_only_ownership():
    row = contest_row()
    payload = m.mirror_payload(row, m.athlete_rows(CID, ATHLETES), row["payout_curve"])
    assert payload["contest_id"] == f"dfsdb-{CID}"
    assert payload["winning_score"] == 136.59
    assert payload["entry_count"] == 88235
    dak = payload["players"]["dakprescott"]
    assert dak["drafted_by_slot"] == {"FLEX": 62.53} and dak["drafted_pct"] == 62.53
    assert dak["fpts"] == 17.64


def test_mirror_refuses_classic_and_other_sports():
    classic = contest_row(contest_series="Milly Maker", contest_name="NFL $2.5M Fantasy Football Millionaire")
    with pytest.raises(ValueError, match="classic"):
        m.mirror_payload(classic, m.athlete_rows(CID, ATHLETES), classic["payout_curve"])
    nba = contest_row(sport="nba")
    with pytest.raises(ValueError, match="NFL"):
        m.mirror_payload(nba, m.athlete_rows(CID, ATHLETES), nba["payout_curve"])


# --------------------------------------------------------------------------
# Users and lineups
# --------------------------------------------------------------------------

USER = {
    "player": {"id": "69a50681-29e3-4885-904c-a23d52a7733c", "display_name": "SouthernDraft",
               "created_at": "2026-01-15T22:33:39.748857+00:00"},
    "stats": [
        {"sport": "golf", "contests_played": 74, "total_entries": 2154, "roi_pct": -68.58, "cash_rate": 18.3},
        {"sport": "nfl", "contests_played": 33, "total_entries": 220, "roi_pct": 412.0, "cash_rate": 21.0,
         "net_profit": 120000, "first_contest": "2023-09-10", "last_contest": "2026-10-08"},
    ],
    "summary": {"first_contest": "2021-12-26", "last_contest": "2026-10-08", "win_count": 1},
    "splits": [
        {"split_type": "buy_in", "split_key": "<$5", "entries": 2788, "roi": 188.52, "cash_rate": 17.8},
        {"split_type": "buy_in", "split_key": "$20-$50", "entries": 60, "roi": 900.0, "cash_rate": 20.0},
        {"split_type": "buy_in", "split_key": "$500+", "entries": 2, "roi": 5000.0, "cash_rate": 50.0},
        {"split_type": "contest_type", "split_key": "Showdown", "entries": 100, "roi": 300.0, "cash_rate": 25.0},
    ],
    "history": [],
}


def test_user_profile_is_the_sport_record_plus_the_best_meaningful_split():
    profile = m.user_profile(USER, "nfl")
    assert profile["contests_played"] == 33 and profile["entries_per_contest"] == pytest.approx(6.7)
    assert profile["roi_pct"] == 412.0
    assert profile["sports_played"] == ["golf", "nfl"]
    # A 2-entry split with a 5000% ROI is noise; the 20-entry floor keeps it out.
    assert profile["best_buy_in"]["key"] == "$20-$50"
    assert profile["best_contest_type"]["key"] == "Showdown"
    assert m.user_profile(USER, "nba")["contests_played"] == 0


def test_history_rows_flatten_the_nested_contest_card():
    rows = m.history_rows("u1", [{"id": "e1", "rank": 3, "points": 100.0, "winnings": 50, "entry_count": 2,
                                  "contests": {"id": CID, "contest_name": "x", "contest_date": "2026-10-08",
                                               "sport": "nfl", "buy_in": 20}},
                                 {"id": "e2", "contests": {}}])
    assert len(rows) == 1 and rows[0]["dfsdb_contest_id"] == CID and rows[0]["buy_in"] == 20.0


def test_lineup_annotation_reads_ownership_from_the_contest():
    lineup = {"id": "l1", "contest_id": CID, "username": "x", "rank": 1, "points": 136.59,
              "lineup_players": [
                  {"playerId": 1, "name": "Dak Prescott", "position": "CPT", "team": "DAL", "salary": 23400, "points": 26.46},
                  {"playerId": 2, "name": "Bucky Irving", "position": "FLEX", "team": "TB", "salary": 12600, "points": 34.5},
                  {"playerId": 3, "name": "Somebody Else", "position": "FLEX", "team": "TB", "salary": 200, "points": 0},
              ]}
    rows = m.lineup_rows("nfl", [lineup, {"id": "no-contest", "lineup_players": []}])
    assert len(rows) == 1
    note = m.annotate_lineup(rows[0], m.athlete_rows(CID, ATHLETES))
    assert note["salary_used"] == 36200
    assert note["ownership_sum"] == pytest.approx(62.53 + 23.55, abs=0.05)   # rounded to 0.1
    assert note["ownership_unknown"] == 1
    assert note["max_same_team"] == 2


def test_report_renders_without_profiles_or_lineups():
    row = contest_row()
    text = m.format_report(row, m.athlete_rows(CID, ATHLETES), m.standings_rows(CID, RESULTS), [], [])
    assert "cash line 90.00 pts" in text
    assert "SouthernDraft" in text and "Dak Prescott" in text


# --------------------------------------------------------------------------
# Client manners (no network)
# --------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status, payload=None, text=""):
        self.status_code, self._payload, self.text = status, payload, text

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls, self.headers = list(responses), [], {}

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append((url, params))
        return self.responses.pop(0)


def test_client_retries_server_errors_and_rate_limits_then_gives_up():
    from ingest.dfsdb_contest import Client, DfsdbError
    session = FakeSession([FakeResponse(503), FakeResponse(429), FakeResponse(200, {"ok": 1})])
    client = Client(delay=0, retries=3, session=session)
    assert client.get("/api/contest/x") == {"ok": 1}
    assert client.calls == 3
    session = FakeSession([FakeResponse(503), FakeResponse(503)])
    with pytest.raises(DfsdbError, match="HTTP 503"):
        Client(delay=0, retries=1, session=session).get("/api/contest/x")


def test_client_does_not_retry_a_refusal_or_a_bad_request():
    from ingest.dfsdb_contest import Client, DfsdbError
    for status, needle in ((403, "refused"), (404, "not found"), (400, "rejected")):
        session = FakeSession([FakeResponse(status, text="Invalid limit parameter")])
        with pytest.raises(DfsdbError, match=needle):
            Client(delay=0, session=session).get("/api/contest/x")
        assert len(session.calls) == 1


def test_fetch_contest_stops_at_the_requested_page_and_reports_completeness():
    from ingest.dfsdb_contest import Client, fetch_contest
    page = lambda n: {"contest": CONTEST, "stats": STATS, "athletes": ATHLETES,
                      "results": [result(n, 100 - n, 0, 1, f"u{n}")],
                      "pagination": {"page": n, "limit": 100, "totalCount": 300, "totalPages": 3}}
    session = FakeSession([FakeResponse(200, page(1)), FakeResponse(200, page(2))])
    fetched = fetch_contest(Client(delay=0, session=session), CID, pages=2, all_pages=False)
    assert len(fetched["results"]) == 2 and fetched["complete"] is False
    assert fetched["standings_total"] == 300
    session = FakeSession([FakeResponse(200, page(1)), FakeResponse(200, page(2)), FakeResponse(200, page(3))])
    assert fetch_contest(Client(delay=0, session=session), CID, pages=1, all_pages=True)["complete"] is True


def test_lineups_feed_failure_is_reported_not_raised():
    from ingest.dfsdb_contest import Client, fetch_lineups
    session = FakeSession([FakeResponse(503), FakeResponse(503)])
    rows, error = fetch_lineups(Client(delay=0, retries=1, session=session), "nfl", 2026, 3)
    assert rows == [] and "503" in error


# --------------------------------------------------------------------------
# Date mode: a slate's contests from the listing
# --------------------------------------------------------------------------

def listing(contest_id, date, entries, name="NFL Showdown $1 (TB @ DAL)", series="Showdown"):
    return {"id": contest_id, "platform": "draftkings", "sport": "nfl", "contest_name": name,
            "contest_date": date, "buy_in": 1, "prize_pool": 1000, "total_entries": entries,
            "contest_type": "Large GPP", "contest_series": series}


DAY = [
    listing("a", "2026-10-08", 237812),
    listing("b", "2026-10-08", 88235),
    listing("c", "2026-10-08", 500),
    listing("d", "2026-10-08", 147058, "NFL $2.5M Fantasy Football Millionaire", "Milly Maker"),
    listing("e", "2026-10-07", 999999),
    listing("b", "2026-10-08", 88235),          # duplicate id across pages
]


def test_select_day_contests_takes_the_biggest_fields_on_the_day_once_each():
    rows = m.contest_listing_rows(DAY)
    picked = m.select_day_contests(rows, "2026-10-08", min_entries=1000, max_contests=10)
    assert [r["id"] for r in picked] == ["a", "d", "b"]
    assert picked[1]["format"] == "classic"
    only_showdown = m.select_day_contests(rows, "2026-10-08", min_entries=1000, max_contests=10,
                                          formats=("showdown",))
    assert [r["id"] for r in only_showdown] == ["a", "b"]
    assert len(m.select_day_contests(rows, "2026-10-08", min_entries=1000, max_contests=1)) == 1
    assert m.select_day_contests(rows, "2026-10-08", min_entries=1, max_contests=10**6)[-1]["id"] == "c"


def test_the_day_cap_is_hard():
    rows = m.contest_listing_rows([listing(str(i), "2026-10-08", 10**6 - i) for i in range(80)])
    assert len(m.select_day_contests(rows, "2026-10-08", min_entries=1, max_contests=500)) == m.MAX_DAY_CONTESTS


def test_listing_is_past_once_a_page_runs_before_the_date():
    assert m.listing_is_past(DAY[:4], "2026-10-08") is False
    assert m.listing_is_past(DAY, "2026-10-08") is True
    assert m.listing_is_past([], "2026-10-08") is False


def test_discover_contests_stops_paging_once_past_the_date():
    from ingest.dfsdb_contest import Client, discover_contests
    page1 = {"data": DAY[:3], "pagination": {"page": 1, "totalPages": 5}}
    page2 = {"data": DAY[3:5], "pagination": {"page": 2, "totalPages": 5}}
    session = FakeSession([FakeResponse(200, page1), FakeResponse(200, page2), FakeResponse(200, {"data": []})])
    found = discover_contests(Client(delay=0, session=session), "nfl", "2026-10-08", search="TB @ DAL")
    assert [r["id"] for r in found["rows"]] == ["a", "b", "c", "d"]
    assert found["reached"] is True and found["pages"] == 2
    assert len(session.calls) == 2                       # page 2 ran past the date; page 3 never asked
    url, params = session.calls[0]
    assert url.endswith("/api/contests") and params["search"] == "TB @ DAL"
    assert params["sortBy"] == "contest_date" and params["sortOrder"] == "desc" and params["year"] == 2026


def test_discover_contests_says_when_the_cap_stopped_it_before_the_date():
    from ingest.dfsdb_contest import MAX_DISCOVERY_PAGES, Client, discover_contests
    newer = {"data": [listing("z", "2026-10-09", 10)], "pagination": {"page": 1, "totalPages": 999}}
    session = FakeSession([FakeResponse(200, newer)] * MAX_DISCOVERY_PAGES)
    found = discover_contests(Client(delay=0, session=session), "nfl", "2026-10-04", contest_type="Milly Maker")
    assert found["rows"] == [] and found["reached"] is False
    assert found["pages"] == MAX_DISCOVERY_PAGES and found["oldest_date"] == "2026-10-09"
    assert session.calls[0][1]["contestType"] == "Milly Maker"


def test_cli_needs_exactly_one_of_link_or_date():
    from ingest.dfsdb_contest import main
    with pytest.raises(SystemExit):
        main([])
    with pytest.raises(SystemExit):
        main([LINK, "--date", "2026-10-08", "--dry-run"])
    with pytest.raises(SystemExit):
        main(["--date", "10/08/2026", "--dry-run"])
