"""stat-api contest import: the pure layer, the score-curve port, and the client's manners.

Fixtures are trimmed from the real week-4 2026 Millionaire (stat-api 20754235,
DraftKings 196151357) and the TB @ DAL Thursday showdown (29870409).
"""

import pytest

from model import statapi_contest as m

CONTEST = {
    "contest_id": 20754235, "external_id": 196151357,
    "name": "NFL $2.75M Fantasy Football Millionaire [$1M to 1st]", "operator": "DraftKings", "sport": "nfl",
    "game_type": "classic", "date": "2026-10-04", "slate_id": 473459, "slate_name": "Sun 1:00PM Classic",
    "entry_fee_cents": 2000, "prize_pool_cents": 275000000, "total_entries": 161764, "stored_lineups": 161764,
    "paid_places": 37425, "slots": ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST"], "has_lineups": True,
}
SHOWDOWN = {**CONTEST, "contest_id": 29870409, "external_id": 196438550, "game_type": "showdown",
            "name": "NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (TB @ DAL)",
            "slots": ["CPT", "FLEX", "FLEX", "FLEX", "FLEX", "FLEX"], "total_entries": 88235}


def seat(row, idx, slot, name, pct, pts=10.0, salary=5000):
    return {"lineup_row": row, "slot_index": idx, "slot": slot, "name": name, "position": slot, "team": "DAL",
            "salary": salary, "fantasy_points": pts, "field_pct": pct, "stack_role": None}


USER = {
    "contest": SHOWDOWN,
    "field": {"roster_slots": 6, "pool_size": 55, "players_owned": 4, "chalk": []},
    "user": {"username": "matgic11", "entries": 2, "best_rank": 1, "cashed": 1, "total_payout_cents": 12980916,
             "in_top_100": 1, "players_used": 4, "avg_points": 82.36},
    "exposure": [{"name": "Dak Prescott", "position": "QB", "team": "DAL", "salary": 15600, "fantasy_points": 17.64,
                  "lineups": 2, "captain_lineups": 1, "exposure_pct": 100.0, "field_pct": 77.33, "leverage": 22.67, "edge": 1.0}],
    "lineups": [
        {"row": 7, "rank": 1, "points": 136.59, "payout_cents": 12833333, "salary_used": 49400, "total_ownership": 187.0},
        {"row": 9, "rank": 500, "points": 120.0, "payout_cents": 0, "salary_used": 48000, "total_ownership": 150.0},
    ],
    "seats": [
        seat(7, 0, "CPT", "Dak Prescott", 14.68, 26.46, 23400), seat(7, 1, "FLEX", "Bucky Irving", 28.58, 34.5),
        seat(7, 2, "FLEX", "George Pickens", 41.55, 31.0),
        seat(9, 0, "CPT", "Bucky Irving", 4.98, 51.75, 18900), seat(9, 1, "FLEX", "Dak Prescott", 77.33, 17.64),
        seat(9, 2, "FLEX", "Cowboys", 17.52, 2.0),
    ],
    "analysis": {"lineups": 2, "stacks": None, "dispersion": {"most_used_pct": 100.0}, "ownership": {"avg": 168.5}},
}


# --------------------------------------------------------------------------
# Contest card and standings
# --------------------------------------------------------------------------

def test_contest_row_is_keyed_by_draftkings_id_and_reads_the_format():
    row = m.contest_row(CONTEST)
    assert row["contest_id"] == "196151357" and row["statapi_id"] == 20754235
    assert row["format"] == "classic" and row["entry_fee"] == 20.0 and row["prize_pool"] == 2750000.0
    assert m.contest_row(SHOWDOWN)["format"] == "showdown"
    assert m.contest_format({"slots": ["CPT", "FLEX"]}) == "showdown"
    with pytest.raises(ValueError):
        m.contest_row({**CONTEST, "external_id": None})


def test_standings_summary_fills_median_and_curve_only_when_complete():
    rows = m.standings_rows([{"row": i, "rank": i, "username": f"u{i}", "points": 200 - i, "payout_cents": 100,
                              "user_entries": 1} for i in range(1, 11)])
    partial = m.standings_summary(rows, 161764)
    assert partial["winning_score"] == 199 and partial["median_score"] is None and partial["score_curve"] is None
    full = m.standings_summary(rows, 10)
    assert full["complete"] and full["median_score"] == 194 and full["min_score"] == 190
    assert full["score_curve"][0] == [1, 199] and full["score_curve"][-1] == [10, 190]


def test_score_curve_matches_the_web_builder():
    scores = [float(1000 - i) for i in range(1000)]
    curve = m.build_score_curve(scores)
    ranks = [r for r, _ in curve]
    assert ranks[:100] == list(range(1, 101))            # exact top 100
    assert ranks[100] == 105 and ranks[-1] == 1000       # then every 0.5% (step 5), and the last
    assert dict(curve)[105] == 896.0
    assert m.build_score_curve([]) == []


# --------------------------------------------------------------------------
# Users: lineups, ownership, top entries
# --------------------------------------------------------------------------

def test_user_lineups_carry_rosters_in_seat_order_and_export_style_entry_names():
    lineups = m.user_lineups(USER)
    assert [l["rank"] for l in lineups] == [1, 500]
    top = lineups[0]
    assert top["entry_id"] == "statapi-7" and top["entry_name"] == "matgic11 (1/2)" and top["user_entries"] == 2
    assert top["lineup_text"] == "CPT Dak Prescott FLEX Bucky Irving FLEX George Pickens"
    assert top["players"][0]["drafted_pct"] == 14.68 and top["ownership_sum"] == 187.0 and top["payout"] == 128333.33
    assert top["players"][2]["drafted_pct"] == 41.55                    # Pickens: no captain seat seen -> overall


def test_ownership_is_overall_with_the_captain_split_out_like_the_export():
    """A flex seat's field_pct is the OVERALL share; the CPT seat carries the captain share.

    Dak: stat-api flex seat 77.33, captain seat 14.68; dfsdb's flex-only
    figure for the same contest was 62.53 = 77.33 - 14.68. So the export's
    FLEX row is overall minus captain, never a sum of the two.
    """
    own = m.field_ownership([USER], "196438550")
    dak = own["players"]["dakprescott"]
    assert dak["drafted_pct"] == 77.33
    assert dak["drafted_by_slot"] == {"CPT": 14.68, "FLEX": pytest.approx(62.65)}
    irving = own["players"]["buckyirving"]
    assert irving["drafted_by_slot"] == {"CPT": 4.98, "FLEX": pytest.approx(23.6)}
    assert own["players"]["cowboys"]["drafted_by_slot"] == {"CPT": 0.0, "FLEX": 17.52}
    assert own["coverage"] == 1.0                        # 4 players seen of players_owned 4
    assert own["mass"] == pytest.approx((77.33 + 28.58 + 41.55 + 17.52) / 600, abs=1e-4)
    # The winning lineup's flex seat for Irving reads flex-only too.
    lineups = m.user_lineups(USER)
    assert lineups[0]["players"][1]["drafted_pct"] == pytest.approx(23.6)
    assert lineups[0]["players"][0]["drafted_pct"] == 14.68


def test_classic_ownership_is_keyed_by_position_with_one_overall_number():
    classic = {**USER, "contest": CONTEST, "field": {"roster_slots": 9, "players_owned": 2},
               "lineups": [{"row": 1, "rank": 1, "points": 1.0}],
               "seats": [seat(1, 0, "RB", "Kyren Williams", 9.15), seat(1, 1, "FLEX", "Kyren Williams", 9.15),
                         seat(1, 2, "QB", "Dak Prescott", 5.19)]}
    own = m.field_ownership([classic], "196151357")
    assert own["players"]["kyrenwilliams"]["drafted_pct"] == 9.15
    assert own["players"]["kyrenwilliams"]["drafted_by_slot"] == {"RB": 9.15}
    assert own["mass"] == pytest.approx((9.15 + 5.19) / 900, abs=1e-4)
    assert "nobody" not in own["players"]                # unseen players are absent, never 0


def test_top_entries_keep_rank_within_the_cut_once_each():
    lineups = m.user_lineups(USER)
    assert [e["row"] for e in m.top_entries([lineups, lineups], 100)] == [7]
    assert [e["row"] for e in m.top_entries([lineups], 500)] == [7, 9]


def test_user_build_keeps_every_lineup_and_the_analysis():
    build = m.user_build(USER, "196438550")
    assert build["entries"] == 2 and build["best_rank"] == 1 and build["total_payout"] == 129809.16
    assert len(build["lineups"]) == 2
    assert build["lineups"][0]["roster"][0] == ["CPT", "Dak Prescott", "DAL", "CPT", 23400, 26.46, 14.68]
    assert build["analysis"]["field"]["players_owned"] == 4 and build["analysis"]["field"]["roster_slots"] == 6
    assert build["analysis"]["dispersion"]["most_used_pct"] == 100.0
    assert build["exposure"][0]["field_pct"] == 77.33 and build["source"] == "stat-api"


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------

def test_select_contests_takes_the_biggest_fields_with_lineups():
    slate = {"id": 583213, "name": "TB @ DAL Showdown", "game_type": "showdown"}
    rows = m.listing_contests([
        {"id": 1, "external_id": 10, "name": "NFL Showdown $100K mini-MAX (TB @ DAL)", "entry_count": 237812, "lineups_status": "available"},
        {"id": 2, "external_id": 11, "name": "NFL Showdown $1.5M Thursday Night Showdown (TB @ DAL)", "entry_count": 88235, "lineups_status": "available"},
        {"id": 3, "external_id": 12, "name": "NFL Showdown $402K Luxury Box (TB @ DAL)", "entry_count": 134, "lineups_status": "available"},
        {"id": 4, "external_id": 13, "name": "NFL Showdown $80K Huddle (TB @ DAL)", "entry_count": 19024, "lineups_status": "pending"},
    ], slate)
    assert [r["statapi_id"] for r in m.select_contests(rows, min_entries=1000, max_contests=5, search=None)] == [1, 2]
    assert [r["statapi_id"] for r in m.select_contests(rows, min_entries=1, max_contests=5, search="1.5M")] == [2]
    assert m.select_contests(rows, min_entries=1, max_contests=5, search=None, formats=("classic",)) == []


# --------------------------------------------------------------------------
# Client manners (no network)
# --------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status, payload=None, headers=None):
        self.status_code, self._payload, self.headers, self.text = status, payload, headers or {}, ""

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls, self.headers = list(responses), [], {}

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return self.responses.pop(0)


def test_client_sends_the_key_when_configured_and_names_its_absence_on_a_refusal():
    from ingest.statapi_contest import Client, StatApiError
    session = FakeSession([FakeResponse(200, {"ok": 1})])
    client = Client(api_key="abc", delay=0, session=session)
    assert client.get("/contests/1/standings") == {"ok": 1}
    assert session.headers["Authorization"] == "Bearer abc"
    session = FakeSession([FakeResponse(401, {"error": "key_required", "message": "This contest needs a key"})])
    with pytest.raises(StatApiError, match="no STAT_API_KEY"):
        Client(delay=0, session=session).get("/contests/1/standings")


def test_client_retries_rate_limits_with_retry_after_and_gives_up():
    from ingest.statapi_contest import Client, StatApiError
    session = FakeSession([FakeResponse(429, {"error": "rate"}, {"Retry-After": "0"}), FakeResponse(200, {"ok": 1})])
    assert Client(delay=0, session=session).get("/x") == {"ok": 1}
    session = FakeSession([FakeResponse(503, {"error": "down"}), FakeResponse(503, {"error": "down"})])
    with pytest.raises(StatApiError, match="503"):
        Client(delay=0, retries=1, session=session).get("/x")


def test_fetch_standings_pages_with_the_row_cursor():
    from ingest.statapi_contest import Client, fetch_standings
    page = lambda start, n: {"contest": {**CONTEST, "total_entries": 2500},
                             "standings": [{"row": i, "rank": i, "username": f"u{i}", "points": 1.0} for i in range(start, start + n)]}
    session = FakeSession([FakeResponse(200, page(1, 1000)), FakeResponse(200, page(1001, 1000)), FakeResponse(200, page(2001, 500))])
    contest, rows = fetch_standings(Client(delay=0, session=session), 20754235, rows=10, all_rows=True)
    assert len(rows) == 2500 and session.calls[1][1]["from_row"] == 1001 and session.calls[2][1]["limit"] == 500
    session = FakeSession([FakeResponse(200, page(1, 300))])
    _, rows = fetch_standings(Client(delay=0, session=session), 20754235, rows=300, all_rows=False)
    assert len(rows) == 300 and len(session.calls) == 1


def test_cli_needs_exactly_one_of_contest_or_date():
    from ingest.statapi_contest import main
    with pytest.raises(SystemExit):
        main([])
    with pytest.raises(SystemExit):
        main(["--contest", "1", "--date", "2026-10-04", "--dry-run"])


def test_fetch_standings_refuses_a_courtesy_preview():
    from ingest.statapi_contest import Client, StatApiError, fetch_standings
    preview = {"contest": {**CONTEST, "total_entries": 118906},
               "standings": [{"row": i, "rank": i, "username": f"u{i}", "points": 1.0} for i in range(1, 6)],
               "access": {"plan": "free", "full": False, "open_rows": 5},
               "_metadata": {"required_tier": "pro", "total_actual_records": 118906}}
    with pytest.raises(StatApiError, match="5-row preview"):
        fetch_standings(Client(api_key="k", delay=0, session=FakeSession([FakeResponse(200, preview)])),
                        29870426, rows=10, all_rows=False)
    flagship = {**preview, "access": {"plan": "flagship", "full": True}}
    _, rows = fetch_standings(Client(delay=0, session=FakeSession([FakeResponse(200, flagship)])),
                              29870409, rows=5, all_rows=False)
    assert len(rows) == 5
