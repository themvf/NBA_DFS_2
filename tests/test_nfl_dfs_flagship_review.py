"""Weekly flagship review: which contest per slate is probed, and only a flagship is imported."""

import pytest

from ingest.nfl_dfs_flagship_review import flagship_candidates, run
from ingest.statapi_contest import Client


class FakeResponse:
    def __init__(self, status, payload):
        self.status_code, self._payload, self.headers, self.text = status, payload, {}, ""

    def json(self):
        return self._payload


class FakeSession:
    """Routes by URL path and query so the review's discovery can be scripted without a network."""

    def __init__(self, routes):
        self.routes, self.calls, self.headers = routes, [], {}

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        for key, payload in self.routes.items():
            if key in url + "?" + "&".join(f"{k}={v}" for k, v in (params or {}).items()):
                return FakeResponse(200, payload)
        return FakeResponse(404, {"error": "not found"})


def contest(cid, ext, name, entries, prize, status="available"):
    return {"id": cid, "external_id": ext, "name": name, "entry_count": entries, "prize_pool": prize,
            "entry_fee": 20, "lineups_status": status}


ROUTES = {
    "/slates?date=2026-10-04": {"slates": [
        {"id": 473459, "name": "Sun 1:00PM Classic", "game_type": "classic"},
        {"id": 527176, "name": "Sun 10:30PM In-Game Showdown (Q4)", "game_type": "showdown"},
        {"id": 512793, "name": "DET @ CAR Showdown", "game_type": "showdown"},
    ]},
    "/contests?slate_id=473459": {"contests": [
        contest(20754235, 196151357, "NFL $2.75M Fantasy Football Millionaire [$1M to 1st]", 161764, 2750000),
        contest(20754300, 196151400, "NFL $400K Flea Flicker [$50K to 1st]", 95124, 400000),
        contest(20754301, 196151401, "NFL $5M Pending", 10, 5000000, status="pending"),
    ]},
    "/contests?slate_id=512793": {"contests": [
        contest(29000001, 196300001, "NFL Showdown $100K (DET @ CAR)", 35671, 100000),
    ]},
    "/contests/20754235/standings?limit=1": {"contest": {"contest_id": 20754235, "external_id": 196151357},
                                             "standings": [], "access": {"plan": "flagship", "full": True}},
    "/contests/29000001/standings?limit=1": {"contest": {"contest_id": 29000001, "external_id": 196300001},
                                             "standings": [], "access": {"plan": "free", "full": False, "open_rows": 5}},
}


def test_flagship_candidates_probe_the_biggest_prize_per_slate_and_skip_in_game():
    session = FakeSession(ROUTES)
    found = flagship_candidates(Client(delay=0, session=session), "2026-10-04")
    # The unprocessed $5M is the slate's biggest prize: named as waiting, never probed.
    assert [(c["statapi_id"], c["plan"], c["full"]) for c in found] == [
        (20754301, "waiting", False), (20754235, "flagship", True), (29000001, "free", False)]
    probed = [u for u, _ in session.calls if "/standings" in u]
    assert probed == ["https://api.stat-api.com/api/v1/dfs/contests/20754235/standings",
                      "https://api.stat-api.com/api/v1/dfs/contests/29000001/standings"]   # not the pending $5M, not in-game
    assert found[1]["external_id"] == 196151357


def test_dry_run_names_flagships_and_writes_nothing(capsys):
    session = FakeSession(ROUTES)
    code = run(None, Client(delay=0, session=session), 2026, 4, ["2026-10-04"], dry_run=True)
    out = capsys.readouterr().out
    assert code == 0
    assert "FLAGSHIP 2026-10-04 NFL $2.75M Fantasy Football Millionaire" in out
    assert "skipped 2026-10-04 NFL Showdown $100K (DET @ CAR)" in out and "not served in full" in out
    assert "nothing written" in out


# 2026-10-27: the $1.25M Monday Night Showdown, every contest on its slate still
# lineups_status 'none' the morning after. The old discovery dropped the slate
# without a word; it must be named as waiting so a later run picks it up.
MONDAY = {
    "/slates?date=2026-10-26": {"slates": [{"id": 828724, "name": "DAL @ PHI Showdown", "game_type": "showdown"}]},
    "/contests?slate_id=828724": {"contests": [
        contest(51596743, 197055073, "NFL Showdown $1.25M Monday Night Showdown (DAL @ PHI)", 73529, 1250000, "none"),
        contest(51596742, 197055072, "NFL Showdown $300K Flea Flicker (DAL @ PHI)", 71343, 300000, "none"),
    ]},
}


def test_a_slate_stat_api_has_not_processed_is_named_waiting_not_dropped(capsys):
    session = FakeSession(MONDAY)
    found = flagship_candidates(Client(delay=0, session=session), "2026-10-26")
    assert [(c["statapi_id"], c["plan"]) for c in found] == [(51596743, "waiting")]
    assert not [u for u, _ in session.calls if "/standings" in u]
    run(None, Client(delay=0, session=FakeSession(MONDAY)), 2026, 7, ["2026-10-26"], dry_run=True)
    out = capsys.readouterr().out
    assert "skipped 2026-10-26 NFL Showdown $1.25M Monday Night Showdown" in out
    assert "has not processed its lineups yet (status none); a later run imports it" in out


def paged_routes():
    # Insertion order matters: FakeSession matches the first key that is a substring.
    return {
        "/slates?date=2026-10-25": {"slates": [{"id": 900, "name": "Sun 1:00PM Classic", "game_type": "classic"}]},
        "/contests?slate_id=900&limit=100&from_id=300": {"contests": [
            contest(301, 3001, "NFL $3M Millionaire", 160000, 3000000)], "has_more": False},
        "/contests?slate_id=900&limit=100&from_id=200": {"contests": [
            contest(250, 2500, "NFL $20 Single Entry", 100, 2000), contest(300, 3000, "NFL $40K Mini", 500, 40000)],
            "has_more": True, "next_from_id": 300},
        "/contests?slate_id=900&limit=100": {"contests": [
            contest(200, 2000, "NFL $400K Flea Flicker", 95000, 400000), contest(150, 1500, "NFL $5 50-50", 50, 500)],
            "has_more": True, "next_from_id": None},
        "/contests/301/standings?limit=1": {"contest": {"external_id": 3001}, "standings": [],
                                            "access": {"plan": "flagship", "full": True}},
        "/contests/200/standings?limit=1": {"contest": {"external_id": 2000}, "standings": [],
                                            "access": {"plan": "free", "full": False}},
    }


def test_discovery_pages_past_the_first_100_with_a_key():
    session = FakeSession(paged_routes())
    found = flagship_candidates(Client(api_key="k", delay=0, session=session), "2026-10-25")
    assert [(c["statapi_id"], c["plan"], c["listing_note"]) for c in found] == [(301, "flagship", None)]
    # Page 2 starts above page 1's largest id when stat-api gives no next_from_id.
    froms = [p.get("from_id") for u, p in session.calls if u.endswith("/contests")]
    assert froms == [None, 200, 300]


def test_without_a_key_the_listing_stops_at_one_page_and_says_so(capsys):
    session = FakeSession(paged_routes())
    found = flagship_candidates(Client(delay=0, session=session), "2026-10-25")
    assert [(c["statapi_id"], c["plan"]) for c in found] == [(200, "free")]
    assert found[0]["listing_note"] == "first 2 contests only: paging needs STAT_API_KEY"
    assert [p.get("from_id") for u, p in session.calls if u.endswith("/contests")] == [None]
    run(None, Client(delay=0, session=FakeSession(paged_routes())), 2026, 7, ["2026-10-25"], dry_run=True)
    assert "[listing: first 2 contests only: paging needs STAT_API_KEY]" in capsys.readouterr().out
