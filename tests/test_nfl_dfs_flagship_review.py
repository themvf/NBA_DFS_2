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
    assert [(c["statapi_id"], c["plan"], c["full"]) for c in found] == [(20754235, "flagship", True), (29000001, "free", False)]
    probed = [u for u, _ in session.calls if "/standings" in u]
    assert probed == ["https://api.stat-api.com/api/v1/dfs/contests/20754235/standings",
                      "https://api.stat-api.com/api/v1/dfs/contests/29000001/standings"]   # not the pending $5M, not in-game
    assert found[0]["external_id"] == 196151357


def test_dry_run_names_flagships_and_writes_nothing(capsys):
    session = FakeSession(ROUTES)
    code = run(None, Client(delay=0, session=session), 2026, 4, ["2026-10-04"], dry_run=True)
    out = capsys.readouterr().out
    assert code == 0
    assert "FLAGSHIP 2026-10-04 NFL $2.75M Fantasy Football Millionaire" in out
    assert "skipped 2026-10-04 NFL Showdown $100K (DET @ CAR)" in out and "not served in full" in out
    assert "nothing written" in out
