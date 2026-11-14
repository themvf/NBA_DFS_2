from datetime import datetime, timezone

from research.nfl_game_leaders_alert import (LABEL, alert, find_problems, kicked_off_unforecast,
                                             marker)

NOW = datetime(2026, 11, 15, 16, 0, tzinfo=timezone.utc)  # Sunday 11:00 AM ET
RUN = "https://github.com/x/y/actions/runs/1"
RERUN = "https://github.com/x/y/actions/workflows/refresh_nfl_game_leaders.yml"


def page(*skipped, week=11):
    return {"week": week, "skipped": list(skipped)}


def skip(gid, reason, kickoff="2026-11-15T18:00:00Z", kind=None):
    away, home = gid.split("_")[2:4]
    entry = {"game_id": gid, "away": away, "home": home, "kickoff": kickoff, "reason": reason}
    if kind:
        entry["kind"] = kind
    return entry


class FakeGitHub:
    def __init__(self, issues=(), comments=()):
        self.issues = list(issues)
        self.comments = list(comments)
        self.calls = []

    def __call__(self, method, path, data=None):
        self.calls.append((method, path, data))
        if method == "GET" and path.startswith("/issues?"):
            return self.issues
        if method == "GET" and path.endswith("/comments?per_page=100"):
            return self.comments
        if method == "POST" and path == "/issues":
            return {"number": 7}
        return {}

    def writes(self):
        return [c for c in self.calls if c[0] != "GET"]


def run(problems, api, p=None):
    return alert(problems, api, RUN, RERUN, (p or {}).get("week", 11), NOW, kicked_off_unforecast(p, NOW))


def test_a_missing_upcoming_game_opens_an_issue_that_mentions_the_owner():
    p = page(skip("2026_11_LA_ARI", "no forecast produced"))
    problems = find_problems(p, None, NOW, {"publish": "success", "commit": "success"}, "failure")
    assert [x["key"] for x in problems] == ["missing:2026_11_LA_ARI:no forecast produced"]
    api = FakeGitHub()
    result = run(problems, api, p)
    assert result == {"action": "opened", "issue": 7, "emailed": True}
    (_, _, issue), = [c for c in api.writes() if c[1] == "/issues"]
    assert issue["labels"] == [LABEL]
    assert issue["body"].startswith("@themvf ")
    assert "**LA at ARI**" in issue["body"] and "no forecast produced" in issue["body"]
    assert RERUN in issue["body"] and RUN in issue["body"] and marker(problems) in issue["body"]


def test_the_same_problem_on_the_next_pass_sends_no_second_email():
    problems = find_problems(page(skip("2026_11_LA_ARI", "no forecast produced")), None, NOW, {}, "failure")
    api = FakeGitHub(issues=[{"number": 7, "state": "open", "body": "x"}],
                     comments=[{"body": f"earlier\n{marker(problems)}"}])
    assert run(problems, api) == {"action": "unchanged", "issue": 7, "emailed": False}
    assert api.writes() == []


def test_a_new_problem_comments_on_the_open_issue():
    old = find_problems(page(skip("2026_11_LA_ARI", "no forecast produced")), None, NOW, {}, "failure")
    new = find_problems(page(skip("2026_11_LA_ARI", "no forecast produced"),
                             skip("2026_11_KC_DEN", "gate failed")), None, NOW, {}, "failure")
    api = FakeGitHub(issues=[{"number": 7, "state": "open", "body": f"first\n{marker(old)}"}])
    assert run(new, api)["action"] == "commented"
    (method, path, data), = api.writes()
    assert (method, path) == ("POST", "/issues/7/comments") and "KC at DEN" in data["body"]


def test_a_clean_run_closes_the_open_issue_and_names_games_that_kicked_off_unforecast():
    p = page(skip("2026_11_LA_ARI", "no forecast produced", kickoff="2026-11-15T15:00:00Z"),
             skip("2026_11_WAS_NYG", "started before any forecast was saved",
                  kickoff="2026-11-14T01:15:00Z"))
    assert find_problems(p, None, NOW, {"publish": "success", "commit": "success"}, "success") == []
    api = FakeGitHub(issues=[{"number": 7, "state": "open", "body": "x"}])
    assert run([], api, p) == {"action": "closed", "issue": 7, "emailed": True}
    (_, _, comment), (_, _, patch) = api.writes()
    assert comment["body"].startswith("@themvf Resolved")
    assert "LA at ARI (no forecast produced)" in comment["body"]
    assert "WAS at NYG" not in comment["body"]  # expected skip, not a data error
    assert patch["state"] == "closed"


def test_a_clean_run_with_no_open_issue_writes_nothing():
    api = FakeGitHub(issues=[{"number": 3, "state": "closed", "body": "x"}])
    assert run([], api) == {"action": "none", "emailed": False}
    assert api.writes() == []


def test_a_new_incident_after_a_closed_one_opens_a_fresh_issue():
    problems = find_problems(None, None, NOW, {"publish": "failure"}, "failure")
    api = FakeGitHub(issues=[{"number": 3, "state": "closed", "body": "x"}])
    assert run(problems, api)["action"] == "opened"


def test_failed_steps_and_setup_failures_are_problems():
    publish = find_problems(page(), None, NOW, {"publish": "failure", "commit": "skipped"}, "failure")
    assert [p["key"] for p in publish] == ["step:publish"]
    setup = find_problems(page(), None, NOW, {"publish": "", "commit": ""}, "failure")
    assert [p["key"] for p in setup] == ["step:setup"]
    unreadable = find_problems(None, "page is missing", NOW, {}, "failure")
    assert [p["key"] for p in unreadable] == ["page"]


def test_an_expected_skip_or_a_game_already_started_is_not_a_problem():
    p = page(skip("2026_11_WAS_NYG", "started before any forecast was saved"),
             skip("2026_11_LA_ARI", "no forecast produced", kickoff="2026-11-15T15:00:00Z"))
    assert find_problems(p, None, NOW, {"publish": "success", "commit": "success"}, "success") == []


def test_dedup_ignores_problem_order():
    a = [{"key": "x", "text": ""}, {"key": "y", "text": ""}]
    assert marker(a) == marker(list(reversed(a)))
