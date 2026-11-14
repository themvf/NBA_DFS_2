"""Email the owner as soon as a Game Leaders run needs attention, not the next morning.

The run already turns red when a step fails or an upcoming game is left off for
a data error (`research.nfl_game_leaders_auto --check`), and /health and the
daily failure sweep report a red run. The sweep emails at 11:07 UTC the next
day, though, so a problem in a Sunday pre-kickoff refresh reached the owner
only after the games. This step, the workflow's last, manages one GitHub issue
labelled `game-leaders-alert` that @mentions the owner, and GitHub emails the
mention within minutes:

  - something wrong, no open issue      -> open one (email);
  - something wrong, issue open         -> comment only when the problems
                                           changed (email); the same problem
                                           on the next Sunday pass stays quiet;
  - nothing wrong, issue open           -> comment what was resolved and close
                                           it (email), naming any game that
                                           kicked off without a fresh forecast,
                                           which can no longer be fixed.

Standard library only at import time, so it still reports a run whose package
install failed. A run whose checkout failed cannot reach it; the daily sweep
still reports that one.

    python -m research.nfl_game_leaders_alert
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

LABEL = "game-leaders-alert"
TITLE = "[NFL Game Leaders] Publication needs attention"
MENTION = "themvf"
PAGE = Path("web/src/data/game-leaders.json")
WORKFLOW = "refresh_nfl_game_leaders.yml"
# The required steps whose failure means the page was not refreshed. Optional
# research and re-pricing steps run with continue-on-error and never alert.
STEPS = (("publish", "Capture availability and publish the upcoming week"),
         ("commit", "Commit and push the page"))


def kickoff_text(kickoff: str | None) -> str:
    if not kickoff:
        return "kickoff unknown"
    moment = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
    try:
        from zoneinfo import ZoneInfo
        local = moment.astimezone(ZoneInfo("America/New_York"))
        return local.strftime("%a %b %d %I:%M %p ET").replace(" 0", " ")
    except Exception:  # no tz database on this machine
        return moment.astimezone(timezone.utc).strftime("%a %b %d %H:%M UTC")


def read_page(path: Path = PAGE) -> tuple[dict | None, str | None]:
    if not path.exists():
        return None, f"{path} is missing"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except Exception as error:  # a half-written or corrupt page is itself the problem
        return None, f"{path} could not be read: {error}"


def find_problems(page: dict | None, page_error: str | None, now: datetime,
                  outcomes: dict[str, str], job_status: str) -> list[dict]:
    """Each problem has a stable `key` (what deduplication compares) and the `text` shown."""
    problems = []
    for step_id, label in STEPS:
        if outcomes.get(step_id) == "failure":
            problems.append({"key": f"step:{step_id}", "text": f"The step \"{label}\" failed, "
                             "so the page still shows the previous publication."})
    if page_error:
        problems.append({"key": "page", "text": page_error})
    elif page is not None:
        try:
            from research.nfl_game_leaders_auto import data_errors, matchup
        except Exception as error:  # dependencies missing: the run failed before publishing
            problems.append({"key": "page-check", "text": f"The page could not be checked: {error}"})
        else:
            for s in data_errors(page, now):
                problems.append({"key": f"missing:{s['game_id']}:{s['reason']}",
                                 "text": f"**{matchup(s)}** ({kickoff_text(s.get('kickoff'))}) has no "
                                         f"fresh forecast: {s['reason']}"})
    if job_status == "failure" and not problems:
        problems.append({"key": "step:setup", "text": "The run failed before publishing "
                         "(checkout, Python setup or package install)."})
    return problems


def kicked_off_unforecast(page: dict | None, now: datetime) -> list[str]:
    """Games left off for a data error that have since kicked off: nothing more can be done."""
    if not page:
        return []
    try:
        from research.nfl_game_leaders_auto import matchup, skip_kind
    except Exception:
        return []
    out = []
    for s in page.get("skipped", []):
        if (s.get("kind") or skip_kind(s["reason"])) != "data_error" or not s.get("kickoff"):
            continue
        if datetime.fromisoformat(s["kickoff"].replace("Z", "+00:00")) <= now:
            out.append(f"{matchup(s)} ({s['reason']})")
    return out


def marker(problems: list[dict]) -> str:
    digest = hashlib.sha256(json.dumps(sorted(p["key"] for p in problems)).encode()).hexdigest()
    return f"<!-- game-leaders-alert:{digest[:16]} -->"


def alert(problems: list[dict], api, run_url: str, rerun_url: str, week, now: datetime,
          kicked_off: list[str]) -> dict:
    """Open, comment on or close the one alert issue. Returns what it did and whether it emailed."""
    issues = api("GET", f"/issues?labels={LABEL}&state=all&sort=created&direction=desc&per_page=10") or []
    issue = next((i for i in issues if "pull_request" not in i), None)
    is_open = bool(issue and issue["state"] == "open")
    stamp = kickoff_text(now.isoformat())
    if not problems:
        if not is_open:
            return {"action": "none", "emailed": False}
        lines = [f"@{MENTION} Resolved: the run at {stamp} published week {week} with no upcoming "
                 "game missing and every required step green."]
        if kicked_off:
            lines += ["", "Kicked off without a fresh forecast (can no longer be fixed):"]
            lines += [f"- {g}" for g in kicked_off]
        lines += ["", run_url]
        api("POST", f"/issues/{issue['number']}/comments", {"body": "\n".join(lines)})
        api("PATCH", f"/issues/{issue['number']}", {"state": "closed", "state_reason": "completed"})
        return {"action": "closed", "issue": issue["number"], "emailed": True}
    tag = marker(problems)
    body = "\n".join([f"@{MENTION} Game Leaders needs attention (week {week}, run at {stamp}):", ""]
                     + [f"- {p['text']}" for p in problems]
                     + ["", f"Re-run it from Actions once the cause is fixed: {rerun_url} "
                        "(\"Run workflow\" on main). This issue closes itself on the next clean run.",
                        "", run_url, tag])
    if is_open:
        comments = api("GET", f"/issues/{issue['number']}/comments?per_page=100") or []
        latest = comments[-1]["body"] if comments else issue.get("body") or ""
        if tag in latest:
            return {"action": "unchanged", "issue": issue["number"], "emailed": False}
        api("POST", f"/issues/{issue['number']}/comments", {"body": body})
        return {"action": "commented", "issue": issue["number"], "emailed": True}
    try:
        api("POST", "/labels", {"name": LABEL, "color": "d73a4a",
                                "description": "Opened by the NFL Game Leaders publish run"})
    except HTTPError as error:
        if error.code != 422:  # 422: the label already exists
            raise
    created = api("POST", "/issues", {"title": TITLE, "body": body, "labels": [LABEL]})
    return {"action": "opened", "issue": created["number"], "emailed": True}


def github_api(repo: str, token: str):
    def api(method: str, path: str, data=None):
        request = Request(f"https://api.github.com/repos/{repo}{path}",
                          data=json.dumps(data).encode() if data is not None else None, method=method,
                          headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                                   "X-GitHub-Api-Version": "2022-11-28", "Content-Type": "application/json"})
        with urlopen(request, timeout=25) as response:
            raw = response.read()
        return json.loads(raw) if raw else None
    return api


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "themvf/NBA_DFS_2")
    now = datetime.now(timezone.utc)
    page, page_error = read_page()
    outcomes = {step_id: os.environ.get(f"{step_id.upper()}_OUTCOME", "") for step_id, _ in STEPS}
    problems = find_problems(page, page_error, now, outcomes, os.environ.get("JOB_STATUS", ""))
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = f"{server}/{repo}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    rerun_url = f"{server}/{repo}/actions/workflows/{WORKFLOW}"
    result = alert(problems, github_api(repo, os.environ["GITHUB_TOKEN"]), run_url, rerun_url,
                   (page or {}).get("week", "?"), now, kicked_off_unforecast(page, now))
    print(json.dumps({"problems": [p["text"] for p in problems], **result}), flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"### Owner alert\n- {result['action']}"
                    + (f" (issue #{result['issue']})" if "issue" in result else "")
                    + (", emailed" if result["emailed"] else ", no email") + "\n")


if __name__ == "__main__":
    main()
