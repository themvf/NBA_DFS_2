"""Tell the owner the same night when a read of the official NBA injury report fails (``nba-injury-alert-v1``).

The collector (``research.nba_points_injury_evidence``) runs as an optional workflow step, so a
failed read left the run green with a warning nobody is sent, and /health notices only after 36
hours. This reads what that step left behind, says why it failed, and keeps one GitHub issue:
opened on a failure (it mentions the owner, so GitHub emails him), not repeated for the same
night and cause, and closed with a comment when a later read succeeds.

A night with no NBA games stored is not a failure: the builder has nothing to protect.
With ``--fail-run`` a real failure exits 1, so an injury-only run goes red.

Usage (from the workflow, after the read):
    python -m research.nba_injury_alert --date 2026-11-13 --archive-root artifacts/nba-ghost/injury --outcome failure --fail-run
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

VERSION = 'nba-injury-alert-v1'
TITLE = '[NBA injury report] Read failed'
EASTERN = ZoneInfo('America/New_York')

WHAT = {
    'not_posted': "The league's site returned no report.",
    'parser_refused': 'A report was downloaded, but its layout or contents were not what the reader expects, so it was refused rather than guessed at.',
    'slate_missing': "The newest report does not list tonight's games.",
    'not_stored': 'The report was read but could not be saved.',
    'unknown': 'The read failed before it could record why.',
}
NEXT = {
    'not_posted': 'This usually clears on the next read (every 15 minutes in the 50 minutes before the first tip). Nothing to fix unless it repeats.',
    'parser_refused': 'Reads will keep failing until the reader (research/nba_points_injury_evidence.py) is updated. The downloaded PDF is in this run\'s artifacts.',
    'slate_missing': 'This usually clears when the league posts its next report.',
    'not_stored': 'The run log has the error. The next read tries again.',
    'unknown': 'The run log has the error.',
}


def index_url(day: str) -> str:
    """The league's page listing every report of the season (same rule as the collector)."""
    d = date.fromisoformat(day)
    year = d.year if d.month >= 7 else d.year - 1
    return f'https://official.nba.com/nba-injury-report-{year}-{str(year + 1)[-2:]}-season/'


def _newest(paths) -> Path | None:
    paths = sorted(paths, key=lambda p: (p.stat().st_mtime, str(p)))
    return paths[-1] if paths else None


def classify(root: Path, day: str, games: int | None) -> dict:
    """Why the read failed, from the files the collector left. ``games`` is how many NBA games are
    stored for the date (None when that could not be read, which is treated as "there are games")."""
    if games == 0:
        return {'kind': 'no_slate', 'detail': f'No NBA games are stored for {day}, so there is nothing to read a report for.'}
    exists = root.exists()
    observation = _newest(root.glob('*/observation.json')) if exists else None
    if observation:
        report = json.loads(observation.read_text(encoding='utf-8'))
        listed = {s.get('game_date') for s in report.get('team_submissions', [])}
        if day not in listed:
            return {'kind': 'slate_missing', 'source_url': report.get('source_url'),
                    'detail': f"The report posted {report.get('published_at')} lists games for {', '.join(sorted(d for d in listed if d)) or 'no date'} only."}
        return {'kind': 'not_stored', 'source_url': report.get('source_url'),
                'detail': f"The report posted {report.get('published_at')} was understood ({len(report.get('rows', []))} player rows)."}
    refused = _newest(root.glob('*/parse-error.json')) if exists else None
    if refused:
        error = json.loads(refused.read_text(encoding='utf-8'))
        return {'kind': 'parser_refused', 'source_url': error.get('source_url'), 'detail': f"The reader said: {error.get('error')}."}
    receipt = _newest(root.glob('failures/*.json')) if exists else None
    if receipt:
        requests = json.loads(receipt.read_text(encoding='utf-8')).get('discovery', {}).get('requests', [])
        tally = Counter(str(r.get('status') or r.get('error') or 'no answer') for r in requests)
        answers = ', '.join(f'{k} x{n}' for k, n in sorted(tally.items()))
        return {'kind': 'not_posted', 'detail': f'{len(requests)} requests found no report (answers: {answers or "none"}).'}
    return {'kind': 'unknown', 'detail': 'The collector left no receipt.'}


def marker(day: str, kind: str) -> str:
    return '<!-- nba-injury-alert:' + hashlib.sha256(f'{day}|{kind}'.encode()).hexdigest() + ' -->'


def body(day: str, cause: dict, run_url: str, now: datetime) -> str:
    kind = cause['kind']
    when = now.astimezone(EASTERN).strftime('%I:%M %p').lstrip('0')
    lines = [f'@themvf The official NBA injury report could not be read for {day} (tried {when} Eastern).', '',
             f"**What failed.** {WHAT[kind]} {cause.get('detail', '')}".rstrip()]
    if cause.get('source_url'):
        lines.append(f"Report: {cause['source_url']}")
    lines += ['', '**Tonight.** The contest builder keeps the last report it has and shows a red warning. A player ruled out since that '
              f'report is still in its pool, so check the league\'s report before you enter: {index_url(day)}',
              '', f'**Next.** {NEXT[kind]}', '', run_url, marker(day, kind)]
    return '\n'.join(lines)


def alert(outcome: str, day: str, cause: dict | None, api, run_url: str, now: datetime) -> dict:
    """One open issue per incident. Returns what was done; ``notification`` is whether GitHub was told to write."""
    issues = api('GET', '/issues?state=open&sort=updated&direction=desc&per_page=100')
    issue = next((i for i in issues if i['title'] == TITLE and 'pull_request' not in i), None)
    if outcome == 'success':
        if not issue:
            return {'status': 'healthy', 'notification': False}
        when = now.astimezone(EASTERN).strftime('%I:%M %p').lstrip('0')
        api('POST', f"/issues/{issue['number']}/comments", {'body': f'Recovered: the injury report for {day} was read at {when} Eastern.\n\n{run_url}'})
        api('PATCH', f"/issues/{issue['number']}", {'state': 'closed'})
        return {'status': 'recovered', 'notification': True}
    if cause is None or cause['kind'] == 'no_slate':
        return {'status': 'no_slate', 'notification': False}
    text = body(day, cause, run_url, now)
    if issue:
        comments = api('GET', f"/issues/{issue['number']}/comments?per_page=100")
        seen = [issue.get('body') or ''] + [c.get('body') or '' for c in comments]
        if any(marker(day, cause['kind']) in s for s in seen):
            return {'status': 'already_reported', 'notification': False}
        api('POST', f"/issues/{issue['number']}/comments", {'body': text})
    else:
        api('POST', '/issues', {'title': TITLE, 'body': text})
    return {'status': 'failure_notified', 'notification': True}


def games_stored(day: str) -> int | None:
    """How many NBA games the stored points lines have for the Eastern date; None when unreadable."""
    try:
        import psycopg2
        from config import load_config
        start = datetime.combine(date.fromisoformat(day), datetime.min.time(), tzinfo=EASTERN)
        conn = psycopg2.connect(load_config().database_url)
        try:
            with conn.cursor() as cur:
                cur.execute("""SELECT count(DISTINCT event_id) FROM prop_odds_history
                               WHERE sport = 'nba' AND commence_time >= %s AND commence_time < %s""",
                            (start.astimezone(timezone.utc), (start + timedelta(days=1)).astimezone(timezone.utc)))
                return int(cur.fetchone()[0])
        finally:
            conn.rollback()
            conn.close()
    except Exception as exc:  # the alert must still go out when the database cannot be read
        print(f'::warning::could not count stored NBA games for {day}: {type(exc).__name__}: {exc}')
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--date', required=True, help='Eastern slate date the read was for')
    parser.add_argument('--archive-root', type=Path, required=True)
    parser.add_argument('--outcome', required=True, help="the read step's outcome: success or failure")
    parser.add_argument('--fail-run', action='store_true', help='exit 1 on a real failure (injury-only runs)')
    args = parser.parse_args()
    if args.outcome not in ('success', 'failure'):
        print(json.dumps({'version': VERSION, 'status': 'not_run', 'outcome': args.outcome}))
        return 0
    cause = classify(args.archive_root, args.date, games_stored(args.date)) if args.outcome == 'failure' else None
    token, repo = os.environ.get('GITHUB_TOKEN'), os.environ.get('GITHUB_REPOSITORY')
    out = {'version': VERSION, 'date': args.date, 'outcome': args.outcome, 'cause': cause}
    if token and repo:
        def api(method, path, data=None):
            request = Request('https://api.github.com/repos/' + repo + path,
                              data=json.dumps(data).encode() if data is not None else None, method=method,
                              headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                                       'Content-Type': 'application/json'})
            with urlopen(request, timeout=25) as response:
                raw = response.read()
            return json.loads(raw) if raw else None
        run_url = 'https://github.com/' + repo + '/actions/runs/' + os.environ.get('GITHUB_RUN_ID', '')
        out.update(alert(args.outcome, args.date, cause, api, run_url, datetime.now(timezone.utc)))
    else:
        out.update({'status': 'not_sent', 'notification': False, 'reason': 'GITHUB_TOKEN or GITHUB_REPOSITORY is not set'})
    print(json.dumps(out))
    failed = cause is not None and cause['kind'] != 'no_slate'
    if failed:
        print(f"::error title=Official injury report not read::{WHAT[cause['kind']]} {cause.get('detail', '')}")
    return 1 if failed and args.fail_run else 0


if __name__ == '__main__':
    raise SystemExit(main())
