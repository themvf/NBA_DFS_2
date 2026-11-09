"""One deduplicated GitHub issue for NBA research failures and recovery."""
import argparse
import json
import os
from pathlib import Path

from urllib.request import Request, urlopen

TITLE = '[NBA research] Recording needs attention'


def alert(results, api, run_url):
    title = '[NBA research] Review needs attention' if results and all(r.get('action') == 'review' for r in results) else TITLE
    bad = [r for r in results if r.get('status') in {
        'failed', 'failed_recording', 'partial_attachments', 'missed_cutoff',
        'missing_before_tip', 'missed_or_incomplete'}]
    issues = api('GET', '/issues?state=open&sort=updated&direction=desc&per_page=100')
    issue = next((i for i in issues if i['title'] == title and 'pull_request' not in i), None)
    if not bad:
        if not all(r.get('status') in {'healthy', 'recorded', 'original_preserved', 'reviewed', 'no_slate'} for r in results):
            return {'status': 'pending', 'notification': False}
        recovered = bool(issue and issue['state'] == 'open')
        if recovered:
            api('POST', f"/issues/{issue['number']}/comments", {'body': f'NBA research recovered. Original records remain unchanged.\n\n{run_url}'})
            api('PATCH', f"/issues/{issue['number']}", {'state': 'closed'})
        return {'status': 'healthy', 'notification': recovered}
    lines = ['@themvf NBA research needs attention.', '',
             'Do not replace an original or attach a test after the full-slate first tip.']
    # Stable problem signature avoids repeated mail for the same failing state.
    problems = [{k: r[k] for k in ('date', 'status', 'first_tip', 'attachments', 'problems', 'reason', 'dates') if k in r} for r in bad]
    signature = json.dumps(problems, sort_keys=True)
    import hashlib
    marker = '<!-- nba-research-state:' + hashlib.sha256(signature.encode()).hexdigest() + ' -->'
    lines += ['', '```json', json.dumps(problems, indent=2), '```', '', run_url, marker]
    body = '\n'.join(lines)
    if issue:
        comments = api('GET', f"/issues/{issue['number']}/comments?per_page=100")
        latest = comments[-1]['body'] if comments else issue.get('body', '')
        if marker in latest and issue['state'] == 'open':
            return {'status': 'unchanged_failure', 'notification': False}
        if issue['state'] != 'open':
            api('PATCH', f"/issues/{issue['number']}", {'state': 'open'})
        api('POST', f"/issues/{issue['number']}/comments", {'body': body})
    else:
        api('POST', '/issues', {'title': title, 'body': body})
    return {'status': 'failure_notified', 'notification': True}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--action', choices=['record', 'check', 'review'])
    args = p.parse_args()
    token, repo = os.environ['GITHUB_TOKEN'], os.environ['GITHUB_REPOSITORY']
    def api(method, path, data=None):
        request = Request('https://api.github.com/repos/' + repo + path,
            data=json.dumps(data).encode() if data is not None else None, method=method,
            headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                     'Content-Type': 'application/json'})
        with urlopen(request, timeout=25) as response:
            body = response.read()
        return json.loads(body) if body else None
    results = [json.loads(p.read_text()) for p in args.root.glob('operations/*/result.json')]
    if not results:
        results = [{'status': 'failed', 'action': args.action, 'reason': 'Research process produced no completion receipt'}]
    else:
        results = [max(results, key=lambda r: r.get('observed_at', ''))]
    print(json.dumps(alert(results, api, 'https://github.com/' + repo + '/actions/runs/' + os.environ['GITHUB_RUN_ID'])))


if __name__ == '__main__':
    main()
