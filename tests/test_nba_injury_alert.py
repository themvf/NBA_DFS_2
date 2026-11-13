"""The same-night alert for a failed official NBA injury report read (nba-injury-alert-v1)."""
from datetime import datetime, timezone
import json

import pytest

import research.nba_injury_alert as m

DAY = '2026-11-13'
NOW = datetime(2026, 11, 13, 23, 23, tzinfo=timezone.utc)   # 6:23 PM Eastern
RUN = 'https://github.com/o/r/actions/runs/1'


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding='utf-8')


def test_no_games_is_not_a_failure(tmp_path):
    write(tmp_path / 'failures' / 'a.json', {'discovery': {'requests': [{'status': 404}]}})
    assert m.classify(tmp_path, DAY, 0)['kind'] == 'no_slate'


def test_league_posted_nothing(tmp_path):
    write(tmp_path / 'failures' / 'a.json', {'discovery': {'requests': [{'status': 403}, {'status': 404}, {'status': 404}, {'error': 'timeout'}]}})
    cause = m.classify(tmp_path, DAY, 9)
    assert cause['kind'] == 'not_posted'
    assert '4 requests' in cause['detail'] and '404 x2' in cause['detail'] and 'timeout x1' in cause['detail']


def test_parser_refusal_carries_the_reason(tmp_path):
    write(tmp_path / '20261113T232300Z-abc' / 'parse-error.json',
          {'error': 'unrecognized injury table layout', 'source_url': 'https://ak-static.cms.nba.com/x.pdf', 'observed_at': NOW.isoformat()})
    # The database being unreadable (games None) must not hide a real failure.
    cause = m.classify(tmp_path, DAY, None)
    assert cause == {'kind': 'parser_refused', 'source_url': 'https://ak-static.cms.nba.com/x.pdf',
                     'detail': 'The reader said: unrecognized injury table layout.'}


def test_report_without_tonights_games_and_report_not_stored(tmp_path):
    report = {'published_at': '2026-11-13T23:15:00+00:00', 'source_url': 'u', 'rows': [{}, {}],
              'team_submissions': [{'game_date': '2026-11-14'}]}
    write(tmp_path / 'a' / 'observation.json', report)
    assert m.classify(tmp_path, DAY, 9)['kind'] == 'slate_missing'
    report['team_submissions'].append({'game_date': DAY})
    write(tmp_path / 'a' / 'observation.json', report)
    cause = m.classify(tmp_path, DAY, 9)
    assert cause['kind'] == 'not_stored' and '2 player rows' in cause['detail']


def test_nothing_left_behind(tmp_path):
    assert m.classify(tmp_path / 'missing', DAY, 9)['kind'] == 'unknown'


class Api:
    """A GitHub issues API with one repository's worth of state."""
    def __init__(self):
        self.issues, self.comments, self.calls = [], {}, []

    def __call__(self, method, path, data=None):
        self.calls.append((method, path))
        if method == 'GET' and path.startswith('/issues?'):
            return [i for i in self.issues if i['state'] == 'open']
        if method == 'GET':
            return self.comments[int(path.split('/')[2])]
        if method == 'POST' and path == '/issues':
            issue = {'number': len(self.issues) + 1, 'title': data['title'], 'body': data['body'], 'state': 'open'}
            self.issues.append(issue); self.comments[issue['number']] = []
            return issue
        number = int(path.split('/')[2])
        if method == 'POST':
            self.comments[number].append({'body': data['body']}); return {}
        self.issues[number - 1].update(data); return {}

    def writes(self):
        return [c for c in self.calls if c[0] != 'GET']


def test_one_issue_per_incident_not_repeated_then_closed_on_recovery():
    api = Api()
    refused = {'kind': 'parser_refused', 'detail': 'The reader said: x.', 'source_url': 'https://ak-static.cms.nba.com/x.pdf'}
    first = m.alert('failure', DAY, refused, api, RUN, NOW)
    assert first == {'status': 'failure_notified', 'notification': True}
    text = api.issues[0]['body']
    assert api.issues[0]['title'] == m.TITLE and text.startswith('@themvf ')
    assert '6:23 PM Eastern' in text and 'official.nba.com/nba-injury-report-2026-27-season/' in text and RUN in text
    assert 'refused rather than guessed at' in text and 'x.pdf' in text

    # The next read fails the same way: no second message.
    assert m.alert('failure', DAY, refused, api, RUN, NOW)['status'] == 'already_reported'
    assert len(api.writes()) == 1
    # A different cause the same night is new information: one comment.
    posted = {'kind': 'not_posted', 'detail': '10 requests found no report.'}
    assert m.alert('failure', DAY, posted, api, RUN, NOW)['notification'] is True
    assert m.alert('failure', DAY, posted, api, RUN, NOW)['status'] == 'already_reported'
    assert len(api.comments[1]) == 1

    # A read succeeds: say so and close. The next success has nothing to do.
    assert m.alert('success', DAY, None, api, RUN, NOW) == {'status': 'recovered', 'notification': True}
    assert api.issues[0]['state'] == 'closed' and 'Recovered' in api.comments[1][-1]['body']
    assert m.alert('success', DAY, None, api, RUN, NOW) == {'status': 'healthy', 'notification': False}
    # A failure after recovery is a new incident: a new issue.
    assert m.alert('failure', DAY, refused, api, RUN, NOW)['status'] == 'failure_notified'
    assert [i['state'] for i in api.issues] == ['closed', 'open']


def test_no_slate_writes_nothing():
    api = Api()
    assert m.alert('failure', DAY, {'kind': 'no_slate', 'detail': 'x'}, api, RUN, NOW) == {'status': 'no_slate', 'notification': False}
    assert api.writes() == []


def test_main_exit_codes(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv('GITHUB_TOKEN', raising=False)
    write(tmp_path / 'failures' / 'a.json', {'discovery': {'requests': [{'status': 404}]}})
    base = ['nba_injury_alert', '--date', DAY, '--archive-root', str(tmp_path)]

    monkeypatch.setattr(m, 'games_stored', lambda day: 9)
    monkeypatch.setattr('sys.argv', base + ['--outcome', 'failure', '--fail-run'])
    assert m.main() == 1                                        # an injury-only run goes red
    out = capsys.readouterr().out
    assert '"kind": "not_posted"' in out and '::error title=Official injury report not read::' in out
    monkeypatch.setattr('sys.argv', base + ['--outcome', 'failure'])
    assert m.main() == 0                                        # the morning freeze goes on without a report

    monkeypatch.setattr(m, 'games_stored', lambda day: 0)
    monkeypatch.setattr('sys.argv', base + ['--outcome', 'failure', '--fail-run'])
    assert m.main() == 0                                        # no games tonight: not a failure
    monkeypatch.setattr('sys.argv', base + ['--outcome', 'success', '--fail-run'])
    assert m.main() == 0
    monkeypatch.setattr('sys.argv', base + ['--outcome', 'skipped', '--fail-run'])
    assert m.main() == 0


def test_index_url_follows_the_season():
    assert m.index_url('2026-11-13').endswith('nba-injury-report-2026-27-season/')
    assert m.index_url('2027-02-01').endswith('nba-injury-report-2026-27-season/')
    with pytest.raises(ValueError):
        m.index_url('not-a-date')
