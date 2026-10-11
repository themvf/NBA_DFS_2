import pytest

from ingest import tennis_data_source as source


@pytest.fixture(autouse=True)
def clear_cache():
    source._workbook_links.cache_clear()
    yield
    source._workbook_links.cache_clear()


def install_index(monkeypatch, html):
    calls = []

    class Response:
        text = html

        def raise_for_status(self):
            pass

    def get(url, **kwargs):
        calls.append(url)
        return Response()

    monkeypatch.setattr(source.requests, "get", get)
    return calls


def test_rotating_prefix_and_both_tours_share_index(monkeypatch):
    calls = install_index(monkeypatch, '''
      <A HREF="new-prefix/2026/2026.xlsx">ATP</A>
      <a href='/new-prefix/2026w/2026.xlsx'>WTA</a>
      <a href='https://other.example/2026/2026.xlsx'>untrusted</a>
    ''')
    assert source.workbook_url("ATP", 2026).endswith('/new-prefix/2026/2026.xlsx')
    assert source.workbook_url("WTA", 2026).endswith('/new-prefix/2026w/2026.xlsx')
    assert calls == [source.INDEX_URL]


@pytest.mark.parametrize('html', ['', '<a href="a/2026/2026.xlsx"></a><a href="b/2026/2026.xlsx"></a>'])
def test_missing_or_ambiguous_links_fail_loudly(monkeypatch, html):
    install_index(monkeypatch, html)
    with pytest.raises(ValueError, match='expected exactly one.*2026/2026.xlsx.*discovered='):
        source.workbook_url('ATP', 2026)


def test_discovery_http_errors_are_not_cached(monkeypatch):
    def fail(*args, **kwargs):
        raise source.requests.HTTPError('403 index denied')
    monkeypatch.setattr(source.requests, 'get', fail)
    with pytest.raises(source.requests.HTTPError, match='403'):
        source.workbook_url('ATP', 2026)
    install_index(monkeypatch, '<a href="rotated/2026/2026.xlsx">ATP</a>')
    assert '/rotated/' in source.workbook_url('ATP', 2026)
