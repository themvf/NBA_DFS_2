"""One broken YouTube channel must not stop every channel after it.

Until 2026-09-29 a terminated channel (BetUS: feed 404, "account has been
terminated due to a Trademark claim") raised inside the per-channel loop on
every run, so the 16 channels ordered after it were never scraped again.
"""

from __future__ import annotations

import pytest
import requests

from ingest import youtube_picks_videos as ytv

STATS = {"stored": 1, "ip_blocked": 0, "other_failed": 0, "candidates": 1}


def _http_error(status: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.exceptions.HTTPError(f"{status} Client Error", response=response)


def _channels(*ids):
    return [{"channel_id": i, "channel_name": f"name-{i}"} for i in ids]


@pytest.fixture
def run(monkeypatch):
    def _run(channels, failing: dict):
        seen = []

        def fake_fetch(db, channel_id, channel_name, limit):
            seen.append(channel_id)
            if channel_id in failing:
                raise failing[channel_id]
            return dict(STATS)

        monkeypatch.setattr(ytv, "_seed_default_channel_if_empty", lambda db: None)
        monkeypatch.setattr(ytv, "get_active_youtube_pick_channels", lambda db: channels)
        monkeypatch.setattr(ytv, "fetch_new_pick_videos", fake_fetch)
        stored = ytv.fetch_new_videos_for_all_channels(db=None)
        return stored, seen

    return _run


def test_a_failing_channel_is_skipped_and_later_channels_still_run(run, capsys):
    stored, seen = run(_channels("a", "dead", "c"), {"dead": _http_error(404)})
    assert seen == ["a", "dead", "c"]
    assert stored == 2
    out = capsys.readouterr().out
    assert "::warning title=YouTube channel skipped::name-dead (dead)" in out
    assert "terminated or renamed" in out
    assert "1 of 3 channel feed(s) failed" in out


def test_every_channel_failing_fails_the_job(run, capsys):
    with pytest.raises(SystemExit) as info:
        run(_channels("a", "b"), {"a": _http_error(404), "b": requests.exceptions.ConnectionError("down")})
    assert info.value.code == 1
    assert "every channel feed failed (2 of 2)" in capsys.readouterr().out


def test_a_non_feed_error_is_not_swallowed(run):
    # Database or programming errors are real failures, not a skipped channel.
    with pytest.raises(RuntimeError):
        run(_channels("a", "b"), {"a": RuntimeError("db broke")})


def test_rss_fetch_retries_a_transient_404_once(monkeypatch):
    calls = []
    feed = ('<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">'
            "<entry><yt:videoId>v1</yt:videoId><title>t</title><published>2026-09-28T00:00:00+00:00</published></entry>"
            "</feed>")

    def fake_get(url, headers, timeout):
        calls.append(url)
        response = requests.Response()
        response.status_code = 404 if len(calls) == 1 else 200
        response._content = feed.encode()
        response.url = url
        return response

    monkeypatch.setattr(ytv.requests, "get", fake_get)
    monkeypatch.setattr(ytv.time, "sleep", lambda s: None)
    entries = ytv._fetch_rss_entries("UCx")
    assert len(calls) == 2
    assert entries == [{"video_id": "v1", "title": "t", "published_at": "2026-09-28T00:00:00+00:00"}]


def test_rss_fetch_raises_a_persistent_404(monkeypatch):
    def fake_get(url, headers, timeout):
        response = requests.Response()
        response.status_code = 404
        response.url = url
        return response

    monkeypatch.setattr(ytv.requests, "get", fake_get)
    monkeypatch.setattr(ytv.time, "sleep", lambda s: None)
    with pytest.raises(requests.exceptions.HTTPError):
        ytv._fetch_rss_entries("UCx")
