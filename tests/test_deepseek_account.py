"""A DeepSeek account problem must fail in one honest line, not a traceback.

2026-09-29: both DeepSeek jobs died on ``httpx.HTTPStatusError: 402 Payment
Required`` because the account ran out of credit. That is not retryable and
not a code bug, so the job now names it and where to fix it.
"""

from __future__ import annotations

import types

import httpx
import pytest

from model import mlb_beat_extraction, youtube_picks_extraction
from model.deepseek_account import DeepSeekAccountError, account_error_for, exit_on_account_error


def _cfg():
    return types.SimpleNamespace(
        api_key="k", base_url="https://api.deepseek.com/chat/completions", model="deepseek-chat",
        timeout_seconds=5, max_retries=3, retry_backoff_seconds=0,
    )


def _client_returning(status: int, calls: list):
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, json={"error": {"message": "Insufficient Balance"}})

    real_client = httpx.Client

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    return factory


@pytest.mark.parametrize("module", [mlb_beat_extraction, youtube_picks_extraction])
def test_402_raises_account_error_without_retrying(module, monkeypatch):
    calls: list = []
    monkeypatch.setattr(module.httpx, "Client", _client_returning(402, calls))
    monkeypatch.setattr(module.time, "sleep", lambda s: None)
    with pytest.raises(DeepSeekAccountError) as info:
        module._call_deepseek(_cfg(), "text")
    assert info.value.status == 402
    assert "no credit (402)" in str(info.value)
    assert "platform.deepseek.com" in str(info.value)
    assert len(calls) == 1  # a balance problem is not retried


@pytest.mark.parametrize("module", [mlb_beat_extraction, youtube_picks_extraction])
def test_transient_status_is_still_retried(module, monkeypatch):
    calls: list = []
    monkeypatch.setattr(module.httpx, "Client", _client_returning(503, calls))
    monkeypatch.setattr(module.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        module._call_deepseek(_cfg(), "text")
    assert len(calls) == 3


def test_only_account_statuses_are_named():
    assert account_error_for(402) is not None
    assert account_error_for(401) is not None
    assert account_error_for(429) is None
    assert account_error_for(500) is None


def test_exit_prints_one_line_and_fails(capsys, monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    with pytest.raises(SystemExit) as info:
        exit_on_account_error(DeepSeekAccountError(402), job="MLB beat extraction")
    assert info.value.code == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert err == ["MLB beat extraction: DeepSeek account has no credit (402); top it up at https://platform.deepseek.com"]


def test_exit_annotates_in_github_actions(capsys, monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(SystemExit):
        exit_on_account_error(DeepSeekAccountError(402), job="YouTube picks extraction")
    assert capsys.readouterr().err.startswith("::error title=YouTube picks extraction::DeepSeek account has no credit")
