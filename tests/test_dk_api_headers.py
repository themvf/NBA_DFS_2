"""DraftKings' API edge 403s a Python client that sends a browser User-Agent."""

from ingest import dk_api


def test_headers_carry_no_browser_user_agent():
    user_agent = {k.lower(): v for k, v in dk_api._HEADERS.items()}.get("user-agent", "")
    assert "mozilla" not in user_agent.lower()
    assert "chrome" not in user_agent.lower()


def test_headers_still_ask_for_json():
    assert dk_api._HEADERS.get("Accept") == "application/json"
