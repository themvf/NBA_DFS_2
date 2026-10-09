from unittest.mock import Mock

import pandas as pd
import pytest

from ingest.nfl_pfr_supplement import AccessBlocked, Fetcher, main, proxy_settings, select_games
from model.nfl_pfr_supplement import SECTIONS, boxscore_id, parse_boxscore, scalar

GAME = dict(game_id="2025_01_DAL_PHI", pfr="202509040phi", season=2025, week=1,
            home_team="PHI", away_team="DAL", home_score=24, away_score=20)


def page(sections=SECTIONS):
    """Synthetic PFR-shaped markup: hidden tables, stable data-stat keys and IDs."""
    tables = []
    for section in sections:
        tables.append(f'''<!-- <table id="{section}"><thead><tr>
          <th data-stat="pass_pressured_pct" data-tip="Pressure percentage">Prss%</th>
          </tr></thead><tbody><tr class="thead"><th>Player</th></tr><tr>
          <th data-stat="player" data-append-csv="TestPl00"><a href="/players/T/TestPl00.htm">Test Player</a></th>
          <td data-stat="team">DAL</td><td data-stat="pass_pressured">10</td>
          <td data-stat="pass_pressured_pct">18.5%</td><td data-stat="pass_drops"></td>
          <td data-stat="pass_sacked">0</td></tr></tbody></table> -->''')
    return '<html><head><link rel="canonical" href="https://www.pro-football-reference.com/boxscores/202509040phi.htm"></head><body>' + ''.join(tables) + '</body></html>'


def test_hidden_tables_null_zero_percent_and_coverage():
    result = parse_boxscore(page(), GAME, captured_at="2026-09-27T10:00:00+00:00")
    assert result["status"] == "complete"
    assert len(result["rows"]) == 8
    passing = result["rows"][0]
    assert passing["stats"]["pass_pressured"] == 10
    assert passing["stats"]["pass_sacked"] == 0
    assert passing["stats"]["pass_drops"] is None
    assert passing["stats"]["pass_pressured_pct"] == 18.5
    assert passing["raw"]["pass_pressured_pct"] == "18.5%"
    assert result["coverage"]["passing_advanced"]["headers"]["pass_pressured_pct"] == "Pressure percentage"
    assert next(r for r in result["rows"] if r["section"] == "home_starters")["team"] == "PHI"


def test_missing_sections_are_partial_not_zero():
    result = parse_boxscore(page(["passing_advanced"]), GAME)
    assert result["status"] == "partial"
    assert result["coverage"]["defense_advanced"] == {"status": "missing", "rows": 0}


@pytest.mark.parametrize("html", ["<html>Access denied</html>", page([]),
    page().replace("202509040phi.htm", "202509070cle.htm"),
    page().replace('data-stat="team">DAL', 'data-stat="team">XXX'),
    page().replace('data-append-csv="TestPl00"', '').replace('/players/T/TestPl00.htm', '/unknown'),
    page(["passing_advanced", "passing_advanced"])])
def test_rejects_invalid_identity_blocked_pages_and_duplicates(html):
    with pytest.raises(ValueError):
        parse_boxscore(html, GAME)


def test_link_fallback_and_team_alias():
    g = dict(GAME, home_team="LAR", away_team="WSH")
    html = page().replace('data-append-csv="TestPl00"', '').replace('data-stat="team">DAL', 'data-stat="team">WAS')
    result = parse_boxscore(html, g)
    assert result["rows"][0]["pfr_player_id"] == "TestPl00"
    assert result["home_team"] == "LA"


@pytest.mark.parametrize("bad", ["https://example.com/boxscores/202509040phi.htm", "../../secret", "202509040phi?key=x"])
def test_rejects_arbitrary_fetch_targets(bad):
    with pytest.raises(ValueError):
        boxscore_id(bad)


def test_scalar_text_and_signed_numbers():
    assert scalar("-2") == -2
    assert scalar("1,234") == 1234
    assert scalar("LT") == "LT"
    assert scalar("--") is None


def test_schedule_only_completed_exact_games():
    pending = dict(GAME, game_id="2025_02_DAL_PHI", pfr="202509140phi", week=2, home_score=None)
    frame = pd.DataFrame([GAME, pending])
    assert len(select_games(frame, 2025)) == 1
    with pytest.raises(ValueError):
        select_games(frame, 2025, game_ids=[pending["game_id"]])
    with pytest.raises(ValueError):
        select_games(pd.DataFrame([GAME, GAME]), 2025)


@pytest.mark.parametrize("status", [403, 407, 429])
def test_blocks_stop_without_retry(status):
    fetcher = Fetcher(direct=True)
    fetcher.session.get = Mock(return_value=Mock(status_code=status, headers={}))
    with pytest.raises(AccessBlocked):
        fetcher.fetch(GAME["pfr"])
    assert fetcher.session.get.call_count == 1


def test_cloudflare_challenge_is_identified_as_site_block():
    fetcher = Fetcher(direct=True)
    fetcher.session.get = Mock(return_value=Mock(status_code=403, headers={"cf-mitigated": "challenge"}))
    with pytest.raises(AccessBlocked, match="PFR Cloudflare browser challenge"):
        fetcher.fetch(GAME["pfr"])
    assert fetcher.session.get.call_count == 1


def test_proxy_credentials_are_url_encoded(monkeypatch):
    monkeypatch.delenv("PFR_PROXY_URL", raising=False)
    monkeypatch.setenv("WEBSHARE_PROXY_USERNAME", "a@b")
    monkeypatch.setenv("WEBSHARE_PROXY_PASSWORD", "p:/#")
    assert proxy_settings()["https"] == "http://a%40b-rotate:p%3A%2F%23@p.webshare.io:80"


def test_offline_cli_and_cached_replay_keep_capture_time(tmp_path):
    import json
    pd.DataFrame([GAME]).to_csv(tmp_path / "games.csv", index=False)
    (tmp_path / f"{GAME['pfr']}.htm").write_text(page(), encoding="utf-8")
    args = ["--season", "2025", "--schedule-csv", str(tmp_path / "games.csv"),
            "--output-dir", str(tmp_path / "out"), "--cache-dir", str(tmp_path / "cache")]
    assert main(args + ["--html-dir", str(tmp_path)]) == 0
    first = json.loads((tmp_path / "out" / f"{GAME['game_id']}.json").read_text())
    assert main(args) == 0
    second = json.loads((tmp_path / "out" / f"{GAME['game_id']}.json").read_text())
    assert first == second


def test_batch_block_marks_remaining_games_not_attempted(tmp_path, monkeypatch):
    import json
    other = dict(GAME, game_id="2025_02_DAL_PHI", pfr="202509140phi", week=2)
    pd.DataFrame([GAME, other]).to_csv(tmp_path / "games.csv", index=False)
    monkeypatch.setattr(Fetcher, "fetch", Mock(side_effect=AccessBlocked("HTTP 403")))
    assert main(["--season", "2025", "--direct", "--schedule-csv", str(tmp_path / "games.csv"),
                 "--output-dir", str(tmp_path / "out"), "--cache-dir", str(tmp_path / "cache")]) == 1
    report = json.loads((tmp_path / "out" / "run-report.json").read_text())
    assert [r["status"] for r in report] == ["failed", "not_attempted"]
    assert not list((tmp_path / "cache").iterdir())
