"""Daily odds must reach pick'em, without laundering failed captures."""

import pytest

from ingest import refresh_nfl_vegas as refresh


@pytest.mark.parametrize("failure", [None, "odds", "freshness", "alerts", "probabilities"])
def test_probability_refresh_dependency_and_failure_reporting(monkeypatch, failure):
    from model import line_alerts

    calls = []

    def stage(name, result=1):
        def run(*args):
            calls.append(name)
            if failure == name:
                if name == "freshness":
                    return False
                raise RuntimeError(name)
            return result
        return run

    monkeypatch.setattr(refresh, "seed_teams", stage("teams"))
    monkeypatch.setattr(refresh, "fetch_events", stage("events"))
    monkeypatch.setattr(refresh, "fetch_odds", stage("odds"))
    monkeypatch.setattr(refresh, "fetch_scores", stage("scores"))
    monkeypatch.setattr(refresh, "verify_fresh_upcoming_odds", stage("freshness", True))
    monkeypatch.setattr(refresh, "collect_nfl_data_health", stage("health", {"status": "pass"}))
    monkeypatch.setattr(line_alerts, "scan", stage("alerts"))
    monkeypatch.setattr(line_alerts, "settle", stage("settlement"))
    monkeypatch.setattr(refresh, "_refresh_pickem_probabilities", stage("probabilities", {}))

    assert refresh.run_refresh(object(), "test", "2026-09-13") == (0 if failure is None else 1)
    assert ("probabilities" in calls) == (failure not in {"odds", "freshness"})
    if "probabilities" in calls:
        assert calls.index("probabilities") > calls.index("freshness") > calls.index("odds")


@pytest.mark.parametrize("date,season", [("2026-09-13", 2026), ("2027-01-10", 2026)])
def test_daily_probability_refresh_links_games_and_scopes_writes(monkeypatch, date, season):
    from ingest import nfl_season_schedule as schedule
    from model import nfl_survivor_model as model

    calls = []
    frame = object()

    class DB:
        def execute(self, query, params):
            if "FROM nfl_matchups" in query:
                assert params == (date,)
                assert "commence_time > NOW()" in query
                assert "season_type = 'regular'" in query
                return [{"id": 99}]
            assert params == (season, [99])
            return [{"id": 7, "matchup_id": 99}]

    monkeypatch.setattr(schedule, "fetch_schedule", lambda: frame)
    monkeypatch.setattr(schedule, "load_season", lambda db, s, f: calls.append(("link", s, f)))
    monkeypatch.setattr(model, "historical_games", lambda f: f)
    monkeypatch.setattr(model, "fit_spread_prob", lambda f: {"fit": True})
    monkeypatch.setattr(model, "compute_and_store", lambda db, s, fit, **kw: calls.append(("compute", s, kw)))
    refresh._refresh_pickem_probabilities(DB(), date)
    assert calls == [("link", season, frame), ("compute", season, {"game_ids": {7}})]


def test_no_games_does_not_load_schedule_or_recompute(monkeypatch):
    from ingest import nfl_season_schedule as schedule

    class DB:
        def execute(self, *_):
            return []

    def unexpected():
        pytest.fail("An empty game day should not fetch the season schedule")

    monkeypatch.setattr(schedule, "fetch_schedule", unexpected)
    assert refresh._refresh_pickem_probabilities(DB(), "2026-09-15") == {"skipped": "no upcoming games"}


def test_daily_model_uses_newer_lines_and_preserves_other_games(monkeypatch):
    from model import nfl_survivor_model as model

    games = [dict(
        id=i, week=1, home_team_id=1, away_team_id=2,
        home_abbrev="BUF", away_abbrev="HOU",
        market_spread_line=1.5, live_spread=-3, quoted_spread_line=None,
        market_captured_at="2026-09-08", live_captured_at="2026-09-13",
        market_home_ml=-110, market_away_ml=100,
        live_home_ml=-150, live_away_ml=130,
        quoted_home_ml=None, quoted_away_ml=None,
    ) for i in (7, 8)]
    writes = []

    class DB:
        def execute(self, query, params):
            if "FROM nfl_season_games" in query:
                return games
            if "INSERT INTO nfl_game_win_probs" in query:
                writes.append(params)
            return []

    monkeypatch.setattr(model, "load_sigma_table", lambda db: {})
    model.compute_and_store(DB(), 2026, {"tie_rate_close": 0}, game_ids={7})
    assert len(writes) == 2  # both teams, only the selected game
    assert {row[0] for row in writes} == {7}
    assert writes[0][6] == pytest.approx(model._american_pair_to_prob(-150, 130))
    assert writes[0][9] == 3  # freshest source's spread, home-favored convention
