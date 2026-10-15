from datetime import datetime, timedelta, timezone
from research.cfb_forecast_health import assess, VERSIONS

NOW = datetime(2026, 10, 15, 20, tzinfo=timezone.utc)


def fixture():
    games = [dict(id=i, season=2026, home_team_id=1, away_team_id=2,
                  completed=True, commence_time=NOW-timedelta(days=i), home_score=20, away_score=10)
             for i in (1, 2)]
    games.append(dict(id=3, season=2026, home_team_id=1, away_team_id=2,
                      completed=False, commence_time=NOW+timedelta(hours=3)))
    plays = {(i,t): {"ppa_plays": 50} for i in (1,2) for t in (1,2)}
    drives = {(i,t): 10 for i in (1,2) for t in (1,2)}
    forecasts = [dict(game_id=3, version=v, kickoff=games[-1]["commence_time"],
                      generated_at=NOW, home_current_games=2, away_current_games=2) for v in VERSIONS]
    return games, plays, drives, forecasts


def test_all_models_and_input_games_required():
    g,p,d,f = fixture()
    assert assess(g,p,d,f,NOW,NOW)["status"] == "pass"
    assert assess(g,p,d,f[:-1],NOW,NOW)["status"] == "fail"
    f[0]["home_current_games"] = 1
    assert assess(g,p,d,f,NOW,NOW)["status"] == "fail"


def test_missing_inputs_fail_even_with_fresh_forecast():
    g,p,d,f = fixture()
    p.pop((1,1))
    assert assess(g,p,d,f,NOW)["status"] == "fail"


def test_old_or_rescheduled_publication_rejected():
    g,p,d,f = fixture()
    f[0]["generated_at"] = NOW-timedelta(days=4)
    assert assess(g,p,d,f,NOW,NOW-timedelta(hours=1))["status"] == "fail"
    f[0]["generated_at"] = NOW
    f[0]["kickoff"] += timedelta(hours=1)
    assert assess(g,p,d,f,NOW,NOW)["status"] == "fail"


def test_finished_games_not_required_and_early_season_excluded():
    g,p,d,f = fixture()
    assert assess(g,p,d,[],NOW+timedelta(hours=4),NOW)["status"] == "pass"
    g.pop(0)
    report = assess(g,p,d,[],NOW,NOW)
    assert report["excluded"] and report["status"] == "pass"
