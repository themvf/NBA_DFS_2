"""ESPN-built NBA rolling stats and the refresh exit code (2026-11-15)."""
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from ingest import nba_stats as ns
from model.dfs_projections import LEAGUE_AVG_DEF_RTG, LEAGUE_AVG_OFF_RTG, LEAGUE_AVG_PACE


def _game(points, minutes=30, **kw):
    row = dict(minutes=minutes, points=points, rebounds=5, assists=4, steals=1, blocks=0,
               turnovers=2, fga=12, fta=4, fg3m=1, fgm=5)
    row.update(kw)
    return row


def test_season_window_matches_config_rule():
    assert ns.season_window("2026-27") == (date(2026, 8, 1), date(2027, 8, 1))


def test_rolling_values_weights_newest_game_most():
    newest_first = pd.DataFrame([_game(30), _game(10), _game(10)])
    v = ns._rolling_values(newest_first)
    # EWMA alpha 0.25 over 10, 10, 30 (oldest first) = 10 + 0.25 * 20 = 15.
    assert v["ppg"] == pytest.approx(15.0)
    assert v["avg_minutes"] == pytest.approx(30.0)
    # Usage proxy unchanged: (FGA + 0.44 FTA + TOV) / (min/48*100) * 100.
    assert v["usage_rate"] == round((12 + 0.44 * 4 + 2) / (30 / 48 * 100) * 100, 1)


def _espn_frame():
    """Two games, four teams; team 1 scores more on the same possessions."""
    rows = []
    for gid, (a, b, pts_a, pts_b) in enumerate([(1, 2, 120, 100), (3, 4, 110, 110)]):
        start = datetime(2026, 11, 1 + gid, 0, tzinfo=timezone.utc)
        for team, pts in ((a, pts_a), (b, pts_b)):
            for k in range(5):
                rows.append(dict(espn_athlete_id=team * 10 + k, name=f"P{team}{k}", team_id=team,
                                 team=f"T{team}", game_id=str(gid), game_date=start.date(), start_time=start,
                                 **_game(pts / 5, minutes=48, fga=18, fgm=8, fta=4, turnovers=3)))
    return pd.DataFrame(rows)


def test_team_stats_rescale_to_model_constants(monkeypatch):
    written = {}
    monkeypatch.setattr(ns, "_espn_player_rows", lambda db, season: _espn_frame())
    monkeypatch.setattr(ns, "upsert_nba_team_stats", lambda db, **kw: written.__setitem__(kw["team_id"], kw))
    monkeypatch.setattr(ns, "TEAM_SHRINK_GAMES", 0.0)      # no shrink: test the rescale alone
    assert ns.build_team_stats_from_espn(object(), "2026-27") == 4
    paces = [w["pace"] for w in written.values()]
    assert sum(paces) / 4 == pytest.approx(LEAGUE_AVG_PACE, abs=0.02)
    offs = {t: w["off_rtg"] for t, w in written.items()}
    defs = {t: w["def_rtg"] for t, w in written.items()}
    assert sum(offs.values()) / 4 == pytest.approx(LEAGUE_AVG_OFF_RTG, abs=0.02)
    assert sum(defs.values()) / 4 == pytest.approx(LEAGUE_AVG_DEF_RTG, abs=0.02)
    assert offs[1] > offs[3] > offs[2] and defs[2] > defs[1]
    assert all(w["source"] == "espn" for w in written.values())


def test_team_stats_shrink_toward_constant(monkeypatch):
    written = {}
    monkeypatch.setattr(ns, "_espn_player_rows", lambda db, season: _espn_frame())
    monkeypatch.setattr(ns, "upsert_nba_team_stats", lambda db, **kw: written.__setitem__(kw["team_id"], kw))
    ns.build_team_stats_from_espn(object(), "2026-27")
    # One game each: weight 1 / (1 + 12), so team 1's offence sits just above the constant.
    assert LEAGUE_AVG_OFF_RTG < written[1]["off_rtg"] < LEAGUE_AVG_OFF_RTG + 3


def test_player_stats_use_negative_espn_id_and_latest_team(monkeypatch):
    frame = _espn_frame()
    # Athlete 10 changes team in the later game: the newest appearance decides.
    frame.loc[(frame.espn_athlete_id == 10) & (frame.game_id == "0"), "team_id"] = 3
    calls = []
    monkeypatch.setattr(ns, "_espn_player_rows", lambda db, season: frame)
    monkeypatch.setattr(ns, "upsert_nba_player_stats", lambda db, **kw: calls.append(kw))
    monkeypatch.setattr(ns, "_drop_other_source_rows", lambda *a: 0)
    assert ns.build_player_stats_from_espn(object(), "2026-27") == 20
    assert all(c["player_id"] < 0 and c["source"] == "espn" for c in calls)


def test_player_stats_refuse_unmapped_team(monkeypatch):
    frame = _espn_frame()
    frame.loc[0, "team_id"] = None
    monkeypatch.setattr(ns, "_espn_player_rows", lambda db, season: frame)
    with pytest.raises(RuntimeError, match="no teams row"):
        ns.build_player_stats_from_espn(object(), "2026-27")


def _refresh(monkeypatch, *, espn, team=30, players=470, fetch_ok=True, season="2026-27"):
    monkeypatch.setattr(ns, "espn_has_season", lambda db, s: espn)
    monkeypatch.setattr(ns, "build_team_stats_from_espn", lambda db, s: team)
    monkeypatch.setattr(ns, "build_player_stats_from_espn", lambda db, s, n: players)

    def fetch(*a, **k):
        if not fetch_ok:
            raise TimeoutError("stats.nba.com")
        return 100
    monkeypatch.setattr(ns, "fetch_team_stats", lambda db, s: fetch() and team)
    monkeypatch.setattr(ns, "fetch_team_game_logs", fetch)
    monkeypatch.setattr(ns, "fetch_player_game_logs", fetch)
    monkeypatch.setattr(ns, "fetch_player_rolling_stats", lambda *a, **k: players)
    monkeypatch.setattr(ns, "nba_season_for", lambda: "2026-27")
    return ns.run_refresh(object(), season, "Regular Season", 10)


def test_refresh_succeeds_from_espn(monkeypatch):
    assert _refresh(monkeypatch, espn=True) == 0


def test_refresh_fails_when_nothing_written(monkeypatch):
    assert _refresh(monkeypatch, espn=True, players=0) == 1
    assert _refresh(monkeypatch, espn=True, team=0) == 1


def test_recomputing_stored_logs_is_not_a_refresh(monkeypatch):
    # The July-November failure: every fetch timed out, the recompute of June
    # logs "succeeded", and the job exited 0.
    assert _refresh(monkeypatch, espn=False, fetch_ok=False, season="2024-25") == 1
    assert _refresh(monkeypatch, espn=False, fetch_ok=True, season="2024-25") == 0


def test_current_season_before_opening_night_is_not_a_failure(monkeypatch):
    # Aug 1 to the first real game: no box scores yet, so nothing to build,
    # and stats.nba.com is not tried (it would time out for ~30 minutes).
    def boom(*a, **k):
        raise AssertionError("stats.nba.com must not be called")
    monkeypatch.setattr(ns, "fetch_team_stats", boom)
    monkeypatch.setattr(ns, "espn_has_season", lambda db, s: False)
    monkeypatch.setattr(ns, "nba_season_for", lambda: "2027-28")
    assert ns.run_refresh(object(), "2027-28", "Regular Season", 10) == 0
