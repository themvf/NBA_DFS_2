from datetime import date, datetime, timezone

from ingest import nba_box_scores as m


def _scoreboard(status="STATUS_FINAL", completed=True):
    return {"events": [{
        "id": "401909834", "date": "2026-10-21T23:00Z", "name": "Atlanta Hawks at Orlando Magic",
        "competitions": [{
            "status": {"type": {"name": status, "completed": completed}},
            "competitors": [
                {"homeAway": "home", "score": "125", "team": {"id": "19", "displayName": "Orlando Magic", "abbreviation": "ORL"}},
                {"homeAway": "away", "score": "132", "team": {"id": "1", "displayName": "Atlanta Hawks", "abbreviation": "ATL"}},
            ],
        }],
    }, {
        # 10:30pm ET tip is the next UTC day; it still belongs to the Eastern date.
        "id": "401909840", "date": "2026-10-22T02:30Z", "name": "Late game",
        "competitions": [{
            "status": {"type": {"name": "STATUS_IN_PROGRESS", "completed": False}},
            "competitors": [
                {"homeAway": "home", "team": {"id": "13", "displayName": "Los Angeles Lakers", "abbreviation": "LAL"}},
                {"homeAway": "away", "team": {"id": "12", "displayName": "LA Clippers", "abbreviation": "LAC"}},
            ],
        }],
    }]}


KEYS = ["minutes", "points", "fieldGoalsMade-fieldGoalsAttempted", "threePointFieldGoalsMade-threePointFieldGoalsAttempted",
        "freeThrowsMade-freeThrowsAttempted", "rebounds", "assists", "turnovers", "steals", "blocks",
        "offensiveRebounds", "defensiveRebounds", "fouls", "plusMinus"]


def _summary():
    return {"boxscore": {"players": [
        {"team": {"id": "1", "displayName": "Atlanta Hawks", "abbreviation": "ATL"}, "statistics": [{"keys": KEYS, "athletes": [
            {"athlete": {"id": "4278039", "displayName": "Nickeil Alexander-Walker", "position": {"abbreviation": "G"}}, "starter": True, "didNotPlay": False,
             "stats": ["30", "31", "8-15", "5-8", "10-13", "4", "3", "2", "1", "1", "3", "1", "3", "+10"]},
            {"athlete": {"id": "9999", "displayName": "Bench Guy"}, "starter": False, "didNotPlay": True, "reason": "COACH'S DECISION", "stats": []},
            {"athlete": {"id": "8888", "displayName": "Hurt Guy"}, "starter": False, "didNotPlay": True, "reason": "INJURY", "stats": []},
        ]}]},
        {"team": {"id": "19", "displayName": "Orlando Magic", "abbreviation": "ORL"}, "statistics": [{"keys": KEYS, "athletes": [
            {"athlete": {"id": "4432809", "displayName": "Franz Wagner"}, "starter": True, "didNotPlay": False,
             "stats": ["36", "0", "0-9", "0-4", "0-0", "6", "4", "3", "0", "0", "1", "5", "2", "-7"]},
        ]}]},
    ]}}


def test_scoreboard_rows_carry_eastern_date_and_status():
    games = m.parse_scoreboard(_scoreboard())
    assert [g.espn_event_id for g in games] == ["401909834", "401909840"]
    first, late = games
    assert first.game_date == date(2026, 10, 21) and first.completed and first.status == "STATUS_FINAL"
    assert first.home_team == "Orlando Magic" and first.away_team == "Atlanta Hawks"
    assert first.home_score == 125 and first.away_score == 132
    assert first.start_time == datetime(2026, 10, 21, 23, 0, tzinfo=timezone.utc)
    assert late.game_date == date(2026, 10, 21) and not late.completed


def test_summary_parses_stats_and_keeps_dnp_as_not_played():
    game = m.parse_scoreboard(_scoreboard())[0]
    players = {p.player: p for p in m.parse_summary(_summary(), game)}
    naw = players["Nickeil Alexander-Walker"]
    assert naw.points == 31 and naw.minutes == 30 and naw.fgm == 8 and naw.fga == 15 and naw.fg3m == 5 and naw.ftm == 10
    assert naw.played and naw.starter and not naw.is_home and naw.team == "Atlanta Hawks"
    assert naw.player_key == "nickeilalexanderwalker"
    bench = players["Bench Guy"]
    assert not bench.played and bench.points is None and bench.dnp_reason == "COACH'S DECISION"
    hurt = players["Hurt Guy"]
    assert not hurt.played and hurt.dnp_reason == "INJURY"
    # A player who played and scored zero is a real zero, not a DNP.
    wagner = players["Franz Wagner"]
    assert wagner.played and wagner.points == 0 and wagner.is_home
    # ESPN's listed position is kept; a player without one stays None, never a guess.
    assert naw.position == "G" and wagner.position is None


def test_season_label_rolls_in_october():
    assert m.season_label(date(2026, 10, 21)) == "2026-27"
    assert m.season_label(date(2027, 4, 10)) == "2026-27"
    assert m.season_label(date(2027, 10, 1)) == "2027-28"


def test_dry_run_fetches_no_summaries(monkeypatch):
    monkeypatch.setattr(m, "fetch_scoreboard", lambda target, session=None: _scoreboard())
    monkeypatch.setattr(m, "fetch_summary", lambda eid, session=None: (_ for _ in ()).throw(AssertionError("summary fetched in dry run")))
    out = m.ingest_date(None, date(2026, 10, 21), apply=False)
    assert out["games"] == 2 and out["final"] == 1 and out["written"] == 0
    assert out["notFinal"] == ["LA Clippers @ Los Angeles Lakers (STATUS_IN_PROGRESS)"]


def test_other_dates_games_are_dropped(monkeypatch):
    monkeypatch.setattr(m, "fetch_scoreboard", lambda target, session=None: _scoreboard())
    out = m.ingest_date(None, date(2026, 10, 22), apply=False)
    assert out["games"] == 0
