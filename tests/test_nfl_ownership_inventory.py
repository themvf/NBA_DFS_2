"""research/nfl_ownership_inventory.attach_contests: an unlinked contest finds its slate through the schedule,
whatever spelling each source uses for a team."""
from datetime import datetime, timedelta, timezone

from research import nfl_ownership_inventory as inv

THURSDAY = datetime(2026, 11, 13, 1, 15, tzinfo=timezone.utc)
SUNDAY = datetime(2026, 11, 9, 1, 20, tzinfo=timezone.utc)
SEPTEMBER = datetime(2026, 9, 14, 0, 20, tzinfo=timezone.utc)


def _slate(fmt, game, kickoff):
    return inv.Slate(f"{fmt}|{game}|{kickoff.isoformat()}", fmt, (game,), kickoff)


def _contest(contest_id, name, imported_at, *, fmt="showdown", week=10, upload=None):
    return {"contest_id": contest_id, "contest_name": name, "format": fmt, "season": 2026, "week": week,
            "slate_upload_id": upload, "entry_count": 1000, "file_name": "stat-api:1", "full_standings": True,
            "imported_at": imported_at}


def _attach(monkeypatch, games, contests, slates, upload_slate=None):
    def fake_rows(conn, sql, params=()):
        return games if "nfl_season_games" in sql else contests
    monkeypatch.setattr(inv, "rows", fake_rows)
    by_key = {s.key: s for s in slates}
    return {c["contest_id"]: c for c in inv.attach_contests(None, by_key, upload_slate or {}, 2026)}


def test_a_washington_contest_matches_the_schedules_spelling(monkeypatch):
    # DraftKings' contest name says WAS; our schedule says WSH. Compared raw, this contest matched no game.
    games = [{"season": 2026, "week": 10, "kickoff": THURSDAY, "away": "WSH", "home": "NYG"},
             {"season": 2026, "week": 9, "kickoff": SUNDAY, "away": "TB", "home": "CHI"}]
    slates = [_slate("showdown", "WAS@NYG", THURSDAY), _slate("showdown", "TB@CHI", SUNDAY)]
    contests = [_contest("1", "NFL Showdown $1.2M Thursday Night Showdown [$300K to 1st] (WAS @ NYG)", THURSDAY + timedelta(hours=11)),
                _contest("2", "NFL Showdown $1M Sunday Night Showdown (TB @ CHI)", SUNDAY + timedelta(days=3), week=9)]
    out = _attach(monkeypatch, games, contests, slates)
    assert out["1"]["link"] == "inferred_from_contest_name" and out["1"]["slate_key"] == slates[0].key
    assert out["2"]["slate_key"] == slates[1].key, "a team both sources spell alike is matched as before"
    assert [c["contest_id"] for c in slates[0].contests] == ["1"]


def test_the_match_still_needs_the_right_game_week_and_format(monkeypatch):
    games = [{"season": 2026, "week": 1, "kickoff": SEPTEMBER, "away": "NYG", "home": "WSH"},
             {"season": 2026, "week": 10, "kickoff": THURSDAY, "away": "WSH", "home": "NYG"}]
    slates = [_slate("showdown", "NYG@WAS", SEPTEMBER), _slate("showdown", "WAS@NYG", THURSDAY)]
    contests = [
        _contest("first", "NFL Showdown (NYG @ WAS)", SEPTEMBER + timedelta(days=2), week=1),        # the first meeting
        _contest("second", "NFL Showdown (WAS @ NYG)", THURSDAY + timedelta(hours=11)),              # the second
        _contest("classic", "NFL $2M Millionaire (WAS @ NYG)", THURSDAY + timedelta(hours=11), fmt="classic"),
        _contest("early", "NFL Showdown (WAS @ NYG)", THURSDAY - timedelta(hours=2)),                # imported before kickoff
        _contest("other", "NFL Showdown (DAL @ PHI)", THURSDAY + timedelta(hours=11)),               # no such game
        _contest("linked", "NFL Showdown (WAS @ NYG)", THURSDAY + timedelta(hours=11), upload="u-1"),
    ]
    out = _attach(monkeypatch, games, contests, slates, {"u-1": slates[1].key})
    assert out["first"]["slate_key"] == slates[0].key and out["second"]["slate_key"] == slates[1].key
    assert out["classic"]["slate_key"] is None and out["classic"]["link"].startswith("no_salary_upload")
    assert out["early"]["slate_key"] is None and out["other"]["slate_key"] is None
    assert out["linked"]["link"] == "slate_upload_id" and out["linked"]["slate_key"] == slates[1].key
