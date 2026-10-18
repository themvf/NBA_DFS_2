"""Prior-season comparison for the CFB pattern watch (cfb-pattern-history-v1). No database."""

from datetime import datetime, timedelta, timezone

import pytest

from model import cfb_pattern_history as h

KICK = datetime(2024, 9, 7, 19, 30, tzinfo=timezone.utc)


def _line(game_id, provider, market, home, away=None, rid=1, hconf="SEC", aconf="ACC", hcls="fbs", acls="fbs"):
    if market == "spread" and away is None:
        away = -home
    if market == "total" and away is None:
        away = home
    return {"game_id": game_id, "provider": provider, "market_type": market, "home_value": home,
            "away_value": away, "id": rid, "home_conference": hconf, "away_conference": aconf,
            "home_classification": hcls, "away_classification": acls}


def _book(game_id, provider, ml=(-300, 240), spread=-7.5, total=52.5, rid=1, **kw):
    return [_line(game_id, provider, "moneyline", ml[0], ml[1], rid, **kw),
            _line(game_id, provider, "spread", spread, None, rid, **kw),
            _line(game_id, provider, "total", total, None, rid, **kw)]


def _game(mid, season, home, away, hs, as_, kick=KICK):
    return {"matchup_id": mid, "season": season, "week": 2, "season_type": "regular", "home_score": hs,
            "away_score": as_, "commence_time": kick, "home_team_id": home, "away_team_id": away,
            "conference_game": False, "home_line_scores": [7, 7, 7, 7], "away_line_scores": [3, 3, 3, 3],
            "neutral_site": False, "hconf": "Big Ten", "aconf": "ACC", "hname": f"T{home}", "aname": f"T{away}",
            "hclass": "fbs", "aclass": "fbs"}


def test_select_lines_prefers_draftkings_and_keeps_one_book():
    rows = _book(1, "bovada", ml=(-280, 230), spread=-7.0, total=51.0) + _book(1, "draftkings")
    sel = h.select_lines(rows)
    assert sel["line_provider"] == "draftkings" and sel["total_provider"] == "draftkings"
    assert (sel["home_ml"], sel["away_ml"], sel["home_spread"], sel["vegas_total"]) == (-300, 240, -7.5, 52.5)


def test_select_lines_skips_a_book_with_a_one_sided_moneyline():
    rows = _book(1, "draftkings", ml=(-300, None)) + _book(1, "bovada", ml=(-280, 230), spread=-7.0)
    sel = h.select_lines(rows)
    assert sel["line_provider"] == "bovada" and sel["home_ml"] == -280 and sel["home_spread"] == -7.0


def test_select_lines_borrows_a_total_only_when_the_book_has_none():
    rows = [r for r in _book(1, "draftkings") if r["market_type"] != "total"] + _book(1, "espn bet", total=49.0)
    sel = h.select_lines(rows)
    assert sel["line_provider"] == "draftkings" and sel["total_provider"] == "espn bet" and sel["vegas_total"] == 49.0


def test_select_lines_without_a_moneyline_returns_no_lines():
    rows = [_line(1, "consensus", "spread", -3.0), _line(1, "consensus", "total", 50.0)]
    sel = h.select_lines(rows)
    assert sel["home_ml"] is None and sel["home_spread"] is None and sel["line_provider"] is None


def test_provider_spelling_is_normalized_and_newest_row_wins():
    assert h.normalize_provider(" Draft Kings ") == "draftkings"
    rows = _book(1, "draft kings", spread=-6.5, rid=1) + _book(1, "draftkings", spread=-7.5, rid=9)
    assert h.select_lines(rows)["home_spread"] == -7.5


def test_build_game_uses_season_correct_conference_and_classification():
    g = h.build_game(_game(1, 2023, 10, 20, 30, 10), _book(1, "draftkings", hconf="Big 12", aconf="American Athletic"))
    assert g["hconf"] == "Big 12" and g["aconf"] == "American Athletic"   # not today's cfb_teams values
    assert g["hclass"] == "fbs" and g["home_ml"] == -300


def test_build_game_keeps_team_conference_when_the_feed_has_none():
    g = h.build_game(_game(1, 2023, 10, 20, 30, 10), [])
    assert g["hconf"] == "Big Ten" and g["home_ml"] is None


def test_context_never_crosses_seasons():
    # Team 10 plays the last game of 2023 and the first of 2024: the 2024 game must have no previous game.
    games = [_game(1, 2023, 10, 20, 30, 10, kick=KICK - timedelta(days=300)),
             _game(2, 2024, 10, 30, 21, 20, kick=KICK)]
    lines = _book(1, "draftkings") + _book(2, "draftkings")
    built = {g["matchup_id"]: g for g in h.build_games(games, lines)}
    assert built[2]["home_rest_days"] is None and built[2]["home_last_margin"] is None


def test_context_within_a_season_is_attached():
    games = [_game(1, 2024, 10, 20, 30, 10, kick=KICK - timedelta(days=7)), _game(2, 2024, 30, 10, 14, 21, kick=KICK)]
    built = {g["matchup_id"]: g for g in h.build_games(games, _book(1, "draftkings") + _book(2, "draftkings"))}
    assert built[2]["away_rest_days"] == 7 and built[2]["away_last_margin"] == 20


def test_breakdown_rows_and_summary_match_current_season_shape():
    games = h.build_games([_game(1, 2023, 10, 20, 30, 10), _game(2, 2024, 30, 40, 10, 30)],
                          _book(1, "draftkings") + _book(2, "draftkings"))
    rows = h.breakdown_rows(games, h.seasons_label((2023, 2024, 2025)))
    assert rows[0]["Seasons"] == "2023-25" and rows[0]["Level name"] == "All games" and rows[0]["Games"] == 2
    moves = [r for r in rows if r["Level name"] == "By line movement"]
    assert moves and all(r["Games"] == 0 for r in moves)   # no early moneyline before 2026
    s = h.summary_row("2023-24", games)
    assert s["Games with lines"] == 2 and s["Favorites won (%)"] == pytest.approx(50.0)
    cov = {c["season"]: c for c in h.coverage(games)}
    assert cov[2023]["games_with_lines"] == 1 and cov[2023]["providers"] == {"draftkings": 1}


def test_seasons_label():
    assert h.seasons_label((2025, 2023, 2024)) == "2023-25" and h.seasons_label([2024]) == "2024"


def test_season_summary_chart(tmp_path):
    assert h.season_summary_chart([], tmp_path / "x.png") is False
    rows = [{"Season": "2023", "Games with lines": 700, "Favorites won (%)": 73.5, "Price implied (%)": 72.6,
             "Favorites covered (%)": 49.2}]
    assert h.season_summary_chart(rows, tmp_path / "s.png") is True and (tmp_path / "s.png").exists()
