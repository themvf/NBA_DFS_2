from datetime import date, datetime, timezone

from model import nba_points_calibration as m

TIP = datetime(2026, 10, 21, 23, 0, tzinfo=timezone.utc)
CAP = datetime(2026, 10, 21, 22, 7, tzinfo=timezone.utc)


def _books(line=24.5, over=-110, under=-110):
    return {"draftkings": {"line": line, "over": over, "under": under}, "fanduel": {"line": line, "over": over, "under": under}}


def _ladder():
    return {"draftkings": {"ladder": [
        {"point": 14.5, "side": "over", "price": -900}, {"point": 19.5, "side": "over", "price": -330},
        {"point": 29.5, "side": "over", "price": 190}, {"point": 34.5, "side": "over", "price": 500},
        {"point": 39.5, "side": "over", "price": 1200},
    ]}}


def _capture(player, event="evt1", home="Orlando Magic", away="Atlanta Hawks", books=None):
    return m.Capture(event_id=event, commence_time=TIP, captured_at=CAP, home_team=home, away_team=away,
                     player=player, books=books or _books())


def _game(players, espn="401909834", home="Orlando Magic", away="Atlanta Hawks"):
    return m.BoxGame(espn_event_id=espn, home_team=home, away_team=away, players=players)


def _player(name, team, points, played=True, is_home=False):
    from model.nba_points_ladder import name_key
    return m.BoxPlayer(player=name, player_key=name_key(name), team=team, is_home=is_home, played=played,
                       points=points, minutes=30.0 if played else None)


def test_graded_rows_match_by_accent_free_name_within_the_game():
    captures = [_capture("Nikola Jokic"), _capture("Franz Wagner"), _capture("Bench Guy"), _capture("Nobody")]
    games = [_game([_player("Nikola Jokić", "Atlanta Hawks", 31), _player("Franz Wagner", "Orlando Magic", 0, is_home=True),
                    _player("Bench Guy", "Atlanta Hawks", None, played=False)])]
    rows = {r.player: r for r in m.grade_captures(date(2026, 10, 21), captures, {("evt1", "nikolajokic"): _ladder()}, games)}
    jokic = rows["Nikola Jokic"]
    assert jokic.status == "graded" and jokic.actual_points == 31 and jokic.team == "Atlanta Hawks"
    assert jokic.espn_event_id == "401909834" and jokic.minutes_before_tip == 53.0
    assert jokic.line == 24.5 and jokic.books == 2 and jokic.ladder_books == 1
    assert jokic.at_least[30] is not None and jokic.ceiling is not None
    # Played and scored zero is a graded zero.
    assert rows["Franz Wagner"].status == "graded" and rows["Franz Wagner"].actual_points == 0
    assert rows["Franz Wagner"].ladder_books == 0 and rows["Franz Wagner"].at_least[30] is None
    assert rows["Bench Guy"].status == "dnp" and rows["Bench Guy"].actual_points is None
    assert rows["Nobody"].status == "unmatched"


def test_a_game_without_a_box_score_or_with_an_ambiguous_name_is_not_guessed():
    captures = [_capture("Twin Player", event="evt2", home="Boston Celtics", away="Miami Heat"), _capture("Twin Player")]
    games = [_game([_player("Twin Player", "Atlanta Hawks", 10), _player("Twin Player", "Orlando Magic", 20, is_home=True)])]
    rows = m.grade_captures(date(2026, 10, 21), captures, {}, games)
    by_event = {r.event_id: r for r in rows}
    assert by_event["evt2"].status == "no_box_score" and by_event["evt2"].espn_event_id is None
    assert by_event["evt1"].status == "ambiguous"


def test_match_game_needs_both_teams_and_handles_the_clippers_alias():
    games = [_game([], home="LA Clippers", away="Denver Nuggets")]
    assert m.match_game("Los Angeles Clippers", "Denver Nuggets", games) is games[0]
    assert m.match_game("Los Angeles Clippers", "Utah Jazz", games) is None


def test_summary_counts_and_brier():
    captures = [_capture("A"), _capture("B", books=_books(line=30.5)), _capture("C")]
    games = [_game([_player("A", "Atlanta Hawks", 31), _player("B", "Atlanta Hawks", 30), _player("C", "Atlanta Hawks", None, played=False)])]
    rows = m.grade_captures(date(2026, 10, 21), captures, {("evt1", "a"): _ladder()}, games)
    s = m.summarize(rows)
    assert s["rows"] == 3 and s["byStatus"] == {"graded": 2, "dnp": 1} and s["games"] == 1
    over = s["cells"]["over"]
    assert over["n"] == 2 and over["observed"] == 0.5 and abs(over["predicted"] - 0.5) < 1e-9 and abs(over["brier"] - 0.25) < 1e-9
    assert s["cells"]["30+"]["n"] == 1 and s["cells"]["30+"]["observed"] == 1.0
    assert s["cells"]["40+"]["n"] == 1 and s["cells"]["40+"]["observed"] == 0.0
    assert s["ceilingBeatRate"] in (0.0, 1.0)


def test_push_on_an_integer_line_is_not_graded():
    rows = m.grade_captures(date(2026, 10, 21), [_capture("A", books=_books(line=30))], {},
                            [_game([_player("A", "Atlanta Hawks", 30)])])
    assert rows[0].status == "push" and m.summarize(rows)["cells"]["over"]["n"] == 0
