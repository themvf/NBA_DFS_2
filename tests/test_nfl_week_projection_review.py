from datetime import datetime, timezone

from model import nfl_week_projection_review as m

UTC = timezone.utc


def _p(pid, proj, info="NE@NYJ 10/18/2026 01:00PM ET", out=False, **kw):
    return {"ffPlayerId": pid, "name": f"P{pid}", "team": "NE", "opponent": "NYJ", "position": "QB",
            "ourProj": proj, "isOut": out, "floor": 10, "ceiling": 30, "gameInfo": info, **kw}


def test_kickoff_parse():
    k = m.kickoff_from_info("ARI@LAR 10/18/2026 04:05PM ET")
    assert k.astimezone(UTC) == datetime(2026, 10, 18, 20, 5, tzinfo=UTC)
    assert m.kickoff_from_info(None) is None


def test_latest_pregame_run_in_the_week_wins():
    early = {"run_id": "a", "created_at": datetime(2026, 10, 17, 12, tzinfo=UTC), "players": [_p(1, 18)]}
    late = {"run_id": "b", "created_at": datetime(2026, 10, 18, 15, tzinfo=UTC), "players": [_p(1, 20)]}
    after = {"run_id": "c", "created_at": datetime(2026, 10, 18, 18, tzinfo=UTC), "players": [_p(1, 99)]}
    other_week = {"run_id": "d", "created_at": datetime(2026, 10, 10, tzinfo=UTC),
                  "players": [_p(2, 15, info="CIN@DET 10/11/2026 01:00PM ET")]}
    window = (datetime(2026, 10, 15, 23, tzinfo=UTC), datetime(2026, 10, 20, 0, 15, tzinfo=UTC))
    picked = m.pick_projections([early, late, after, other_week], "QB", window)
    assert set(picked) == {1} and picked[1]["ourProj"] == 20


def test_review_rows_and_summary():
    proj = {1: _p(1, 20), 2: _p(2, 0, out=True), 3: _p(3, 15)}
    rows, surprises, pending = m.review_rows(proj, {1: {"points": 25.0}, 2: {"points": 17.3}})
    assert [r["name"] for r in rows] == ["P1"] and rows[0]["actual"] == 25.0
    assert surprises == [{"name": "P2", "team": "NE", "points": 17.3}]
    assert pending == [{"name": "P3", "team": "NE"}]
    s = m.summary(rows + [rows[0] | {"proj": 10.0, "actual": 5.0}, rows[0] | {"proj": 30.0, "actual": 29.0}])
    assert s["n"] == 3 and s["inside"] == 2 and s["below"] == 1


def test_chart_writes_png(tmp_path):
    rows = [{"name": "A", "team": "NE", "opp": "NYJ", "proj": 20.0, "p10": 10.0, "p90": 30.0, "actual": 25.0,
             "salary": 6000, "anchored": True, "model_proj": 19.0},
            {"name": "B", "team": "NYJ", "opp": "NE", "proj": 15.0, "p10": None, "p90": None, "actual": 9.0,
             "salary": 5000, "anchored": False, "model_proj": None}]
    assert m.review_chart(rows, [], 6, "QB", tmp_path / "q.png")
    assert (tmp_path / "q.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not m.review_chart([], [], 6, "QB", tmp_path / "x.png")


def test_top_by_projection_counts_unscored_players():
    rows = [{"name": "A", "team": "NE", "proj": 20.0}, {"name": "B", "team": "NE", "proj": 10.0}]
    pending = [{"name": "C", "team": "NE"}]
    proj = {3: {"name": "C", "team": "NE", "ourProj": 15.0}}
    kept, left = m.top_by_projection(rows, pending, proj, 2)
    assert [r["name"] for r in kept] == ["A"] and left == pending
    assert m.top_by_projection(rows, pending, proj, None) == (rows, pending)


def test_no_stat_line_on_a_finished_team_is_a_zero():
    rows, _, pending = m.review_rows({1: _p(1, 12)}, {}, {"NE"})
    assert pending == [] and rows[0]["actual"] == 0.0 and rows[0]["dnp"]
    _, _, pending = m.review_rows({1: _p(1, 12)}, {}, set())
    assert pending == [{"name": "P1", "team": "NE"}]
