"""Drive and play stories: pure shaping over synthetic rows (no database)."""
from model import cfb_drive_stories as ds


def _drive(start, result, pts=0, dpts=0, season=2023, n=1, rz=False, period=1):
    return {"season": season, "start_yards_to_goal": start, "drive_result": result, "offense_points": pts,
            "defense_points": dpts, "n": n, "reached_red_zone": rz, "start_period": period}


def _by(rows, key, value):
    return next(r for r in rows if r[key] == value)


def test_field_position_buckets_and_rates():
    rows = ds.points_by_field_position([
        _drive(75, "TD", 7), _drive(75, "PUNT"), _drive(70, "INT TD", 0, 7), _drive(70, "FG", 3),
        _drive(85, "PUNT", n=3),
        _drive(15, "TD", 14, n=2),  # points are summed over the row
    ])
    assert [r["Start"] for r in rows] == [b[0] for b in ds.FIELD_BUCKETS]
    own21 = _by(rows, "Start", "Own 21-40")
    assert own21["Drives"] == 4
    assert own21["TD (%)"] == 25.0 and own21["FG (%)"] == 25.0 and own21["Turnover (%)"] == 25.0
    assert own21["Points per drive"] == 2.5
    assert own21["Net points per drive"] == 0.75
    assert _by(rows, "Start", "Own 1-20")["Punt (%)"] == 100.0
    rz = _by(rows, "Start", "Red zone (20-1)")
    assert rz["Drives"] == 2 and rz["Points per drive"] == 7.0
    assert _by(rows, "Start", "Opp 29-21")["Drives"] == 0
    assert _by(rows, "Start", "Opp 29-21")["Points per drive"] is None


def test_bucket_edges():
    rows = ds.points_by_field_position([_drive(y, "PUNT") for y in (99, 80, 79, 60, 59, 50, 49, 30, 29, 21, 20, 1)])
    assert [r["Drives"] for r in rows] == [2, 2, 2, 2, 2, 2]


def test_excluded_drives_are_left_out():
    rows = ds.points_by_field_position([
        _drive(75, "END OF HALF"), _drive(75, "Uncategorized"), _drive(25, "TD", 7, period=5),
        _drive(0, "TD", 7), _drive(100, "PUNT"), _drive(75, "PUNT"),
    ])
    assert sum(r["Drives"] for r in rows) == 1


def test_red_zone_per_season_with_current_label():
    rows = ds.red_zone([
        _drive(60, "TD", 7, rz=True), _drive(40, "FG", 3, rz=True), _drive(30, "DOWNS", rz=True),
        _drive(30, "MISSED FG", rz=True), _drive(70, "TD", 7, rz=False),
        _drive(10, "TD", 6, season=2026, rz=True),
    ])
    assert [r["Season"] for r in rows] == ["2023", "2026 (to date)"]
    s = rows[0]
    assert s["Trips"] == 4 and s["TD (%)"] == 25.0 and s["FG (%)"] == 25.0 and s["No points (%)"] == 50.0
    assert s["Points per trip"] == 2.5


def _play(ptype, dist, ytg, gained, season=2023, n=1, period=4):
    return {"season": season, "play_type": ptype, "distance": dist, "yards_to_goal": ytg,
            "gained_distance": gained, "n": n, "period": period}


def test_classification_and_conversion():
    assert ds.classify_fourth_down("Rush") == "go"
    assert ds.classify_fourth_down("Punt") == "punt"
    assert ds.classify_fourth_down("Field Goal Missed") == "fg"
    assert ds.classify_fourth_down("Penalty") is None and ds.classify_fourth_down("Timeout") is None
    assert ds.go_converted("Passing Touchdown", False)
    assert not ds.go_converted("Interception Return Touchdown", True)
    assert ds.go_converted("Rush", True) and not ds.go_converted("Sack", False)


def test_fourth_down_cells_and_all_field():
    rows = ds.fourth_down([
        _play("Rush", 1, 40, True), _play("Pass Incompletion", 1, 40, False), _play("Punt", 1, 40, False),
        _play("Rush", 1, 70, True), _play("Punt", 1, 70, False, n=3),
        _play("Field Goal Good", 8, 15, False, n=2), _play("Rushing Touchdown", 9, 5, False),
        _play("Penalty", 1, 40, False), _play("Rush", 1, 40, True, period=5),
    ])
    cell = next(r for r in rows if r["Distance"] == "4th & 1" and r["Field zone"] == "Opp 50-36")
    assert cell["Fourth downs"] == 3 and cell["Went for it"] == 2 and cell["Converted"] == 1
    assert cell["Go rate (%)"] == 66.7 and cell["Conversion (%)"] == 50.0
    all1 = next(r for r in rows if r["Distance"] == "4th & 1" and r["Field zone"] == ds.ALL_ZONES)
    assert all1["Fourth downs"] == 7 and all1["Went for it"] == 3 and all1["Converted"] == 2
    long_rz = next(r for r in rows if r["Distance"] == "4th & 7+" and r["Field zone"] == "Red zone (20-1)")
    assert long_rz["Went for it"] == 1 and long_rz["Conversion (%)"] == 100.0
    assert not any(r["Distance"] == "4th & 2-3" for r in rows)


def test_fourth_down_trend():
    rows = ds.fourth_down_trend([
        _play("Rush", 2, 40, True, season=2022), _play("Punt", 2, 60, False, season=2022),
        _play("Punt", 9, 60, False, season=2022, n=2),
        _play("Rush", 1, 30, False, season=2023),
    ])
    assert [r["Season"] for r in rows] == ["2022", "2023"]
    r = rows[0]
    assert r["Fourth downs"] == 4 and r["Went for it"] == 1 and r["Go rate (%)"] == 25.0
    assert r["Short go rate (%)"] == 50.0 and r["Conversion (%)"] == 100.0
    assert rows[1]["Conversion (%)"] == 0.0


def test_comebacks_from_both_sides():
    rows = ds.comebacks([
        {"margin_after_3": -3, "home_won": True, "n": 1},             # home trailed by 3, won
        {"margin_after_3": 2, "home_won": True, "n": 3},              # away trailed by 2, lost
        {"margin_after_3": 7, "home_won": False, "went_to_overtime": True, "n": 1},
        {"margin_after_3": -20, "home_won": False, "n": 4},
        {"margin_after_3": 0, "home_won": True, "n": 9},              # tied: left out
    ])
    assert [r["Deficit entering 4th"] for r in rows] == ["1-3", "4-8", "9-16", "17+"]
    close = rows[0]
    assert close["Games"] == 4 and close["Trailing team won"] == 1 and close["Comeback (%)"] == 25.0
    assert rows[1]["Comeback (%)"] == 100.0 and rows[1]["Went to overtime (%)"] == 100.0
    assert rows[2]["Games"] == 0 and rows[2]["Comeback (%)"] is None
    assert rows[3]["Comeback (%)"] == 0.0


def test_charts_refuse_empty(tmp_path):
    assert ds.field_position_chart([], tmp_path / "a.png") is False
    assert ds.comeback_chart([], tmp_path / "b.png") is False
    assert ds.fourth_down_chart([], tmp_path / "c.png") is False
    assert ds.fourth_down_trend_chart([], tmp_path / "d.png") is False


def test_charts_draw(tmp_path):
    fp = ds.points_by_field_position([_drive(75, "TD", 7), _drive(15, "FG", 3)])
    cb = ds.comebacks([{"margin_after_3": -3, "home_won": True}])
    fd = ds.fourth_down([_play("Rush", 1, 40, True)])
    assert ds.field_position_chart(fp, tmp_path / "a.png") and (tmp_path / "a.png").exists()
    assert ds.comeback_chart(cb, tmp_path / "b.png")
    assert ds.fourth_down_chart(fd, tmp_path / "c.png")
    assert ds.fourth_down_trend_chart(ds.fourth_down_trend([_play("Rush", 1, 40, True)]), tmp_path / "d.png")
