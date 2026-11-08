"""nfl-first-td-lines-v2: injury-report removals and the dropped starter's redirected volume."""
import itertools

from model import cfb_first_td as cf
from model import nfl_first_td as n


def test_ruled_out_mirrors_the_game_leaders_rule():
    from research.nfl_game_leaders import availability_status
    states = (None, "HEALTHY", "QUESTIONABLE", "DOUBTFUL", "OUT", "IR", "PUP", "NFI", "SUSPENDED")
    officials = (None, "OUT", "INACTIVE")
    for sleeper, fp, official in itertools.product(states, states, officials):
        assert n.ruled_out(sleeper, fp, official) == (availability_status(sleeper, fp, official) == "out"), \
            (sleeper, fp, official)


def test_ruled_out_cases():
    assert n.ruled_out("OUT", "OUT", None)
    assert not n.ruled_out("OUT", "QUESTIONABLE", None), "a weekly OUT needs FantasyPros to agree"
    assert not n.ruled_out("DOUBTFUL", "DOUBTFUL", None), "doubtful is not ruled out; the DK list may still drop him"
    assert n.ruled_out("IR", None, None), "a reserve designation settles it alone"
    assert n.ruled_out("QUESTIONABLE", "QUESTIONABLE", "INACTIVE"), "the official list settles it"
    assert n.out_reason("QUESTIONABLE", None, "INACTIVE") == "inactive (official list)"
    assert n.out_reason("IR", None, None) == "IR (Sleeper)"
    assert n.out_reason("OUT", "OUT", None) == "out (Sleeper and FantasyPros)"


def _row(gid, week, pid, team="NYG", **kw):
    r = {"cfbd_game_id": gid, "pid": pid, "name": f"A {pid}", "team": team, "season": 2026, "week": week,
         "season_type": "regular", "rush_att": 0, "receptions": 0, "pass_att": 0, "inside10": 0,
         "rush_td": 0, "rec_td": 0}
    return {**r, **kw}


def _history():
    rows = []
    for w in range(1, 9):
        rows += [_row(w, w, "rb1", rush_att=16, receptions=3, rush_td=1 if w % 2 else 0),
                 _row(w, w, "rb2", rush_att=5, receptions=1),
                 _row(w, w, "wr1", receptions=9, rec_td=1 if w % 3 == 0 else 0),
                 _row(w, w, "te1", receptions=4),
                 _row(w, w, "qb", pass_att=33, rush_att=3)]
        rows += [_row(w, w, "oppqb", team="PHI", pass_att=30), _row(w, w, "opprb", team="PHI", rush_att=20, rush_td=1)]
    return cf.History(rows)


def _short(name):
    return name.split()[-1]


def _statuses(**overrides):
    base = {"rb1": {"position": "RB"}, "rb2": {"position": "RB"}, "wr1": {"position": "WR"},
            "te1": {"position": "TE"}, "qb": {"position": "QB"}, "oppqb": {"position": "QB"}, "opprb": {"position": "RB"}}
    out = {}
    for pid, st in base.items():
        out[pid] = {"sleeper": None, "fantasypros": None, "official": None, "as_of": None, **st, **overrides.get(pid, {})}
    return out


def _game():
    return {"game_id": "2026_09_NYG_PHI", "season": 2026, "week": 9, "away": "NYG", "home": "PHI",
            "commence_time": "2026-11-08T18:00:00+00:00", "home_implied": 26.0, "away_implied": 20.0}


def test_injury_reports_remove_a_player_and_redirect_half_his_volume_to_his_group():
    h = _history()
    statuses = _statuses(rb1={"sleeper": "OUT", "fantasypros": "OUT"})
    base = n.predict([_game()], n.FITTED, h, {}, {}, lines=None, filter_lines=False, statuses=statuses)[0]
    avail = n.predict([_game()], n.FITTED, h, {}, {}, lines=None, filter_lines=True, statuses=statuses)[0]
    assert base["availability"] is None and "rb1" in base["dist"]
    a = avail["availability"]
    assert a["status"] == "applied" and a["lines_status"] == "no_lines" and a["sources"] == ["injury_reports"]
    assert [_short(d["name"]) for d in a["dropped"]] == ["rb1"] and a["dropped"][0]["reason"] == "out (Sleeper and FantasyPros)"
    assert "rb1" not in avail["dist"]
    moved = [m for m in a["redirected"] if _short(m["from"]) == "rb1"][0]
    # rb1 had 8 x 19 = 152 touches; half (76) go to the only other back, rb2 (48 touches), not to wr1/te1.
    assert moved["touches"] == 76.0 and moved["to"] == [{"name": "A rb2", "added_touches": 76.0}]
    by_name = {_short(p["name"]): p for p in avail["players"]}
    assert by_name["rb2"]["touches"] == 48 + 76
    assert avail["dist"]["rb2"] > base["dist"]["rb2"] * 2, "the backup carries the starter's role"
    assert abs(sum(avail["dist"].values()) - 1) < 1e-9
    # Efficiency does not transfer: rb2's touchdowns are his own.
    assert by_name["rb2"]["td"] == 0


def test_no_redirect_when_the_dropped_player_already_missed_the_last_game():
    rows = []
    for w in range(1, 9):
        rows += [_row(w, w, "rb2", rush_att=5 if w < 8 else 18), _row(w, w, "wr1", receptions=8),
                 _row(w, w, "oppqb", team="PHI", pass_att=30)]
        if w < 8:
            rows.append(_row(w, w, "rb1", rush_att=16))  # on the roster in week 7 (a candidate), absent in week 8
    h = cf.History(rows)
    statuses = _statuses(rb1={"sleeper": "IR"})
    avail = n.predict([_game()], n.FITTED, h, {}, {}, lines=None, statuses=statuses)[0]
    moved = [m for m in avail["availability"]["redirected"] if _short(m["from"]) == "rb1"][0]
    assert moved["to"] == [] and moved["note"] == "did not play the team's last game"
    assert {_short(p["name"]): p["touches"] for p in avail["players"]}["rb2"] == 5 * 7 + 18


def test_a_quarterback_is_dropped_but_nobody_inherits_his_volume():
    h = _history()
    statuses = _statuses(qb={"sleeper": "QUESTIONABLE", "official": "INACTIVE"})
    avail = n.predict([_game()], n.FITTED, h, {}, {}, lines=None, statuses=statuses)[0]
    a = avail["availability"]
    assert _short(a["dropped"][0]["name"]) == "qb" and a["dropped"][0]["reason"] == "inactive (official list)"
    assert a["redirected"][0]["to"] == [] and a["redirected"][0]["note"] == "no position group to inherit"


def test_draftkings_list_and_reports_combine_with_reasons():
    h = _history()
    listed = {cf.name_key(x) for x in ("A. rb2", "A. wr1", "A. te1", "A. qb", "A. oppqb", "A. opprb")}
    # wr1 is on the list but out on our reports; rb1 is off the list and IR: the report's reason wins.
    # DraftKings' coverage (4 of our 5 NYG candidates listed) is judged over the full candidate set.
    lines = {"2026_09_NYG_PHI": {"names": listed, "captured_at": "2026-11-08T17:00:00+00:00"}}
    statuses = _statuses(wr1={"sleeper": "OUT", "fantasypros": "OUT"}, te1={"sleeper": "IR"})
    avail = n.predict([_game()], n.FITTED, h, {}, {}, lines=lines, statuses=statuses)[0]
    a = avail["availability"]
    assert a["sources"] == ["draftkings_list", "injury_reports"] and a["captured_at"] == "2026-11-08T17:00:00+00:00"
    assert {(_short(d["name"]), d["reason"]) for d in a["dropped"]} == {("rb1", "not on DraftKings list"),
                                                                         ("wr1", "out (Sleeper and FantasyPros)"),
                                                                         ("te1", "IR (Sleeper)")}
    assert [d["reason"] for d in a["dropped"]][-1] == "not on DraftKings list", "report removals are listed first"
    assert len(a["dropped"]) == 3, "a player both sources exclude is listed once"
    # rb1's volume goes to rb2; wr1 and te1 leave no receiver to inherit.
    to = {_short(m["from"]): ([_short(t["name"]) for t in m["to"]], m.get("note")) for m in a["redirected"]}
    assert to == {"rb1": (["rb2"], None), "wr1": ([], "nobody in his position group"),
                  "te1": ([], "nobody in his position group")}


def test_status_chips_only_for_questionable_or_doubtful():
    assert n.status_chips("x", {"x": {"sleeper": "DOUBTFUL"}}) == [{"code": "INJURY_DOUBTFUL", "label": "Doubtful (Sleeper)"}]
    assert n.status_chips("x", {"x": {"sleeper": "OUT"}}) == []
    assert n.status_chips("x", None) == []
