"""Every DraftKings rushing-yards under, forward (research/nfl_rushing_under.py)."""
from datetime import datetime, timedelta, timezone

from research import nfl_rushing_under as ru

KICK = datetime(2026, 10, 25, 17, 0, tzinfo=timezone.utc)
GAME = {"game_id": "2026_07_SF_ATL", "week": 7, "kickoff": KICK, "completed": True,
        "home_name": "Atlanta Falcons", "away_name": "San Francisco 49ers"}
REG = {**ru.REGISTRATION, "registered_at": "2026-10-23T12:00:00+00:00"}


def _cap(player, dk, minutes_before, home="Atlanta Falcons", away="San Francisco 49ers", commence=KICK):
    return {"event_id": "e1", "player": player, "dk": dk, "captured_at": KICK - timedelta(minutes=minutes_before),
            "commence_time": commence, "home_team_name": home, "away_team_name": away}


def _dk(line, over=-115, under=-105):
    return {"line": line, "over": over, "under": under}


def test_close_is_the_latest_pregame_complete_draftkings_quote():
    caps = [_cap("Bijan Robinson", _dk(80.5), 19 * 60), _cap("Bijan Robinson", _dk(78.5, -110, -110), 70),
            _cap("Bijan Robinson", _dk(70.5), -5),            # after kickoff: never the close
            _cap("Bijan Robinson", {"line": 77.5, "over": -110}, 30)]  # one-sided: skipped
    [line] = ru.closing_lines(caps, [GAME], REG)
    assert (line["line"], line["over_price"], line["under_price"]) == (78.5, -110, -110)
    assert line["fair_over"] == 0.5 and line["key"] == "bijanrobinson" and line["in_final_capture"] is False


def test_line_dropped_before_the_final_capture_still_counts_and_is_flagged():
    caps = [_cap("Christian McCaffrey", _dk(70.5), 19 * 60), _cap("Christian McCaffrey", None, 70)]
    [line] = ru.closing_lines(caps, [GAME], REG)
    assert line["line"] == 70.5 and line["in_final_capture"] is False


def test_games_before_registration_or_outside_weeks_are_not_in_the_cohort():
    early = {**GAME, "kickoff": datetime(2026, 10, 23, 0, 15, tzinfo=timezone.utc)}
    cap = _cap("A Back", _dk(40.5), 70, commence=early["kickoff"])
    cap["captured_at"] = early["kickoff"] - timedelta(minutes=70)
    assert ru.closing_lines([cap], [early], REG) == []
    week6 = {**GAME, "week": 6}
    assert ru.closing_lines([_cap("A Back", _dk(40.5), 70)], [week6], REG) == []
    assert len(ru.closing_lines([_cap("A Back", _dk(40.5), 70)], [week6], REG, cohort_only=False)) == 1


def test_event_must_match_both_teams_and_kickoff_window():
    swapped = _cap("A Back", _dk(40.5), 70, home="San Francisco 49ers", away="Atlanta Falcons")
    late = _cap("A Back", _dk(40.5), 70, commence=KICK + timedelta(hours=3))
    assert ru.closing_lines([swapped, late], [GAME], REG) == []


def test_settlement_reuses_parent_rules_and_voids_missing_or_ambiguous_players():
    lines = ru.closing_lines([_cap("Bijan Robinson", _dk(78.5, -110, -110), 70),
                              _cap("Kyle Pitts Sr.", _dk(0.5), 70), _cap("Twin Name", _dk(10.5), 70),
                              _cap("Inactive Back", _dk(20.5), 70)], [GAME], REG)
    names = {(GAME["game_id"], "bijanrobinson"): ["b1"], (GAME["game_id"], "kylepitts"): ["k1"],
             (GAME["game_id"], "twinname"): ["t1", "t2"]}
    stats = {(7, "b1"): {"rushing_yards": 60}, (7, "k1"): {"rushing_yards": 0}}
    bets, pending = ru.settle_lines(lines, names, stats, {GAME["game_id"]})
    by = {b["player"]: b for b in bets}
    assert pending == 0
    assert by["Bijan Robinson"]["result"] == "win" and abs(by["Bijan Robinson"]["pnl"] - 100 / 110) < 1e-9
    assert by["Kyle Pitts Sr."]["result"] == "win"  # a stat row with no rushes is an active zero
    assert (by["Twin Name"]["result"], by["Twin Name"]["void_reason"]) == ("void", "ambiguous_name")
    assert (by["Inactive Back"]["result"], by["Inactive Back"]["void_reason"]) == ("void", "no_stat_row")
    assert ru.settle_lines(lines, names, stats, set())[1] == 4


def _bet(week, result, fair=0.5):
    pnl = {"win": 100 / 110, "loss": -1.0, "push": 0.0, "void": None}[result]
    return {"game_id": f"g{week}", "week": week, "result": result, "pnl": pnl, "fair_over": fair,
            "in_final_capture": True, "void_reason": "no_stat_row" if result == "void" else None}


def test_verdicts_only_at_the_final_look_and_only_past_the_floors():
    winning = [_bet(w, "win") for w in range(7, 19) for _ in range(40)] + \
              [_bet(w, "loss") for w in range(7, 19) for _ in range(20)]
    out = ru.summarize(winning, final=True)
    assert out["floors_met"] is True and out["primary"]["roi_interval"][0] > 0
    assert out["verdict_if_first"] == "PASS"
    assert ru.summarize(winning, final=False)["verdict_if_first"] is None
    losing = [_bet(w, "loss") for w in range(7, 19) for _ in range(40)] + \
             [_bet(w, "win") for w in range(7, 19) for _ in range(20)]
    assert ru.summarize(losing, final=True)["verdict_if_first"] == "LOSES"
    even = [_bet(w, r) for w in range(7, 19) for r in (["win"] * 30 + ["loss"] * 20 if w % 2 else ["win"] * 22 + ["loss"] * 28)]
    assert ru.summarize(even, final=True)["verdict_if_first"] == "NO_EDGE"
    assert ru.summarize(winning[:100], final=True)["verdict_if_first"] == "INCONCLUSIVE"


def test_calibration_counts_over_hits_against_the_devigged_price():
    bets = [_bet(7, "loss", 0.5), _bet(7, "win", 0.5), _bet(7, "win", 0.4), _bet(7, "push"), _bet(7, "void")]
    cal = ru.calibration(bets)
    assert cal == {"lines": 3, "over_hit": round(1 / 3, 4), "implied_over": round(1.4 / 3, 4),
                   "gap": round(1 / 3 - 1.4 / 3, 4)}


def test_registered_constants_are_frozen():
    """Registered before the first week-7 Sunday game: changing any of these is a new study version."""
    r = ru.REGISTRATION
    assert r["study"] == "nfl-rushing-under-v1"
    assert r["status"] == "registered"
    assert r["registered_at"] == "2026-10-23T14:05:00+00:00"
    assert datetime.fromisoformat(r["registered_at"]) < datetime(2026, 10, 25, 13, 30, tzinfo=timezone.utc)
    assert (r["season"], r["weeks"], r["book"], r["market"], r["stat"], r["side"], r["stake"]) == \
        (2026, [7, 18], "draftkings", "player_rush_yds", "rushing_yards", "under", 1.0)
    assert r["event_match_window_minutes"] == 120
    assert r["settlement"]["no_stat_row"] == "void" and r["settlement"]["push"] == 0.0
    assert r["floors"] == {"graded_bets": 500, "game_weeks": 8}
    assert r["bootstrap"] == {"resamples": 2000, "seed": 20261023, "cluster": "game_week", "interval": 0.95}
    assert r["verdict"]["last_week"] == 18
    assert set(r["verdict"]["outcomes"]) == {"PASS", "NO_EDGE", "LOSES", "INCONCLUSIVE"}
