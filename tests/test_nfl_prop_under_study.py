"""nfl-prop-under-v1: frozen constants, model pins, selection, settlement and the one-time verdict."""
from datetime import datetime, timedelta, timezone

import pytest

from research import nfl_prop_under_study as study

UTC = timezone.utc
REG = study.REGISTRATION
START = datetime.fromisoformat(REG["registered_at"])
KICK = START + timedelta(days=1)
# The synthetic cohorts below run for weeks; grade them under v1 as registered, before its amendment.
UNAMENDED = {**REG, "amendments": []}


def test_registration_constants_are_the_frozen_ones():
    assert REG["study"] == "nfl-prop-under-v1"
    assert REG["registered_at"] == "2026-10-17T16:30:00+00:00"
    assert REG["edge_threshold"] == -0.10 and REG["side"] == "under" and REG["book"] == "draftkings"
    assert REG["markets"] == ["rushing_yards", "receiving_yards", "receptions"]
    assert REG["floors"] == {"graded_bets": 300, "game_weeks": 10}
    assert REG["bootstrap"] == {"resamples": 2000, "seed": 20261017, "cluster": "game_week", "interval": 0.95}


def test_model_files_match_their_pins():
    """A change to the game-leaders model or pricing needs a dated amendment in
    docs/nfl-prop-under-study.json; rows priced after it leave this cohort."""
    held = study.pins_hold()
    assert all(held.values()), f"pinned files changed without an amendment: {[k for k, v in held.items() if not v]}"


def row(**over):
    base = dict(game_id="2026_06_AAA_BBB", metric="receptions", identity="00-1", player="P", book="draftkings",
                line=4.5, over_price=-115, under_price=-105, fair_over=0.55, model_p_over=0.40,
                prop_captured_at=KICK - timedelta(minutes=90), computed_at=KICK - timedelta(minutes=89),
                version="nfl-leaders-market-implied-v1")
    base.update(over)
    return base


def test_selection_keeps_the_latest_pregame_row_and_drops_ineligible_ones():
    kick = {"2026_06_AAA_BBB": KICK}
    rows = [row(), row(computed_at=KICK - timedelta(minutes=30), line=5.5),           # later pregame reprice wins
            row(identity="00-2", computed_at=KICK + timedelta(minutes=5)),            # priced after kickoff
            row(identity="00-3", prop_captured_at=START - timedelta(hours=1)),        # captured before registration
            row(identity="00-4", book="fanduel"),                                     # not the registered book
            row(identity="OTHER:BBB"),                                                # unnamed slot
            row(identity="00-5", metric="total_yards"),                               # not a registered market
            row(identity="00-6", under_price=None)]                                   # incomplete quote
    chosen = study.select_rows(rows, kick, START, None)
    assert [(r["identity"], r["line"]) for r in chosen] == [("00-1", 5.5)]
    discovery = study.select_rows(rows, kick, START, None, before_start=True)
    assert [r["identity"] for r in discovery] == ["00-3"]


def test_game_before_registration_is_never_confirmation_data():
    early = START - timedelta(hours=2)
    rows = [row(prop_captured_at=early - timedelta(minutes=90), computed_at=early - timedelta(minutes=89))]
    assert study.select_rows(rows, {"2026_06_AAA_BBB": early}, START, None) == []


def test_settle_prices_wins_at_the_under_price_and_voids_missing_players():
    assert study.settle(4.5, 3.0, -105) == ("win", pytest.approx(100 / 105))
    assert study.settle(4.5, 3.0, +120) == ("win", pytest.approx(1.2))
    assert study.settle(4.5, 6.0, -105) == ("loss", -1.0)
    assert study.settle(4.0, 4.0, -105) == ("push", 0.0)
    assert study.settle(4.5, None, -105) == ("void", None)


def _lines(n_weeks, per_week, win_every):
    rows, kicks, weeks, stats, covered = [], {}, {}, {}, set()
    for w in range(n_weeks):
        gid = f"2026_{7 + w:02d}_AAA_BBB"
        kick = KICK + timedelta(days=7 * w)
        kicks[gid], weeks[gid] = kick, 7 + w
        covered.add(gid)
        for i in range(per_week):
            ident = f"00-{w}-{i}"
            rows.append(row(game_id=gid, identity=ident, under_price=-110,
                            prop_captured_at=kick - timedelta(minutes=90), computed_at=kick - timedelta(minutes=89)))
            stats[(7 + w, ident)] = {"receptions": 3.0 if i % win_every else 6.0}
    return rows, kicks, weeks, stats, covered


def test_no_verdict_before_both_floors():
    rows, kicks, weeks, stats, covered = _lines(n_weeks=5, per_week=70, win_every=2)   # 350 bets, 5 weeks
    report = study.grade(rows, kicks, weeks, stats, covered, registration=UNAMENDED)
    assert report["primary"]["bets"] == 350 and not report["floors_met"] and report["verdict_if_first"] is None


def test_verdict_pass_and_fail_at_the_floors():
    rows, kicks, weeks, stats, covered = _lines(n_weeks=10, per_week=30, win_every=4)   # 75% under wins at -110
    report = study.grade(rows, kicks, weeks, stats, covered, registration=UNAMENDED)
    assert report["floors_met"] and report["verdict_if_first"] == "PASS"
    rows, kicks, weeks, stats, covered = _lines(n_weeks=10, per_week=30, win_every=2)   # 50% at -110 loses
    report = study.grade(rows, kicks, weeks, stats, covered, registration=UNAMENDED)
    assert report["floors_met"] and report["verdict_if_first"] == "FAIL"


def test_unflagged_lines_are_controls_not_bets_and_unfinished_games_wait():
    rows, kicks, weeks, stats, covered = _lines(n_weeks=2, per_week=4, win_every=2)
    rows.append(row(game_id="2026_07_AAA_BBB", identity="00-x", model_p_over=0.50,
                    prop_captured_at=kicks["2026_07_AAA_BBB"] - timedelta(minutes=90),
                    computed_at=kicks["2026_07_AAA_BBB"] - timedelta(minutes=89)))
    stats[(7, "00-x")] = {"receptions": 2.0}
    covered.discard("2026_08_AAA_BBB")
    report = study.grade(rows, kicks, weeks, stats, covered, registration=UNAMENDED)
    assert report["primary"]["bets"] == 4 and report["controls"]["every_under"]["bets"] == 5
    assert report["pending_lines"] == 4


def test_a_model_amendment_ends_the_cohort():
    amended = {**UNAMENDED, "amendments": [{"effective_at": (KICK + timedelta(days=1)).isoformat(),
                                      "model_pins": {"model/nfl_game_leaders.py": "x"}, "reason": "test"}]}
    rows, kicks, weeks, stats, covered = _lines(n_weeks=3, per_week=2, win_every=2)
    report = study.grade(rows, kicks, weeks, stats, covered, registration=amended)
    assert report["primary"]["game_weeks"] == 1


# --- v2 (2026-10-18): the same rules re-registered on the roles-v2 model -------------------------

V2_PATH = study.REGISTRATION_PATH.with_name("nfl-prop-under-study-v2.json")
V2 = __import__("json").loads(V2_PATH.read_text())


def test_v1_ended_by_one_dated_model_amendment():
    amendments = REG["amendments"]
    assert len(amendments) == 1 and amendments[0]["effective_at"] == "2026-10-18T19:45:00+00:00"
    assert set(amendments[0]["model_pins"]) == {"model/nfl_game_leaders_roles.py"}
    assert study.cohort_window(REG) == (START, datetime.fromisoformat("2026-10-18T19:45:00+00:00"))
    assert all(study.pins_hold(REG).values())


def test_v2_keeps_v1_rules_and_its_pins_hold():
    assert V2["study"] == "nfl-prop-under-v2" and V2["registered_at"] == REG["amendments"][0]["effective_at"]
    assert V2["supersedes"]["study"] == "nfl-prop-under-v1" and V2["amendments"] == []
    for key in ("season", "last_week", "markets", "market_version", "book", "side", "edge_threshold", "stake",
                "floors", "bootstrap"):
        assert V2[key] == REG[key], key
    assert set(V2["model_pins"]) == set(REG["model_pins"])
    assert all(study.pins_hold(V2).values()), "v2 pins must match the files as shipped"
    assert study.cohort_window(V2) == (datetime.fromisoformat(V2["registered_at"]), None)
