"""nfl-showdown-ownership-grade-v1: what is graded, on whom, and how a contest is matched to its slate."""
from datetime import datetime, timedelta, timezone

from ingest import nfl_showdown_ownership_grade as job
from model import nfl_ownership_provenance as prov
from model import nfl_showdown_ownership_grade as g
from research.nfl_ownership_inventory import Slate


def player(name, captain, flex, source="nfl-ownership-field-showdown-v2", *, out=False, ours=None, shadow=None,
           shadow_version="nfl-ownership-fit-showdown-v1", live_version="nfl-ownership-field-showdown-v2"):
    ownership = {"total": None if captain is None else captain + flex, "captain": captain, "flex": flex, "source": source}
    if ours is not None:
        ownership["model"] = {"captain": ours[0], "flex": ours[1]}
    if shadow is not None:
        ownership.update({"shadowCaptain": shadow[0], "shadowFlex": shadow[1], "shadowVersion": shadow_version,
                          "liveVersion": live_version})
    return {"name": name, "isOut": out, "ownership": ownership}


def test_family_names_each_kind_of_estimate():
    assert g.family("nfl-ownership-field-showdown-v2") == g.FAMILY_LINEUP_MODEL
    assert g.family("nfl-ownership-fit-showdown-v1") == g.FAMILY_FITTED_MODEL
    assert g.family("stat-api") == g.FAMILY_STAT_API
    assert g.family("nfl-ownership-prior-v5") == g.FAMILY_RULE_OF_THUMB
    assert g.family("linestar") == g.FAMILY_LINESTAR
    # The bands clamp appends its own version to the source; the model behind it is what is graded.
    assert g.family("nfl-ownership-prior-v5+nfl-ownership-bands-v1") == g.FAMILY_RULE_OF_THUMB
    assert g.family(None) == g.family("something else") == g.FAMILY_OTHER


def test_one_stored_model_is_graded_as_the_number_the_build_read():
    players = [player("A One", 10.0, 40.0), player("B Two", 2.0, 20.0)]
    labels = {"aone": {"CPT": 14.0, "FLEX": 30.0}, "btwo": {"CPT": 1.0, "FLEX": 26.0}}
    result = g.grade_slate(players, labels)
    assert [m["role"] for m in result["models"]] == ["shown"]
    shown = result["models"][0]
    assert shown["family"] == g.FAMILY_LINEUP_MODEL and shown["version"] == "nfl-ownership-field-showdown-v2"
    assert shown["captain"] == {"n": 2, "miss": 2.5, "bias": -1.5, "chalk_n": 1, "chalk_miss": 4.0,
                                "worst": [{"name": "A One", "estimate": 10.0, "real": 14.0},
                                          {"name": "B Two", "estimate": 2.0, "real": 1.0}]}
    assert shown["flex"]["miss"] == 8.0 and shown["flex"]["bias"] == 2.0 and shown["flex"]["chalk_n"] == 2
    assert result["detail"]["shown_totals"] == {"captain": 12.0, "flex": 60.0}


def test_stat_api_shown_keeps_our_model_and_the_comparison_beside_it():
    players = [
        player("A One", 12.0, 45.0, "stat-api", ours=(9.0, 38.0), shadow=(7.0, 30.0), live_version="nfl-ownership-field-showdown-v2"),
        player("B Two", 1.0, 22.0, "stat-api", ours=(2.0, 28.0), shadow=(3.0, 26.0), live_version="nfl-ownership-field-showdown-v2"),
    ]
    labels = {"aone": {"CPT": 12.0, "FLEX": 44.0}, "btwo": {"CPT": 1.0, "FLEX": 20.0}}
    result = g.grade_slate(players, labels)
    by_role = {m["role"]: m for m in result["models"]}
    assert [m["role"] for m in result["models"]] == ["shown", "ours", "shadow"]
    assert by_role["shown"]["family"] == g.FAMILY_STAT_API
    assert by_role["ours"]["family"] == g.FAMILY_LINEUP_MODEL
    assert by_role["shadow"]["family"] == g.FAMILY_FITTED_MODEL
    assert by_role["shown"]["flex"]["miss"] == 1.5 and by_role["ours"]["flex"]["miss"] == 7.0
    assert by_role["shadow"]["flex"]["miss"] == 10.0


def test_every_model_is_graded_on_the_same_players():
    players = [
        player("A One", 10.0, 40.0, shadow=(8.0, 35.0)),
        player("B Two", 2.0, 20.0, shadow=(None, None)),      # the comparison model has no number for him
        player("C Three", 1.0, 10.0, shadow=(1.0, 12.0)),     # no real-ownership label
        player("D Four", 5.0, 30.0, shadow=(5.0, 30.0), out=True),  # we had ruled him out
    ]
    labels = {"aone": {"CPT": 9.0, "FLEX": 41.0}, "btwo": {"CPT": 2.0, "FLEX": 22.0}, "dfour": {"CPT": 3.0, "FLEX": 18.0},
              "enobody": {"CPT": 0.0, "FLEX": 4.0}}
    result = g.grade_slate(players, labels)
    for model in result["models"]:
        assert model["flex"]["n"] == model["captain"]["n"] == 1, "only A One has a label and a number from both models"
    detail = result["detail"]
    assert detail["active_players"] == 3
    assert detail["ruled_out_but_drafted"] == [{"name": "D Four", "real_flex": 18.0}]
    assert detail["drafted_not_in_snapshot"] == 1


def test_a_slot_label_can_be_missing_for_one_slot_only():
    players = [player("A One", 10.0, 40.0), player("B Two", 2.0, 20.0)]
    labels = {"aone": {"FLEX": 30.0}, "btwo": {"CPT": 1.0, "FLEX": 26.0}}
    shown = g.grade_slate(players, labels)["models"][0]
    assert shown["captain"]["n"] == 1 and shown["flex"]["n"] == 2


def test_a_snapshot_without_a_captain_and_flex_split_has_nothing_to_grade():
    players = [{"name": "A One", "isOut": False, "ownership": {"total": 44.0, "flex": None, "captain": None, "source": "linestar"}},
               {"name": "B Two", "isOut": False, "ownership": 12.0}]
    assert g.snapshot_models(players)[1] == []
    assert g.grade_slate(players, {"aone": {"CPT": 1.0, "FLEX": 2.0}})["models"] == []


def test_a_repeated_name_keeps_its_first_row_and_worst_misses_are_capped():
    rows, models = g.snapshot_models([player("Same Name", 1.0, 2.0), player("Same  Name", 9.0, 9.0)])
    assert len(rows) == 1 and models[0]["values"]["samename"] == (1.0, 2.0)
    players = [player(f"P {letter}", 1.0, 10.0 * (i + 1)) for i, letter in enumerate("abcdef")]
    labels = {f"p{letter}": {"CPT": 0.0, "FLEX": 0.0} for letter in "abcdef"}
    worst = g.grade_slate(players, labels)["models"][0]["flex"]["worst"]
    assert [w["name"] for w in worst] == ["P f", "P e", "P d"] and len(worst) == g.WORST_KEPT


def test_whole_field_split_needs_both_slots_and_fills_zeros():
    full = {"aone": {"drafted_by_slot": {"CPT": 60.0, "FLEX": 300.0}}, "btwo": {"drafted_by_slot": {"CPT": 39.6, "FLEX": 198.0}}}
    assert g.slot_sums(full) == {"CPT": 99.6, "FLEX": 498.0}
    assert g.whole_field_split(full)
    short_flex = {"aone": {"drafted_by_slot": {"CPT": 100.0, "FLEX": 497.0}}}
    assert not g.whole_field_split(short_flex), "497 of 500 flex points is under 99.5%"
    assert not g.whole_field_split({"aone": {"drafted_by_slot": {"CPT": 99.4, "FLEX": 500.0}}})
    labels = g.whole_field_labels({"aone": {"drafted_by_slot": {"CPT": 60.0}}, "": {"drafted_by_slot": {"CPT": 1.0, "FLEX": 1.0}}},
                                  frozenset({"aone", "cthree"}))
    assert labels == {"aone": {"CPT": 60.0, "FLEX": 0.0}, "cthree": {"CPT": 0.0, "FLEX": 0.0}}


def test_labels_of_takes_only_usable_captain_and_flex_labels():
    record = prov.ContestRecord(contest_id="1", fmt="showdown", path="dk_export", full_standings=True,
                                ownership={"aone": {"drafted_pct": 50.0, "drafted_by_slot": {"CPT": 10.0, "FLEX": 40.0}}},
                                salary_pool=frozenset({"aone", "btwo"}))
    labels = job.labels_of(prov.classify_contest(record))
    assert labels == {"aone": {"CPT": 10.0, "FLEX": 40.0}, "btwo": {"CPT": 0.0, "FLEX": 0.0}}


def _slate(game, kickoff):
    return Slate(f"showdown|{game}|{kickoff.isoformat()}", "showdown", (game,), kickoff)


def test_a_contest_is_matched_to_its_slate_by_the_game_in_its_name():
    first = datetime(2026, 9, 14, 0, 20, tzinfo=timezone.utc)
    second = datetime(2026, 11, 13, 1, 15, tzinfo=timezone.utc)
    slates = [_slate("WAS@NYG", first), _slate("WAS@NYG", second), _slate("TB@CHI", second)]
    contests = [
        # The schedule spells Washington WSH, so the name match through the schedule never linked this one.
        {"contest_id": "1", "contest_name": "NFL Showdown $1.2M Thursday Night Showdown (WAS @ NYG)", "format": "showdown",
         "slate_key": None, "link": None, "imported_at": second + timedelta(hours=11)},
        {"contest_id": "2", "contest_name": "NFL Showdown (NYG vs WAS)", "format": "showdown", "slate_key": None, "link": None,
         "imported_at": first + timedelta(days=2)},
        {"contest_id": "3", "contest_name": "NFL Showdown (TB @ CHI)", "format": "showdown", "slate_key": None, "link": None,
         "imported_at": second + timedelta(days=30)},                      # imported too long after the game
        {"contest_id": "4", "contest_name": "NFL $2M Millionaire (WAS @ NYG)", "format": "classic", "slate_key": None,
         "link": None, "imported_at": second + timedelta(hours=11)},
        {"contest_id": "5", "contest_name": "NFL Showdown (TB @ CHI)", "format": "showdown", "slate_key": slates[2].key,
         "link": "slate_upload_id", "imported_at": second + timedelta(hours=11)},  # already attached; left alone
    ]
    assert job.attach_by_game(slates, contests) == 2
    assert [c["contest_id"] for c in slates[1].contests] == ["1"], "the later meeting, the one played before the import"
    assert [c["contest_id"] for c in slates[0].contests] == ["2"], "either order of the two teams"
    assert slates[2].contests == [] and contests[2]["slate_key"] is None and contests[3]["slate_key"] is None
    assert contests[0]["link"] == "matched_on_slate_game"


def test_input_key_changes_with_the_forecast_or_the_stored_contest():
    capture = {"source": "ghost_freeze", "id": "abc"}
    signature = [["1", 70588, True, 49, 599.98, 100.0, 0]]
    key = job.input_key(capture, signature)
    assert key == job.input_key(dict(capture), [list(signature[0])]) and len(key) == 32
    assert key != job.input_key({"source": "optimizer_run", "id": "abc"}, signature)
    assert key != job.input_key(capture, [["1", 70588, True, 50, 599.98, 100.0, 0]])
