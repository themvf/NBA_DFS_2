"""Label provenance classifier (model/nfl_ownership_provenance.py, scorecard section 3)."""

from model import nfl_ownership_provenance as prov
from model.nfl_ownership_provenance import ContestRecord, classify_contest, import_path, user_seats


def _labels(p):
    return {(l.normalized_name, l.slot): l for l in p.labels}


def _statapi_showdown(**kw):
    # 600% of mass: QB overall 90 (CPT 30), WR 100 (CPT 0 synthesized: no fetched captain),
    # TE seen only as captain (overall = CPT = 70), the rest fill flex.
    own = {
        "joeqb": {"drafted_pct": 90.0, "drafted_by_slot": {"CPT": 30.0, "FLEX": 60.0}},
        "flexonly": {"drafted_pct": 100.0, "drafted_by_slot": {"CPT": 0.0, "FLEX": 100.0}},
        "captainonly": {"drafted_pct": 70.0, "drafted_by_slot": {"CPT": 70.0, "FLEX": 0.0}},
        "filler": {"drafted_pct": 340.0, "drafted_by_slot": {"CPT": 0.0, "FLEX": 340.0}},
    }
    lineups = [{"roster": [["CPT", "Joe QB"], ["FLEX", "Flex Only"], ["FLEX", "Filler"], ["FLEX", "Joe QB"]]},
               {"roster": [["CPT", "Captain Only"], ["FLEX", "Flex Only"], ["FLEX", "Filler"], ["CPT", None]]}]
    captained, flex = user_seats(lineups)
    base = dict(contest_id="1", fmt="showdown", path="stat_api", full_standings=True, ownership=own,
                salary_pool=frozenset(own) | {"nobody"}, captained=frozenset(captained), flex_seen=frozenset(flex))
    base.update(kw)
    return ContestRecord(**base)


def test_user_seats_reads_slots_and_skips_nameless_seats():
    captained, flex = user_seats([{"roster": [["CPT", "Joe QB"], ["FLEX", "A.J. Brown"], ["CPT", None], []]}])
    assert captained == {"joeqb"}
    assert flex == {"ajbrown"}


def test_player_seen_only_as_flex_has_a_synthesized_captain_zero():
    p = classify_contest(_statapi_showdown())
    labels = _labels(p)
    cpt = labels[("flexonly", "CPT")]
    assert cpt.label_class == "synthesized_zero"
    assert cpt.label_pct is None and cpt.stored_pct == 0.0
    assert cpt.basis == "statapi_no_fetched_captain"
    assert labels[("flexonly", "overall")].label_class == "observed"
    assert labels[("flexonly", "FLEX")].label_class == "observed"
    assert labels[("flexonly", "FLEX")].basis == "statapi_overall_minus_unseen_captain"


def test_every_statapi_captain_zero_without_a_captain_seat_is_synthesized():
    p = classify_contest(_statapi_showdown())
    for l in p.labels:
        if l.slot == "CPT" and l.in_ownership_table and l.stored_pct == 0.0:
            assert l.label_class == "synthesized_zero", l


def test_captained_player_is_observed_on_every_slot():
    labels = _labels(classify_contest(_statapi_showdown()))
    assert labels[("joeqb", "CPT")].label_class == "observed"
    assert labels[("joeqb", "CPT")].label_pct == 30.0
    assert labels[("joeqb", "FLEX")].label_pct == 60.0


def test_player_seen_only_as_captain_has_no_overall_and_a_synthesized_flex():
    labels = _labels(classify_contest(_statapi_showdown()))
    assert labels[("captainonly", "overall")].label_class == "unresolved"
    assert labels[("captainonly", "CPT")].label_class == "observed"
    assert labels[("captainonly", "FLEX")].label_class == "synthesized_zero"


def test_missing_player_is_reconciled_only_with_mass_and_full_standings():
    complete = _labels(classify_contest(_statapi_showdown()))
    assert all(complete[("nobody", s)].label_class == "reconciled_zero" for s in ("overall", "CPT", "FLEX"))
    assert complete[("nobody", "overall")].label_pct == 0.0
    partial = _labels(classify_contest(_statapi_showdown(full_standings=False)))
    assert all(partial[("nobody", s)].label_class == "unresolved" for s in ("overall", "CPT", "FLEX"))


def test_mass_floor_is_a_parameter_and_week4_mass_falls_below_the_provisional_floor():
    own = {"a": {"drafted_pct": 895.44, "drafted_by_slot": {"QB": 895.44}}}
    rec = ContestRecord("196151357", "classic", "stat_api", True, own, salary_pool=frozenset({"a", "b"}))
    assert classify_contest(rec).complete is False                  # 0.99493 < 0.995
    assert _labels(classify_contest(rec))[("b", "overall")].label_class == "unresolved"
    assert classify_contest(rec, mass_floor=0.99).complete is True
    assert _labels(classify_contest(rec, mass_floor=0.99))[("b", "overall")].label_class == "reconciled_zero"


def test_dk_export_missing_slot_and_missing_player_are_reconciled_zeros():
    own = {"capt": {"drafted_pct": 20.0, "drafted_by_slot": {"CPT": 20.0}},
           "rounded": {"drafted_pct": 0.01, "drafted_by_slot": {"CPT": 0, "FLEX": 0.01}}}
    rec = ContestRecord("2", "showdown", "dk_export", False, own, salary_pool=frozenset({"capt", "rounded", "zed"}))
    labels = _labels(classify_contest(rec))
    assert labels[("capt", "FLEX")].label_class == "reconciled_zero"
    assert labels[("rounded", "CPT")].label_class == "observed"       # a printed 0.00% row
    assert labels[("zed", "CPT")].label_class == "reconciled_zero"


def test_dfsdb_is_flex_only_and_never_reconciles():
    own = {"a": {"drafted_pct": 40.0, "drafted_by_slot": {"FLEX": 40.0}}}
    rec = ContestRecord("dfsdb-x", "showdown", "dfsdb", False, own, salary_pool=frozenset({"a", "b"}))
    labels = _labels(classify_contest(rec))
    assert labels[("a", "FLEX")].label_class == "observed"
    assert labels[("a", "overall")].label_class == "unresolved"
    assert labels[("a", "CPT")].label_class == "unresolved"
    assert labels[("b", "FLEX")].label_class == "unresolved"


def test_no_salary_pool_labels_only_stored_rows():
    p = classify_contest(_statapi_showdown(salary_pool=None))
    assert {l.normalized_name for l in p.labels} == {"joeqb", "flexonly", "captainonly", "filler"}
    assert any("no linked salary pool" in n for n in p.notes)


def test_gate_classes_carry_a_value_and_the_others_do_not():
    for l in classify_contest(_statapi_showdown()).labels:
        assert (l.label_pct is not None) == (l.label_class in prov.GATE_CLASSES)


def test_nameless_row_is_unresolved_on_every_slot():
    own = {"": {"drafted_pct": 0.15, "drafted_by_slot": {"CPT": 0.15, "FLEX": 0.0}}}
    p = classify_contest(_statapi_showdown(ownership=own, salary_pool=None))
    assert {(l.slot, l.label_class) for l in p.labels} == {
        ("overall", "unresolved"), ("CPT", "unresolved"), ("FLEX", "unresolved")}


def test_import_path():
    assert import_path("196151357", "stat-api:20754235") == "stat_api"
    assert import_path("195648006", "contest-standings-195648006.csv") == "dk_export"
    assert import_path("dfsdb-abc", "https://www.dfsdb.com/contest/abc") == "dfsdb"
