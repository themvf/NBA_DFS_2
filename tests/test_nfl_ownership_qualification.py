from copy import deepcopy
import pytest
from model.nfl_ownership_qualification import qualify, grade_pairs
from tests.test_nfl_ownership import contest


def test_no_training_and_same_slate_never_count_as_holdouts():
    history = [contest(20), contest(20,contest_id="second")]
    result = qualify(history)
    assert result["formats"]["classic"]["heldOutSlateIds"] == []
    assert result["formats"]["classic"]["leverageEnabled"] is False
    assert len(result["excluded"]) == 2


def test_contests_on_same_slate_add_no_independent_units():
    first=contest(20)
    following=contest(22)
    extra=deepcopy(following); extra["contest_id"]="second"
    report=qualify([first,following,extra])
    assert report["formats"]["classic"]["heldOutSlateIds"] == ["slate-22"]
    assert report["formats"]["classic"]["status"] == "withheld"
    assert all(f["trainedContests"] == ["20"] for f in report["folds"])


def test_future_labels_never_train_an_earlier_decision():
    first=contest(20); first["labels_available_at"]="2026-09-28T00:00:00Z"
    assert qualify([first,contest(22)])["formats"]["classic"]["heldOutSlateIds"] == []


def test_partial_labels_are_not_zero_or_complete_field():
    later=contest(22); later["labels"]=later["labels"][:2]
    report=qualify([contest(20),later])
    assert not report["folds"]
    assert "coverage" in report["excluded"][-1]["reason"]


def test_rank_ties_and_empty_pairs_are_explicit():
    metrics=grade_pairs([(1,1),(1,1),(2,2),(3,3)])
    assert metrics["spearman"] == pytest.approx(1)
    assert metrics["maePp"] == 0
    assert grade_pairs([(1,1),(1,1)])["spearman"] is None
    assert grade_pairs([])["maePp"] is None


def test_showdown_cannot_inherit_classic_gate():
    report=qualify([contest(20,"showdown"),contest(22,"showdown")])
    assert report["formats"]["showdown"]["leverageEnabled"] is False
    assert "own registered" in report["formats"]["showdown"]["reasons"][0]


def test_duplicate_contest_rejected():
    with pytest.raises(ValueError,match="Duplicate"): qualify([contest(20),contest(20)])


def test_four_independent_accurate_holdouts_are_required(monkeypatch):
    import model.nfl_ownership_qualification as module
    history=[contest(day) for day in range(20,25)]
    by_slate={c['snapshot']['slate_id']:c for c in history}
    # Gate mechanics only: controlled perfect predictions, not model evidence.
    monkeypatch.setattr(module,'forecast',lambda model,snapshot,cutoff:{'players':deepcopy(by_slate[snapshot['slate_id']]['labels'])})
    assert qualify(history)['formats']['classic']['leverageEnabled'] is True
    assert qualify(history[:-1])['formats']['classic']['leverageEnabled'] is False
    monkeypatch.setattr(module,'forecast',lambda model,snapshot,cutoff:{'players':[{**r,'ownership_pct':r['ownership_pct']+3} for r in by_slate[snapshot['slate_id']]['labels']]})
    assert qualify(history)['formats']['classic']['leverageEnabled'] is False
