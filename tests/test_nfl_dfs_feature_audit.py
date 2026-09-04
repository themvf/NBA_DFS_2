from copy import deepcopy
import pytest
from model.nfl_dfs_feature_audit import build_audit, normalize, digest, numeric

NOW = "2026-09-04T12:00:00+00:00"


def row(**overrides):
    return {"record_id": "1", "identity": "p1", "season": 2025, "week": 1, "team": "BUF", "opponent": "NYJ",
            "position": "QB", "source": "test", "captured_at": NOW, "position_basis": "payload",
            "values": {"attempts": 0, "carries": 1, "completions": 0, "passing_yards": -2, "passing_tds": 0}, **overrides}


def audit(rows):
    return build_audit({"working_source": rows}, NOW, "study")["datasets"][0]


def cell(report, field="Workload:attempts"):
    return next(c for c in report["cells"] if c["field_id"] == field and c["position"] == "QB")


def test_zero_is_valid_and_yards_can_be_negative():
    result = audit([row()])
    assert cell(result)["zero"] == 1
    assert result["cohorts"][0]["complete"] == 1
    assert cell(result)["status"] == "retrospective_only"


@pytest.mark.parametrize("value", [True, "3", -1, 1.5, float("nan"), float("inf")])
def test_counts_reject_bad_values(value):
    assert not numeric(value, "count")


def test_missing_and_invalid_are_separate():
    result = audit([row(values={}), row(record_id="2", identity="p2", values={"attempts": -1})])
    assert (cell(result)["n"], cell(result)["missing"], cell(result)["invalid"]) == (2, 1, 1)


def test_exclusions_reconcile_and_do_not_inflate_denominator():
    rows = [row(), row(record_id="2"), row(record_id="3", identity=None), row(record_id="4", position="K"),
            row(record_id="5", captured_at="2026-09-05T00:00:00+00:00")]
    result = audit(rows)
    assert result["scanned"] == result["eligible"] + sum(result["excluded"].values()) == 5
    assert cell(result)["n"] == 1


def test_unsupported_field_cannot_become_approved_from_presence():
    result = audit([row(values={"routes": 25})])
    c = cell(result, "Deferred:routes")
    assert (c["status"], c["present"], c["valid"]) == ("unsupported", 1, 0)


def test_missing_timestamp_does_not_become_pregame():
    result = audit([row(captured_at=None)])
    assert cell(result)["captured"] == 0
    assert cell(result)["status"] == "retrospective_only"


def test_team_payload_not_wrapper_or_player_sum():
    raw = {"id": 1, "position": "DST", "season": 2025, "week": 1, "team": "BUF", "opponent": "NYJ",
           "source_row": {"sacks": 100, "raw_team_stats": {"attempts": 30, "def_sacks": 2}}}
    normalized = normalize(raw, "working_source")
    assert normalized["values"] == {"attempts": 30, "def_sacks": 2}
    assert normalized["identity"] == "DST:BUF"


def test_frozen_history_does_not_inherit_working_availability():
    r = normalize({"row_key": "abc", "payload": {"position": "QB", "player_gsis_id": "p1", "stats": {"attempts": 4}}}, "frozen_history")
    assert r["captured_at"] is None
    assert r["position_basis"] == "payload"


def test_audit_replay_is_deterministic_and_revision_changes_digest():
    rows = [row()]
    before = deepcopy(rows)
    report = audit(rows)
    assert rows == before
    assert digest(report) == digest(audit(rows))
    rows[0]["values"]["attempts"] = 3
    assert digest(report) != digest(audit(rows))


def test_empty_dataset_is_not_zero_percent_coverage():
    result = audit([])
    assert result["cells"] == []
    assert result["eligible"] == 0


def test_datasets_stay_separate():
    report = build_audit({"working_source": [row()], "frozen_history": [row()]}, NOW, "study")
    assert len(report["datasets"]) == 2
    assert [d["scanned"] for d in report["datasets"]] == [1, 1]


def test_naive_evaluation_time_rejected():
    with pytest.raises(ValueError):
        build_audit({}, "2026-09-04", "study")


def test_legacy_default_warning_is_saved():
    report = build_audit({"frozen_history": [row()]}, NOW, "study")
    assert "missing-to-zero" in report["datasets"][0]["normalization_warning"]


@pytest.mark.parametrize("payload", [[], {"raw_team_stats": None}, {"raw_team_stats": []}])
def test_malformed_payload_does_not_invent_values(payload):
    normalized = normalize({"id": 1, "position": "QB", "source_row": payload}, "working_source")
    assert normalized["values"] == {}
