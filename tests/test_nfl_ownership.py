import copy
import csv
import json

import numpy as np
import pytest

from model.nfl_ownership import (code_digest, digest, fit, forecast, seal, validate_snapshot, verify,
                                 walk_forward, write_artifact)
from research.nfl_ownership import import_labels, load_history, read_complete_field, write_forecast_csv


def snapshot(day=20, fmt="classic", slate=None):
    players = []
    for pos, count in (("QB", 2), ("RB", 4), ("WR", 6), ("TE", 3), ("DST", 2)):
        for i in range(count):
            players.append({"player_id": len(players) + 1, "name": f"{pos} Player {i}", "team": "AAA" if i % 2 else "BBB",
                            "position": pos, "salary": 7000 - 500 * i if pos != "DST" else 3000,
                            "projection": 25 - i * 3 if pos != "DST" else 8, "dk_average": 23 - i * 3,
                            "is_out": False, "role": "Expected starter" if i == 0 else None})
    return {"version": "test", "kind": "snapshot", "slate_id": slate or f"slate-{day}", "format": fmt,
            "captured_at": f"2026-09-{day:02}T12:00:00Z", "lock_at": f"2026-09-{day:02}T17:00:00Z",
            "source_digest": "test-source", "players": players}


def contest(day=20, fmt="classic", contest_id=None):
    s = snapshot(day, fmt)
    labels = []
    if fmt == "classic":
        for pos, total in (("QB", 100), ("RB", 240), ("WR", 350), ("TE", 110), ("DST", 100)):
            group = [p for p in s["players"] if p["position"] == pos]
            labels.extend({"player_id": p["player_id"], "slot": "OVERALL", "ownership_pct": total / len(group)} for p in group)
    else:
        for p in s["players"]:
            labels.extend({"player_id": p["player_id"], "slot": slot, "ownership_pct": total / len(s["players"])} for slot, total in (("CPT", 100), ("FLEX", 500)))
    return {"contest_id": contest_id or str(day), "snapshot": s, "labels_available_at": f"2026-09-{day + 1:02}T12:00:00Z",
            "source_digest": f"contest-{day}", "labels": labels, "flex_allocation": {"RB": .4, "WR": .5, "TE": .1}}


def test_future_labels_and_same_slate_are_excluded():
    late = contest(20)
    late["labels_available_at"] = "2026-09-28T00:00:00Z"
    m = fit([late], "2026-09-27T12:00:00Z")
    assert m["training_contests"] == []
    assert m["excluded_contests"][0]["contest_id"] == "20"
    m = fit([contest()], "2026-09-27T12:00:00Z")
    with pytest.raises(ValueError, match="training slate"):
        forecast(m, snapshot(28, slate="slate-20"), "2026-09-28T13:00:00Z")


def test_names_and_postgame_points_do_not_change_model_features():
    a = snapshot(28)
    b = copy.deepcopy(a)
    for p in b["players"]:
        p["name"] = "A newly drafted rookie"
        p["actual_points"] = 10000
        p["winning_lineup"] = True
    model = fit([contest()], "2026-09-27T12:00:00Z")
    first, second = [forecast(model, s, "2026-09-28T13:00:00Z") for s in (a, b)]
    assert [r["ownership_pct"] for r in first["players"]] == [r["ownership_pct"] for r in second["players"]]


def test_classic_roster_totals_salary_and_explanations():
    m = fit([contest()], "2026-09-27T12:00:00Z")
    f = forecast(m, snapshot(28), "2026-09-28T13:00:00Z")
    assert sum(p["ownership_pct"] for p in f["players"]) == pytest.approx(900)
    totals = {pos: sum(p["ownership_pct"] for p in f["players"] if p["position"] == pos) for pos in ("QB", "RB", "WR", "TE", "DST")}
    assert totals["QB"] == pytest.approx(100)
    assert totals["DST"] == pytest.approx(100)
    assert 200 <= totals["RB"] <= 300 and 300 <= totals["WR"] <= 400 and 100 <= totals["TE"] <= 200
    assert all(0 <= p["ownership_pct"] <= 100 + 1e-8 for p in f["players"])
    assert f["normalization"]["expected_salary"] <= 50000.001
    assert f["players"][0]["explanation"]["feature_contributions"]
    assert f["status"] == "experimental_uncalibrated"


def test_showdown_joint_player_cap_and_separate_slots():
    m = fit([contest(20, "showdown")], "2026-09-27T12:00:00Z")
    f = forecast(m, snapshot(28, "showdown"), "2026-09-28T13:00:00Z")
    assert sum(p["ownership_pct"] for p in f["players"] if p["slot"] == "CPT") == pytest.approx(100)
    assert sum(p["ownership_pct"] for p in f["players"] if p["slot"] == "FLEX") == pytest.approx(500)
    for pid in {p["player_id"] for p in f["players"]}:
        assert sum(p["ownership_pct"] for p in f["players"] if p["player_id"] == pid) <= 100.000001


def test_explicit_out_zero_but_unknown_features_receive_disclosed_estimate():
    m = fit([], "2026-09-27T12:00:00Z")
    s = snapshot(28)
    s["players"][2]["is_out"] = True
    s["players"][3]["projection"] = None
    f = forecast(m, s, "2026-09-28T13:00:00Z")
    out = next(p for p in f["players"] if p["player_id"] == s["players"][2]["player_id"])
    missing = next(p for p in f["players"] if p["player_id"] == s["players"][3]["player_id"])
    assert out["ownership_pct"] == 0 and out["method"] == "explicitly_out"
    assert missing["ownership_pct"] > 0 and missing["features"]["projection_missing"] == 1
    assert missing["method"] == "salary_prior_no_matching_history"


def test_walk_forward_groups_slates_and_waits_for_actual_label_availability():
    a, b, c = contest(20), contest(24), contest(28)
    b["labels_available_at"] = "2026-09-29T00:00:00Z"
    twin = copy.deepcopy(c)
    twin["contest_id"] = "second-contest-same-slate"
    twin["source_digest"] = "another-field-same-slate"
    report = walk_forward([a, b, c, twin])
    last = [f for f in report["folds"] if f["slate_id"] == "slate-28"]
    assert all(f["trained_contests"] == ["20"] for f in last)
    assert last[0]["status"] == "forward_scored"
    assert report["folds"][0]["status"] == "prior_only_no_earlier_same_format_history"


def test_timestamp_identity_and_artifact_guards(tmp_path):
    s = snapshot()
    s["captured_at"] = s["lock_at"]
    with pytest.raises(ValueError, match="before slate lock"):
        validate_snapshot(s)
    s = snapshot()
    s["players"].append(s["players"][0])
    with pytest.raises(ValueError, match="duplicate"):
        validate_snapshot(s)
    m = fit([contest()], "2026-09-27T12:00:00Z")
    first = write_artifact(tmp_path, "model", m)
    assert write_artifact(tmp_path, "model", m) == first
    changed = copy.deepcopy(m)
    changed["as_of"] = "2026-09-26T00:00:00Z"
    with pytest.raises(ValueError, match="digest"):
        verify(changed)
    first.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="overwrite"):
        write_artifact(tmp_path, "model", m)


def field_file(path, incomplete=False):
    lineup = "QB QB Player 0 RB RB Player 0 RB RB Player 1 WR WR Player 0 WR WR Player 1 WR WR Player 2 TE TE Player 0 FLEX RB Player 2 DST DST Player 0"
    # Player fixture names must not themselves contain DK slot tokens.
    lineup = lineup.replace("QB Player", "Quarterback").replace("RB Player", "Runner").replace("WR Player", "Receiver").replace("TE Player", "Tightend").replace("DST Player", "Defense")
    with path.open("w", newline="", encoding="utf-8") as h:
        w = csv.writer(h)
        w.writerow(["EntryId", "EntryName", "Lineup", "Player", "Roster Position", "%Drafted", "FPTS"])
        w.writerow(["1", "private entrant", lineup if not incomplete else "QB Quarterback 0", "Runner 2", "FLEX", "100%", "999"])
    s = snapshot()
    for p in s["players"]:
        p["name"] = p["name"].replace("QB Player", "Quarterback").replace("RB Player", "Runner").replace("WR Player", "Receiver").replace("TE Player", "Tightend").replace("DST Player", "Defense")
    return s


def test_complete_field_recount_aggregates_classic_flex_and_observes_zeros(tmp_path):
    path = tmp_path / "standings.csv"
    s = field_file(path)
    labels, n, recount = read_complete_field(path, s, 1)
    assert recount["complete_lineups"] == 1
    imported = import_labels(s, labels, "one", "2026-09-21T12:00:00Z", "raw-hash", n)
    third_rb = next(p for p in s["players"] if p["name"] == "Runner 2")
    ownership = {r["player_id"]: r["ownership_pct"] for r in imported["labels"]}
    assert ownership[third_rb["player_id"]] == 100
    assert sum(ownership.values()) == 900
    assert any(v == 0 for v in ownership.values())
    assert "private entrant" not in json.dumps(imported)
    assert "999" not in json.dumps(imported)
    with pytest.raises(ValueError, match="incomplete"):
        read_complete_field(path, s, 2)
    field_file(path, incomplete=True)
    with pytest.raises(ValueError, match="Incomplete"):
        read_complete_field(path, s, 1)


def test_history_import_is_idempotent_and_conflicts_are_rejected(tmp_path):
    a = seal({"contests": [contest()]})
    p = write_artifact(tmp_path, "history", a)
    assert len(load_history([p, p])) == 1
    b = copy.deepcopy(a["contests"][0])
    b["labels"][0]["ownership_pct"] = 0
    q = write_artifact(tmp_path, "history", seal({"contests": [b]}))
    with pytest.raises(ValueError, match="Conflicting"):
        load_history([p, q])


def test_impossible_pool_fails_instead_of_fabricating_roster_mass():
    m = fit([], "2026-09-27T12:00:00Z")
    s = snapshot(28)
    s["players"] = [p for p in s["players"] if p["position"] != "QB"]
    with pytest.raises(ValueError, match="mandatory"):
        forecast(m, s, "2026-09-28T13:00:00Z")


def test_empty_entries_are_audited_and_conditional_labels_are_not_diluted(tmp_path):
    path = tmp_path / "standings.csv"
    s = field_file(path)
    with path.open(newline="", encoding="utf-8") as h:
        rows = list(csv.reader(h))
    rows[1][5] = "50%"
    rows.append(["2", "empty entrant", "", "", "", "", ""])
    with path.open("w", newline="", encoding="utf-8") as h:
        csv.writer(h).writerows(rows)
    labels, entries, audit = read_complete_field(path, s, 2)
    assert entries == 2 and audit["complete_lineups"] == 1 and audit["empty_entries"] == 1
    assert sum(r["observed_ownership_pct"] for r in labels) == 900
    assert audit["max_published_difference_pp"] == 0


def test_csv_preserves_exact_forecast_and_implementation_change_requires_refit(tmp_path):
    m = fit([contest()], "2026-09-27T12:00:00Z")
    f = forecast(m, snapshot(28), "2026-09-28T13:00:00Z")
    path = write_forecast_csv(tmp_path, f)
    assert write_forecast_csv(tmp_path, f) == path
    with path.open(newline="", encoding="utf-8") as h:
        rows = list(csv.DictReader(h))
    assert len(rows) == len(f["players"])
    assert all(r["forecast_digest"] == f["artifact_digest"] for r in rows)
    assert sum(float(r["ownership_pct"]) for r in rows) == pytest.approx(900)
    changed = {**m, "implementation_digest": "changed"}
    changed.pop("artifact_digest")
    with pytest.raises(ValueError, match="refit"):
        forecast(seal(changed), snapshot(28), "2026-09-28T13:00:00Z")


def test_cli_does_not_backdate_a_new_forecast_into_a_locked_slate(tmp_path, monkeypatch):
    from research.nfl_ownership import main
    m = write_artifact(tmp_path, "model", fit([contest()], "2026-09-27T12:00:00Z"))
    s = write_artifact(tmp_path, "snapshot", seal(snapshot(28)))
    monkeypatch.setattr("sys.argv", ["nfl_ownership", "forecast", "--model", str(m), "--snapshot", str(s),
                                   "--as-of", "2026-09-28T13:00:00Z", "--output-dir", str(tmp_path)])
    with pytest.raises(ValueError, match="after lock"):
        main()


def test_duplicate_source_and_disagreeing_published_ownership_fail(tmp_path):
    duplicate = copy.deepcopy(contest())
    duplicate["contest_id"] = "same-export-renamed"
    with pytest.raises(ValueError, match="Duplicate contest source"):
        fit([contest(), duplicate], "2026-09-27T12:00:00Z")
    path = tmp_path / "standings.csv"
    s = field_file(path)
    path.write_text(path.read_text().replace("100%", "10%"), encoding="utf-8")
    with pytest.raises(ValueError, match="disagrees"):
        read_complete_field(path, s, 1)


def test_implementation_digest_survives_git_line_ending_conversion(tmp_path):
    lf, crlf = tmp_path / "lf.py", tmp_path / "crlf.py"
    lf.write_bytes(b"a = 1\nb = 2\n")
    crlf.write_bytes(b"a = 1\r\nb = 2\r\n")
    assert code_digest(lf) == code_digest(crlf)
