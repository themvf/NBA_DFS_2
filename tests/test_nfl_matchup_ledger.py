"""The context grader and the freeze check read a slim ledger, and grade it exactly as the whole one.

2026-11-12: `ledger_inputs` selected `p.*` for every stored forecast of the
season. One grading pass sent 247 MB and each pipeline-health run 206 MB,
about 4 GB a day between them. These tests hold the replacement to two
things: it never loads a payload it does not grade, and what it does load
grades identically.
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from model.nfl_dfs_context_variant_study import evaluate_context_variants, freeze_health, selected_records
from model.nfl_matchup_study import digest
from research import nfl_matchup_study as study

START = datetime(2026, 10, 4, 17, tzinfo=timezone.utc)
NOW = START + timedelta(weeks=10)
PIN, OTHER = "pin", "older-pin"
VARIANTS = {"version": "nfl-dfs-context-variants-v1", "env_baseline": {"mean": 10, "p10": 0, "p90": 20},
            "opp_carries": {"mean": 12, "p10": 0, "p90": 20, "inputs": {"allowed": 31.5}}}


def full_ledger():
    """Stored rows as the database holds them: two captures per player-week, plus another pin's rows."""
    rows = []
    for week in range(1, 9):
        kick = START + timedelta(weeks=week - 1)
        for position in ("QB", "RB", "WR", "TE"):
            for player in range(15):
                for hours in (30, 1):
                    rows.append({"id": len(rows) + 1, "study_run_id": PIN, "season": 2026, "week": week,
                        "player_id": f"{position}:{player}", "captured_at": kick - timedelta(hours=hours), "kickoff": kick,
                        "input_digest": f"d{len(rows) + 1}",
                        "payload": {"position": position, "team": "BUF", "history_games": 20, "boom_threshold": 25,
                                    "context_variants": VARIANTS, "stat_means": {"receptions": 5}, "baseline_config": {"k": 1}},
                        "outcome": {"actual": 12, "scoring_status": "exact", "scoring_version": "nfl-dk-realized-v2"}})
    for player in range(7):
        rows.append({"id": len(rows) + 1, "study_run_id": OTHER, "season": 2026, "week": 1, "player_id": f"WR:{player}",
            "captured_at": START - timedelta(hours=2), "kickoff": START, "input_digest": f"d{len(rows) + 1}",
            "payload": {"position": "WR", "context_variants": VARIANTS}, "outcome": None})
    return rows


GAMES = [{"id": week, "season": 2026, "week": week, "kickoff": START + timedelta(weeks=week - 1), "completed": True,
          "home_team": "BUF", "away_team": "MIA"} for week in range(1, 9)]


class Database:
    """Answers the grader's queries from `full_ledger`, the way the SQL would."""

    def __init__(self, rows, output_digest="registered"):
        self.rows, self.output_digest, self.statements = rows, output_digest, []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if "nfl_dfs_research_runs" in sql:
            return [{"report": {"output_digest": self.output_digest}}]
        if "FROM nfl_season_games g" in sql and "nfl_dfs_shadow_predictions" not in sql:
            return [dict(game) for game in GAMES]
        if "GROUP BY p.study_run_id" in sql:
            season, now = params
            by_study = {}
            for row in self.rows:
                if row["season"] == season and row["captured_at"] <= now:
                    by_study.setdefault(row["study_run_id"], []).append(row)
            return [{"study_run_id": key, "n": len(group), "digest": digest([(r["id"], r["input_digest"]) for r in group])}
                    for key, group in sorted(by_study.items())]
        if "jsonb_object_agg" in sql:
            wanted, keys = {int(value) for value in params["ids"].strip("{}").split(",")}, set(params["keys"])
            return [{"id": row["id"], "payload": {k: v for k, v in row["payload"].items() if k in keys},
                     "outcome": row["outcome"] if "LATERAL" in sql else None}
                    for row in self.rows if row["id"] in wanted]
        if "FROM nfl_dfs_shadow_predictions p" in sql:
            pin, season, now, week, _ = params
            return [{k: row[k] for k in ("id", "player_id", "season", "week", "captured_at", "kickoff")}
                    for row in self.rows if row["study_run_id"] == pin and row["season"] == season
                    and row["captured_at"] <= now and (week is None or row["week"] == week)]
        raise AssertionError(f"unexpected query: {sql[:80]}")


def test_only_the_selected_forecasts_carry_a_payload():
    rows = full_ledger()
    db = Database(rows)
    ledger = study.context_ledger(db, 2026, NOW, PIN)
    selected, rejected = selected_records(ledger, PIN, NOW)
    assert len(ledger) == 8 * 4 * 15 * 2 and len(selected) == 8 * 4 * 15 and rejected == {}
    assert all("payload" in row and "outcome" in row for row in selected)
    assert sum("payload" in row for row in ledger) == len(selected)
    assert {row["study_run_id"] for row in ledger} == {PIN}
    details = next(params for sql, params in db.statements if "jsonb_object_agg" in sql)
    assert isinstance(details["ids"], str)  # one array literal, not a list written out element by element
    assert sorted(int(value) for value in details["ids"].strip("{}").split(",")) == sorted(row["id"] for row in selected)
    assert tuple(details["keys"]) == study.CONTEXT_PAYLOAD_KEYS
    # The selected capture is the one an hour before kickoff, never the earlier one.
    assert all(row["kickoff"] - row["captured_at"] == timedelta(hours=1) for row in selected)
    assert all(set(row["payload"]) <= set(study.CONTEXT_PAYLOAD_KEYS) for row in selected)


def test_no_statement_selects_whole_forecast_rows():
    db = Database(full_ledger())
    study.context_report(db, 2026, NOW, {"study_run_id": PIN, "output_digest": "registered"})
    for sql, _ in db.statements:
        assert "p.*" not in sql and "SELECT *" not in sql, sql[:120]
    assert not hasattr(study, "ledger_inputs")


class Recording(dict):
    """A payload that remembers which keys were asked of it."""
    seen = set()

    def get(self, key, default=None):
        Recording.seen.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        Recording.seen.add(key)
        return super().__getitem__(key)


def test_the_graders_read_no_payload_key_outside_the_list():
    Recording.seen = set()
    rows = [{**row, "payload": Recording(row["payload"])} for row in full_ledger()]
    complete = [(2026, week) for week in range(1, 9)]
    evaluate_context_variants(rows, PIN, NOW, complete_weeks=complete)
    freeze_health(GAMES, rows, PIN, NOW)
    assert Recording.seen and Recording.seen <= set(study.CONTEXT_PAYLOAD_KEYS), Recording.seen - set(study.CONTEXT_PAYLOAD_KEYS)


def test_context_report_grades_exactly_as_the_whole_ledger_did():
    rows = full_ledger()
    config = {"study_run_id": PIN, "output_digest": "registered"}
    report = study.context_report(Database(rows), 2026, NOW, config)
    compatibility = json.loads(Path("research/nfl_dfs_context_scoring_compatibility.json").read_text())
    whole = evaluate_context_variants(rows, PIN, NOW, complete_weeks=study.complete_weeks(GAMES, NOW),
                                      outcome_versions=compatibility["versions"])
    for key, value in whole.items():
        assert report[key] == value, key
    assert report["rejected"] == {"other_study_pin": 7}
    assert report["all_study_pins_present"] == sorted([PIN, OTHER])
    assert report["freeze_health"] == freeze_health(GAMES, rows, PIN, NOW)
    assert report["dataset_digest_basis"] == study.DATASET_DIGEST_BASIS


def test_dataset_digest_changes_with_any_stored_forecast_or_graded_outcome():
    config = {"study_run_id": PIN, "output_digest": "registered"}
    base = study.context_report(Database(full_ledger()), 2026, NOW, config)["dataset_digest"]
    assert study.context_report(Database(full_ledger()), 2026, NOW, config)["dataset_digest"] == base
    superseded = full_ledger()
    superseded[0]["input_digest"] = "edited"  # a capture the grader does not select
    assert study.context_report(Database(superseded), 2026, NOW, config)["dataset_digest"] != base
    other = full_ledger()
    other[-1]["input_digest"] = "edited"  # another pin's forecast
    assert study.context_report(Database(other), 2026, NOW, config)["dataset_digest"] != base
    corrected = full_ledger()
    corrected[1]["outcome"] = {**corrected[1]["outcome"], "actual": 30}
    assert study.context_report(Database(corrected), 2026, NOW, config)["dataset_digest"] != base


def test_a_changed_pin_is_still_refused():
    try:
        study.context_report(Database(full_ledger(), output_digest="something-else"), 2026, NOW,
                             {"study_run_id": PIN, "output_digest": "registered"})
    except ValueError as error:
        assert "pin" in str(error)
    else:
        raise AssertionError("a pin whose registered output digest differs must be refused")


def test_health_check_loads_one_week_and_no_outcomes(monkeypatch):
    from model import pipeline_health

    rows = full_ledger()
    db = Database(rows)
    pin = json.loads(Path("artifacts/nfl_dfs_shadow_config.json").read_text())["study_run_id"]
    for row in rows:
        if row["study_run_id"] == PIN:
            row["study_run_id"] = pin
    now = START + timedelta(weeks=2, hours=-2)  # week 3 is the next to kick off
    result = pipeline_health.check_nfl_context_freezes(db, now)[0]
    assert result.status == pipeline_health.FRESH, result.detail
    assert result.detail.startswith("week 3: healthy; 60 eligible player-weeks")
    index = next(params for sql, params in db.statements if "FROM nfl_dfs_shadow_predictions p" in sql and "jsonb_object_agg" not in sql)
    assert index[3] == 3
    details = next(sql for sql, _ in db.statements if "jsonb_object_agg" in sql)
    assert "LATERAL" not in details and "nfl_dfs_player_week_results" not in details
