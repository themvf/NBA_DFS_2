from datetime import datetime, timedelta, timezone

from model.nfl_dfs_context_variant_study import evaluate_context_variants, freeze_health, context_forecasts

START = datetime(2026, 10, 4, 17, tzinfo=timezone.utc)
NOW = START+timedelta(weeks=10)


def ledger(weeks=8, wr_harm=False):
    records = []
    for week in range(1, weeks+1):
        for position in ("QB", "RB", "WR", "TE"):
            for player in range(15):
                kick = START+timedelta(weeks=week-1)
                mean = 12 if position == "RB" else 8 if wr_harm and position == "WR" else 10
                variants = {"version": "nfl-dfs-context-variants-v1", "env_baseline": {"mean": 10, "p10": 0, "p90": 20},
                            "opp_carries": {"mean": mean, "p10": 0, "p90": 20}}
                records.append({"id": len(records)+1, "study_run_id": "pin", "season": 2026, "week": week,
                    "player_id": f"{position}:{player}", "captured_at": kick-timedelta(hours=1), "kickoff": kick,
                    "payload": {"position": position, "team": "BUF", "history_games": 20, "context_variants": variants},
                    "outcome": {"actual": 12, "scoring_status": "exact", "scoring_version": "nfl-dk-realized-v2"}})
    return records


def evaluate(rows):
    return evaluate_context_variants(rows, "pin", NOW, complete_weeks=[(2026, i) for i in range(1, 9)])


def test_registered_opp_gate_pass_and_no_promotion():
    result = evaluate(ledger())
    assert result["studies"]["opp_carries"]["verdict"] == "PASS"
    assert not result["production_promotion"]


def test_wr_harm_kills_even_when_rb_improves():
    assert evaluate(ledger(wr_harm=True))["studies"]["opp_carries"]["verdict"] == "FAIL"


def test_floor_and_missing_position_prevent_passing():
    assert evaluate(ledger(7))["studies"]["opp_carries"]["verdict"] == "NO_VERDICT"
    assert evaluate([r for r in ledger() if r["payload"]["position"] != "QB"])["studies"]["opp_carries"]["verdict"] == "NO_VERDICT"


def test_dedup_pin_pregame_and_missing_latest_bundle():
    rows = ledger()
    base = rows[0]
    rows.extend([{**base, "id": 10000, "study_run_id": "different"},
                 {**base, "id": 10001, "captured_at": base["kickoff"]},
                 {**base, "id": 10002, "captured_at": base["kickoff"]-timedelta(minutes=1), "payload": {**base["payload"], "context_variants": None}}])
    result = evaluate(rows)
    assert result["selected_player_weeks"] == 480
    assert result["coverage"]["opp_carries"] == 479
    assert result["rejected"] == {"other_study_pin": 1, "not_available_pregame": 1}


def test_incomplete_week_is_not_a_scorable_forward_week():
    result = evaluate_context_variants(ledger(), "pin", NOW, complete_weeks=[(2026, i) for i in range(1, 8)])
    assert result["studies"]["opp_carries"]["verdict"] == "NO_VERDICT"


def test_missing_context_on_saturday_deadline_is_health_failure():
    games = [{"id": 1, "season": 2026, "week": 1, "kickoff": START, "home_team": "BUF", "away_team": "MIA"}]
    assert freeze_health(games, [], "pin", START-timedelta(days=2))[0]["status"] == "pending"
    assert freeze_health(games, [], "pin", START-timedelta(hours=18))[0]["status"] == "failure"
    assert freeze_health(games, ledger(1), "pin", START-timedelta(minutes=10))[0]["status"] == "healthy"


def test_context_adapter_does_not_make_up_missing_median():
    row = ledger(1)[0]
    adapted = context_forecasts({"forecast_id": "f", "run_id": "pin"}, row["payload"])
    assert adapted[0]["variant"] == "context:env_baseline"
    assert adapted[0]["median"] is None


def test_interval_study_cannot_pass_after_changing_a_shallow_player_mean():
    rows = ledger()
    for row in rows:
        payload = row["payload"]
        payload["boom_threshold"] = 30
        variants = payload["context_variants"]
        variants["env_baseline"].update(p10=0, p90=11, boom_probability=0.)
        variants["interval_rq"] = {"mean":10,"p10":0,"p90":20,"boom_probability":0.}
    assert evaluate(rows)["studies"]["interval_rq"]["verdict"] == "PASS"
    rows[0]["payload"]["history_games"] = 4
    rows[0]["payload"]["context_variants"]["interval_rq"]["mean"] = 11
    assert evaluate(rows)["studies"]["interval_rq"]["verdict"] == "FAIL"


def test_outcome_compatibility_keeps_skill_gate_and_rejects_changed_or_unknown_scoring():
    records = ledger()
    for row in records:
        row["outcome"].update(scoring_version="nfl-dk-realized-v3", computed_at=row["kickoff"]+timedelta(hours=5))
    assert evaluate(records)["studies"]["opp_carries"]["verdict"] == "NO_VERDICT"
    versions = {"nfl-dk-realized-v3": {"positions": ["QB", "RB", "WR", "TE"], "effective_at": START-timedelta(days=1)}}
    result = evaluate_context_variants(records,"pin",NOW,complete_weeks=[(2026,i) for i in range(1,9)],outcome_versions=versions)
    assert result["studies"]["opp_carries"]["verdict"] == "PASS"
    assert result["outcome_version_counts"] == {"nfl-dk-realized-v3": 480}
    records[0]["payload"]["position"] = "DST"
    records[1]["outcome"]["scoring_version"] = "unknown"
    result = evaluate_context_variants(records,"pin",NOW,complete_weeks=[(2026,i) for i in range(1,9)],outcome_versions=versions)
    assert result["coverage"]["incompatible_context_outcome_version"] == 2
