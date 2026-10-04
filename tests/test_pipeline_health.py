"""Pipeline health: does every scheduled job still write?

The checks that matter here are the refusals to cry wolf and the refusal to
stay quiet. A monitor that flags a dormant sport gets ignored within a week; a
monitor that misses a dead pipeline is the reason this file exists at all.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import model.pipeline_health as P


NOW = datetime(2026, 9, 19, 21, 0, tzinfo=timezone.utc)   # September: NFL + MLB in season


def ds(**over) -> P.Dataset:
    base = dict(key="k", label="L", table="t", timestamp_column="c",
                max_age_hours=36, owner_workflow="w.yml", season_months=(), note="")
    base.update(over)
    return P.Dataset(**base)


def test_the_registry_is_coherent() -> None:
    keys = [d.key for d in P.DATASET_REGISTRY]
    assert len(keys) == len(set(keys)), "dataset keys must be unique"
    assert P.CHECK_VERSION == "pipeline-health-v3"
    for d in P.DATASET_REGISTRY:
        assert d.max_age_hours > 0
        assert d.owner_workflow.endswith(".yml"), d.key
        assert all(1 <= m <= 12 for m in d.season_months), d.key
        # A filter is a WHERE fragment, never a statement of its own.
        assert ";" not in d.filter and "--" not in d.filter, d.key
        assert (d.min_rows > 0) == (d.window_hours > 0), d.key


def test_every_owner_is_a_scheduled_or_follower_workflow() -> None:
    """The row links to the job that writes the table; a manual-only or missing
    workflow there would send the reader to the wrong place."""
    from scripts.build_workflow_manifest import build
    by_file = {w["file"]: w for w in build()["workflows"]}
    for d in P.DATASET_REGISTRY:
        w = by_file.get(d.owner_workflow)
        assert w is not None, f"{d.key}: {d.owner_workflow} is not in .github/workflows"
        assert w["crons"] or w["afterWorkflows"], f"{d.key}: {d.owner_workflow} has no schedule"


def test_shared_tables_are_watched_per_sport() -> None:
    """One MAX over a table shared by five sports hides one sport dying while
    another keeps writing. Each in-scope sport gets its own filtered row."""
    by_table: dict[str, set[str]] = {}
    for d in P.DATASET_REGISTRY:
        by_table.setdefault(d.table, set()).add(d.filter)
    for table in ("game_odds_history", "event_closing_lines", "line_alerts"):
        sports = {f for f in by_table[table] if f.startswith("sport = ")}
        assert {"sport = 'mlb'", "sport = 'nfl'", "sport = 'nhl'"} <= sports, table
    assert "sport = 'cfb'" in by_table["game_odds_history"]


def test_mlb_stats_are_watched_on_the_append_only_history_tables() -> None:
    """mlb_team_stats.fetched_at froze on 2026-04-06 (the FanGraphs path 403s
    and the fallback writes only the snapshot) while the daily run stayed
    green; the current-state tables cannot say whether the job ran."""
    tables = {d.table: d for d in P.DATASET_REGISTRY}
    assert "mlb_team_stats" not in tables and "mlb_pitcher_stats" not in tables
    for key in ("mlb_team_stats_history", "mlb_pitcher_stats_history"):
        assert tables[key].timestamp_column == "available_at"
        assert tables[key].owner_workflow == "refresh_mlb_stats.yml"


def test_a_projection_run_row_is_not_the_only_mark_of_the_projection_workflow() -> None:
    """nfl_dfs_projection_runs is also written hourly by the availability job,
    so a dead projection workflow would still read fresh there."""
    by_key = {d.key: d for d in P.DATASET_REGISTRY}
    own = by_key["nfl_matchup_forecasts"]
    assert own.table == "nfl_fact_releases" and own.filter == "dataset_key = 'nfl_matchup'"
    assert own.owner_workflow == "refresh_nfl_dfs_projections.yml"


# --- Per-sport filters and windowed floors (v3) ----------------------------


def test_min_rows_and_window_hours_go_together() -> None:
    with pytest.raises(ValueError):
        ds(min_rows=5)
    with pytest.raises(ValueError):
        ds(window_hours=24)


def test_freshness_sql_applies_the_filter_and_the_window() -> None:
    plain = P.freshness_sql(ds(table="t", timestamp_column="c"))
    assert plain == "SELECT MAX(c) AS last_at FROM t"
    filtered = P.freshness_sql(ds(table="t", timestamp_column="c", filter="sport = 'mlb'", min_rows=3, window_hours=24))
    assert filtered == ("SELECT MAX(c) AS last_at, COUNT(*) FILTER (WHERE c > NOW() - INTERVAL '24 hours') AS in_window "
                        "FROM t WHERE sport = 'mlb'")


def test_freshness_sql_doubles_percent_for_psycopg2() -> None:
    """cursor.execute(sql, ()) still scans for % placeholders; the first live
    run of a LIKE filter failed with 'tuple index out of range'."""
    sql = P.freshness_sql(ds(table="t", timestamp_column="c", filter="dataset LIKE 'players-live-%'"))
    assert sql.endswith("WHERE dataset LIKE 'players-live-%%'")


def test_too_few_rows_in_the_window_is_stale_even_when_the_newest_row_is_fresh() -> None:
    floor = ds(max_age_hours=8, min_rows=6, window_hours=24, owner_workflow="refresh_nfl_availability_context.yml")
    h = P.classify(floor, NOW - timedelta(minutes=20), NOW, rows_in_window=2)
    assert h.status == P.STALE
    assert h.age_hours is None, "the reading is the count, not '0.0x its budget'"
    assert "only 2 rows in the last 24h, expected at least 6" in h.detail
    assert "refresh_nfl_availability_context.yml" in h.detail
    assert P.classify(floor, NOW - timedelta(minutes=20), NOW, rows_in_window=6).status == P.FRESH


def test_the_floor_never_softens_an_already_stale_or_dormant_reading() -> None:
    floor = ds(max_age_hours=8, min_rows=6, window_hours=24)
    late = P.classify(floor, NOW - timedelta(hours=30), NOW, rows_in_window=100)
    assert late.status == P.STALE and late.age_hours == pytest.approx(30)
    asleep = ds(max_age_hours=8, min_rows=6, window_hours=24, season_months=(6,))
    assert P.classify(asleep, NOW - timedelta(hours=1), NOW, rows_in_window=0).status == P.DORMANT
    # A dataset without a floor ignores the count entirely.
    assert P.classify(ds(), NOW - timedelta(hours=1), NOW, rows_in_window=0).status == P.FRESH


def test_a_recent_write_is_fresh() -> None:
    h = P.classify(ds(), NOW - timedelta(hours=5), NOW)
    assert h.status == P.FRESH
    assert h.age_hours == pytest.approx(5)


def test_a_write_past_its_budget_is_stale_and_names_the_workflow() -> None:
    h = P.classify(ds(max_age_hours=36, owner_workflow="refresh_nfl_dfs_projections.yml"),
                   NOW - timedelta(hours=80), NOW)
    assert h.status == P.STALE
    assert "refresh_nfl_dfs_projections.yml" in h.detail
    assert "2.2x over" in h.detail, h.detail


def test_an_empty_table_is_reported_not_treated_as_fresh() -> None:
    assert P.classify(ds(), None, NOW).status == P.EMPTY


def test_an_out_of_season_pipeline_is_dormant_not_broken() -> None:
    """The detector-health lesson: flagging a dormant sport makes the page noise."""
    soccer = ds(season_months=(6, 7))          # World Cup months only
    h = P.classify(soccer, NOW - timedelta(days=400), NOW)
    assert h.status == P.DORMANT
    assert "out of season" in h.detail


def test_an_in_season_pipeline_is_judged_normally() -> None:
    nfl = ds(season_months=(9, 10, 11, 12, 1, 2), max_age_hours=36)
    assert P.classify(nfl, NOW - timedelta(hours=10), NOW).status == P.FRESH
    assert P.classify(nfl, NOW - timedelta(hours=200), NOW).status == P.STALE


def test_a_naive_timestamp_is_treated_as_utc_rather_than_crashing() -> None:
    naive = (NOW - timedelta(hours=2)).replace(tzinfo=None)
    h = P.classify(ds(), naive, NOW)
    assert h.status == P.FRESH and h.age_hours == pytest.approx(2)


def test_the_budgets_are_loose_against_their_crons() -> None:
    """GitHub's scheduler runs 60-95 minutes late and drops overnight slots
    (CLAUDE.md). A budget at the nominal interval would flag constantly."""
    by_key = {d.key: d for d in P.DATASET_REGISTRY}
    assert by_key["nfl_projections"].max_age_hours >= 24, "twice daily -> at least a day of slack"
    assert by_key["odds_history"].max_age_hours >= 3, "every 30 min -> hours, not minutes"
    assert by_key["nfl_specials_board"].max_age_hours >= 96, "Thu+Sun -> at least 4 days"
    assert by_key["odds_mlb"].max_age_hours >= 24, "no dispatch 04:00-14:00 UTC and off days write nothing"
    assert by_key["nfl_survivor_lines"].max_age_hours > 120, "Thu 13:20 -> Tue 13:20 is exactly 120h before GitHub's delay"
    for key in ("mlb_team_stats_history", "mlb_pitcher_stats_history", "mlb_batter_stats", "cfb_player_stats"):
        assert by_key[key].max_age_hours >= 72, f"{key}: a GitHub daily cron starts ~26h apart"
    for key in ("nfl_week_results", "nfl_replacement_upside_grade", "closes_nfl", "nfl_signal_grading"):
        assert by_key[key].max_age_hours >= 168, f"{key}: written once a game week"


def test_the_report_puts_problems_first() -> None:
    results = [
        P.classify(ds(key="a", label="Healthy"), NOW - timedelta(hours=1), NOW),
        P.classify(ds(key="b", label="Broken"), NOW - timedelta(hours=500), NOW),
        P.classify(ds(key="c", label="Asleep", season_months=(6,)), None, NOW),
    ]
    text = P.report(results)
    assert text.index("Broken") < text.index("Healthy") < text.index("Asleep")
    assert "1 needing attention of 3 datasets" in text


# --- Deliberately paused pipelines (2026-09-29: MLB player-prop odds read
# "STALE ... 36.9x over" although prop capture was switched off on the
# schedule on purpose on 2026-08-24 for Odds API quota). --------------------

PROP_GATE = "inputs.run_props == true"


def test_a_step_gated_to_manual_dispatch_is_a_pause_not_an_outage() -> None:
    props = ds(max_age_hours=24, owner_workflow="refresh_mlb_vegas.yml",
               schedule_step="Capture MLB player-prop odds")
    last = datetime(2026, 8, 23, 13, 47, tzinfo=timezone.utc)
    h = P.classify(props, last, datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc), PROP_GATE)
    assert h.status == P.DORMANT
    assert "paused on the schedule" in h.detail
    assert "`inputs.run_props == true`" in h.detail
    assert "(2026-08-23)" in h.detail
    assert "0 needing attention of 1 datasets" in P.report([h])


def test_fresh_data_wins_over_the_gate() -> None:
    # A manual run with run_props=true wrote rows: that is just fresh.
    props = ds(max_age_hours=24, schedule_step="Capture")
    assert P.classify(props, NOW - timedelta(hours=2), NOW, PROP_GATE).status == P.FRESH


def test_without_a_gate_the_same_age_is_stale() -> None:
    props = ds(max_age_hours=24, schedule_step="Capture")
    assert P.classify(props, NOW - timedelta(days=37), NOW, None).status == P.STALE


def test_gate_is_read_from_the_real_workflow_file() -> None:
    # If someone removes the `if:` to resume capture, this returns None and
    # STALE reporting comes back on its own.
    by_key = {d.key: d for d in P.DATASET_REGISTRY}
    props = by_key["mlb_props"]
    assert props.schedule_step
    assert P.manual_only_gate(props.owner_workflow, props.schedule_step) == PROP_GATE
    assert P.manual_only_gate(props.owner_workflow, "Scan + settle sharp line alerts") is None


def test_gate_reader_ignores_schedule_conditions_and_missing_files(tmp_path) -> None:
    (tmp_path / "w.yml").write_text(
        "jobs:\n  j:\n    steps:\n"
        "      - name: Weekly rebuild\n"
        "        if: github.event.schedule == '0 5 * * 1' || inputs.rebuild == true\n"
        "        run: x\n"
        "      - name: Manual only\n"
        "        # a comment inside the step\n"
        "        if: github.event_name == 'workflow_dispatch'\n"
        "        run: y\n"
        "      - name: Always\n"
        "        run: z\n",
        encoding="utf-8",
    )
    assert P.manual_only_gate("w.yml", "Weekly rebuild", tmp_path) is None
    assert P.manual_only_gate("w.yml", "Manual only", tmp_path) == "github.event_name == 'workflow_dispatch'"
    assert P.manual_only_gate("w.yml", "Always", tmp_path) is None
    assert P.manual_only_gate("missing.yml", "Always", tmp_path) is None


def test_the_monitor_does_not_fail_the_run_by_default() -> None:
    """A monitor that reports red is one more red nobody reads. It informs; the
    page and the log are where it speaks. --fail-on-stale is opt-in."""
    import inspect
    source = inspect.getsource(P.main)
    assert '"--fail-on-stale"' in source
    assert "action=\"store_true\"" in source
    assert "return 1" in source and "args.fail_on_stale" in source
