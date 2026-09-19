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
    assert P.CHECK_VERSION == "pipeline-health-v1"
    for d in P.DATASET_REGISTRY:
        assert d.max_age_hours > 0
        assert d.owner_workflow.endswith(".yml"), d.key
        assert all(1 <= m <= 12 for m in d.season_months), d.key


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


def test_the_report_puts_problems_first() -> None:
    results = [
        P.classify(ds(key="a", label="Healthy"), NOW - timedelta(hours=1), NOW),
        P.classify(ds(key="b", label="Broken"), NOW - timedelta(hours=500), NOW),
        P.classify(ds(key="c", label="Asleep", season_months=(6,)), None, NOW),
    ]
    text = P.report(results)
    assert text.index("Broken") < text.index("Healthy") < text.index("Asleep")
    assert "1 needing attention of 3 datasets" in text


def test_the_monitor_does_not_fail_the_run_by_default() -> None:
    """A monitor that reports red is one more red nobody reads. It informs; the
    page and the log are where it speaks. --fail-on-stale is opt-in."""
    import inspect
    source = inspect.getsource(P.main)
    assert '"--fail-on-stale"' in source
    assert "action=\"store_true\"" in source
    assert "return 1" in source and "args.fail_on_stale" in source
