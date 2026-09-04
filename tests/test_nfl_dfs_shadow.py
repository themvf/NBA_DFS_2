from datetime import datetime, timezone, timedelta

import pytest

from ingest.nfl_dfs_shadow import candidate_allowed, require_pregame, freeze
from ingest.nfl_dfs_shadow import matches_source_digest, weekly_metrics
import hashlib


def test_gate_only_allows_explicitly_qualified_opportunity_candidate():
    report = {"candidates": {
        "WR:closing_exploratory": {"status": "eligible_for_shadow_only"},
        "RB:opportunity": {"status": "eligible_for_shadow_only"},
        "DST:opportunity": {"status": "not_eligible"},
    }}
    assert candidate_allowed(report, "RB")
    assert not candidate_allowed(report, "WR")
    assert not candidate_allowed(report, "DST")


def test_shadow_timestamp_must_be_strictly_pregame():
    kickoff = datetime(2026,9,13,17,tzinfo=timezone.utc)
    require_pregame(kickoff-timedelta(seconds=1),kickoff)
    for now in (kickoff,kickoff+timedelta(seconds=1),kickoff.replace(tzinfo=None)):
        with pytest.raises(ValueError):
            require_pregame(now,kickoff)


def test_changed_baseline_cannot_silently_use_old_candidate_recipe():
    report = {"implementation": {"model/nfl_dfs_historical.py": "wrong-hash"}}
    with pytest.raises(ValueError, match="drifted"):
        freeze(None, report, 2026, 1, datetime(2026,9,3,tzinfo=timezone.utc))


def test_pin_survives_windows_linux_checkout_but_rejects_code_changes():
    lf = b"def f():\n    return 1\n"
    crlf = lf.replace(b"\n", b"\r\n")
    for observed in (lf, crlf):
        for pinned in (lf, crlf):
            assert matches_source_digest(observed, hashlib.sha256(pinned).hexdigest())
        assert not matches_source_digest(observed.replace(b"return 1", b"return 2"), hashlib.sha256(lf).hexdigest())


def test_weekly_metrics_keeps_week_and_baseline_separate():
    def record(week, actual):
        p = dict(season=2026, week=week, position="WR", baseline=10, p10=2,
                 p90=20, boom_probability=.2, boom_threshold=25,
                 candidate=dict(prediction=12, p10=3, p90=21, boom_probability=.3))
        return dict(payload=p, outcome=dict(actual=actual))
    weeks = weekly_metrics([record(1, 12), record(2, 8)])
    assert len(weeks) == 2
    assert weeks[0]["candidate"]["mae"] == 0
    assert weeks[0]["baseline"]["mae"] == 2
    assert weeks[1]["baseline"]["n"] == 1
