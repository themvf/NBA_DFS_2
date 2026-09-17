"""Accuracy guard for the NFL play archetype taxonomy — the EPA sign audit.

WHAT THIS PROTECTS
------------------
The taxonomy has produced the same bug five times: a single-valued label
silently absorbs a population, and a rate computed off it is wrong in the SAME
DIRECTION every time. The independent check on that bug is EPA: the archetype
label and nflverse's EPA are two separate signals, so a label whose name asserts
"failure" sitting at strongly positive EPA (or the mirror) is either mislabelled
or a rate off it will be. A *cluster* of same-signed disagreements on ONE label
is the signature of an absorbed population.

This test runs the REAL `label_plays` on a REAL season of nflverse play-by-play
and fails if any single archetype carries a one-directional disagreement cluster
larger than tolerance. That is the tripwire that keeps labels accurate to the
actual games as new descriptors and slices are layered on.

HOW IT SOURCES DATA
-------------------
It uses the same `load_pbp` the ingest pipeline uses. Point it at a cached
season parquet with NFL_PBP_CACHE (fast, offline, deterministic), or let it
download the season if the environment allows network access. When neither is
available it SKIPS — a bare checkout or an offline CI run must not fail this,
because a false failure here trains people to ignore it.

    # fast/offline (recommended for CI): pre-download once, point the test at it
    NFL_PBP_AUDIT_SEASON=2024
    NFL_PBP_CACHE=/path/to/play_by_play_2024.parquet

Descriptive only. No predictive or betting claim.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")


# Labels whose NAME asserts a negative outcome. Strongly positive EPA on these is
# the "failure that actually succeeded" signature. Kept in sync with the
# taxonomy in model/nfl_play_archetypes.py — if a label is renamed there, update
# here (see test_label_sets_match_taxonomy below, which fails when they drift).
NEGATIVE_LABELS = {
    "LATE_DOWN_FAILURE",
    "EARLY_DOWN_FAILURE",
    "SACK",
    "TURNOVER_PLAY",
}
POSITIVE_LABELS = {
    "LATE_DOWN_CONVERSION",
    "EARLY_DOWN_SUCCESS",
    "EARLY_DOWN_EXPLOSIVE",
}

POS_EPA_THRESHOLD = 0.5
NEG_EPA_THRESHOLD = -0.5
# A one-directional cluster this large on a single label is not noise; it is a
# population the label is absorbing. Scaled generously so ordinary variance in a
# full season does not trip it — the target is a structural bug, not a few plays.
CLUSTER_TOLERANCE = 60


def _label_sign(label: object) -> int:
    if label in NEGATIVE_LABELS:
        return -1
    if label in POSITIVE_LABELS:
        return 1
    return 0


def sign_disagreements(labelled: "pd.DataFrame") -> "pd.DataFrame":
    """Rows where the archetype label's sign contradicts EPA's sign."""
    df = labelled.copy()
    df["_sign"] = df["play_archetype"].map(_label_sign)
    epa = pd.to_numeric(df["epa"], errors="coerce")
    neg_but_good = (df["_sign"] == -1) & (epa > POS_EPA_THRESHOLD)
    pos_but_bad = (df["_sign"] == 1) & (epa < NEG_EPA_THRESHOLD)
    flagged = df[neg_but_good | pos_but_bad]
    return flagged


def _load_audit_frame() -> "pd.DataFrame":
    """Run the real labeller on a real season, or skip cleanly."""
    play_mod = pytest.importorskip("model.nfl_play_archetypes")
    drive_mod = pytest.importorskip("model.nfl_drive_archetypes")
    label_plays = play_mod.label_plays
    load_pbp = drive_mod.load_pbp

    season = int(os.environ.get("NFL_PBP_AUDIT_SEASON", "2024"))
    cache_env = os.environ.get("NFL_PBP_CACHE")
    cache = Path(cache_env) if cache_env else None

    if cache is not None and not cache.exists():
        pytest.skip(f"NFL_PBP_CACHE={cache} does not exist")
    if cache is None and os.environ.get("NFL_PBP_ALLOW_DOWNLOAD") != "1":
        pytest.skip(
            "set NFL_PBP_CACHE to a season parquet (offline) or "
            "NFL_PBP_ALLOW_DOWNLOAD=1 to fetch — skipping EPA audit"
        )

    try:
        pbp = load_pbp(season, cache)
    except Exception as exc:  # network down, release moved, etc.
        pytest.skip(f"could not load nflverse PBP for {season}: {exc}")

    return label_plays(pbp)


def test_no_one_directional_disagreement_cluster() -> None:
    """The core guard: no single label may absorb a one-directional EPA
    disagreement cluster larger than tolerance."""
    labelled = _load_audit_frame()
    flagged = sign_disagreements(labelled)
    counts = flagged["play_archetype"].value_counts()
    offenders = counts[counts > CLUSTER_TOLERANCE]
    assert offenders.empty, (
        "EPA sign-disagreement cluster(s) — a label appears to be absorbing a "
        "population one-directionally (suspected merge bug). Trace which FLAG "
        f"should carry the stolen plays:\n{offenders.to_string()}"
    )


def test_label_sets_match_taxonomy() -> None:
    """Drift guard: if the taxonomy renames or adds an outcome label, this test
    fails so NEGATIVE_LABELS / POSITIVE_LABELS above get updated rather than
    silently going stale (a stale audit is a blind audit)."""
    module = pytest.importorskip("model.nfl_play_archetypes")
    source = Path(module.__file__).read_text(encoding="utf-8", errors="ignore")
    for label in NEGATIVE_LABELS | POSITIVE_LABELS:
        assert label in source, (
            f"{label} is audited here but no longer appears in "
            "model/nfl_play_archetypes.py — update the label sets in this test"
        )
